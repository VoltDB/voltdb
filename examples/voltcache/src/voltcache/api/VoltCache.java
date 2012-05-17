/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package voltcache.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.utils.MiscUtils;

public class VoltCache implements IVoltCache
{
    private static final Lock lock = new ReentrantLock();
    // Pool of cleanup tasks: we need to ensure there is one background thread running to scrub out expired
    // items from the cache for a given cluster connection.
    private static final HashMap<String,CleanupTask> cleanupTaskPool = new HashMap<String,CleanupTask>();


    /*
     * Timer task wrapper to scrub out expired items from the cache
     */
    private class CleanupTask
    {
        private final Client client;
        private final Timer timer;
        protected long users = 1;
        private CleanupTask(String servers) throws Exception
        {
            client = connect(servers);
            timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask()
            {
                @Override
                public void run()
                {
                    try {client.callProcedure("Cleanup");} catch(Exception x) {}
                }
            }
            , 10000
            , 10000
            );
        }
        protected void use()
        {
            this.users++;
        }
        protected void dispose() throws InterruptedException
        {
            this.users--;
            if (this.users <= 0)
            {
                this.timer.cancel();
                this.client.close();
            }
        }
    }

    // Client connection to the underlying VoltDB cluster
    private final Client client;
    private final String servers;

    // For internal use: NullCallback for "noreply" operations
    private static final NullCallback nullCallback = new NullCallback();

    /**
     * Creates a new VoltCache instance with a given VoltDB client.
     * Optionally creates a background timer thread to cleanup the
     * underlying cache from obsolete items.
     * @param servers The comma separated list of VoltDB servers in
     * hostname[:port] format that the instance will use.
     */
    public VoltCache(String servers) throws Exception {
        this.servers = servers;

        client = connect(servers);
        // Make sure there is at least one cleanup task for this cluster
        lock.lock();
        try
        {
            String key = this.servers;
            if (!cleanupTaskPool.containsKey(key))
                cleanupTaskPool.put(key, new CleanupTask(this.servers));
            else
                cleanupTaskPool.get(key).use();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Creates a new VoltCache instance with a given VoltDB client.
     * Optionally creates a background timer thread to cleanup the
     * underlying cache from obsolete items.
     * @param servers The comma separated list of VoltDB servers the
     * instance will use.
     * @param port The client port to connect to on the VoltDB servers.
     */
    @Deprecated
    public VoltCache(String servers, int port) throws Exception {
        // make the ports work
        String[] serverArray = servers.split(",");
        for (int i = 0; i < serverArray.length; ++i) {
            serverArray[i] = MiscUtils.getHostnameFromHostnameColonPort(serverArray[i]);
            serverArray[i] = MiscUtils.getHostnameColonPortString(serverArray[i], port);
        }
        this.servers = StringUtils.join(serverArray, ',');

        client = connect(servers);
        // Make sure there is at least one cleanup task for this cluster
        lock.lock();
        try
        {
            String key = this.servers;
            if (!cleanupTaskPool.containsKey(key))
                cleanupTaskPool.put(key, new CleanupTask(this.servers));
            else
                cleanupTaskPool.get(key).use();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    Client connect(final String servers) throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        final Client client = ClientFactory.createClient(clientConfig);
        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(client, server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
        return client;
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServerWithRetry(Client client, String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
    }



    /**
     * Closes the VoltCache connection.
     */
    public void close()
    {
        try {
            this.client.drain();
            this.client.close();
        } catch (NoConnectionsException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Deal with cleanup task
        lock.lock();
        try {
            String key = this.servers;
            cleanupTaskPool.get(key).dispose();
            if (cleanupTaskPool.get(key).users <= 0) {
                cleanupTaskPool.remove(key);
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Executes an operation
     * @param type Type of response expected
     * @param noreply Flag indicating the client doesn't care about receiving a response - operation will return immediately.
     * @param procedure Name of the VoltProcedure to call on the server
     * @param parameters Ordered list of procedure parameters
     * @returns Result of the operation
     */
    private VoltCacheResult execute(VoltCacheResult.Type type, boolean noreply, String procedure, Object... parameters)
    {
        VoltCacheResult results = null;
        try
        {
            if (noreply) {
                results = this.client.callProcedure(
                                                     VoltCache.nullCallback
                                                   , procedure
                                                   , parameters
                                                   ) ? VoltCacheResult.SUBMITTED() : VoltCacheResult.ERROR();
            } else {
                results = VoltCacheResult.get(
                                            type
                                          , this.client.callProcedure(
                                                                     procedure
                                                                   , parameters
                                                                   )
                                          );
            }
        }
        catch(Exception x)
        {
            results= VoltCacheResult.ERROR();
        }
                return results;
    }

    /**
     * Asynchronously Executes an operation
     * @param type Type of response expected
     * @param procedure Name of the VoltProcedure to call on the server
     * @param parameters Ordered list of procedure parameters
     * @returns Future result of the operation
     */
    private Future<VoltCacheResult> asyncExecute(VoltCacheResult.Type type, final String procedure, final Object... parameters)
    {
        try
        {
                Callable<ClientResponse> callable = new Callable<ClientResponse>() {
                        public ClientResponse call() {
                                ClientResponse response = null;
                                        try {
                                                response = client.callProcedure (
                                                                procedure,
                                                                parameters);
                                        } catch (Exception e) {
                                                e.printStackTrace();
                                        }
                                        return response;
                                }
                };

                FutureTask<ClientResponse> future = new FutureTask<ClientResponse>(
                                callable
                );

            return VoltCacheFuture.cast(type,future);
        }
        catch(Exception x)
        {
            return VoltCacheFuture.fail();
        }
    }

    /**
     * Adds a cache item.
     * @param key Key of the cache item to add.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @param noreply Flag indicating the client doesn't care about receiving a response - operation will return immediately.
     * @returns Result of the operation
     */
    public VoltCacheResult add(String key, int flags, int exptime, byte[] data, boolean noreply)
    {
        return execute(VoltCacheResult.Type.CODE, noreply, "Add", key, flags, exptime, data);
    }

    /**
     * Asynchronously Adds a cache item.
     * @param key Key of the cache item to add.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncAdd(String key, int flags, int exptime, byte[] data)
    {
        return asyncExecute(VoltCacheResult.Type.CODE, "Add", key, flags, exptime, data);
    }

    /**
     * Appends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to append to the current item's data.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    public VoltCacheResult append(String key, byte[] data, boolean noreply)
    {
        return execute(VoltCacheResult.Type.CODE, noreply, "Append", key, data);
    }

    /**
     * Asynchronously Appends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to append to the current item's data.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncAppend(String key, byte[] data)
    {
        return asyncExecute(VoltCacheResult.Type.CODE, "Append", key, data);
    }

    /**
     * Checks and Sets a cache item.
     * @param key Key of the cache item to add.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @param casVersion Concurrency check version for the operation: this is the CASVersion value of the item when it was retrieved.
     *        If another thread modified the underlying cache item, the version mismatch will cause a concurrency check failure and the
     *        requested update will not be performed (other thread's value, set earlier, wins).
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    public VoltCacheResult cas(String key, int flags, int exptime, byte[] data, long casVersion, boolean noreply)
    {
        return execute(VoltCacheResult.Type.CODE, noreply, "CheckAndSet", key, flags, exptime, data, casVersion);
    }

    /**
     * Asynchronously Checks and Sets a cache item.
     * @param key Key of the cache item to add.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @param casVersion Concurrency check version for the operation: this is the CASVersion value of the item when it was retrieved.
     *        If another thread modified the underlying cache item, the version mismatch will cause a concurrency check failure and the
     *        requested update will not be performed (other thread's value, set earlier, wins).
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncCas(String key, int flags, int exptime, byte[] data, long casVersion)
    {
        return asyncExecute(VoltCacheResult.Type.CODE, "CheckAndSet", key, flags, exptime, data, casVersion);
    }

    /**
     * Cleans up all expired item (effectively deleting them from the cache).
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    public VoltCacheResult cleanup(boolean noreply)
    {
        return execute(VoltCacheResult.Type.CODE, noreply, "Cleanup");
    }

    /**
     * Asynchronously Cleans up all expired item (effectively deleting them from the cache).
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncCleanup()
    {
        return asyncExecute(VoltCacheResult.Type.CODE, "Cleanup");
    }

    /**
     * Deletes a cache item.
     * @param key Key of the cache item to delete.
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    public VoltCacheResult delete(String key, int exptime, boolean noreply)
    {
        return execute(VoltCacheResult.Type.CODE, noreply, "Delete", key, exptime);
    }

    /**
     * Asynchronously Deletes a cache item.
     * @param key Key of the cache item to delete.
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncDelete(String key, int exptime)
    {
        return asyncExecute(VoltCacheResult.Type.CODE, "Delete", key, exptime);
    }

    /**
     * Flushes out all items in the cache.  Items that had already expired are immediately reclaimed.  If a delay is given, the other
     * items are simply queued for deletion (if no delay is provided, everything is effectively deleted).
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    public VoltCacheResult flushAll(int exptime, boolean noreply)
    {
        return execute(VoltCacheResult.Type.CODE, noreply, "FlushAll", exptime);
    }

    /**
     * Asynchronously Flushes out all items in the cache.  Items that had already expired are immediately reclaimed.  If a delay is given, the other
     * items are simply queued for deletion (if no delay is provided, everything is effectively deleted).
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncFlushAll(int exptime)
    {
        return asyncExecute(VoltCacheResult.Type.CODE, "FlushAll", exptime);
    }

    /**
     * Gets a single cache item.
     * @param key Key of the cache item to retrieve.
     * @returns Result of the operation
     */
    public VoltCacheResult get(String key)
    {
        return execute(VoltCacheResult.Type.DATA, false, "Get", key);
    }

    /**
     * Asynchronously Gets a single cache item.
     * @param key Key of the cache item to retrieve.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncGet(String key)
    {
        return asyncExecute(VoltCacheResult.Type.DATA, "Get", key);
    }

    /**
     * Gets a multiple cache items.
     * @param keys Array of key for the cache items to retrieve.
     * @returns Result of the operation
     */
    public VoltCacheResult get(String[] keys)
    {
        return execute(VoltCacheResult.Type.DATA, false, "Gets", (Object)keys);
    }

    /**
     * Asynchronously Gets a multiple cache items.
     * @param keys Array of key for the cache items to retrieve.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncGet(String[] keys)
    {
        return asyncExecute(VoltCacheResult.Type.DATA, "Gets", (Object)keys);
    }

    /**
     * Increments or Decrements an Int64 value to a given cache item representing an Int64 value.
     * @param key Key of the cache item to increment/decrement.
     * @param by Long amount by which to increment/decrement.
     * @param increment Flag indicating true for increment, false for decrement.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    public VoltCacheResult incrDecr(String key, long by, boolean increment, boolean noreply)
    {
        return execute(VoltCacheResult.Type.IDOP, noreply, "IncrDecr", key, by, (byte)(increment ? 1 : 0));
    }

    /**
     * Asynchronously Increments or Decrements an Int64 value to a given cache item representing an Int64 value.
     * @param key Key of the cache item to increment/decrement.
     * @param by Long amount by which to increment/decrement.
     * @param increment Flag indicating true for increment, false for decrement.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncIncrDecr(String key, long by, boolean increment)
    {
        return asyncExecute(VoltCacheResult.Type.IDOP, "IncrDecr", key, by, (byte)(increment ? 1 : 0));
    }

    /**
     * Prepends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to prepend to the current item's data.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    public VoltCacheResult prepend(String key, byte[] data, boolean noreply)
    {
        return execute(VoltCacheResult.Type.CODE, noreply, "Prepend", key, data);
    }

    /**
     * Asynchronously Prepends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to prepend to the current item's data.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncPrepend(String key, byte[] data)
    {
        return asyncExecute(VoltCacheResult.Type.CODE, "Prepend", key, data);
    }

    /**
     * Replaces a cache item.
     * @param key Key of the cache item to replace.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    public VoltCacheResult replace(String key, int flags, int exptime, byte[] data, boolean noreply)
    {
        return execute(VoltCacheResult.Type.CODE, noreply, "Replace", key, flags, exptime, data);
    }

    /**
     * Asynchronously Replaces a cache item.
     * @param key Key of the cache item to replace.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncReplace(String key, int flags, int exptime, byte[] data)
    {
        return asyncExecute(VoltCacheResult.Type.CODE, "Replace", key, flags, exptime, data);
    }

    /**
     * Adds or Replaces a cache item.
     * @param key Key of the cache item to set.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    public VoltCacheResult set(String key, int flags, int exptime, byte[] data, boolean noreply)
    {
        return execute(VoltCacheResult.Type.CODE, noreply, "Set", key, flags, exptime, data);
    }

    /**
     * Asynchronously Adds or Replaces a cache item.
     * @param key Key of the cache item to set.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @returns Future result of the operation.
     */
    public Future<VoltCacheResult> asyncSet(String key, int flags, int exptime, byte[] data)
    {
        return asyncExecute(VoltCacheResult.Type.CODE, "Set", key, flags, exptime, data);
    }


    /**
     * Returns underlying performance statistics
     * @returns Full map of performance counters for the underlying VoltDB connection
     */
    public ClientStatsContext getStatistics()
    {
        return this.client.createStatsContext();
    }

    /**
     * Saves performance statistics to a file
     * @param stats The stats instance used to generate the statistics data
     * @param file The path to the file where statistics will be saved
     */
    public void saveStatistics(ClientStats stats, String file) throws IOException
    {
        client.writeSummaryCSV(stats, file);
    }

}

