/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.utils.MiscUtils;

import voltcache.procedures.VoltCacheProcBase;
import voltcache.procedures.VoltCacheProcBase.Result;
import voltcache.procedures.VoltCacheProcBase.Result.Type;

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
    @Override
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
    private VoltCacheProcBase.Result execute(VoltCacheProcBase.Result.Type type, boolean noreply, String procedure, Object... parameters)
    {
        VoltCacheProcBase.Result results = null;
        try
        {
            if (noreply) {
                results = this.client.callProcedure(
                                                     VoltCache.nullCallback
                                                   , procedure
                                                   , parameters
                                                   ) ? VoltCacheProcBase.Result.SUBMITTED() : VoltCacheProcBase.Result.ERROR();
            } else {
                results = getResult(
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
            results= VoltCacheProcBase.Result.ERROR();
        }
                return results;
    }

    public static VoltCacheProcBase.Result getResult(Type type, ClientResponse response)
    {
        if (type == Type.CODE)
        {
            return VoltCacheProcBase.Result.getResult((int)response.getResults()[0].asScalarLong());
        }
        else if (type == Type.DATA)
        {
            final VoltTable data = response.getResults()[0];
            if (data.getRowCount() > 0)
            {
                final Result result = Result.OK();
                result.data = new HashMap<String,VoltCacheItem>();
                while(data.advanceRow())
                    result.data.put(
                                     data.getString(0)
                                   , new VoltCacheItem(
                                                        data.getString(0)
                                                      , (int)data.getLong(1)
                                                      , data.getVarbinary(2)
                                                      , data.getLong(3)
                                                      , (int)data.getLong(4)
                                                      )
                                   );
                return result;
            }
            else
                return Result.NOT_FOUND();
        }
        else if (type == Type.IDOP)
        {
            final long value = response.getResults()[0].asScalarLong();
            if (value == VoltType.NULL_BIGINT)
                return Result.NOT_FOUND();
            else
            {
                final Result result = Result.OK();
                result.incrDecrValue = value;
                return result;
            }
        }
        else
            throw new RuntimeException("Invalid Result Type: " + type);
    }

    /**
     * Asynchronously Executes an operation
     * @param type Type of response expected
     * @param procedure Name of the VoltProcedure to call on the server
     * @param parameters Ordered list of procedure parameters
     * @returns Future result of the operation
     */
    private Future<VoltCacheProcBase.Result> asyncExecute(VoltCacheProcBase.Result.Type type, final String procedure, final Object... parameters)
    {
        try
        {
                Callable<ClientResponse> callable = new Callable<ClientResponse>() {
                        @Override
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
    @Override
    public VoltCacheProcBase.Result add(String key, int flags, int exptime, byte[] data, boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.CODE, noreply, "Add", key, flags, exptime, data);
    }

    /**
     * Asynchronously Adds a cache item.
     * @param key Key of the cache item to add.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncAdd(String key, int flags, int exptime, byte[] data)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.CODE, "Add", key, flags, exptime, data);
    }

    /**
     * Appends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to append to the current item's data.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    @Override
    public VoltCacheProcBase.Result append(String key, byte[] data, boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.CODE, noreply, "Append", key, data);
    }

    /**
     * Asynchronously Appends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to append to the current item's data.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncAppend(String key, byte[] data)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.CODE, "Append", key, data);
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
    @Override
    public VoltCacheProcBase.Result cas(String key, int flags, int exptime, byte[] data, long casVersion, boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.CODE, noreply, "CheckAndSet", key, flags, exptime, data, casVersion);
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
    @Override
    public Future<VoltCacheProcBase.Result> asyncCas(String key, int flags, int exptime, byte[] data, long casVersion)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.CODE, "CheckAndSet", key, flags, exptime, data, casVersion);
    }

    /**
     * Cleans up all expired item (effectively deleting them from the cache).
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    @Override
    public VoltCacheProcBase.Result cleanup(boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.CODE, noreply, "Cleanup");
    }

    /**
     * Asynchronously Cleans up all expired item (effectively deleting them from the cache).
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncCleanup()
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.CODE, "Cleanup");
    }

    /**
     * Deletes a cache item.
     * @param key Key of the cache item to delete.
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    @Override
    public VoltCacheProcBase.Result delete(String key, int exptime, boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.CODE, noreply, "Delete", key, exptime);
    }

    /**
     * Asynchronously Deletes a cache item.
     * @param key Key of the cache item to delete.
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncDelete(String key, int exptime)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.CODE, "Delete", key, exptime);
    }

    /**
     * Flushes out all items in the cache.  Items that had already expired are immediately reclaimed.  If a delay is given, the other
     * items are simply queued for deletion (if no delay is provided, everything is effectively deleted).
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    @Override
    public VoltCacheProcBase.Result flushAll(int exptime, boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.CODE, noreply, "FlushAll", exptime);
    }

    /**
     * Asynchronously Flushes out all items in the cache.  Items that had already expired are immediately reclaimed.  If a delay is given, the other
     * items are simply queued for deletion (if no delay is provided, everything is effectively deleted).
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncFlushAll(int exptime)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.CODE, "FlushAll", exptime);
    }

    /**
     * Gets a single cache item.
     * @param key Key of the cache item to retrieve.
     * @returns Result of the operation
     */
    @Override
    public VoltCacheProcBase.Result get(String key)
    {
        return execute(VoltCacheProcBase.Result.Type.DATA, false, "Get", key);
    }

    /**
     * Asynchronously Gets a single cache item.
     * @param key Key of the cache item to retrieve.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncGet(String key)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.DATA, "Get", key);
    }

    /**
     * Gets a multiple cache items.
     * @param keys Array of key for the cache items to retrieve.
     * @returns Result of the operation
     */
    @Override
    public VoltCacheProcBase.Result get(String[] keys)
    {
        return execute(VoltCacheProcBase.Result.Type.DATA, false, "Gets", (Object)keys);
    }

    /**
     * Asynchronously Gets a multiple cache items.
     * @param keys Array of key for the cache items to retrieve.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncGet(String[] keys)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.DATA, "Gets", (Object)keys);
    }

    /**
     * Increments or Decrements an Int64 value to a given cache item representing an Int64 value.
     * @param key Key of the cache item to increment/decrement.
     * @param by Long amount by which to increment/decrement.
     * @param increment Flag indicating true for increment, false for decrement.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    @Override
    public VoltCacheProcBase.Result incrDecr(String key, long by, boolean increment, boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.IDOP, noreply, "IncrDecr", key, by, (byte)(increment ? 1 : 0));
    }

    /**
     * Asynchronously Increments or Decrements an Int64 value to a given cache item representing an Int64 value.
     * @param key Key of the cache item to increment/decrement.
     * @param by Long amount by which to increment/decrement.
     * @param increment Flag indicating true for increment, false for decrement.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncIncrDecr(String key, long by, boolean increment)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.IDOP, "IncrDecr", key, by, (byte)(increment ? 1 : 0));
    }

    /**
     * Prepends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to prepend to the current item's data.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    @Override
    public VoltCacheProcBase.Result prepend(String key, byte[] data, boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.CODE, noreply, "Prepend", key, data);
    }

    /**
     * Asynchronously Prepends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to prepend to the current item's data.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncPrepend(String key, byte[] data)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.CODE, "Prepend", key, data);
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
    @Override
    public VoltCacheProcBase.Result replace(String key, int flags, int exptime, byte[] data, boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.CODE, noreply, "Replace", key, flags, exptime, data);
    }

    /**
     * Asynchronously Replaces a cache item.
     * @param key Key of the cache item to replace.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncReplace(String key, int flags, int exptime, byte[] data)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.CODE, "Replace", key, flags, exptime, data);
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
    @Override
    public VoltCacheProcBase.Result set(String key, int flags, int exptime, byte[] data, boolean noreply)
    {
        return execute(VoltCacheProcBase.Result.Type.CODE, noreply, "Set", key, flags, exptime, data);
    }

    /**
     * Asynchronously Adds or Replaces a cache item.
     * @param key Key of the cache item to set.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @returns Future result of the operation.
     */
    @Override
    public Future<VoltCacheProcBase.Result> asyncSet(String key, int flags, int exptime, byte[] data)
    {
        return asyncExecute(VoltCacheProcBase.Result.Type.CODE, "Set", key, flags, exptime, data);
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

