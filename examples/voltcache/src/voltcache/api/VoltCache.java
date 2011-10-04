/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.voltdb.client.Client;
import org.voltdb.client.NullCallback;
import org.voltdb.client.exampleutils.ClientConnection;
import org.voltdb.client.exampleutils.ClientConnectionPool;
import org.voltdb.client.exampleutils.PerfCounter;
import org.voltdb.client.exampleutils.PerfCounterMap;

public class VoltCache implements IVoltCache
{
    private static final Lock lock = new ReentrantLock();
    // Pool of cleanup tasks: we need to ensure there is one background thread running to scrub out expired
    // items from the cache for a given cluster connection.
    private static final HashMap<String,CleanupTask> CleanupTaskPool = new HashMap<String,CleanupTask>();

    /*
     * Timer task wrapper to scrub out expired items from the cache
     */
    private static class CleanupTask
    {
        private final ClientConnection connection;
        private final Timer timer;
        protected long Users = 1;
        private CleanupTask(String servers, int port) throws Exception
        {
            this.connection = ClientConnectionPool.get(servers, port);
            this.timer = new Timer();
            this.timer.scheduleAtFixedRate(new TimerTask()
            {
                private final ClientConnection con;
                {
                    con = connection;
                }
                @Override
                public void run()
                {
                    try {con.execute("Cleanup");} catch(Exception x) {}
                }
            }
            , 10000
            , 10000
            );
        }
        protected void use()
        {
            this.Users++;
        }
        protected void dispose()
        {
            this.Users--;
            if (this.Users <= 0)
            {
                this.timer.cancel();
                this.connection.close();
            }
        }
    }

    // Client connection to the underlying VoltDB cluster
    private final ClientConnection Connection;
    private final String Servers;
    private final int Port;

    // For internal use: NullCallback for "noreply" operations
    private static final NullCallback NullCallback = new NullCallback();

    /**
     * Creates a new VoltCache instance with a given VoltDB client.  Optionally creates a background timer thread to cleanup the underlying cache from obsolete items.
     * @param servers The list of VoltDB servers the instance will use.
     * @param port The client port to connect to on the VoltDB servers.
     */
    public VoltCache(String servers, int port) throws Exception
    {
        this.Servers = servers;
        this.Port = port;
        this.Connection = ClientConnectionPool.get(servers, port);
        // Make sure there is at least one cleanup task for this cluster
        lock.lock();
        try
        {
            String key = this.Servers + ":" + this.Port;
            if (!CleanupTaskPool.containsKey(key))
                CleanupTaskPool.put(key, new CleanupTask(this.Servers, this.Port));
            else
                CleanupTaskPool.get(key).use();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Closes the VoltCache connection.
     */
    public void close()
    {
        ClientConnectionPool.dispose(this.Connection);
        // Deal with cleanup task
        lock.lock();
        try
        {
            String key = this.Servers + ":" + this.Port;
            CleanupTaskPool.get(key).dispose();
            if (CleanupTaskPool.get(key).Users <= 0)
                CleanupTaskPool.remove(key);
        }
        finally
        {
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
        try
        {
            if (noreply)
                return this.Connection.executeAsync(
                                                     VoltCache.NullCallback
                                                   , procedure
                                                   , parameters
                                                   ) ? VoltCacheResult.SUBMITTED() : VoltCacheResult.ERROR();
            else
                return VoltCacheResult.get(
                                            type
                                          , this.Connection.execute(
                                                                     procedure
                                                                   , parameters
                                                                   )
                                          );
        }
        catch(Exception x)
        {
            return VoltCacheResult.ERROR();
        }
    }

    /**
     * Asynchronously Executes an operation
     * @param type Type of response expected
     * @param procedure Name of the VoltProcedure to call on the server
     * @param parameters Ordered list of procedure parameters
     * @returns Future result of the operation
     */
    private Future<VoltCacheResult> asyncExecute(VoltCacheResult.Type type, String procedure, Object... parameters)
    {
        try
        {
            return VoltCacheFuture.cast(
                                         type
                                       , this.Connection.executeAsync(
                                                                       procedure
                                                                     , parameters
                                                                     )
                                       );
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
    public PerfCounterMap getStatistics()
    {
        return this.Connection.getStatistics();
    }

    /**
     * Returns underlying performance statistics for a specific procedure
     * @param procedure Name of the specific procedure for which to retrieve the performance counter.
     * @returns The performance counter for the given procedure
     */
    public PerfCounter getStatistics(String procedure)
    {
        return this.Connection.getStatistics(procedure);
    }

    /**
     * Returns underlying performance statistics for a specific list of procedures
     * @param procedures Names of the specific procedures for which to retrieve an aggregated performance counter.
     * @returns The performance counter for the given list of procedures
     */
    public PerfCounter getStatistics(String... procedures)
    {
        return this.Connection.getStatistics(procedures);
    }

}

