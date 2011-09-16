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

import java.io.Closeable;
import java.util.concurrent.Future;

public interface IVoltCache extends Closeable
{
    /**
     * Adds a cache item.
     * @param key Key of the cache item to add.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @param noreply Flag indicating the client doesn't care about receiving a response - operation will return immediately.
     * @returns Result of the operation
     */
    VoltCacheResult add(String key, int flags, int exptime, byte[] data, boolean noreply);

    /**
     * Asynchronously Adds a cache item.
     * @param key Key of the cache item to add.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncAdd(String key, int flags, int exptime, byte[] data);

    /**
     * Appends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to append to the current item's data.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    VoltCacheResult append(String key, byte[] data, boolean noreply);

    /**
     * Asynchronously Appends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to append to the current item's data.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncAppend(String key, byte[] data);

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
    VoltCacheResult cas(String key, int flags, int exptime, byte[] data, long casVersion, boolean noreply);

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
    Future<VoltCacheResult> asyncCas(String key, int flags, int exptime, byte[] data, long casVersion);

    /**
     * Cleans up all expired item (effectively deleting them from the cache).
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    VoltCacheResult cleanup(boolean noreply);

    /**
     * Asynchronously Cleans up all expired item (effectively deleting them from the cache).
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncCleanup();

    /**
     * Deletes a cache item.
     * @param key Key of the cache item to delete.
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    VoltCacheResult delete(String key, int exptime, boolean noreply);

    /**
     * Asynchronously Deletes a cache item.
     * @param key Key of the cache item to delete.
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncDelete(String key, int exptime);

    /**
     * Flushes out all items in the cache.  Items that had already expired are immediately reclaimed.  If a delay is given, the other
     * items are simply queued for deletion (if no delay is provided, everything is effectively deleted).
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    VoltCacheResult flushAll(int exptime, boolean noreply);

    /**
     * Asynchronously Flushes out all items in the cache.  Items that had already expired are immediately reclaimed.  If a delay is given, the other
     * items are simply queued for deletion (if no delay is provided, everything is effectively deleted).
     * @param exptime Time delay before the delete operation (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for immediate deletion.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncFlushAll(int exptime);

    /**
     * Gets a single cache item.
     * @param key Key of the cache item to retrieve.
     * @returns Result of the operation
     */
    VoltCacheResult get(String key);

    /**
     * Asynchronously Gets a single cache item.
     * @param key Key of the cache item to retrieve.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncGet(String key);

    /**
     * Gets a multiple cache items.
     * @param keys Array of key for the cache items to retrieve.
     * @returns Result of the operation
     */
    VoltCacheResult get(String[] keys);

    /**
     * Asynchronously Gets a multiple cache items.
     * @param keys Array of key for the cache items to retrieve.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncGet(String[] keys);

    /**
     * Increments or Decrements an Int64 value to a given cache item representing an Int64 value.
     * @param key Key of the cache item to increment/decrement.
     * @param by Long amount by which to increment/decrement.
     * @param increment Flag indicating true for increment, false for decrement.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    VoltCacheResult incrDecr(String key, long by, boolean increment, boolean noreply);

    /**
     * Asynchronously Increments or Decrements an Int64 value to a given cache item representing an Int64 value.
     * @param key Key of the cache item to increment/decrement.
     * @param by Long amount by which to increment/decrement.
     * @param increment Flag indicating true for increment, false for decrement.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncIncrDecr(String key, long by, boolean increment);

    /**
     * Prepends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to prepend to the current item's data.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    VoltCacheResult prepend(String key, byte[] data, boolean noreply);

    /**
     * Asynchronously Prepends data to and existing cache item.
     * @param key Key of the cache item to prepend data to.
     * @param data Raw byte data to prepend to the current item's data.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncPrepend(String key, byte[] data);

    /**
     * Replaces a cache item.
     * @param key Key of the cache item to replace.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    VoltCacheResult replace(String key, int flags, int exptime, byte[] data, boolean noreply);

    /**
     * Asynchronously Replaces a cache item.
     * @param key Key of the cache item to replace.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncReplace(String key, int flags, int exptime, byte[] data);

    /**
     * Adds or Replaces a cache item.
     * @param key Key of the cache item to set.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @param noreply Flag indicating the client doesn't care about receiving a response- SUBMITTED will be returned unless an error occurs.
     * @returns Result of the operation
     */
    VoltCacheResult set(String key, int flags, int exptime, byte[] data, boolean noreply);

    /**
     * Asynchronously Adds or Replaces a cache item.
     * @param key Key of the cache item to set.
     * @param flags Custom flags to save along with the cache item.
     * @param exptime Expiration time for the item (number of second, up to 30 days), or UNIX time in seconds - use <= 0 for no expiration.
     * @param data Raw byte data for the item.
     * @returns Future result of the operation.
     */
    Future<VoltCacheResult> asyncSet(String key, int flags, int exptime, byte[] data);
}

