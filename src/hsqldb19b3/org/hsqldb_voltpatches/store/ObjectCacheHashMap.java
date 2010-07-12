/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.store;

import org.hsqldb_voltpatches.lib.Collection;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.Set;

/**
 * Maps integer keys to Objects. Hashes the keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.8.0
 */
public final class ObjectCacheHashMap extends BaseHashMap {

    Set        keySet;
    Collection values;

    public ObjectCacheHashMap(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.intKeyOrValue,
              BaseHashMap.objectKeyOrValue, true);
    }

    public Object get(int key) {

        if (accessCount == Integer.MAX_VALUE) {
            resetAccessCount();
        }

        int lookup = getLookup(key);

        if (lookup == -1) {
            return null;
        }

        accessTable[lookup] = accessCount++;

        return objectValueTable[lookup];
    }

    public Object put(int key, Object value) {

        if (accessCount == Integer.MAX_VALUE) {
            resetAccessCount();
        }

        return super.addOrRemove(key, value, false);
    }

    public Object remove(int key) {
        return super.addOrRemove(key, null, true);
    }

    /**
     * for count number of elements with the given margin, return the access
     * count.
     */
    public int getAccessCountCeiling(int count, int margin) {
        return super.getAccessCountCeiling(count, margin);
    }

    /**
     * This is called after all elements below count accessCount have been
     * removed
     */
    public void setAccessCountFloor(int count) {
        super.accessMin = count;
    }

    public ObjectCacheIterator iterator() {
        return new ObjectCacheIterator();
    }

    public final class ObjectCacheIterator extends BaseHashIterator
    implements Iterator {}
}
