/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb_voltpatches.lib;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb_voltpatches.map.BaseHashMap;

/**
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 1.9.0
 */
public class LongKeyHashMap extends BaseHashMap {

    Set        keySet;
    Collection values;

    //
    ReentrantReadWriteLock           lock = new ReentrantReadWriteLock(true);
    ReentrantReadWriteLock.ReadLock  readLock  = lock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    public LongKeyHashMap() {
        this(16);
    }

    public LongKeyHashMap(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
    }

    public Lock getReadLock() {
        return readLock;
    }

    public Lock getWriteLock() {
        return writeLock;
    }

    public Object get(long key) {

        readLock.lock();

        try {
            int lookup = getLookup(key);

            if (lookup != -1) {
                return objectValueTable[lookup];
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Object put(long key, Object value) {

        writeLock.lock();

        try {
            return super.addOrRemove(key, 0, null, value, false);
        } finally {
            writeLock.unlock();
        }
    }

    public boolean containsValue(Object value) {

        readLock.lock();

        try {
            return super.containsValue(value);
        } finally {
            readLock.unlock();
        }
    }

    public Object remove(long key) {

        writeLock.lock();

        try {
            return super.addOrRemove(key, 0, null, null, true);
        } finally {
            writeLock.unlock();
        }
    }

    public boolean containsKey(long key) {

        readLock.lock();

        try {
            return super.containsKey(key);
        } finally {
            readLock.unlock();
        }
    }

    public void clear() {

        writeLock.lock();

        try {
            super.clear();
        } finally {
            writeLock.unlock();
        }
    }

    public Object[] toArray() {

        readLock.lock();

        try {
            if (isEmpty()) {
                return emptyObjectArray;
            }

            Object[] array = new Object[size()];
            int      i     = 0;
            Iterator it    = LongKeyHashMap.this.new BaseHashIterator(false);

            while (it.hasNext()) {
                array[i++] = it.next();
            }

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public int getOrderedMatchCount(int[] array) {

        int i = 0;

        readLock.lock();

        try {
            for (; i < array.length; i++) {
                if (!super.containsKey(array[i])) {
                    break;
                }
            }
        } finally {
            readLock.unlock();
        }

        return i;
    }

    public Set keySet() {

        if (keySet == null) {
            keySet = new KeySet();
        }

        return keySet;
    }

    public Collection values() {

        if (values == null) {
            values = new Values();
        }

        return values;
    }

    class KeySet implements Set {

        public Iterator iterator() {
            return LongKeyHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return LongKeyHashMap.this.size();
        }

        public boolean contains(Object o) {
            throw new RuntimeException();
        }

        public Object get(Object key) {
            throw new RuntimeException();
        }

        public boolean add(Object value) {
            throw new RuntimeException();
        }

        public boolean addAll(Collection c) {
            throw new RuntimeException();
        }

        public boolean remove(Object o) {
            throw new RuntimeException();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public void clear() {
            LongKeyHashMap.this.clear();
        }
    }

    class Values implements Collection {

        public Iterator iterator() {
            return LongKeyHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return LongKeyHashMap.this.size();
        }

        public boolean contains(Object o) {
            throw new RuntimeException();
        }

        public boolean add(Object value) {
            throw new RuntimeException();
        }

        public boolean addAll(Collection c) {
            throw new RuntimeException();
        }

        public boolean remove(Object o) {
            throw new RuntimeException();
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public void clear() {
            LongKeyHashMap.this.clear();
        }
    }
}
