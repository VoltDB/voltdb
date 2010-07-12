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


package org.hsqldb_voltpatches.lib;

import java.util.NoSuchElementException;

import org.hsqldb_voltpatches.store.BaseHashMap;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.7.2
 * @since 1.7.2
 */
public class LongKeyIntValueHashMap extends BaseHashMap {

    private Set        keySet;
    private Collection values;

    public LongKeyIntValueHashMap() {
        this(8);
    }

    public LongKeyIntValueHashMap(boolean minimize) {

        this(8);

        minimizeOnEmpty = minimize;
    }

    public LongKeyIntValueHashMap(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.intKeyOrValue, false);
    }

    public int get(long key) throws NoSuchElementException {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return intValueTable[lookup];
        }

        throw new NoSuchElementException();
    }

    public int get(long key, int defaultValue) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return intValueTable[lookup];
        }

        return defaultValue;
    }

    public boolean get(long key, int[] value) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            value[0] = intValueTable[lookup];

            return true;
        }

        return false;
    }

    public boolean put(long key, int value) {

        int oldSize = size();

        super.addOrRemove(key, value, null, null, false);

        return oldSize != size();
    }

    public boolean remove(long key) {

        int oldSize = size();

        super.addOrRemove(key, 0, null, null, true);

        return oldSize != size();
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
            return LongKeyIntValueHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return LongKeyIntValueHashMap.this.size();
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
            LongKeyIntValueHashMap.this.clear();
        }
    }

    class Values implements Collection {

        public Iterator iterator() {
            return LongKeyIntValueHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return LongKeyIntValueHashMap.this.size();
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
            LongKeyIntValueHashMap.this.clear();
        }
    }
}
