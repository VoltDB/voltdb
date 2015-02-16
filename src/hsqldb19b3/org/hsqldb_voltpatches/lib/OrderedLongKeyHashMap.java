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

import org.hsqldb_voltpatches.map.BaseHashMap;

/**
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.7
 * @since 1.9.0
 */
public class OrderedLongKeyHashMap extends BaseHashMap {

    Set        keySet;
    Collection values;

    public OrderedLongKeyHashMap() {
        this(8);
    }

    public OrderedLongKeyHashMap(int initialCapacity)
    throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);

        isList = true;
    }

    public OrderedLongKeyHashMap(int initialCapacity,
                                 boolean hasThirdValue)
                                 throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);

        objectKeyTable   = new Object[objectValueTable.length];
        isTwoObjectValue = true;
        isList           = true;

        if (hasThirdValue) {
            objectValueTable2 = new Object[objectValueTable.length];
        }

        minimizeOnEmpty = true;
    }

    public Object get(long key) {

        int lookup = getLookup(key);

        if (lookup != -1) {
            return objectValueTable[lookup];
        }

        return null;
    }

    public Object getValueByIndex(int index) {
        return objectValueTable[index];
    }

    public Object getSecondValueByIndex(int index) {
        return objectKeyTable[index];
    }

    public Object getThirdValueByIndex(int index) {
        return objectValueTable2[index];
    }

    public Object setSecondValueByIndex(int index, Object value) {

        Object oldValue = objectKeyTable[index];

        objectKeyTable[index] = value;

        return oldValue;
    }

    public Object setThirdValueByIndex(int index, Object value) {

        Object oldValue = objectValueTable2[index];

        objectValueTable2[index] = value;

        return oldValue;
    }

    public Object put(long key, Object value) {
        return super.addOrRemove(key, value, null, false);
    }

    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }

    public Object remove(long key) {
        return super.addOrRemove(key, null, null, false);
    }

    public boolean containsKey(long key) {
        return super.containsKey(key);
    }

    /* methods for two object lookups */
    public Object put(long key, Object valueOne, Object valueTwo) {
        return super.addOrRemove(key, valueOne, valueTwo, false);
    }

    public int getLookup(long key) {
        return super.getLookup(key);
    }

    public Object getFirstByLookup(int lookup) {

        if (lookup == -1) {
            return null;
        }

        return objectValueTable[lookup];
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
            return OrderedLongKeyHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return OrderedLongKeyHashMap.this.size();
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
            OrderedLongKeyHashMap.this.clear();
        }
    }

    class Values implements Collection {

        public Iterator iterator() {
            return OrderedLongKeyHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return OrderedLongKeyHashMap.this.size();
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
            OrderedLongKeyHashMap.this.clear();
        }
    }
}
