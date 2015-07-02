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
 * This class does not store null keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class HashMap extends BaseHashMap {

    Set        keySet;
    Collection values;

    public HashMap() {
        this(8);
    }

    public HashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
    }

    public Object get(Object key) {

        int hash   = key.hashCode();
        int lookup = getLookup(key, hash);

        if (lookup != -1) {
            return objectValueTable[lookup];
        }

        return null;
    }

    public Object put(Object key, Object value) {
        return super.addOrRemove(0, 0, key, value, false);
    }

    public Object remove(Object key) {
        return super.removeObject(key, false);
    }

    public boolean containsKey(Object key) {
        return super.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }

    public void putAll(HashMap t) {

        Iterator it = t.keySet().iterator();

        while (it.hasNext()) {
            Object key = it.next();

            put(key, t.get(key));
        }
    }

    public void valuesToArray(Object[] array) {

        Iterator it = values().iterator();
        int      i  = 0;

        while (it.hasNext()) {
            array[i] = it.next();

            i++;
        }
    }

    public void keysToArray(Object[] array) {

        Iterator it = keySet().iterator();
        int      i  = 0;

        while (it.hasNext()) {
            array[i] = it.next();

            i++;
        }
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
            return HashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return HashMap.this.size();
        }

        public boolean contains(Object o) {
            return containsKey(o);
        }

        public Object get(Object key) {

            int lookup = HashMap.this.getLookup(key, key.hashCode());

            if (lookup < 0) {
                return null;
            } else {
                return HashMap.this.objectKeyTable[lookup];
            }
        }

        public boolean add(Object value) {
            throw new RuntimeException();
        }

        public boolean addAll(Collection c) {
            throw new RuntimeException();
        }

        public boolean remove(Object o) {

            int oldSize = size();

            HashMap.this.remove(o);

            return size() != oldSize;
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public void clear() {
            HashMap.this.clear();
        }
    }

    class Values implements Collection {

        public Iterator iterator() {
            return HashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return HashMap.this.size();
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
            HashMap.this.clear();
        }
    }
}
