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

/**
 * Implementation of an Map which maintains the user-defined order of the keys.
 * Key/value pairs can be accessed by index or by key. Iterators return the
 * keys or values in the index order.
 *
 * This class does not store null keys.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class HashMappedList extends HashMap {

    public HashMappedList() {
        this(8);
    }

    public HashMappedList(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity);

        isList = true;
    }

    public Object get(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return objectValueTable[index];
    }

    public Object remove(Object key) {

        int lookup = getLookup(key, key.hashCode());

        if (lookup < 0) {
            return null;
        }

        Object returnValue = super.remove(key);

        removeRow(lookup);

        return returnValue;
    }

    public Object remove(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return remove(objectKeyTable[index]);
    }

    public boolean add(Object key, Object value) {

        int lookup = getLookup(key, key.hashCode());

        if (lookup >= 0) {
            return false;
        }

        super.put(key, value);

        return true;
    }

    public Object put(Object key, Object value) {
        return super.put(key, value);
    }

    public Object set(int index,
                      Object value) throws IndexOutOfBoundsException {

        checkRange(index);

        Object returnValue = objectKeyTable[index];

        objectKeyTable[index] = value;

        return returnValue;
    }

    public boolean insert(int index, Object key,
                          Object value) throws IndexOutOfBoundsException {

        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }

        int lookup = getLookup(key, key.hashCode());

        if (lookup >= 0) {
            return false;
        }

        if (index == size()) {
            return add(key, value);
        }

        HashMappedList hm = new HashMappedList(size());

        for (int i = index; i < size(); i++) {
            hm.add(getKey(i), get(i));
        }

        for (int i = size() - 1; i >= index; i--) {
            remove(i);
        }

        for (int i = 0; i < hm.size(); i++) {
            add(hm.getKey(i), hm.get(i));
        }

        return true;
    }

    public boolean set(int index, Object key,
                       Object value) throws IndexOutOfBoundsException {

        checkRange(index);

        if (keySet().contains(key) && getIndex(key) != index) {
            return false;
        }

        super.remove(objectKeyTable[index]);
        super.put(key, value);

        return true;
    }

    public boolean setKey(int index,
                          Object key) throws IndexOutOfBoundsException {

        checkRange(index);

        Object value = objectValueTable[index];

        return set(index, key, value);
    }

    public boolean setValue(int index,
                            Object value) throws IndexOutOfBoundsException {

        boolean result;
        Object  existing = objectValueTable[index];

        if (value == null) {
            result = value != existing;
        } else {
            result = !value.equals(existing);
        }

        objectValueTable[index] = value;

        return result;
    }

    public Object getKey(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return objectKeyTable[index];
    }

    public int getIndex(Object key) {
        return getLookup(key, key.hashCode());
    }

    public Object[] toValuesArray(Object[] a) {

        int size = size();

        if (a == null || a.length < size) {
            a = new Object[size];
        }

        for (int i = 0; i < size; i++) {
            a[i] = super.objectValueTable[i];
        }

        return a;
    }

    public Object[] toKeysArray(Object[] a) {

        int size = size();

        if (a == null || a.length < size) {
            a = new Object[size];
        }

        for (int i = 0; i < size; i++) {
            a[i] = super.objectKeyTable[i];
        }

        return a;
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
