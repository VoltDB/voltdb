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
 * Implementation of an ordered Set which maintains the inserted order of
 * elements and allows access by index. Iterators return the
 * elements in the index order.
 *
 * This class does not store null elements.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class OrderedHashSet extends HashSet implements HsqlList {

    public OrderedHashSet() {

        super(8);

        isList = true;
    }

    public boolean remove(Object key) {

        int oldSize = size();

        super.removeObject(key, true);

        return oldSize != size();
    }

    public Object remove(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        Object result = objectKeyTable[index];

        remove(result);

        return result;
    }

    public boolean insert(int index,
                          Object key) throws IndexOutOfBoundsException {

        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }

        if (contains(key)) {
            return false;
        }

        if (index == size()) {
            return add(key);
        }

        Object[] set = toArray(new Object[size()]);

        super.clear();

        for (int i = 0; i < index; i++) {
            add(set[i]);
        }

        add(key);

        for (int i = index; i < set.length; i++) {
            add(set[i]);
        }

        return true;
    }

    public Object set(int index, Object key) throws IndexOutOfBoundsException {
        throw new IndexOutOfBoundsException();
    }

    public void add(int index, Object key) throws IndexOutOfBoundsException {
        throw new IndexOutOfBoundsException();
    }

    public Object get(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return objectKeyTable[index];
    }

    public int getIndex(Object key) {
        return getLookup(key, key.hashCode());
    }

    public int getIndexByQueryTableColumnIndex(Object key) {
        return getLookupSameIndex(key, key.hashCode());
    }

    public int getLargestIndex(OrderedHashSet other) {

        int max = -1;

        for (int i = 0, size = other.size(); i < size; i++) {
            int index = getIndex(other.get(i));

            if (index > max) {
                max = index;
            }
        }

        return max;
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
