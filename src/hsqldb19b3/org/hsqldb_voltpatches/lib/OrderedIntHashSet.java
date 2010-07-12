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

import org.hsqldb_voltpatches.store.BaseHashMap;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class OrderedIntHashSet extends BaseHashMap {

    public OrderedIntHashSet() {
        this(8);
    }

    public OrderedIntHashSet(int initialCapacity)
    throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.intKeyOrValue,
              BaseHashMap.noKeyOrValue, false);

        isList = true;
    }

    public boolean contains(int key) {
        return super.containsKey(key);
    }

    public boolean add(int key) {

        int oldSize = size();

        super.addOrRemove(key, 0, null, null, false);

        return oldSize != size();
    }

    public boolean remove(int key) {

        int oldSize = size();

        super.addOrRemove(key, 0, null, null, true);

        boolean result = oldSize != size();

        if (result) {
            int[] array = toArray();

            super.clear();

            for (int i = 0; i < array.length; i++) {
                add(array[i]);
            }
        }

        return result;
    }

    public int get(int index) {

        checkRange(index);

        return intKeyTable[index];
    }

    public int getOrderedMatchCount(int[] array) {

        int i = 0;

        for (; i < array.length; i++) {
            if (!super.containsKey(array[i])) {
                break;
            }
        }

        return i;
    }

    public int[] toArray() {

        int   lookup = -1;
        int[] array  = new int[size()];

        for (int i = 0; i < array.length; i++) {
            lookup = super.nextLookup(lookup);

            int value = intKeyTable[lookup];

            array[i] = value;
        }

        return array;
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
