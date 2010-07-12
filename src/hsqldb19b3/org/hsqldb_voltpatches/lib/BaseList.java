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

/**
 * Abstract base for Lists
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
abstract class BaseList {

    protected int elementCount;

    abstract Object get(int index);

    abstract Object remove(int index);

    abstract boolean add(Object o);

    abstract int size();

    public boolean contains(Object o) {
        return indexOf(o) == -1 ? false
                             : true;
    }

    public boolean remove(Object o) {

        int i = indexOf(o);

        if (i == -1) {
            return false;
        }

        remove(i);

        return true;
    }

    public int indexOf(Object o) {

        for (int i = 0, size = size(); i < size; i++) {
            Object current = get(i);

            if (current == null) {
                if (o == null) {
                    return i;
                }
            } else if (current.equals(o)) {
                return i;
            }
        }

        return -1;
    }

    public boolean addAll(Collection other) {

        boolean  result = false;
        Iterator it     = other.iterator();

        while (it.hasNext()) {
            result = true;

            add(it.next());
        }

        return result;
    }

    public boolean addAll(Object[] array) {

        boolean  result = false;
        for (int i = 0; i < array.length; i++) {
            result = true;

            add(array[i]);
        }

        return result;
    }

    public boolean isEmpty() {
        return elementCount == 0;
    }

    /** Returns a string representation */
    public String toString() {

        StringBuffer sb = new StringBuffer(32 + elementCount * 3);

        sb.append("List : size=");
        sb.append(elementCount);
        sb.append(' ');
        sb.append('{');

        Iterator it = iterator();

        while (it.hasNext()) {
            sb.append(it.next());

            if (it.hasNext()) {
                sb.append(',');
                sb.append(' ');
            }
        }

        sb.append('}');

        return sb.toString();
    }

    public Iterator iterator() {
        return new BaseListIterator();
    }

    private class BaseListIterator implements Iterator {

        int     counter = 0;
        boolean removed;

        public boolean hasNext() {
            return counter < elementCount;
        }

        public Object next() {

            if (counter < elementCount) {
                removed = false;

                Object returnValue = get(counter);

                counter++;

                return returnValue;
            }

            throw new NoSuchElementException();
        }

        public int nextInt() {
            throw new NoSuchElementException();
        }

        public long nextLong() {
            throw new NoSuchElementException();
        }

        public void remove() {

            if (removed) {
                throw new NoSuchElementException("Iterator");
            }

            removed = true;

            if (counter != 0) {
                BaseList.this.remove(counter - 1);

                counter--;    // above can throw, so decrement if successful

                return;
            }

            throw new NoSuchElementException();
        }

        public void setValue(Object value) {
            throw new NoSuchElementException();
        }
    }
}
