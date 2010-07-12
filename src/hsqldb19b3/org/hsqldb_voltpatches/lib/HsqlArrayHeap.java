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
 * An HsqlHeap implementation backed by an array of objects and an
 * {@link ObjectComparator ObjectComparator}.  This implementation
 * is non-blocking, dynamically resizing and thread-safe.
 *
 * @author boucherb@users
 * @version 1.7.2
 * @since 1.7.2
 */
public class HsqlArrayHeap implements HsqlHeap {

// --------------------------------- members -----------------------------------
    protected ObjectComparator oc;
    protected int              count;
    protected Object[]         heap;

// ------------------------------ constructors ---------------------------------

    /**
     * Creates a new HsqlArrayHeap with the given initial capacity, using
     * the specified ObjectComparator to maintain the heap invariant.
     *
     * @exception IllegalArgumentException if capacity less or equal to zero
     *      or comparator is null
     */
    public HsqlArrayHeap(int capacity,
                         ObjectComparator comparator)
                         throws IllegalArgumentException {

        if (capacity <= 0) {
            throw new IllegalArgumentException("" + capacity);
        }

        if (comparator == null) {
            throw new IllegalArgumentException("null comparator");
        }

        heap = new Object[capacity];
        oc   = comparator;
    }

//    /** Copy constructor (optional) */
//    public HsqlArrayHeap(HsqlArrayHeap other) {
//        count = other.count;
//        oc    = other.oc;
//        heap  = new Object[count];
//        System.arraycopy(other.heap,0, heap, 0, count);
//    }
// -------------------------- interface Implementation -------------------------
    public synchronized void clear() {

        for (int i = 0; i < count; ++i) {
            heap[i] = null;
        }

        count = 0;
    }

    public synchronized void add(Object o)
    throws IllegalArgumentException, RuntimeException {

        int ci;    // current index
        int pi;    // parent index

        if (o == null) {
            throw new IllegalArgumentException("null element");
        }

        if (isFull()) {
            throw new RuntimeException("full");
        }

        if (count >= heap.length) {
            increaseCapacity();
        }

        ci = count;

        count++;

        do {
            if (ci <= 0) {
                break;
            }

            pi = (ci - 1) >> 1;

            try {
                if (oc.compare(o, heap[pi]) >= 0) {
                    break;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(e.toString());
            }

            heap[ci] = heap[pi];
            ci       = pi;
        } while (true);

        heap[ci] = o;
    }

    public synchronized boolean isEmpty() {
        return count == 0;
    }

    public synchronized boolean isFull() {

        // almost impossible for this to happen
        return count == Integer.MAX_VALUE;
    }

    public synchronized Object peek() {
        return heap[0];
    }

    public synchronized Object remove() {

        int    ci;     // current index
        int    li;     // left index
        int    ri;     // right index
        int    chi;    // child index
        Object co;
        Object ro;

        if (count == 0) {
            return null;
        }

        ci = 0;
        ro = heap[ci];

        count--;

        if (count == 0) {
            heap[0] = null;

            return ro;
        }

        co          = heap[count];
        heap[count] = null;

        do {
            li = (ci << 1) + 1;

            if (li >= count) {
                break;
            }

            ri  = (ci << 1) + 2;
            chi = (ri >= count || oc.compare(heap[li], heap[ri]) < 0) ? li
                                                                      : ri;

            if (oc.compare(co, heap[chi]) <= 0) {
                break;
            }

            heap[ci] = heap[chi];
            ci       = chi;
        } while (true);

        heap[ci] = co;

        return ro;
    }

    public synchronized int size() {
        return count;
    }

// ------------- standard object and collection methods (optional) -------------
//    public synchronized Object clone() throws CloneNotSupportedException {
//        return new HsqlArrayHeap(this);
//    }
//
//    public synchronized java.util.Enumeration elements() {
//
//        Object[] elements;
//
//        elements = new Object[count];
//
//        System.arraycopy(heap, 0, elements, 0, count);
//
//        return new HsqlEnumeration(elements);
//    }
//
//    public synchronized boolean equals(Object o) {
//
//        HsqlArrayHeap other;
//        HsqlArrayHeap thiscopy;
//        HsqlArrayHeap othercopy;
//
//        if (this == o) {
//            return true;
//        }
//
//        if (!(o instanceof HsqlArrayHeap)) {
//            return false;
//        }
//
//        other = (HsqlArrayHeap) o;
//
//        if (count != other.size()) {
//            return false;
//        }
//
//        // this is a bit "iffy"... non-equal comparators _might_ still
//        // be _equivalent_ under current element content...
//
//        if (!oc.equals(other.oc)) {
//            return false;
//        }
//
//        thiscopy = new HsqlArrayHeap(this);
//        othercopy = new HsqlArrayHeap(other);
//
//        while(!thiscopy.isEmpty()) {
//            if (!thiscopy.remove().equals(othercopy.remove())) {
//                return false;
//            }
//        }
//
//        return true;
//    }
//
//    public synchronized Object[] toArray(Object a[]) {
//
//        if (a == null) {
//            a = new Object[count];
//        } else if ( a.length < count) {
//            a = (Object[]) java.lang.reflect.Array.newInstance(
//                a.getClass().getComponentType(), count);
//        }
//
//        System.arraycopy(heap, 0, a, 0, count);
//
//        for (int i = count; i < a.length; i++) {
//            a[i] = null;
//        }
//
//        return a;
//    }
//
    public synchronized String toString() {

        StringBuffer sb = new StringBuffer();

        sb.append(super.toString());
        sb.append(" : size=");
        sb.append(count);
        sb.append(' ');
        sb.append('[');

        for (int i = 0; i < count; i++) {
            sb.append(heap[i]);

            if (i + 1 < count) {
                sb.append(',');
                sb.append(' ');
            }
        }

        sb.append(']');

        return sb.toString();
    }

//
//    public void trim() {
//
//        Object[] oldheap;
//
//        oldheap = heap;
//
//        heap = new Object[count == 0 ? 16 : count];
//
//        System.arraycopy(oldheap, 0, heap, 0, count);
//    }
// -------------------- internal implementation methods ------------------------
    private void increaseCapacity() {

        Object[] oldheap;

        // no handling of boundary conditions.
        // In the highly unlikely event of a rollover,
        // in theory, an exception will be thrown (negative array index in
        // array allocation?)
        oldheap = heap;

        // as per java collections, v.s. JDK 1.1 java util.
        heap = new Object[3 * heap.length / 2 + 1];

        System.arraycopy(oldheap, 0, heap, 0, count);
    }
}
