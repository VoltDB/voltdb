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
 *  An Iterator that returns the elements of a specified array, or other
 *  iterators etc. The collection of objects returned depends on the
 *  constructor used.<p>
 *
 *  Based on similar Enumerator code by boucherb@users
 *
 * @author fred@users
 * @version 1.9.0
 * @since HSQLDB 1.7.2
 */
public class WrapperIterator implements Iterator {

    private static final Object[] emptyelements = new Object[0];
    private Object[]              elements;
    private int                   i;

    // chained iterators
    private boolean  chained;
    private Iterator it1;
    private Iterator it2;

    /** return only not null elements */
    private boolean notNull;

    /**
     * Constructor for an empty iterator. <p>
     */
    public WrapperIterator() {
        this.elements = emptyelements;
    }

    /**
     * Constructor for all elements of the specified array. <p>
     *
     * @param elements the array of objects to enumerate
     */
    public WrapperIterator(Object[] elements) {
        this.elements = elements;
    }

    /**
     * Constructor for not-null elements of specified array. <p>
     *
     * @param elements the array of objects to iterate
     */
    public WrapperIterator(Object[] elements, boolean notNull) {
        this.elements = elements;
        this.notNull  = notNull;
    }

    /**
     * Constructor for a singleton object iterator
     *
     * @param element the single object to iterate
     */
    public WrapperIterator(Object element) {
        this.elements = new Object[]{ element };
    }

    /**
     * Constructor for a chained iterator that returns the elements of the two
     * specified iterators.
     */
    public WrapperIterator(Iterator it1, Iterator it2) {

        this.it1 = it1;
        this.it2 = it2;
        chained  = true;
    }

    /**
     * Tests if this iterator contains more elements. <p>
     *
     * @return  <code>true</code> if this iterator contains more elements;
     *          <code>false</code> otherwise.
     */
    public boolean hasNext() {

        // for chained iterators
        if (chained) {
            if (it1 == null) {
                if (it2 == null) {
                    return false;
                }

                if (it2.hasNext()) {
                    return true;
                }

                it2 = null;

                return false;
            } else {
                if (it1.hasNext()) {
                    return true;
                }

                it1 = null;

                return hasNext();
            }
        }

        // for other interators
        if (elements == null) {
            return false;
        }

        for (; notNull && i < elements.length && elements[i] == null; i++) {}

        if (i < elements.length) {
            return true;
        } else {

            // release elements for garbage collection
            elements = null;

            return false;
        }
    }

    /**
     * Returns the next element.
     *
     * @return the next element
     * @throws NoSuchElementException if there is no next element
     */
    public Object next() {

        // for chained iterators
        if (chained) {
            if (it1 == null) {
                if (it2 == null) {
                    throw new NoSuchElementException();
                }

                if (it2.hasNext()) {
                    return it2.next();
                }

                it2 = null;

                next();
            } else {
                if (it1.hasNext()) {
                    return it1.next();
                }

                it1 = null;

                next();
            }
        }

        // for other itertors
        if (hasNext()) {
            return elements[i++];
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
        throw new NoSuchElementException();
    }

    public void setValue(Object value) {
        throw new NoSuchElementException();
    }
}
