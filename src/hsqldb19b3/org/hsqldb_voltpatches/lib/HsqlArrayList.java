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

import java.lang.reflect.Array;

// fredt@users - 1.8.0, 1.9.0 - enhancements

/**
 * Intended as an asynchronous alternative to Vector.  Use HsqlLinkedList
 * instead if its better suited.
 *
 * @author dnordahl@users
 * @version 1.9.0
 * @since 1.7.0
 */
public class HsqlArrayList extends BaseList implements HsqlList {

//fredt@users
/*
    private static Reporter reporter = new Reporter();

    private static class Reporter {

        private static int initCounter   = 0;
        private static int updateCounter = 0;

        Reporter() {

            try {
                System.runFinalizersOnExit(true);
            } catch (SecurityException e) {}
        }

        protected void finalize() {

            System.out.println("HsqlArrayList init count: " + initCounter);
            System.out.println("HsqlArrayList update count: "
                               + updateCounter);
        }
    }
*/
    private static final int   DEFAULT_INITIAL_CAPACITY = 10;
    private static final float DEFAULT_RESIZE_FACTOR    = 2.0f;
    Object[]                   elementData;
    Object[]                   reserveElementData;
    private boolean            minimizeOnClear;

    public HsqlArrayList(Object[] data, int count) {
        elementData  = data;
        elementCount = count;
    }

    /** Creates a new instance of HsqlArrayList */
    public HsqlArrayList() {

//        reporter.initCounter++;
        elementData = new Object[DEFAULT_INITIAL_CAPACITY];
    }

    /**
     * Creates a new instance of HsqlArrayList that minimizes the size when
     * empty
     */
    public HsqlArrayList(boolean minimize) {

//        reporter.initCounter++;
        elementData     = new Object[DEFAULT_INITIAL_CAPACITY];
        minimizeOnClear = minimize;
    }

    /** Creates a new instance with the given initial capacity */
    public HsqlArrayList(int initialCapacity) {

//        reporter.initCounter++;
        if (initialCapacity < 0) {
            throw new NegativeArraySizeException(
                "Invalid initial capacity given");
        }

        if (initialCapacity == 0) {
            elementData = new Object[1];
        } else {
            elementData = new Object[initialCapacity];
        }
    }

    /** Inserts an element at the given index */
    public void add(int index, Object element) {

//        reporter.updateCounter++;
        if (index > elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + ">" + elementCount);
        }

        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }

        if (elementCount >= elementData.length) {
            increaseCapacity();
        }

        for (int i = elementCount; i > index; i--) {
            elementData[i] = elementData[i - 1];
        }

        elementData[index] = element;

        elementCount++;
    }

    /** Appends an element to the end of the list */
    public boolean add(Object element) {

//        reporter.updateCounter++;
        if (elementCount >= elementData.length) {
            increaseCapacity();
        }

        elementData[elementCount] = element;

        elementCount++;

        return true;
    }

    /** Gets the element at given position */
    public Object get(int index) {

        if (index >= elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= "
                                                + elementCount);
        }

        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }

        return elementData[index];
    }

    /** returns the index of given object or -1 if not found */
    public int indexOf(Object o) {

        if (o == null) {
            for (int i = 0; i < elementCount; i++) {
                if (elementData[i] == null) {
                    return i;
                }
            }

            return -1;
        }

        for (int i = 0; i < elementCount; i++) {
            if (o.equals(elementData[i])) {
                return i;
            }
        }

        return -1;
    }

    /** Removes and returns the element at given position */
    public Object remove(int index) {

        if (index >= elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= "
                                                + elementCount);
        }

        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }

        Object removedObj = elementData[index];

        for (int i = index; i < elementCount - 1; i++) {
            elementData[i] = elementData[i + 1];
        }

        elementCount--;

        if (elementCount == 0) {
            clear();
        } else {
            elementData[elementCount] = null;
        }

        return removedObj;
    }

    /** Replaces the element at given position */
    public Object set(int index, Object element) {

        if (index >= elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= "
                                                + elementCount);
        }

        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }

        Object replacedObj = elementData[index];

        elementData[index] = element;

        return replacedObj;
    }

    /** Returns the number of elements in the array list */
    public final int size() {
        return elementCount;
    }

    private void increaseCapacity() {

        int baseSize = elementData.length == 0 ? 1
                                               : elementData.length;

        baseSize = (int) (baseSize * DEFAULT_RESIZE_FACTOR);

        resize(baseSize);
    }

    private void resize(int baseSize) {

        if (baseSize == elementData.length) {
            return;
        }

        Object[] newArray = (Object[]) Array.newInstance(
            elementData.getClass().getComponentType(), baseSize);
        int count = elementData.length > newArray.length ? newArray.length
                                                         : elementData.length;

        System.arraycopy(elementData, 0, newArray, 0, count);

        if (minimizeOnClear && reserveElementData == null) {
            ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_OBJECT, elementData, 0,
                                 elementData.length);

            reserveElementData = elementData;
        }

        elementData = newArray;
    }

    /** Trims the array to be the same size as the number of elements. */
    public void trim() {

        // 0 size array is possible
        resize(elementCount);
    }

    // fredt@users
    public void clear() {

        if (minimizeOnClear && reserveElementData != null) {
            elementData        = reserveElementData;
            reserveElementData = null;
            elementCount       = 0;

            return;
        }

        for (int i = 0; i < elementCount; i++) {
            elementData[i] = null;
        }

        elementCount = 0;
    }

    public void setSize(int newSize) {

        if (newSize == 0) {
            clear();

            return;
        }

        if (newSize <= elementCount) {
            for (int i = newSize; i < elementCount; i++) {
                elementData[i] = null;
            }

            elementCount = newSize;

            return;
        }

        for (; newSize > elementData.length; ) {
            increaseCapacity();
        }

        elementCount = newSize;
    }

// fredt@users
    public Object[] toArray() {

        Object[] newArray = (Object[]) Array.newInstance(
            elementData.getClass().getComponentType(), elementCount);

        System.arraycopy(elementData, 0, newArray, 0, elementCount);

        return newArray;
    }

    public Object[] toArray(int start, int limit) {

        Object[] newArray = (Object[]) Array.newInstance(
            elementData.getClass().getComponentType(), limit - start);

        System.arraycopy(elementData, start, newArray, 0, limit - start);

        return newArray;
    }

    /**
     * Copies all elements of the list to a[]. It is assumed a[] is of the
     * correct type. If a[] is too small, a new array or the same type is
     * returned. If a[] is larger, only the list elements are copied and no
     * other change is made to the array.
     * Differs from the implementation in java.util.ArrayList in the second
     * aspect.
     */
    public Object toArray(Object a) {

        if (Array.getLength(a) < elementCount) {
            a = Array.newInstance(a.getClass().getComponentType(),
                                  elementCount);
        }

        System.arraycopy(elementData, 0, a, 0, elementCount);

        return a;
    }

    public void sort(ObjectComparator c) {

        if (elementCount < 2) {
            return;
        }

        Sort.sort(elementData, c, 0, elementCount - 1);
    }

    public Object[] getArray() {
        return elementData;
    }
}
