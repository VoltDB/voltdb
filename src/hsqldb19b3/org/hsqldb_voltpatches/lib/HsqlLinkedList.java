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
 * Intended as an asynchronous alternative to HsqlArrayList.  Use HsqlArrayList if
 * the list won't be initialized sequentially and if frequent references to
 * specific element positions will be made.
 *
 * @author jcpeck@users
 * @version 1.9.0
 * @since 1.7.2
 */
public class HsqlLinkedList extends BaseList implements HsqlList {

    /**
     * A reference to the head of the list.  It is a dummy head (that is, the
     * Node for index 0 is actually first.next).
     */
    private Node first;

    /** A reference to the tail of the list */
    private Node last;

    /**
     * Creates a new instance of HsqlLinkedList.
     */
    public HsqlLinkedList() {

        first        = new Node(null, null);
        last         = first;
        elementCount = 0;
    }

    /**
     * Returns the first node to allow iterators to be used.
     */
    public Node getHeadNode() {
        return first;
    }

    /**
     * Removes the given node to allow removel from iterators
     */
    public boolean removeAfter(Node node) {

        if (node == null || node.next == null) {
            return false;
        }

        if (node.next == last) {
            last = node;
        }

        node.next = node.next.next;

        return true;
    }

    /**
     * Inserts <code>element</code> at <code>index</code>.
     * @throws IndexOutOfBoundsException if <code>index</code> &lt; 0 or is &gt;
     * <code>size</code>.
     */
    public void add(int index, Object element) {

        if (index == size()) {
            add(element);    //Add to the end of this.
        }

        //If index > size() an exception should be thrown with a slightly
        //different message than the exception thrown by getInternal.
        else if (index > size()) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " > " + size());
        } else {
            Node current = getInternal(index);
            Node newNext = new Node(current.data, current.next);

            current.data = element;
            current.next = newNext;

            elementCount++;

            //If they inserted at the last valid index, then a new last element
            //was created, therfore update the last pointer
            if (last == current) {
                last = newNext;
            }
        }
    }

    /**
     * Appends <code>element</code> to the end of this list.
     * @return true
     */
    public boolean add(Object element) {

        last.next = new Node(element, null);
        last      = last.next;

        elementCount++;

        return true;
    }

    public void clear() {
        first.next = null;
        last = first;
        elementCount = 0;
    }

    /**
     * Gets the element at given position
     * @throws IndexOutOfBoundsException if index is not valid
     * index within the list (0 &lt;= <code>index</code> &lt;
     * <code>size</code>).
     */
    public Object get(int index) {
        return getInternal(index).data;
    }

    /**
     * Removes and returns the element at <code>index</code>.
     * @throws IndexOutOfBoundsException if index is not valid
     * index within the list (0 &lt;= <code>index</code> &lt;
     * <code>size</code>).
     */
    public Object remove(int index) {

        //Check that the index is less than size because the getInternal
        //method is being called with index - 1 and its checks will therefore
        //not be useful in this case.
        if (index >= size()) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= " + size());
        }

        //Get the node that is previous to the node being removed
        Node previousToRemove;

        if (index == 0) {
            previousToRemove = first;
        } else {
            previousToRemove = getInternal(index - 1);
        }

        //previousToRemove.next will never be null because of the check above
        //that index < size.
        Node toRemove = previousToRemove.next;

        previousToRemove.next = toRemove.next;

        elementCount--;

        //If they removed at the last valid index, then a the last element
        //was removed, therfore update the last pointer
        if (last == toRemove) {
            last = previousToRemove;
        }

        return toRemove.data;
    }

    /**
     * Replaces the current element at <code>index/code> with
     * <code>element</code>.
     * @return The current element at <code>index</code>.
     */
    public Object set(int index, Object element) {

        Node   setMe   = getInternal(index);
        Object oldData = setMe.data;

        setMe.data = element;

        return oldData;
    }

    /**
     * Accessor for the size of this linked list.  The size is the total number
     * of elements in the list and is one greater than the largest index in the
     * list.
     * @return The size of this.
     */
    public final int size() {
        return elementCount;
    }

    /**
     * Helper method that returns the Node at <code>index</code>.
     * @param index The index of the Node to return.
     * @return The Node at the given index.
     * @throws IndexOutOfBoundsException if index is not valid
     * index within the list (0 &lt;= <code>index</code> &lt;
     * <code>size</code>).
     */
    protected final Node getInternal(int index) {

        //Check preconditions for the index variable
        if (index >= size()) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= " + size());
        }

        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }

        if (index == 0) {
            return first.next;
        } else if (index == (size() - 1)) {
            return last;
        } else {
            Node pointer = first.next;

            for (int i = 0; i < index; i++) {
                pointer = pointer.next;
            }

            return pointer;
        }
    }

    /**
     * Inner class that represents nodes within the linked list.  This should
     * be a static inner class to avoid the uneccesary overhead of the
     * containing class "this" pointer.
     * jcpeck@users
     * @version 05/24/2002
     */
    public static class Node {

        public Node   next;
        public Object data;

        public Node() {
            next = null;
            data = null;
        }

        public Node(Object data) {
            this.next = null;
            this.data = data;
        }

        public Node(Object data, Node next) {
            this.next = next;
            this.data = data;
        }
    }
}
