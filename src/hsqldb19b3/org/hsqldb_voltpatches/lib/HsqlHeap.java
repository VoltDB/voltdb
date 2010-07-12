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
 * Provides the base HSQLDB interface for Heap ADT implementations. <p>
 *
 * In this context, a Heap is simply a collection-like ADT that allows addition
 * of elements and provides a way to remove the least element, given some
 * implementation-dependent strategy for imposing an order over its
 * elements. <p>
 *
 * Typically, an HsqlHeap will be implemented as a tree-like structure that
 * recursively guarantees a <i>Heap Invariant</i>, such that all nodes below
 * the root are greater than the root, given some comparison stragegy. <p>

 * This in turn provides the basis for an efficient implementation of ADTs such
 * PriorityQueue, since Heap operations using the typical implementation are,
 * in theory, guaranteed to be O(log n).
 *
 * @author boucherb@users
 * @version 1.7.2
 * @since 1.7.2
 */
public interface HsqlHeap {

    /**
     * Removes all of the elements from this Heap.
     */
    void clear();

    /**
     * Retrieves whether this Heap is empty.
     */
    boolean isEmpty();

    /**
     * Retrieves whether this Heap is full.
     */
    boolean isFull();

    /**
     * Adds the specified element to this Heap.
     *
     * @param o The element to add
     * @throws IllegalArgumentException if the implementation does
     *      not accept elements of the supplied type (optional)
     * throws RuntimeException if the implementation
     *      dictates that this Heap is not currently accepting additions
     *      or that this Heap is currently full (optional)
     */
    void add(Object o) throws IllegalArgumentException, RuntimeException;

    /**
     * Retrieves the least element from this Heap, without removing it.
     *
     * @return the least element from this Heap
     */
    Object peek();

    /**
     * Retrieves the least element from this Heap, removing it in the process.
     *
     * @return the least element from this Heap
     */
    Object remove();

    /**
     * Retrieves the number of elements currently in this Heap.
     *
     * @return the number of elements currently in this Heap
     */
    int size();
}
