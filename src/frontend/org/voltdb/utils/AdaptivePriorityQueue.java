/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import org.voltcore.logging.VoltTimerCount;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.Queue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.AbstractQueue;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * An unbounded priority queue based on a priority heap.
 * The elements of the priority queue are ordered based on the policy set for the Queue.
 * The PRIORITY_POLICY values are as following
 *
 *      PRIORITY.NO_PRIORITY_POLICY   - no priority assigned to the elements of the queue. The Queue behaves as FIFO
 *      PRIORITY.USER_DEFINED_POLICY  - elements in the queue are ordered by the assigned priority.
 *      PRIORITY.MAX_WAIT_POLICY      - elements are ordered by assigned priority as with PRIORITY.USER_DEFINED_POLICY
 *                                      unless the element remained in the queue longer than certain maximum.
 *      PRIORITY.FREQUENCY_DEFINED_POLICY  - all elements in the queue are assigned a sequence number. Order is proportional to
 *                                      how much the element is behind the current sequence number for the queue normalized with
 *                                      respect to the policy:   (seqNum - elem.seqNum)/(elem.priority + 1 - MIN_PRIORITY_LEVELS). Since this order changes
 *                                      with every insertion and removal of an element, the queue attempts to satisfy this ordering
 *                                      not in a strict manner, but by balancing efficiency versus ordering requirements.
 *                                      If an element remains is the queue longer than a certain maximum,
 *                                      it becomes available immediately for the consumer.
 *
 * A adaptive priority queue does not permit null elements.
 * Objects inserted into the queue can be of type Prioritized. In this case, the queue will used its naturally assigned priority.
 *
 * @see Prioritized
 *
 * An adaptive priority queue is unbounded.
 * This class provides 2 iterators. One iterator yields elements ordered by their priority
 * when elements with the same priority ordered in the FIFO manner. The second iterator yields element in the FIFO order
 * without regard to a priority.
 *
 * Note that this implementation is not synchronized. Multiple threads should not access a PriorityQueue instance concurrently
 * if any of the threads modifies the queue. Instead, use the thread-safe BlockingAdaptivePriorityQueue class.
 *
 * @see BlockingAdaptivePriorityQueue
 *
 * Implementation note: this implementation provides O(log(k)) time for the enqueuing and dequeuing methods where k is the number of
 * different priorities for elements currently in the queue;
 * linear time for contains(Object) methods;
 * The method remove(Object) throws UnsupportedOperationException
 * and constant time for the retrieval methods (peek, element, and size).
 *
 *
 * @param <T>
 */
public class AdaptivePriorityQueue<T extends Prioritized> extends AbstractQueue<T>  {

    /**
     * The minimal priority level that can be handled by this queue
     * It represents the HIGHEST priority that can be assigned to an element
     */
    static public final int MIN_PRIORITY_LEVEL = 0;  // Highest priority

    /**
     * The maximum priority level that can be handled by this queue
     * It represents the LOWEST priority that can be assigned to an element
     */
    static public final int MAX_PRIORITY_LEVEL = 62;

    /**
     * The timeout period in milliseconds for the elements in the queue.
     * If message remains longer than the timeout period, it is consumed next regardless of its priority
     */
    static public final int MAX_DEFAULT_TIMEOUT_MILLIS = 1000;

     static private final int PRIORITY_RANGE = MAX_PRIORITY_LEVEL - MIN_PRIORITY_LEVEL + 1;

    /**
     * Enum representing the ordering policy for the queue
     */
    public enum OrderingPolicy {
        /**
         * With NO_PRIORITY_POLICY priority has no effect. The queue behaves as FIFO
         */
        NO_PRIORITY_POLICY,

        /**
         * With USER_DEFINED_POLICY ordering of elements are strictly based on priority.
         * Elements with the same priority are ordered as FIFO between themselves.
         */
        USER_DEFINED_POLICY,

        /**
         * With MAX_WAIT_POLICY elements are ordered as with USER_DEFINED_POLICY but could expire
         * if stay in the queue longer than the set timeout. They are consumed immediately afterwards
         */
        MAX_WAIT_POLICY,

        /**
         * With FREQUENCY_DEFINED_POLICY elements consumed proportionally to the assigned priority.
         */
        FREQUENCY_DEFINED_POLICY;

    }

    private int queueSize = 0;
    private final OrderingPolicy policy;
    private TimedDeque<T> deque;
    private final PriorityElement.ElementFactoryPool<T> elementFactory;
    private long maxTimeout;  //In nanosec
    private boolean enableBatching = false;
    private int batchSize;
    private int batchPriority;

    // an index how many elements passed through the queue.
    // It is used to manage "Frequency-based" policy.
    private long seqIdx = 0;

    private MaxHeap<T> maxHeap;

    private boolean hasMaxWait() {
        return (policy == OrderingPolicy.MAX_WAIT_POLICY) || (policy == OrderingPolicy.FREQUENCY_DEFINED_POLICY);
    }

    /**
     * Creates an instance of the AdaptivePriorityQueue with the specified policy
     *
     * @param policy for the Queue
     */
    public AdaptivePriorityQueue(OrderingPolicy policy) {
        this(policy, MAX_DEFAULT_TIMEOUT_MILLIS, MIN_PRIORITY_LEVEL -1, 0);
    }


    /**
     * Creates an instance of the AdaptivePriorityQueue with the specified policy and the timeout period
     *
     * @param policy for the Queue
     * @param timeout (milliseconds) timeout set for the elements of the queue
     */
    public AdaptivePriorityQueue(OrderingPolicy policy, int timeout) {
        this(policy,timeout, MIN_PRIORITY_LEVEL-1, 0);
    }

    /**
     * Creates an instance of the AdaptivePriorityQueue with the specified policy and the timeout period
     *
     * @param policy for the Queue
     * @param timeout (milliseconds) timeout set for the elements of the queue
     * @param batchPriority priority that should be processed in the batch-like manner.
     *                      Any illegal value beyond the range MIN_PRIORITY_LEVEL - MAX_PRIORITY_LEVEL disables batch processing
     * @param batchSize the size of the batch for batch-wise processing. The value <= 0 disables batching processing.
     */
    public AdaptivePriorityQueue(OrderingPolicy policy, int timeout, int batchPriority, int batchSize) {
        this.policy = policy;
        if( hasMaxWait() ) {
            this.deque = new TimedDeque<>();
        }
        if(policy == OrderingPolicy.FREQUENCY_DEFINED_POLICY) {
            this.maxHeap = new MaxHeap<>(this::compareRank);
        }
        else{
            this.maxHeap = new MaxHeap<>(this::comparePrio);
        }
        this.elementFactory = new PriorityElement.ElementFactoryPool<>();
        this.maxTimeout = TimeUnit.MILLISECONDS.toNanos(timeout);

        this.enableBatching = (this.policy != OrderingPolicy.NO_PRIORITY_POLICY) &&
                              (batchPriority >= MIN_PRIORITY_LEVEL && batchPriority <=MAX_PRIORITY_LEVEL) &&
                              (batchSize > 0);
        this.batchPriority = batchPriority;
        this.batchSize = batchSize;
    }

    /**
     * @return policy of the queue
     */
    public OrderingPolicy getPolicy() {
        return this.policy;
    }

    /**
     * Get the value for the timeout for elements in this queue
     *
     * @return timeout (milliseconds) for elements in this queue
     */
    public long getTimeout(){
        return TimeUnit.NANOSECONDS.toMillis(maxTimeout);
    }

    @Override
    public int size(){
        return queueSize;
    }

    @Override
    public boolean isEmpty() {
        return queueSize == 0;
    }

    /**
     * Inserts the specified element into this queue.
     * The priority of the prioritized object is used as the priority of the new element of the queue.
     * Priority of the new elements is handled consistently with the assigned policy for this Adaptive Priority Queue.
     * Otherwise, the element is assigned the HIGHEST priority {@link AdaptivePriorityQueue#MIN_PRIORITY_LEVEL }
     *
     * @param elem  element to be added
     * @return {@code true} (as specified by {@link Collection#add})
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if the implied priority in the element of type {@link Prioritized} is outside of the range {@link #MIN_PRIORITY_LEVEL}-{@link #MAX_PRIORITY_LEVEL}
     */
    @Override
    public boolean add(T elem) {
        return offer(elem);
    }

    /**
     * Inserts the specified element into this queue.
     * The priority of the prioritized object is used as the priority of the new element of the queue.
     * Priority of the new elements is handled consistently with the assigned policy for this Adaptive Priority Queue.
     * Otherwise, the element is assigned the HIGHEST priority {@link AdaptivePriorityQueue#MIN_PRIORITY_LEVEL }
     *
     * @param elem  element to be added
     * @return {@code true} (as specified by {@link Queue#offer(Object)}  )
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if the implied priority in the element of type {@link Prioritized} is outside of the range {@link #MIN_PRIORITY_LEVEL}-{@link #MAX_PRIORITY_LEVEL}
     */
    @Override
    public boolean offer(T elem) {
        return offer(elem,elem.getPriority());
    }

    private boolean offer(T elem, int priority) {
        long now = System.nanoTime();
        PriorityElement<T> cur = elementFactory.newElement(elem,priority,now, seqIdx++);
        maxHeap.offer(cur);
        if(hasMaxWait()){
            deque.add(cur);
        }
        queueSize++;
        return true;
    }


    @Override
    public T element(){
        T t = peek();
        if( t == null ) {
            throw new NoSuchElementException();
        }
        return t;
    }

    /**
     * Obtain the head of this queue
     *
     * @return the head of this queue or null if queue is empty
     */
    @Override
    public T peek() {
        PriorityElement<T> cur = maxHeap.peek();
        return cur==null ? null : cur.elem;
    }

    /**
     * Remove the head of this queue
     *
     * @return the head of this queue
     * @throws NoSuchElementException – if this queue is empty
     */
    @Override
    public T remove() {
        T t = poll();
        if( t == null ) {
            throw new NoSuchElementException();
        }
        return t;
    }

    static VoltTimerCount timeoutCounter = VoltTimerCount.newVoltCounter("AdaptivePriorityQueue.timeout");

    /**
     * Remove the head of this queue or null if the queue is empty
     *
     * @return the head of this queue
     */
    @Override
    public T poll() {
        if(hasMaxWait() && maxTimeout>0) {
            PriorityElement<T> longest = deque.peek();
            if(longest == null) {
                return null;
            }
            long now = System.nanoTime();
            if(now - longest.ts > maxTimeout){
                timeoutCounter.count();
                PriorityElement<T> cur = maxHeap.poll(longest.priority);
                assert longest == cur : "Improper queue";
                deque.remove(cur);
                T res = cur.elem;
                elementFactory.release(cur);
                queueSize--;
                return res;
            }
        }
        PriorityElement<T> cur = maxHeap.poll();
        if(cur == null) {
            return null;
        }
        if(hasMaxWait()) {
            deque.remove(cur);
        }
        T res = cur.elem;
        elementFactory.release(cur);
        queueSize--;
        return res;
    }

    /**
     * Returns an iterator over the elements in this queue. The iterator returns elements in the order of its priority.
     * The order of elements with the same priority is FIFO.
     *
     * @return an iterator over the elements in this queue
     */
    @Override
    public Iterator<T> iterator() {
        return new PriorityIterator();
    }

    /**
     * Returns an iterator over the elements in this queue.
     * The iterator returns elements in the order how these elements are inserted (FIFO order).
     *
     * @return an iterator over the elements in this queue
     * @throws UnsupportedOperationException if the policy is {@link  OrderingPolicy#NO_PRIORITY_POLICY}
     */
    // used for testing
    Iterator<T> insertionOrderIterator() {
        switch( policy) {
            case NO_PRIORITY_POLICY:
                return iterator();
            case MAX_WAIT_POLICY:
            case FREQUENCY_DEFINED_POLICY:
                return new OrderIterator();
            default : // USER_DEFINED_PRIORITY
                throw new UnsupportedOperationException();
        }
    }

    private class OrderIterator implements Iterator<T> {

        private PriorityElement<T> cur;

        OrderIterator() {
            cur = deque.head;
        }

        public boolean hasNext(){
            return cur.prev != deque.tail;
        }

        public T next(){
            if( cur.prev == deque.tail) {
                throw new NoSuchElementException();
            }
            cur = cur.prev;
            return cur.elem;
        }
    }

    private class PriorityIterator implements Iterator<T> {

        private final Iterator<T> iter;

        PriorityIterator() {
            iter = maxHeap.priorityIterator();
        }

        public boolean hasNext(){
            return iter.hasNext();
        }

        public T next(){
            return iter.next();
        }
    }

    public boolean hasTimeOutElement() {
        if(!hasMaxWait() || maxTimeout<=0) {
            return false;
        }

        PriorityElement<T> longest = deque.peek();

        if(longest == null) {
            return false;
        }
        return (System.nanoTime() - longest.ts > maxTimeout);
    }

    // A queue for elements with identical priority. It is used by MaxHeap to manage order between elements.
    private static class OrderedList<T> implements Prioritized{

        int priority;
        PriorityElement<T> head;
        PriorityElement<T> tail;
        long size;
        VoltTimerCount  priorityCounter;

        OrderedList(int priority){
            this.priority = priority;
            head = null;
            tail = null;
            size = 0;
            priorityCounter = VoltTimerCount.newVoltCounter(
                "AdaptivePriorityQueue.PRIORITY_"+priority, "QueueLatency", "queueDepth");
        }

        public void setPriority(int priority){
            this.priority = priority;
        }

        public int getPriority(){
            return this.priority;
        }

        public void setSequence(long seqNum) {
            throw new UnsupportedOperationException();
        }

        public long getSequence(){
            throw new UnsupportedOperationException();
        }

        void add(PriorityElement<T> el){
            if(head == null) {
                head = el;
            }
            else {
                tail.inorder = el;
            }
            tail = el;
            el.inorder = null;
            size++;
        }

        PriorityElement<T> poll() {
            if( head == null ) {
                return null;
            }
            PriorityElement<T> taken = head;
            head = head.inorder;
            if( head == null ) {
                assert (tail==taken && size==1) : "Failed to keep track of the last element";
                tail = null;
            }
            size--;
            return taken;
        }

        PriorityElement<T> peek() {
            return head;
        }

        boolean isEmpty(){
            return (head == null);
        }

        long size(){
            return size;
        }

        Iterator<T> iterator(){
            return new Iterator<T>() {
                PriorityElement<T> cur = head;

                public boolean hasNext(){
                    return (cur != null);
                }

                public T next(){
                    if(!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    T elem = cur.elem;
                    cur = cur.inorder;
                    return elem;
                }
           };
       }
    }

    // A deque to manage order of insertion of elements into AdaptivePriorityQueue
    private static class TimedDeque<T> {
        PriorityElement<T> head;
        PriorityElement<T> tail;

        TimedDeque(){
            head = new PriorityElement<>() ;
            tail = new PriorityElement<>();

            head.prev = tail;
            tail.next = head;
        }

        void add(PriorityElement<T> el){
            el.next = tail.next;
            el.next.prev = el;
            el.prev = tail;
            tail.next = el;
        }

        PriorityElement<T> take() {
            if( head.prev == tail) {
                return null;
            }
            PriorityElement<T> taken = head.prev;
            head.prev = taken.prev;
            taken.prev.next = head;
            taken.prev = null;
            taken.next = null;
            return taken;
        }

        void remove(PriorityElement<T> el) {
            if( el != tail && el != head ) {
                el.prev.next = el.next;
                el.next.prev = el.prev;
                el.next = null;
                el.prev = null;
            }
        }

        PriorityElement<T> peek() {
            return ( head.prev == tail) ? null : head.prev;
        }
    }

    public int comparePrio(OrderedList<T> left, OrderedList<T> right) {
        return left.priority < right.priority ? 1 : -1;
    }

    public int compareRank(OrderedList<T> left, OrderedList<T> right) {
        long leftFreq = (seqIdx - left.peek().seq)/(left.priority+1-MIN_PRIORITY_LEVEL);
        long rightFreq = (seqIdx - right.peek().seq)/(right.priority+1-MIN_PRIORITY_LEVEL);
        return Long.compare(leftFreq, rightFreq);
    }

    private class MaxHeap<E> {

        // this array contains a map between a set of priorities defined for the maxheap and queues containing requests for every priority.
        private int[]  heapLookup;
        // heap consist a set of queues containing requests with with a given priority.
        private OrderedList<E>[] heap;
        private int heapSize;

        private Comparator<OrderedList<E>> comparator;

        OrderedList<E> batchCache = new OrderedList<>(batchPriority);
        boolean cacheOpen = false;

        MaxHeap(Comparator<OrderedList<E>> comparator) {
            heapLookup = new int[PRIORITY_RANGE];
            Arrays.fill(heapLookup,-1);
            heapLookup[0] = 0;
            heap = (OrderedList<E>[]) java.lang.reflect.Array.newInstance(OrderedList.class,PRIORITY_RANGE );
            heap[0] = new OrderedList<>(MIN_PRIORITY_LEVEL);
            heapSize = 1;
            this.comparator = comparator;
        }

        void offer(PriorityElement<E> element) {
            if (!(element.priority >= MIN_PRIORITY_LEVEL && element.priority <= MAX_PRIORITY_LEVEL)) {
               throw new IllegalArgumentException("Wrong priority " + element.priority + " for the element");
            }

            if(policy == OrderingPolicy.NO_PRIORITY_POLICY) {
                heap[0].add(element);
                return;
            }

            if( enableBatching &&  element.priority == batchPriority ) {
                batchCache.add(element);
                if(!cacheOpen && batchCache.size >= batchSize) {
                    cacheOpen = true;
                }
            }
            else {
                int idx = heapLookup[element.priority - MIN_PRIORITY_LEVEL];
                if (idx == -1) {
                    idx = heapSize;
                    heap[heapSize++] = new OrderedList<>(element.priority);
                    heapLookup[element.priority - MIN_PRIORITY_LEVEL] = idx;
                }
                OrderedList<E> prioQueue = heap[idx];
                boolean isEmpty = prioQueue.isEmpty();

                if (policy == OrderingPolicy.FREQUENCY_DEFINED_POLICY) {
                    prioQueue.add(element);
                    moveDown(idx);
                    moveUp(idx);
                }
                // if you add something at the root, do not bother to check anything
                else if (idx == 0) {
                    prioQueue.add(element);
                }
                //only if the queue for the priority was empty before, move it up.
                else if (isEmpty) {
                    //check that the root is not empty and, if it is, move it down first
                    if (heap[0].isEmpty()) {
                        moveDown(0);
                    }
                    prioQueue.add(element);
                    moveUp(idx);
                }
                //otherwise just add an element
                else {
                    prioQueue.add(element);
                }
            }
        }

        PriorityElement<E> peek() {
            if( isEmpty() ) {
                return null;
            }
            else if( enableBatching &&
                    (cacheOpen || batchCache.size == queueSize) ) {
                return batchCache.peek();
            }
            // the root might get empty waiting for the next offer/poll
            else if ( heap[0].isEmpty() ) {
                moveDown(0);

            }
            return heap[0].peek();
        }

        PriorityElement<E> poll() {
            if( queueSize == 0 ) {
                return null;
            }
            else if( enableBatching && (cacheOpen || queueSize == batchCache.size) ) {
                return pollFromCache();
            }
            else {
                return pollAtIdx(0);
            }
        }

        PriorityElement<E> poll(int priority)  {
            if (!(priority >= MIN_PRIORITY_LEVEL && priority <= MAX_PRIORITY_LEVEL)) {
               throw new IllegalArgumentException("Wrong priority for the element");
            }
            else if( queueSize == 0 ) {
                return null;
            }
            else if( enableBatching && priority == batchPriority ) {
                return pollFromCache();
            }
            else if(heapLookup[priority-MIN_PRIORITY_LEVEL] == -1){
                return null;
            }
            else {
                return pollAtIdx(heapLookup[priority-MIN_PRIORITY_LEVEL]);
            }
        }

        PriorityElement<E> pollFromCache() {
            if( batchCache.size == 1 ) {
                cacheOpen = false;
            }
            PriorityElement<E> cur = batchCache.poll();
            if( cur != null && batchCache.priorityCounter.isEnabled() ) {
                batchCache.priorityCounter.count(System.nanoTime() - cur.ts,
                                                 batchCache.size() + 1);
            }
            return cur;
        }

        PriorityElement<E> pollAtIdx(int idx)  {
            assert (idx < heapSize) : "Trying to poll at a wrong index";
            OrderedList<E> prioQueue = heap[idx];
            // if idx != 0, the poll happens because of a time-out condition.
            // In this case, the List for that priority must not be empty
            assert idx == 0 || (idx > 0 && !prioQueue.isEmpty());
            // if current List for currennt priority's is empty, move it down and try the priority at the highest level.
            if( prioQueue.isEmpty() ) {
                moveDown(idx);
                prioQueue = heap[idx];
            }
            PriorityElement<E> cur = prioQueue.poll();
            if(cur != null) {  // There only time when cur == null is when maxHeap is empty.
                if( prioQueue.priorityCounter.isEnabled() ) {
                    prioQueue.priorityCounter.count(System.nanoTime() - cur.ts,
                                                        prioQueue.size()+1);
                }
                if( idx != 0 ||   // if idx != 0 - it is the time-out case.
                                  // otherwise do not move the root down immediately.
                                  // The root is moved down during the next poll().
                                  // It is an optimization for the case if "offer"
                                  // inserts something at the root level to avoid unnecessary up/down move.
                    policy == OrderingPolicy.FREQUENCY_DEFINED_POLICY) {
                    moveDown(idx);
                }
            }
            else{
                // since queueSize is not 0, cur must not be null
                assert false : "Wrong queue size";
            }
            return cur;
        }

        boolean moveDown( int idx ) {
            boolean moved = false;
            OrderedList<E> cur = heap[idx];
            while( true /* forever */){
                // get index of the left child
                int childIdx = 2*idx + 1;
                if( childIdx >= heapSize ) {
                    break;
                }
                OrderedList<E> child = heap[childIdx];
                //get index of the right child
                int rightChildIdx = childIdx + 1;
                //compare children
                if(rightChildIdx < heapSize) {
                    //left chidl is empty - use right child for comparison with current
                    if(child.isEmpty()) {
                        child = heap[rightChildIdx];
                        childIdx = rightChildIdx;
                    }
                    // if right child is not empty - compare children between themselves and use the largest one
                    // for comparison with the "cur" at the parent
                    else {
                        OrderedList<E> right = heap[rightChildIdx];
                        if(!right.isEmpty() &&
                            comparator.compare(right,child) >= 0 ) {
                            child = right;
                            childIdx = rightChildIdx;
                        }
                    }
                }
                // check current against the largest child
                // if parent is large than the child, break the cycle
                if( child.isEmpty() ||
                    ( !cur.isEmpty() &&
                       comparator.compare(cur,child) >= 0) ) {
                    break;
                }
                //move the largest child up into the parent location.
                moved = true;
                heap[idx] = child;
                heapLookup[child.priority - MIN_PRIORITY_LEVEL] = idx;
                idx = childIdx;

                // After child is move to "cur" location,
                // repeat the cycle by comparing old cur to the next pair of children
            }
            // put the node at the initial "root" into the place of the final larger child.
            if( moved ) {
                heap[idx] = cur;
                heapLookup[cur.priority - MIN_PRIORITY_LEVEL] = idx;
            }
            return moved;
        }

        boolean moveUp( int idx ){
            if( idx == 0 ) {
                return false;
            }
            boolean moved = false;
            int parentIdx;
            OrderedList<E> cur = heap[idx];
            do {
                // get parent idx in the heap
                parentIdx = (idx - 1) / 2;
                OrderedList<E> parent = heap[parentIdx];
                // if parent is not compare the current to its parent
                if (parent.peek() != null &&
                    comparator.compare(parent,cur) >= 0) {
                    break;
                }
                //move parent down to the idx
                moved = true;
                heap[idx] = parent;
                heapLookup[parent.priority - MIN_PRIORITY_LEVEL] = idx;
                idx = parentIdx;

                // Now repeat the cycle by comparing old node to the next parent in its path to the root
            } while( parentIdx != 0 );
            // move cur to the found parent location
            if(moved){
                heap[idx]=cur;
                heapLookup[cur.priority - MIN_PRIORITY_LEVEL] = idx;
            }
            return moved;
        }

        public Iterator<E> priorityIterator() {
            return new HeapPriorityIterator();
        }

        private class HeapPriorityIterator implements Iterator<E> {

            OrderedList<E>[] sortedHeap;
            int curIdx;
            Iterator<E> curIterator;


            HeapPriorityIterator() {
                sortedHeap = Arrays.copyOf(heap,heapSize);
                Arrays.sort(sortedHeap, (p,q) -> ((p.priority > q.priority) ? 1 : -1));
                curIdx = 0;
                curIterator = sortedHeap[curIdx].iterator();
            }

            public boolean hasNext(){
                if(curIterator.hasNext()){
                    return true;
                }
                while( ++curIdx < sortedHeap.length ){
                    if(!sortedHeap[curIdx].isEmpty()) {
                        curIterator = sortedHeap[curIdx].iterator();
                        return true;
                    }
                }
                return false;
            }

            public E next(){
                if(curIterator.hasNext()){
                    return curIterator.next();
                }
                while(++curIdx < sortedHeap.length ){
                    if(!sortedHeap[curIdx].isEmpty()) {
                        curIterator = sortedHeap[curIdx].iterator();
                        return curIterator.next();
                    }
                }
                throw new NoSuchElementException();
            }
        }

    }


    // can be accessed by tests
    static int poolInitialCapacity = 2000;
    static int poolIncrement = poolInitialCapacity/2;
    static boolean enablePool = true;

    private static class PriorityElement<T> {
        T elem;
        int priority=-1;
        long ts;
        long seq;
        PriorityElement<T> next;
        PriorityElement<T> prev;
        PriorityElement<T> inorder;

        private PriorityElement() {
        }

        private PriorityElement(T element, int priority, long ts, long idx) {
            this.elem = element;
            this.priority = priority;
            this.ts = ts;
            this.seq = idx;
        }

        void clear() {
            elem = null;
            priority = -1;
            ts = 0;
            seq = 0;
            next = null;
            prev = null;
            inorder = null;
        }

        /**
         * It is a trivial implementation of a pool to avoid repeated allocation and deallocation of objects of type
         * PriorityElement<T>. Though JVM GS should handle allocation of a large number of small object efficiently,
         * there is no need to do that here. This objects live only within the Queue as an envelope for a Record
         * to pass through the AdaptiveHeapQueue.
         * TODO this pool does not resize itself back if the Queue grows temporarily too large.
         * It is assumed that it can grown large again and there is no need to release those small holders.
         * We can improve it by creating an automated reduceSize() method to better handle such a use case.
         * @param <T>
         */
        static class ElementFactoryPool<T> {
            int currentSize;
            int capacity;
            PriorityElement<T>[] pool;

            public ElementFactoryPool() {
                currentSize = 0;
                pool = (PriorityElement<T>[]) java.lang.reflect.Array.newInstance(PriorityElement.class,poolInitialCapacity );
                capacity = poolInitialCapacity;
                incrementPool();
            }

            private void incrementPool() {
                for (int ii = 0; ii < poolIncrement; ii++) {
                    pool[currentSize++] = new PriorityElement<>();
                }
            }

            private void extendPool() {
                if( capacity < currentSize + poolIncrement ) {
                    capacity = currentSize + poolIncrement;
                    pool = Arrays.copyOf(pool,capacity);
                }
            }

            PriorityElement<T> newElement(T element, int priority, long ts, long idx) {
                if(!enablePool) {
                    return new PriorityElement<>(element,priority,ts,idx);

                }
                if (currentSize == 0) {
                    incrementPool();
                }
                PriorityElement<T> elem = pool[currentSize - 1];
                elem.elem = element;
                elem.priority = priority;
                elem.ts = ts;
                elem.seq = idx;
                pool[currentSize - 1] = null;
                currentSize--;
                return elem;
            }

            void release(PriorityElement<T> elem) {
                if(!enablePool) {
                    return;
                }
                if (currentSize == capacity ) {
                    extendPool();
                }
                elem.clear();
                pool[currentSize++] = elem;
            }

        }
    }

    // to be used by tests
    int getPoolSize() {
        return elementFactory.currentSize;
    }
    int getPoolCapacity() {
        return elementFactory.capacity;
    }
}
