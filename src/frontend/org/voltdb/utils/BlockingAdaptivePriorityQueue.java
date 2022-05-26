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

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Synchronized blocking queue based on the implementation of {@link AdaptivePriorityQueue}
 *
 * All producer-side methods for this queue are synchronized but not blocking.
 *
 * Note that this implementation does not generate synchronized iterator.
 * If a queue is modified from one thread, corresponding iterator does not catch this modification and might fail catastrophically
 *
 * @see AdaptivePriorityQueue
 *
 *
 * @param <T>
 */

public class BlockingAdaptivePriorityQueue<T extends Prioritized> extends AbstractQueue<T> implements BlockingQueue<T>  {

    final private ReentrantLock lock;

    /**
     * Condition for blocking when empty
     */
    final private Condition notEmpty;


    /**
     * A plain AdaptivePriorityQueue
     */
    final private AdaptivePriorityQueue<T> queue;

    final private AdaptivePriorityQueue.OrderingPolicy policy;

    /**
     * Creates an instance of the BlockingAdaptivePriorityQueue with the specified policy
     *
     * @param policy for the Queue
     */
    public BlockingAdaptivePriorityQueue(AdaptivePriorityQueue.OrderingPolicy policy) {
        this(policy, AdaptivePriorityQueue.MAX_DEFAULT_TIMEOUT_MILLIS,
             AdaptivePriorityQueue.MIN_PRIORITY_LEVEL-1, 0);
    }

    /**
     * Creates an instance of the AdaptivePriorityQueue with the specified policy and the timeout period
     *
     * @param policy for the Queue
     * @param timeout(milliseconds) timeout set for the elements of the queue
     */
    public BlockingAdaptivePriorityQueue(AdaptivePriorityQueue.OrderingPolicy policy, int timeout) {
        this(policy, timeout, AdaptivePriorityQueue.MIN_PRIORITY_LEVEL-1, 0);
    }

    /**
     * Creates an instance of the AdaptivePriorityQueue with the specified policy and the timeout period
     *
     * @param policy for the Queue
     * @param timeout(milliseconds) timeout set for the elements of the queue
     * @param batchPriority priority that should be processed in the batch-like manner.
     *                      Any illegal value beyond the range MIN_PRIORITY_LEVEL - MAX_PRIORITY_LEVEL disables batch processing
     * @param batchSize the size of the batch for batch-wise processing. The value <= 0 disables batching processing.
     */
    public BlockingAdaptivePriorityQueue(AdaptivePriorityQueue.OrderingPolicy policy, int timeout,
                                         int batchPriority, int batchSize) {
        this.policy = policy;
        this.queue = new AdaptivePriorityQueue<>(policy, timeout, batchPriority, batchSize);
        this.lock = new ReentrantLock();
        this.notEmpty = lock.newCondition();
    }

    /**
     * @return policy of the queue
     */
    public AdaptivePriorityQueue.OrderingPolicy getPolicy() {
        return this.policy;
    }


    /**
     * Get the value for the timeout for elements in this queue
     *
     * @return timeout for elements in this queue
     */
    public long getTimeout(){
        lock.lock();
        try {
            return queue.getTimeout();
        }
        finally{
            lock.unlock();
        }
    }

    @Override
    public int size(){
        lock.lock();
        try {
            return queue.size();
        }
        finally{
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return queue.isEmpty();
        }
        finally{
            lock.unlock();
        }
    }

    /**
     * A synchronized method consistent with {@link AdaptivePriorityQueue#add(T)}
     * @see AdaptivePriorityQueue#add(T)
     *
     * @param t  element to be added
     * @return {@code true} (as specified by {@link Collection#add})
     * @throws NullPointerException if the specified element is null
     * @throws AssertionError if the implied priority in the element of type {@link Prioritized} is outside of the range
     *           {@link AdaptivePriorityQueue#MIN_PRIORITY_LEVEL}-{@link AdaptivePriorityQueue#MAX_PRIORITY_LEVEL}
     */
    @Override
    public boolean add(T t) {
        return offer(t);
    }

    /**
     * A synchronized method consistent with {@link AdaptivePriorityQueue#offer(T)}
     * @see AdaptivePriorityQueue#offer(T)
     *
     * @param t  element to be added
     * @return {@code true} (as specified by {@link Queue#offer(Object)}  )
     * @throws NullPointerException if the specified element is null
     * @throws AssertionError if the implied priority in the element of type {@link Prioritized} is outside of the range
     *                 {@link AdaptivePriorityQueue#MIN_PRIORITY_LEVEL}-{@link AdaptivePriorityQueue#MAX_PRIORITY_LEVEL}
     */
    @Override
    public boolean offer(T t) {
        if (t == null)
            throw new NullPointerException();
        boolean status;
        lock.lock();
        try {
            status = queue.offer(t);
            notEmpty.signalAll();
        }
        finally{
            lock.unlock();
        }
        return status;
    }

    /**
     * default implementation of the timed offer method.
     * It is still non-blocking and this method can not time-out
     *
     * {@link #offer(T)}
     *
     * @param t  element to be added
     * @param timeout  ignored parameter
     * @param unit     ignored parameter
     * @return {@code true} (as specified by {@link Collection#add})
     * @throws NullPointerException if the specified element is null
     * @throws AssertionError if the implied priority in the element of type {@link Prioritized} is outside of the range
     *           {@link AdaptivePriorityQueue#MIN_PRIORITY_LEVEL}-{@link AdaptivePriorityQueue#MAX_PRIORITY_LEVEL}
     * @throws InterruptedException
     */
    @Override
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException{
        return offer(t);
    }

    @Override
    public void put(T t) throws InterruptedException {
       offer(t);
    }

    /**
     * A synchronized method consistent with {@link AdaptivePriorityQueue#element()}
     * @see AdaptivePriorityQueue#element()
     *
     * @return the head of this queue
     */
    @Override
    public T element(){
        lock.lock();
        try {
            return queue.element();
        }
        finally{
            lock.unlock();
        }
    }

    /**
     * A synchronized method consistent with {@link AdaptivePriorityQueue#peek()}
     * @see AdaptivePriorityQueue#peek()
     *
     * @return the head of this queue or null if queue is empty
     */
    @Override
    public T peek() {
        lock.lock();
        try {
            return queue.peek();
        }
        finally{
            lock.unlock();
        }
    }

    /**
     * A synchronized method consistent with {@link AdaptivePriorityQueue#remove()}
     * @see AdaptivePriorityQueue#peek()
     *
     * @return the head of this queue
     * @throws NoSuchElementException – if this queue is empty
     */
    @Override
    public T remove() {
        lock.lock();
        try {
            return queue.remove();
        }
        finally{
            lock.unlock();
        }
    }

    /**
     * A synchronized method consistent with {@link AdaptivePriorityQueue#poll()}
     * @see AdaptivePriorityQueue#poll()
     *
     * @return the head of this queue or null if the queue is empty
     */
    @Override
    public T poll() {
        lock.lock();
        try {
            return queue.poll();
        }
        finally{
            lock.unlock();
        }
    }

    /**
     * A synchronized method consistent with {@link AdaptivePriorityQueue#poll()}
     * This is a blocking call which set the thread asleep while the queue is empty
     * @see AdaptivePriorityQueue#poll()
     *
     * @param timeout how long to wait before giving up, in units of unit
     * @param unit a TimeUnit determining how to interpret the timeout parameter
     * @return the head of this queue
     * @throws InterruptedException
     */
    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException{
        long nanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        T result;
        try {
            while ( (result = queue.poll()) == null && nanos > 0)
                nanos = notEmpty.awaitNanos(nanos);
        } finally {
            lock.unlock();
        }
        return result;
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary until an element becomes available.
     *
     * @return the head of this queue
     * @throws InterruptedException
     */
    @Override
    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        T result;
        try {
            while ( (result = queue.poll()) == null)
                notEmpty.await();
        } finally {
            lock.unlock();
        }
        return result;
    }

    /**
     * A synchronized method consistent with {@link AdaptivePriorityQueue#iterator()}
     * @see AdaptivePriorityQueue#iterator()
     * Note that this iterator is not synchronized and does not assume concurrent modification of the underlying queue
     *
     * @return the head of this queue or null if the queue is empty
     */
    @Override
    public Iterator<T> iterator() {
        lock.lock();
        try {
            return queue.iterator();
        }
        finally{
            lock.unlock();
        }
    }

    /**
     * A synchronized method consistent with {@link AdaptivePriorityQueue#insertionOrderIterator()}
     * @see AdaptivePriorityQueue#insertionOrderIterator()
     * Note that this iterator is not synchronized and does not assume concurrent modification of the underlying queue
     *
     * @return the head of this queue or null if the queue is empty
     */
    Iterator<T> insertionOrderIterator() {
        lock.lock();
        try {
            return queue.insertionOrderIterator();
        }
        finally{
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super T> c, int maxElements) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        if (maxElements <= 0)
            return 0;
        lock.lock();
        try {
            int n = Math.min(queue.size(), maxElements);
            for (int i = 0; i < n; i++) {
                c.add(queue.poll());
            }
            return n;
        } finally {
            lock.unlock();
        }
    }
}