/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jctools_voltpatches.queues.atomic;

/**
 * This is a direct Java port of the MPSC algorithm as presented <a
 * href="http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue"> on 1024
 * Cores</a> by D. Vyukov. The original has been adapted to Java and it's quirks with regards to memory model and
 * layout:
 * <ol>
 * <li>Use XCHG functionality provided by AtomicReference (which is better in JDK 8+).
 * </ol>
 * The queue is initialized with a stub node which is set to both the producer and consumer node references. From this
 * point follow the notes on offer/poll.
 *
 * @author nitsanw
 *
 * @param <E>
 */
public final class MpscLinkedAtomicQueue<E> extends BaseLinkedAtomicQueue<E> {

    public MpscLinkedAtomicQueue() {
        super();
        LinkedQueueAtomicNode<E> node = new LinkedQueueAtomicNode<E>();
        spConsumerNode(node);
        xchgProducerNode(node);// this ensures correct construction: StoreLoad
    }
    /**
     * {@inheritDoc} <br>
     * <p>
     * IMPLEMENTATION NOTES:<br>
     * Offer is allowed from multiple threads.<br>
     * Offer allocates a new node and:
     * <ol>
     * <li>Swaps it atomically with current producer node (only one producer 'wins')
     * <li>Sets the new node as the node following from the swapped producer node
     * </ol>
     * This works because each producer is guaranteed to 'plant' a new node and link the old node. No 2 producers can
     * get the same producer node as part of XCHG guarantee.
     *
     * @see org.jctools_voltpatches.queues.MessagePassingQueue#offer(Object)
     * @see java.util.Queue#offer(java.lang.Object)
     */
    @Override
    public final boolean offer(final E e) {
        if (null == e) {
            throw new NullPointerException();
        }
        final LinkedQueueAtomicNode<E> nextNode = new LinkedQueueAtomicNode<E>(e);
        final LinkedQueueAtomicNode<E> prevProducerNode = xchgProducerNode(nextNode);
        // Should a producer thread get interrupted here the chain WILL be broken until that thread is resumed
        // and completes the store in prev.next.
        prevProducerNode.soNext(nextNode); // StoreStore
        return true;
    }

    /**
     * {@inheritDoc} <br>
     * <p>
     * IMPLEMENTATION NOTES:<br>
     * Poll is allowed from a SINGLE thread.<br>
     * Poll reads the next node from the consumerNode and:
     * <ol>
     * <li>If it is null, the queue is assumed empty (though it might not be).
     * <li>If it is not null set it as the consumer node and return it's now evacuated value.
     * </ol>
     * This means the consumerNode.value is always null, which is also the starting point for the queue. Because null
     * values are not allowed to be offered this is the only node with it's value set to null at any one time.
     *
     * @see org.jctools_voltpatches.queues.MessagePassingQueue#poll()
     * @see java.util.Queue#poll()
     */
    @Override
    public final E poll() {
        LinkedQueueAtomicNode<E> currConsumerNode = lpConsumerNode(); // don't load twice, it's alright
        LinkedQueueAtomicNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            return getSingleConsumerNodeValue(currConsumerNode, nextNode);
        }
        else if (currConsumerNode != lvProducerNode()) {
            // spin, we are no longer wait free
            while((nextNode = currConsumerNode.lvNext()) == null);
            // got the next node...

            return getSingleConsumerNodeValue(currConsumerNode, nextNode);
        }
        return null;
    }

    @Override
    public final E peek() {
        LinkedQueueAtomicNode<E> currConsumerNode = lpConsumerNode(); // don't load twice, it's alright
        LinkedQueueAtomicNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            return nextNode.lpValue();
        }
        else if (currConsumerNode != lvProducerNode()) {
            // spin, we are no longer wait free
            while((nextNode = currConsumerNode.lvNext()) == null);
            // got the next node...
            return nextNode.lpValue();
        }
        return null;
    }

}
