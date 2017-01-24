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
package org.jctools_voltpatches.queues;

import static org.jctools_voltpatches.util.UnsafeAccess.UNSAFE;

import java.util.AbstractQueue;
import java.util.Iterator;

abstract class BaseLinkedQueuePad0<E> extends AbstractQueue<E>implements MessagePassingQueue<E> {
    long p00, p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16;
}

abstract class BaseLinkedQueueProducerNodeRef<E> extends BaseLinkedQueuePad0<E> {
    protected final static long P_NODE_OFFSET;

    static {
        try {
            P_NODE_OFFSET = UNSAFE
                    .objectFieldOffset(BaseLinkedQueueProducerNodeRef.class.getDeclaredField("producerNode"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    protected LinkedQueueNode<E> producerNode;

    protected final void spProducerNode(LinkedQueueNode<E> node) {
        producerNode = node;
    }

    @SuppressWarnings("unchecked")
    protected final LinkedQueueNode<E> lvProducerNode() {
        return (LinkedQueueNode<E>) UNSAFE.getObjectVolatile(this, P_NODE_OFFSET);
    }

    protected final LinkedQueueNode<E> lpProducerNode() {
        return producerNode;
    }
}

abstract class BaseLinkedQueuePad1<E> extends BaseLinkedQueueProducerNodeRef<E> {
    long p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16, p17;
}

abstract class BaseLinkedQueueConsumerNodeRef<E> extends BaseLinkedQueuePad1<E> {
    protected final static long C_NODE_OFFSET;

    static {
        try {
            C_NODE_OFFSET = UNSAFE
                    .objectFieldOffset(BaseLinkedQueueConsumerNodeRef.class.getDeclaredField("consumerNode"));
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    protected LinkedQueueNode<E> consumerNode;

    protected final void spConsumerNode(LinkedQueueNode<E> node) {
        consumerNode = node;
    }

    @SuppressWarnings("unchecked")
    protected final LinkedQueueNode<E> lvConsumerNode() {
        return (LinkedQueueNode<E>) UNSAFE.getObjectVolatile(this, C_NODE_OFFSET);
    }

    protected final LinkedQueueNode<E> lpConsumerNode() {
        return consumerNode;
    }
}

/**
 * A base data structure for concurrent linked queues. For convenience also pulled in common single consumer
 * methods since at this time there's no plan to implement MC.
 *
 * @author nitsanw
 *
 * @param <E>
 */
abstract class BaseLinkedQueue<E> extends BaseLinkedQueueConsumerNodeRef<E> {
    long p01, p02, p03, p04, p05, p06, p07;
    long p10, p11, p12, p13, p14, p15, p16, p17;

    @Override
    public final Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc} <br>
     * <p>
     * IMPLEMENTATION NOTES:<br>
     * This is an O(n) operation as we run through all the nodes and count them.<br>
     * The accuracy of the value returned by this method is subject to races with producer/consumer threads.
     *
     * @see java.util.Queue#size()
     */
    @Override
    public final int size() {
        // Read consumer first, this is important because if the producer is node is 'older' than the consumer
        // the consumer may overtake it (consume past it). This will lead to an infinite loop below.
        LinkedQueueNode<E> chaserNode = lvConsumerNode();
        final LinkedQueueNode<E> producerNode = lvProducerNode();
        int size = 0;
        // must chase the nodes all the way to the producer node, but there's no need to chase a moving
        // target.
        while (chaserNode != producerNode && chaserNode != null && size < Integer.MAX_VALUE) {
            LinkedQueueNode<E> next;
            next = chaserNode.lvNext();
            // check if this node has been consumed
            if (next == chaserNode) {
                next = lvConsumerNode();
            }
            chaserNode = next;
            size++;
        }
        return size;
    }

    /**
     * {@inheritDoc} <br>
     * <p>
     * IMPLEMENTATION NOTES:<br>
     * Queue is empty when producerNode is the same as consumerNode. An alternative implementation would be to
     * observe the producerNode.value is null, which also means an empty queue because only the
     * consumerNode.value is allowed to be null.
     *
     * @see MessagePassingQueue#isEmpty()
     */
    @Override
    public final boolean isEmpty() {
        return lvConsumerNode() == lvProducerNode();
    }

    @Override
    public int capacity() {
        return UNBOUNDED_CAPACITY;
    }

    protected E getSingleConsumerNodeValue(LinkedQueueNode<E> currConsumerNode, LinkedQueueNode<E> nextNode) {
        // we have to null out the value because we are going to hang on to the node
        final E nextValue = nextNode.getAndNullValue();

        // Fix up the next ref of currConsumerNode to prevent promoted nodes from keep new ones alive.
        // We use a reference to self instead of null because null is already a meaningful value (the next of
        // producer node is null).
        currConsumerNode.soNext(currConsumerNode);
        spConsumerNode(nextNode);
        // currConsumerNode is now no longer referenced and can be collected
        return nextValue;
    }

    @Override
    public E relaxedPoll() {
        LinkedQueueNode<E> currConsumerNode = lpConsumerNode(); // don't load twice, it's alright
        LinkedQueueNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            return getSingleConsumerNodeValue(currConsumerNode, nextNode);
        }
        return null;
    }

    @Override
    public int drain(Consumer<E> c) {
        long result = 0;// use long to force safepoint into loop below
        int drained;
        do {
            drained = drain(c, 4096);
            result += drained;
        } while (drained == 4096 && result <= Integer.MAX_VALUE - 4096);
        return (int) result;
    }

    @Override
    public int drain(Consumer<E> c, int limit) {
        LinkedQueueNode<E> chaserNode = this.consumerNode;
        for (int i = 0; i < limit; i++) {
            final LinkedQueueNode<E> nextNode = chaserNode.lvNext();

            if (nextNode == null) {
                return i;
            }
            // we have to null out the value because we are going to hang on to the node
            final E nextValue = getSingleConsumerNodeValue(chaserNode, nextNode);
            chaserNode = nextNode;
            c.accept(nextValue);
        }
        return limit;
    }

    @Override
    public void drain(Consumer<E> c, WaitStrategy wait, ExitCondition exit) {
        LinkedQueueNode<E> chaserNode = this.consumerNode;
        int idleCounter = 0;
        while (exit.keepRunning()) {
            for (int i = 0; i < 4096; i++) {
                final LinkedQueueNode<E> nextNode = chaserNode.lvNext();
                if (nextNode == null) {
                    idleCounter = wait.idle(idleCounter);
                    continue;
                }

                idleCounter = 0;
                // we have to null out the value because we are going to hang on to the node
                final E nextValue = getSingleConsumerNodeValue(chaserNode, nextNode);
                chaserNode = nextNode;
                c.accept(nextValue);
            }
        }
    }

    @Override
    public E relaxedPeek() {
        LinkedQueueNode<E> currConsumerNode = consumerNode; // don't load twice, it's alright
        LinkedQueueNode<E> nextNode = currConsumerNode.lvNext();
        if (nextNode != null) {
            return nextNode.lpValue();
        }
        return null;
    }

    @Override
    public boolean relaxedOffer(E e) {
        return offer(e);
    }

}
