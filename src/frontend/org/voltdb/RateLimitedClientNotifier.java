/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.cache.Cache;
import com.google_voltpatches.common.cache.CacheBuilder;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.RateLimiter;

/**
 * A helper class for gradually emitting notifications to client connections as well as coalescing updates
 * to individual connections by deduplicating suppliers that have already been queued.
 *
 * It is required that a supplier instance for a given type of event will be equal by identity as that
 * will be used to identify when the same event is signaled multiple times. Internally the supplier
 * is free to return the most recent value for that type of event (such as cluster topology)
 *
 * With a few adapters this could be a generic event coalescing and rate limiting mechanism, but I
 * don't see a reason to over engineer it until we actually have a second use case
 *
 */
public class RateLimitedClientNotifier {
    private final ListeningExecutorService m_es =
            CoreUtils.getCachedSingleThreadExecutor("RateLimitedClientNotifier", 60 * 1000);

    private final ConcurrentMap<Connection, Object> m_clientsPendingNotification
                                = new ConcurrentHashMap<Connection, Object>(2048, .75f, 128);
    private final LinkedBlockingQueue<Runnable> m_submissionQueue = new LinkedBlockingQueue<Runnable>();

    static double NOTIFICATION_RATE = Long.getLong("CLIENT_NOTIFICATION_RATE", 1000).doubleValue();
    static long WARMUP_MS = Long.getLong("CLIENT_NOTIFICATION_WARMUP_MS", 5000);
    private RateLimiter m_limiter;

    //Cache nodes use to build linked lists of notifications
    //This avoids having to create and promote large numbers of objects and then GC them later
    static final Cache<Node, Node> m_cachedNodes =
                                            CacheBuilder.newBuilder()
                                                    .maximumSize(10000).concurrencyLevel(1).build();

    /*
     * Linked list node saves allocating a dedicated list object and allows for object pooling
     */
    public static class Node implements Callable<Node> {
        private final Supplier<DeferredSerialization> notification;
        private final Node next;

        public Node(Supplier<DeferredSerialization> notification, Node next) {
            Preconditions.checkNotNull(notification);
            this.notification = notification;
            this.next = next;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (o == null) return false;
            if (!(o instanceof Node)) return false;
            Node other = (Node)o;
            if (other.notification == notification) {
                if (other.next == next) return true;
                if (other.next != null && next != null) {
                    return next.equals(other.next);
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            if (next == null) return notification.hashCode();
            final int prime = 31;
            int result = 1;
            Node head = this;
            do {
                result = result * prime + head.notification.hashCode();
            } while ((head = head.next) != null);
            return result;
        }

        @Override
        public Node call() {
            return this;
        }
    }

    private final Runnable m_loop = new Runnable() {
        @Override
        public void run() {
            try {
                RateLimitedClientNotifier.this.run();
            } catch (Throwable t) {
                VoltDB.crashLocalVoltDB("Unexpected exception in client notifier", true, t);
            }
        }
    };

    private Iterator<Map.Entry<Connection, Object>> m_iter;

    private void run() throws Exception {
        while (true) {
            if (m_es.isShutdown()) return;
            if (m_clientsPendingNotification.isEmpty()) {
                //Block until submissions create further work
                runSubmissions(true);
                //Create a fresh limiter each time so there isn't a burst of permits
                m_limiter = RateLimiter.create(NOTIFICATION_RATE, WARMUP_MS, TimeUnit.MILLISECONDS);
            } else {
                //Non-blocking poll for changes
                runSubmissions(false);
            }

            //Regenerate the iterator every time we are done sweeping the map
            if (m_iter == null) m_iter = m_clientsPendingNotification.entrySet().iterator();

            if (m_iter.hasNext()) {
                //The limiter has no dependencies so this will never pause for long waiting to acquire
                m_limiter.acquire();
                //Poll and remove, there are not other threads modifying the map concurrently
                final Map.Entry<Connection, Object> entry = m_iter.next();
                m_iter.remove();
                dispatchNotifications(entry.getKey(), entry.getValue());
            } else {
                m_iter = null;
            }
        }
    }

    private void dispatchNotifications(Connection key, Object value) {
        //This is a scalar notification
        if (value instanceof Supplier) {
            @SuppressWarnings("unchecked")
            final Supplier<DeferredSerialization> s = (Supplier<DeferredSerialization>)value;
            key.writeStream().enqueue(s.get());
        } else {
            //Notification is a linked list containing multiple events
            Node head = (Node)value;
            do {
                key.writeStream().enqueue(head.notification.get());
            } while ((head = head.next) != null);
        }

    }

    //Check for tasks that generate new notifications, optionally blocking
    //if there is no other work to do
    private void runSubmissions(boolean block) throws InterruptedException {
        if (block) {
            Runnable r = m_submissionQueue.take();
            do {
                r.run();
            } while ((r = m_submissionQueue.poll()) != null);
        } else {
            Runnable r = null;
            while ((r = m_submissionQueue.poll()) != null) {
                r.run();
            }
        }
    }

    //Start here instead of in constructor to avoid leaking this
    public void start() {
        m_es.execute(m_loop);
    }

    //Queue a notification to a collection of connections
    //The collection will be filtered to exclude non VoltPort connections
    public void queueNotification(
            final Collection<ClientInterfaceHandleManager> connections,
            final Supplier<DeferredSerialization> notification,
            final Predicate<ClientInterfaceHandleManager> wantsNotificationPredicate) {
        m_submissionQueue.offer(new Runnable() {
            @Override
            public void run() {
                for (ClientInterfaceHandleManager cihm : connections) {
                    if (!wantsNotificationPredicate.apply(cihm)) continue;
                    final Connection c = cihm.connection;

                    /*
                     * To avoid extra allocations and promotion we initially store a single event
                     * as just the event. Once we have two or more events we create a linked list
                     * and walk the list to dedupe events by identity
                     */
                    Object pendingNotifications = m_clientsPendingNotification.get(c);
                    try {
                        if (pendingNotifications == null) {
                            m_clientsPendingNotification.put(c, notification);
                        } else if (pendingNotifications instanceof Supplier) {
                            //Identity duplicate check
                            if (pendingNotifications == notification) return;
                            //Convert to a two node linked list
                            @SuppressWarnings("unchecked")
                            Node n1 = new Node((Supplier<DeferredSerialization>)pendingNotifications, null);
                            n1 = m_cachedNodes.get(n1, n1);
                            Node n2 = new Node(notification, n1);
                            n2 = m_cachedNodes.get(n2, n2);
                            m_clientsPendingNotification.put(c,  n2);
                        } else {
                            //Walk the list and check if the notification is a duplicate
                            Node head = (Node)pendingNotifications;
                            boolean dup = false;
                            while (head != null) {
                                if (head.notification == notification) {
                                    dup = true;
                                    break;
                                }
                                head = head.next;
                            }
                            //If it's a dupe, no new work
                            if (dup) continue;
                            //Otherwise replace the head of the list which is the value in the map
                            Node replacement = new Node(notification, (Node)pendingNotifications);
                            replacement = m_cachedNodes.get(replacement, replacement);
                            m_clientsPendingNotification.put(c, replacement);
                        }
                    } catch (ExecutionException e) {
                        VoltDB.crashLocalVoltDB(
                                "Unexpected exception pushing client notifications",
                                true,
                                Throwables.getRootCause(e));
                    }
                }
            }
        });
    }

    public void removeConnection(Connection c) {
        /*
         * It's a concurrent map so this is safe
         * If there is a race with the notifier thread it will be fine
         * Failing to remove it completely will cause the notification to be delivered
         * to the network and dropped. Technically we don't even have to remove it
         * we could just let the notification thread eventually remove it.
         */
        m_clientsPendingNotification.remove(c);
    }

    public void shutdown() throws InterruptedException {
        m_es.shutdown();
        m_submissionQueue.add(new Runnable() {
            @Override
            public void run() {
                return;
            }
        });
        m_es.awaitTermination(356, TimeUnit.DAYS);
    }
}
