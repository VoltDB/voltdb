/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltcore.zk;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google_voltpatches.common.collect.ImmutableList;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;

/**
 * BabySitter watches a zookeeper node and alerts on appearances
 * and disappearances of direct children.
 *
 * Note: if you are using this to watch a LeaderElector chain,
 * be aware that the BabySitter notices race with notices of
 * becoming the Leader. That is, a non-leader's babysitter can
 * change before the non-leader knows it is the new leader.
 *
 * It is easier to use as the Leader -- in this case the leader
 * can observe appearances and disappearances of its non-leader
 * peers.
 */
public class BabySitter
{
    private final String m_dir; // the directory to monitor
    private final Callback m_cb; // the callback when children change
    private final ZooKeeper m_zk;
    private final ExecutorService m_es;
    private volatile List<String> m_children = ImmutableList.of();
    private AtomicBoolean m_shutdown = new AtomicBoolean(false);

    /**
     * Callback is passed an immutable child list when a child changes.
     * Callback runs in the BabySitter's ES (not the zk trigger thread).
     */
    public abstract static class Callback
    {
        abstract public void run(List<String> children);
    }

    /** lastSeenChildren returns the last recorded list of children */
    public List<String> lastSeenChildren()
    {
        if (m_shutdown.get()) {
            throw new RuntimeException("Requested children from shutdown babysitter.");
        }
        return m_children;
    }

    /**
     * shutdown silences the babysitter and causes watches to not reset.
     * Note that shutting down will churn ephemeral ZK nodes - shutdown
     * allows the programmer to not set watches on nodes from terminated session.
     */
    synchronized public void shutdown()
    {
        m_shutdown.set(true);
    }

    private BabySitter(ZooKeeper zk, String dir, Callback cb, ExecutorService es)
    {
        m_zk = zk;
        m_dir = dir;
        m_cb = cb;
        m_es = es;
    }

    /**
     * Create a new BabySitter and block on reading the initial children list.
     */
    public static Pair<BabySitter, List<String>> blockingFactory(ZooKeeper zk, String dir, Callback cb)
        throws InterruptedException, ExecutionException
    {
        ExecutorService es = CoreUtils.getCachedSingleThreadExecutor("Babysitter-" + dir, 15000);
        return blockingFactory(zk, dir, cb, es);
    }

    /**
     * Create a new BabySitter and block on reading the initial children list.
     * Use the provided ExecutorService to queue events to, rather than
     * creating a private ExecutorService. The initial set of children will be retrieved
     * in the current thread and not the ExecutorService because it is assumed
     * this is being called from the ExecutorService
     */
    public static Pair<BabySitter, List<String>> blockingFactory(ZooKeeper zk, String dir, Callback cb,
            ExecutorService es)
        throws InterruptedException, ExecutionException
    {
        BabySitter bs = new BabySitter(zk, dir, cb, es);
        List<String> initialChildren;
        try {
            initialChildren = bs.m_eventHandler.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
        return new Pair<BabySitter, List<String>>(bs, initialChildren);
    }

    /**
     * Create a new BabySitter and make sure it reads the initial children list.
     * Use the provided ExecutorService to queue events to, rather than
     * creating a private ExecutorService.
     */
    public static BabySitter nonblockingFactory(ZooKeeper zk, String dir,
                                                Callback cb, ExecutorService es)
        throws InterruptedException, ExecutionException
    {
        BabySitter bs = new BabySitter(zk, dir, cb, es);
        bs.m_es.submit(bs.m_eventHandler);
        return bs;
    }

    // eventHandler fetches the new children and resets the watch.
    // It is always run in m_es (the ExecutorService).
    private final Callable<List<String>> m_eventHandler = new Callable<List<String>>() {
        @Override
        public List<String> call() throws InterruptedException, KeeperException
        {
            synchronized(BabySitter.this) {
                // ignore post-shutdown events. don't reset watch.
                if (m_shutdown.get() == true) {
                    return null;
                }

                List<String> newChildren = watch();
                if (m_cb != null) {
                    m_cb.run(newChildren);
                }
                return newChildren;
            }
        }
    };

    private final Watcher m_watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            try {
                m_es.submit(m_eventHandler);
            } catch (RejectedExecutionException e) {
                if (m_shutdown.get()) return;
                VoltDB.crashLocalVoltDB("Unexpected rejected execution exception", true, e);
            }
        }
    };

    private List<String> watch() throws InterruptedException, KeeperException
    {
        Stat stat = new Stat();
        List<String> zkchildren = m_zk.getChildren(m_dir, m_watcher, stat);
        // Sort on the ephemeral sequential part, the prefix is not padded, so string sort doesn't work
        Collections.sort(zkchildren, new Comparator<String>() {
            @Override
            public int compare(String left, String right)
            {
                return CoreZK.getSuffixFromChildName(left).compareTo(CoreZK.getSuffixFromChildName(right));
            }
        });
        m_children = ImmutableList.copyOf(zkchildren);
        return m_children;
    }
}
