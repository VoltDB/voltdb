/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.List;

import org.apache.zookeeper_voltpatches.data.Stat;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;

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
    private List<String> m_children = new ArrayList<String>();
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
    public synchronized List<String> lastSeenChildren()
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

    private BabySitter(ZooKeeper zk, String dir, Callback cb)
    {
        m_zk = zk;
        m_dir = dir;
        m_cb = cb;
        m_es = Executors.newSingleThreadExecutor(CoreUtils.getThreadFactory("Babysitter-" + dir));
    }

    /**
     * Create a new BabySitter and block on reading the initial children list.
     */
    public static Pair<BabySitter, List<String>> blockingFactory(ZooKeeper zk, String dir, Callback cb)
        throws InterruptedException, ExecutionException
    {
        BabySitter bs = new BabySitter(zk, dir, cb);
        Future<List<String>> task = bs.m_es.submit(bs.m_eventHandler);
        List<String> initialChildren = task.get();
        return new Pair<BabySitter, List<String>>(bs, initialChildren);
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
            m_es.submit(m_eventHandler);
        }
    };

    private List<String> watch() throws InterruptedException, KeeperException
    {
        Stat stat = new Stat();
        List<String> zkchildren = m_zk.getChildren(m_dir, m_watcher, stat);
        ArrayList<String> tmp = new ArrayList<String>(zkchildren.size());
        tmp.addAll(zkchildren);
        ZKUtil.sortSequentialNodes(tmp);
        m_children = Collections.unmodifiableList(tmp);
        return m_children;
    }
}
