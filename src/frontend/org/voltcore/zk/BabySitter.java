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
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.List;

import org.apache.zookeeper_voltpatches.data.Stat;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.voltcore.agreement.ZKUtil;
import org.voltcore.utils.CoreUtils;

/**
 * BabySitter watches a zookeeper node and alerts on appearances
 * and disappearances of direct children.
 */
public class BabySitter
{
    private final String dir; // the directory to monitor
    private final Callback cb; // the callback when children change

    private final ZooKeeper zk;
    private final ExecutorService es;
    private final Object lock = new Object();
    private List<String> children = null;
    private AtomicBoolean shutdown = new AtomicBoolean(false);

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
        if (shutdown.get()) {
            throw new RuntimeException("Requested children from shutdown babysitter.");
        }
        return children;
    }

    /**
     * shutdown silences the babysitter and causes watches to not reset.
     * Note that shutting down will churn ephemeral ZK nodes - shutdown
     * allows the programmer to not set watches on nodes from terminated session.
     */
    public void shutdown()
    {
        shutdown.set(true);
    }

    /**
     * Create a new BabySitter and fetch the current child list.
     * This ctor performs a blocking ZK transaction.
     */
    public BabySitter(ZooKeeper zk, String dir, Callback cb)
    {
        this.zk = zk;
        this.dir = dir;
        this.cb = cb;
        this.es = Executors.newSingleThreadExecutor(CoreUtils.getThreadFactory("Babysitter-" + dir));
        try {
            watch();
        } catch (KeeperException e) {
            org.voltdb.VoltDB.crashLocalVoltDB("Failed to start babysitter watch", true, e);
        } catch (InterruptedException e) {
            org.voltdb.VoltDB.crashLocalVoltDB("Interrupted while starting babysitter watch", true, e);
        }
    }

    // eventHandler fetches the new children and resets the watch.
    // It is always run in this.es.
    private final Runnable eventHandler = new Runnable() {
        @Override
        public void run()
        {
            if (shutdown.get() == true) {
                // silently ignore churn in ZK after shutdown.
                // Don't reset a watch.
                return;
            }

            try {
                watch();
                if (cb != null) {
                    cb.run(children);
                }
            } catch (KeeperException e) {
                org.voltdb.VoltDB.crashLocalVoltDB("Failed to reset babysitter watch", true, e);
            } catch (InterruptedException e) {
                org.voltdb.VoltDB.crashLocalVoltDB("Interrupted resetting babysitter watch", true, e);
            }
        }
    };

    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            es.submit(eventHandler);
        }
    };

    private void watch() throws InterruptedException, KeeperException
    {
        if (shutdown.get() == false) {
            Stat stat = new Stat();
            List<String> zkchildren = zk.getChildren(dir, watcher, stat);

            synchronized (lock) {
                ArrayList<String> tmp = new ArrayList<String>(zkchildren.size());
                tmp.addAll(zkchildren);
                ZKUtil.sortSequentialNodes(tmp);
                children = Collections.unmodifiableList(tmp);
            }
        }
    }

}
