/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.apache.zookeeper_voltpatches.*;
import org.apache.zookeeper_voltpatches.KeeperException.NoNodeException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.voltdb.logging.VoltLogger;

public class SnapshotCompletionMonitor {
    private static final VoltLogger LOG = new VoltLogger("LOGGING");
    final LinkedList<SnapshotCompletionInterest> m_interests = new LinkedList<SnapshotCompletionInterest>();
    private ZooKeeper m_zk;
    private final ExecutorService m_es = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    return new Thread(r, "SnapshotCompletionMonitor");
                }
            },
            new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());

    private final Watcher m_newSnapshotWatcher = new Watcher() {
        @Override
        public void process(final WatchedEvent event) {
            switch (event.getType()) {
            case NodeChildrenChanged:
                m_es.execute(new Runnable() {
                    @Override
                    public void run() {
                        processSnapshotChildrenChanged(event);
                    }
                });
            default:
                break;
            }
        }
    };

    private TreeSet<String> m_lastKnownSnapshots = new TreeSet<String>();

    private void processSnapshotChildrenChanged(final WatchedEvent event) {
        try {
            TreeSet<String> children = new TreeSet<String>(m_zk.getChildren(
                    "/completed_snapshots", m_newSnapshotWatcher));
            TreeSet<String> newChildren = new TreeSet<String>(children);
            newChildren.removeAll(m_lastKnownSnapshots);
            m_lastKnownSnapshots = children;
            for (String newSnapshot : newChildren) {
                String path = "/completed_snapshots/" + newSnapshot;
                try {
                    byte data[] = m_zk.getData(path, new Watcher() {
                        @Override
                        public void process(final WatchedEvent event) {
                            switch (event.getType()) {
                            case NodeDataChanged:
                                m_es.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        processSnapshotDataChangedEvent(event);
                                    }
                                });
                                break;
                            default:
                                break;
                            }
                        }

                    }, null);
                    processSnapshotData(data);
                } catch (NoNodeException e) {
                }
            }
        } catch (Exception e) {
            LOG.fatal("Exception in snapshot completion monitor", e);
            VoltDB.crashVoltDB();
        }
    }

    private void processSnapshotDataChangedEvent(final WatchedEvent event) {
        try {
            byte data[] = m_zk.getData(event.getPath(), new Watcher() {
                @Override
                public void process(final WatchedEvent event) {
                    switch (event.getType()) {
                    case NodeDataChanged:
                        m_es.execute(new Runnable() {
                            @Override
                            public void run() {
                                processSnapshotDataChangedEvent(event);
                            }
                        });
                        break;
                    default:
                        break;
                    }
                }

            }, null);
            processSnapshotData(data);
        } catch (NoNodeException e) {
        } catch (Exception e) {
            LOG.fatal("Exception in snapshot completion monitor", e);
            VoltDB.crashVoltDB();
        }
    }

    private void processSnapshotData(byte data[]) {
        if (data == null) {
            return;
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        long txnId = buf.getLong();
        int totalNodes = buf.getInt();
        int totalNodesFinished = buf.getInt();
        boolean truncation = buf.get() == 1;

        if (totalNodes == totalNodesFinished) {
            for (SnapshotCompletionInterest interest : m_interests) {
                interest.snapshotCompleted(txnId, truncation);
            }
        }
    }

    public void addInterest(final SnapshotCompletionInterest interest) {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                m_interests.add(interest);
            }
        });
    }

    public void removeInterest(final SnapshotCompletionInterest interest) {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                m_interests.remove(interest);
            }
        });
    }

    public void shutdown() throws InterruptedException {
        m_es.shutdown();
        m_es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    public void init(final ZooKeeper zk) {
        m_es.execute(new Runnable() {
            @Override
            public void run() {
                m_zk = zk;
                try {
                    m_zk.create("/completed_snapshots", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (Exception e){}
                try {
                    m_lastKnownSnapshots =
                        new TreeSet<String>(m_zk.getChildren("/completed_snapshots", m_newSnapshotWatcher));
                } catch (Exception e) {
                    LOG.fatal("Error initializing snapshot completion monitor", e);
                    VoltDB.crashVoltDB();
                }
            }
        });
    }
}
