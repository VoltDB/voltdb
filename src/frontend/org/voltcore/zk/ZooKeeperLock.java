/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;

public class ZooKeeperLock {
    private final ZooKeeper m_zk;
    private final String m_nodePath;
    private final String m_lockName;
    private String m_lockedPath;

    public ZooKeeperLock(ZooKeeper zk, String nodePath, String lockName) {
        m_zk = zk;
        m_nodePath = nodePath;
        m_lockName = lockName;
    }

    //Create a lock on ZooKeeper to synchronize the tasks on multiple nodes
    public void lock() throws IOException {
        try {
            //Create a sequential node
            m_lockedPath = m_zk.create(ZKUtil.joinZKPath(m_nodePath, m_lockName),
                    null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            final Object lock = new Object();
            synchronized(lock) {
                while(true) {
                    List<String> nodes = m_zk.getChildren(m_nodePath, new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            //Called from a separate thread to keep track of updates.
                            //Wake up the loop to ensure no lose of updates
                            synchronized (lock) {
                                lock.notifyAll();
                            }
                        }
                    });

                    //Sort the nodes. Grant the lock to the node with the lowest sequence number gets the lock
                    Collections.sort(nodes);
                    if (m_lockedPath.endsWith(nodes.get(0))) {
                        return;
                    } else {
                        //wait for the removal of the node which has been granted the lock
                        lock.wait();
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    public void unlock() throws IOException {
        try {
            m_zk.delete(m_lockedPath, -1);
            m_lockedPath = null;
        } catch (KeeperException | InterruptedException e) {
            throw new IOException(e);
        }
    }
}
