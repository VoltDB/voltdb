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
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper_voltpatches.AsyncCallback.VoidCallback;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;

//Use the mechanism similar to ZooKeeper leader election by creating sequential ephemeral nodes.
//The node with lowest sequence number will be first granted the lock, exit the protocol.
//Then the node with next lowest sequence number gets the lock.
public class ZooKeeperLock {
    private final ZooKeeper m_zk;
    private final String m_lockPath;
    private String m_sequentialPath = null;
    final Object lock = new Object();

    public ZooKeeperLock(ZooKeeper zk, String nodePath) {
        m_zk = zk;
        m_lockPath = nodePath;
    }

    public void acquireLock() throws IOException {
        try {
            //Create a sequential node
            m_sequentialPath = m_zk.create(ZKUtil.joinZKPath(m_lockPath, "lock"),
                    null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            synchronized(lock) {
                while(true) {
                    List<String> nodes = syncAndGetChildren();

                    //Sort the nodes by name which ends with a 10 digit number
                    //Grant the lock to the first entry with the lowest sequence number
                    Collections.sort(nodes);
                    if (m_sequentialPath.endsWith(nodes.get(0))) {
                        return;
                    } else {
                        //does not get the lock this time. Wait for next update: node deletion or addition
                        lock.wait();
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    private List<String> syncAndGetChildren() throws KeeperException, InterruptedException {
        //sync ZooKeeper leader and followers
        CountDownLatch latch = new CountDownLatch(1);
        m_zk.sync(m_lockPath, new VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                latch.countDown();
            }
        }, null);
        latch.await();

        //get a list of sequential nodes and watch the node update
        return m_zk.getChildren(m_lockPath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //Invoked from a separate thread
                //Wake up the acquireLock() thread to check if it is its turn to own the lock.
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        });
    }

    synchronized public void releaseLock() throws IOException {
        if (m_sequentialPath == null) {
            return;
        }
        try {
            m_zk.delete(m_sequentialPath, -1);
            m_sequentialPath = null;
        } catch (KeeperException | InterruptedException e) {
            throw new IOException(e);
        }
    }
}
