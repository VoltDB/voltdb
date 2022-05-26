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
package org.voltcore.zk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.voltcore.logging.VoltLogger;

import org.apache.zookeeper_voltpatches.ZooKeeper;

//Use the mechanism similar to leader election by creating sequential ephemeral nodes.
//The node with lowest sequence number will be first granted the lock, exit the protocol.
//Then the node with next lowest sequence number gets the lock.
public class ZooKeeperLock implements Watcher {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private final ZooKeeper m_zk;
    private final String m_basePath;
    private final String m_lockPath;
    private String m_currentPath = null;
    private final Object m_lock = new Object();

    public ZooKeeperLock(ZooKeeper zk, String nodePath, String lockName) {
        m_zk = zk;
        m_basePath = nodePath;
        m_lockPath = ZKUtil.joinZKPath(m_basePath, lockName);
    }

    /**
    * @return true if a lock is successfully acquired
    */
    public boolean acquireLock() {
        return acquireLockWithTimeout(0);
    }

    /**
     * @param timeout The value of timeout in millisecond. if timeout < 0, no timeout
     * @return true if a lock is successfully acquired
     */
    public boolean acquireLockWithTimeout(long timeout) {
        try {
            //Create a sequential node, example: /db/action_lock/lock0000000000
            m_currentPath = m_zk.create(m_lockPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            synchronized(m_lock) {
                long startTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                while(true) {
                    List<String> nodes = m_zk.getChildren(m_basePath, this);

                    //Sort the nodes by name which ends with a 10 digit number
                    //Grant the lock to the first entry with the lowest sequence number
                    Collections.sort(nodes);
                    if (nodes.isEmpty() || m_currentPath.endsWith(nodes.get(0))) {
                        return true;
                    } else {
                        //does not get the lock this time. Wait for next update upon node deletion or addition if not timeout
                        if (timeout > 0) {
                            long nextWaitingTime = timeout - (TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - startTime);
                            if (nextWaitingTime <= 0) {
                                return false;
                            }
                            m_lock.wait(nextWaitingTime);
                        } else {
                            m_lock.wait();
                        }
                    }
                }
            }
        } catch (InterruptedException | KeeperException e) {
            hostLog.warn("Could not acquire a ZK lock:" + e.getMessage());
        }
        return false;
    }

    public void releaseLock() throws IOException {
        if (m_currentPath != null) {
            try {
                m_zk.delete(m_currentPath, -1);
                m_currentPath = null;
            } catch (KeeperException | InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        //Invoked from a separate callback thread
        //Wake up the acquireLock() thread to check if it is its turn to own the lock.
        synchronized (m_lock) {
            m_lock.notifyAll();
        }
    }
}
