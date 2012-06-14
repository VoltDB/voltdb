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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.voltcore.logging.VoltLogger;

import org.voltcore.zk.LeaderElector;

import org.voltdb.VoltZK;

/**
 * Cartographer provides answers to queries about the components in a cluster.
 */
public class Cartographer
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    final ZooKeeper m_zk;

    public Cartographer(ZooKeeper zk)
    {
        m_zk = zk;
    }

    /**
     * Take a ZK path of a child in the VoltZK.iv2masters and return
     * all of the HSIDs of the sites replicating that partition
     */
    public List<Long> getReplicasForIv2Master(String zkPath) {
        List<Long> retval = null;
        if (!zkPath.startsWith(VoltZK.iv2masters)) {
            hostLog.error("Invalid ZK path given to getReplicasForIv2Master: " + zkPath +
                    ".  It must be a child of " + VoltZK.iv2masters);
        }
        else {
            int partId = Integer.valueOf(zkPath.split("/")[zkPath.split("/").length - 1]);
            retval = getReplicasForPartition(partId);
        }
        return retval;
    }

    public List<Long> getReplicasForPartition(int partition) {
        String zkpath = LeaderElector.electionDirForPartition(partition);
        List<Long> retval = new ArrayList<Long>();
        try {
            List<String> children = m_zk.getChildren(zkpath, null);
            for (String child : children) {
                retval.add(Long.valueOf(child.split("_")[0]));
            }
        }
        catch (KeeperException ke) {
            org.voltdb.VoltDB.crashLocalVoltDB("KeeperException getting replicas for partition: " + partition,
                    true, ke);
        }
        catch (InterruptedException ie) {
            org.voltdb.VoltDB.crashLocalVoltDB("InterruptedException getting replicas for partition: " +
                    partition, true, ie);
        }
        return retval;
    }

    /**
     * Convenience method to return the immediate count of replicas for the given partition
     */
    public int getReplicaCountForPartition(int partition) {
        return getReplicasForPartition(partition).size();
    }
}
