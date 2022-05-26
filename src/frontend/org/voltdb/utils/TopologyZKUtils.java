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

package org.voltdb.utils;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONException;
import org.voltdb.AbstractTopology;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

import com.google_voltpatches.common.base.Charsets;

public abstract class TopologyZKUtils {

    public static AbstractTopology registerTopologyToZK(ZooKeeper zk, AbstractTopology topology) {
        // First, race to write the topology to ZK using Highlander rules
        // (In the end, there can be only one)
        try {
            byte[] payload = topology.topologyToJSON().toString().getBytes(Charsets.UTF_8);
            zk.create(VoltZK.topology, payload, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            // It's fine if we didn't win, we'll pick up the topology below
        } catch (KeeperException | InterruptedException | JSONException e) {
            VoltDB.crashLocalVoltDB("Unable to write topology to ZK, dying", true, e);
        }

        // Then, have everyone read the topology data back from ZK
        return readTopologyFromZK(zk);
    }

    public static AbstractTopology readTopologyFromZK(ZooKeeper zk) {
        try {
            return readTopologyFromZK(zk, null);
        } catch (KeeperException | InterruptedException | JSONException e) {
            VoltDB.crashLocalVoltDB("Unable to read topology from ZK, dying", true, e);
        }
        return null;
    }

    public static AbstractTopology readTopologyFromZK(ZooKeeper zk, Stat stat)
            throws KeeperException, InterruptedException, JSONException {
        byte[] data = zk.getData(VoltZK.topology, false, stat);
        String jsonTopology = new String(data, Charsets.UTF_8);
        return AbstractTopology.topologyFromJSON(jsonTopology);
    }

    public static void updateTopologyToZK(ZooKeeper zk, AbstractTopology topology) {
        try {
            updateTopologyToZK(zk, topology, -1);
        } catch (KeeperException | InterruptedException | JSONException e) {
            VoltDB.crashLocalVoltDB("Unable to update topology to ZK, dying", true, e);
        }
    }

    public static void updateTopologyToZK(ZooKeeper zk, AbstractTopology topology, int version)
            throws KeeperException, InterruptedException, JSONException {
        byte[] payload = topology.topologyToJSON().toString().getBytes(Charsets.UTF_8);
        zk.setData(VoltZK.topology, payload, version);
    }
}
