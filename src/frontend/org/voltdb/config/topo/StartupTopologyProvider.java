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
package org.voltdb.config.topo;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.json_voltpatches.JSONObject;
import javax.inject.Inject;

import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.config.CatalogContextProvider;

/**
 * @author black
 *
 */

public class StartupTopologyProvider implements TopologyProvider {


    @Inject
    private CatalogContextProvider catalogContextProvider;

    @Inject
    private HostMessenger m_messenger;

    @Override
    public JSONObject getTopo() {
        CatalogContext m_catalogContext;
        try {
            m_catalogContext = catalogContextProvider
                    .getCatalogContext();
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException("Cannot retrieve catalog context", e);
        }
        int sitesperhost = m_catalogContext.getDeployment().getCluster()
                .getSitesperhost();
        int hostcount = m_catalogContext.getDeployment().getCluster()
                .getHostcount();
        int kfactor = m_catalogContext.getDeployment().getCluster()
                .getKfactor();
        ClusterConfig clusterConfig = new ClusterConfig(hostcount,
                sitesperhost, kfactor);
        if (!clusterConfig.validate()) {
            VoltDB.crashLocalVoltDB(clusterConfig.getErrorMsg(), false,
                    null);
        }
        return registerClusterConfig(clusterConfig);
    }

    private JSONObject registerClusterConfig(ClusterConfig config)
    {
        // First, race to write the topology to ZK using Highlander rules
        // (In the end, there can be only one)
        JSONObject topo = null;
        try
        {
            topo = config.getTopology(m_messenger.getLiveHostIds());
            byte[] payload = topo.toString(4).getBytes("UTF-8");
            m_messenger.getZK().create(VoltZK.topology, payload,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        catch (KeeperException.NodeExistsException nee)
        {
            // It's fine if we didn't win, we'll pick up the topology below
        }
        catch (Exception e)
        {
            VoltDB.crashLocalVoltDB("Unable to write topology to ZK, dying",
                    true, e);
        }

        // Then, have everyone read the topology data back from ZK
        try
        {
            byte[] data = m_messenger.getZK().getData(VoltZK.topology, false, null);
            topo = new JSONObject(new String(data, "UTF-8"));
        }
        catch (Exception e)
        {
            VoltDB.crashLocalVoltDB("Unable to read topology from ZK, dying",
                    true, e);
        }
        return topo;
    }

}
