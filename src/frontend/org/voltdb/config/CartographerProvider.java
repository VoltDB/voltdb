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
package org.voltdb.config;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import javax.inject.Inject;

import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.config.topo.TopologyProviderFactory;
import org.voltdb.config.topo.TopologyProviderFactoryImpl;
import org.voltdb.iv2.Cartographer;

/**
 * @author black
 *
 */

public class CartographerProvider {
    @Inject
    private HostMessenger m_messenger;

    @Inject
    private CatalogContextProvider catalogContextProvider;

    @Inject
    private TopologyProviderFactory topologyProviderFactory;

    public Cartographer getCartographer() {
        JSONObject topo = topologyProviderFactory.getTopo();
        int m_configuredReplicationFactor;
        try {
            m_configuredReplicationFactor = new ClusterConfig(topo).getReplicationFactor();
        } catch (JSONException e) {
            throw new IllegalStateException("Cannot retrieve cluster topology", e);//TODO: use specialized exception
        }
        CatalogContext m_catalogContext;
        try {
            m_catalogContext = catalogContextProvider.getCatalogContext();
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException("Cannot retrieve catalogContext", e);//TODO: use specialized exception
        }
        return new Cartographer(m_messenger, m_configuredReplicationFactor,
                m_catalogContext.cluster.getNetworkpartition());

    }
}
