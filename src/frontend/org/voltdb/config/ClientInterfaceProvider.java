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

import java.net.InetAddress;

import javax.inject.Inject;

import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface;
import org.voltdb.VoltDB;
import org.voltdb.config.topo.PartitionsInformer;
import org.voltdb.config.topo.TopologyProviderFactory;
import org.voltdb.iv2.Cartographer;

/**
 * @author black
 *
 */

public class ClientInterfaceProvider {
    @Inject
    private org.voltdb.config.Configuration m_config;
    @Inject
    private CartographerProvider cartographerProvider;
    @Inject
    private HostMessenger m_messenger;
    @Inject
    private PartitionsInformer partitionsInformer;
    @Inject
    private TopologyProviderFactory topologyProviderFactory;
    @Inject
    private CatalogContextProvider catalogContextProvider;

    public ClientInterface getClientInterface() {
        try {
            InetAddress clientIntf = null;
            InetAddress adminIntf = null;
            if (!m_config.m_externalInterface.trim().equals("")) {
                clientIntf = InetAddress.getByName(m_config.m_externalInterface);
                //client and admin interfaces are same by default.
                adminIntf = clientIntf;
            }
            //If user has specified on command line host:port override client and admin interfaces.
            if (m_config.m_clientInterface != null && m_config.m_clientInterface.trim().length() > 0) {
                clientIntf = InetAddress.getByName(m_config.m_clientInterface);
            }
            if (m_config.m_adminInterface != null && m_config.m_adminInterface.trim().length() > 0) {
                adminIntf = InetAddress.getByName(m_config.m_adminInterface);
            }
            CatalogContext m_catalogContext = catalogContextProvider.getCatalogContext();
            Cartographer m_cartographer = cartographerProvider.getCartographer();
            return ClientInterface.create(m_messenger, m_catalogContext, m_config.m_replicationRole,
                    m_cartographer,
                    partitionsInformer.getNumberOfPartitions(),
                    clientIntf,
                    m_config.m_port,
                    adminIntf,
                    m_config.m_adminPort,
                    m_config.m_timestampTestingSalt);
        } catch (Exception e) {
            throw VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
    }
}
