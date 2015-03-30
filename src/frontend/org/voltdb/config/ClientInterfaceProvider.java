/**
 * 
 */
package org.voltdb.config;

import java.net.InetAddress;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
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
@Component
public class ClientInterfaceProvider {
	@Autowired
	private org.voltdb.config.Configuration m_config;
	@Autowired
	private CartographerProvider cartographerProvider;
	@Autowired
	private HostMessenger m_messenger;
	@Autowired
	private PartitionsInformer partitionsInformer;
	@Autowired
	private TopologyProviderFactory topologyProviderFactory;
	@Autowired
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
