/**
 * 
 */
package org.voltdb.config;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.CatalogContext;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.config.topo.TopologyProviderFactory;
import org.voltdb.iv2.Cartographer;

/**
 * @author black
 *
 */
@Component
public class CartographerProvider {
	@Autowired
	private HostMessenger m_messenger;
	
	@Autowired
	private CatalogContextProvider catalogContextProvider;
	
	@Autowired
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
