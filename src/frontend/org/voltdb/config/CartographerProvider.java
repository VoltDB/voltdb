/**
 * 
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
