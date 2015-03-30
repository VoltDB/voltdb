/**
 * 
 */
package org.voltdb.config.topo;

import java.util.List;

import org.json_voltpatches.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author black
 *
 */
@Component
public class TopologyProviderFactory {

	@Autowired
	private List<TopologyProvider> topologyProvidersChain;

	/**
	 * Gets topology information. If rejoining, get it directly from ZK.
	 * Otherwise, try to do the write/read race to ZK on startup. NOTE: override
	 */
	public JSONObject getTopo() {
		for(TopologyProvider provider:topologyProvidersChain) {
			JSONObject topo = provider.getTopo();
			if(topo != null) {
				return topo;
			}
		}
		throw new UnsupportedOperationException("Cannot find relevant topology provider");
	}

}
