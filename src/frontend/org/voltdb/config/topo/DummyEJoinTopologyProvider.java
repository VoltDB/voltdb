/**
 * 
 */
package org.voltdb.config.topo;

import org.json_voltpatches.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.voltdb.StartAction;
import org.voltdb.config.Configuration;

/**
 * @author black
 *
 */
@Component
public class DummyEJoinTopologyProvider implements TopologyProvider {

	@Autowired
	private Configuration config;

	@Override
	public JSONObject getTopo() {
		StartAction startAction = config.m_startAction;

		if (startAction == StartAction.JOIN) {
	        throw new UnsupportedOperationException("getTopology is only supported for elastic join");
		} 
		return null;
	}

}
