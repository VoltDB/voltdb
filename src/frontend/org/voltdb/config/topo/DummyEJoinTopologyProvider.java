/**
 * 
 */
package org.voltdb.config.topo;

import javax.inject.Inject;

import org.json_voltpatches.JSONObject;
import org.voltdb.StartAction;
import org.voltdb.config.Configuration;

/**
 * @author black
 *
 */

public class DummyEJoinTopologyProvider implements TopologyProvider {

	@Inject
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
