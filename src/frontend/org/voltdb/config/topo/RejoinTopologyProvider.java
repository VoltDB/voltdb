/**
 * 
 */
package org.voltdb.config.topo;

import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.StartAction;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.config.Configuration;

/**
 * @author black
 *
 */
@Component
public class RejoinTopologyProvider implements TopologyProvider {

	@Autowired
	private HostMessenger m_messenger;
	@Autowired
	private Configuration config;


	@Override
	public JSONObject getTopo() {
		StartAction startAction = config.m_startAction;

		if (startAction.doesRejoin()) {

			Stat stat = new Stat();
			try {
				return new JSONObject(new String(m_messenger.getZK().getData(
						VoltZK.topology, false, stat), "UTF-8"));
			} catch (Exception e) {
				VoltDB.crashLocalVoltDB("Unable to get topology from ZK", true,
						e);
			}
		}
		return null;
	}

}
