/**
 * 
 */
package org.voltdb.config.topo;

import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONObject;
import javax.inject.Inject;

import org.voltcore.messaging.HostMessenger;
import org.voltdb.StartAction;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.config.Configuration;

/**
 * @author black
 *
 */
public class RejoinTopologyProvider implements TopologyProvider {

	@Inject
	private HostMessenger m_messenger;
	@Inject
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
