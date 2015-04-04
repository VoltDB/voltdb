/**
 * 
 */
package org.voltdb.config.topo;

import java.util.List;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import javax.inject.Inject;

import org.voltcore.messaging.HostMessenger;
import org.voltdb.VoltDB;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.config.CartographerProvider;
import org.voltdb.config.Configuration;
import org.voltdb.iv2.Cartographer;

/**
 * @author black
 *
 */

public class PartitionsInformer {
	@Inject
	private CartographerProvider cartographerProvider;

	@Inject
	private TopologyProviderFactory topologyProviderFactory;

	@Inject
	private Configuration config;

	@Inject
	private HostMessenger m_messenger;

	public int getNumberOfPartitions() {
		Cartographer m_cartographer = cartographerProvider.getCartographer();
		if (config.m_startAction.doesRejoin()) {
			return m_cartographer.getPartitionCount();
		} else {
			JSONObject topo = topologyProviderFactory.getTopo();
			try {
				return new ClusterConfig(topo).getPartitionCount();
			} catch (JSONException e) {
				throw new IllegalStateException(
						"Cannot retrieve topology information");
			}
		}
	}

	public List<Integer> getPartitions() {

		if (config.m_startAction.doesRejoin()) {
			JSONObject topo = topologyProviderFactory.getTopo();
			ClusterConfig clusterConfig;
			try {
				clusterConfig = new ClusterConfig(topo);
			} catch (JSONException e) {
				throw new IllegalStateException("Cannot get cluster config", e);// TODO:
																				// use
																				// specialized
																				// exception
			}

			int m_configuredReplicationFactor = clusterConfig
					.getReplicationFactor();
			List<Integer> partitions;
			try {
				Cartographer m_cartographer = cartographerProvider.getCartographer();

				partitions = m_cartographer.getIv2PartitionsToReplace(
						m_configuredReplicationFactor,
						clusterConfig.getSitesPerHost());
			} catch (JSONException e) {
				throw new IllegalStateException(
						"Cannot get partiions from cartographer", e);// TODO:
																		// use
																		// specialized
																		// exception
			}
			if (partitions.size() == 0) {
				VoltDB.crashLocalVoltDB(
						"The VoltDB cluster already has enough nodes to satisfy "
								+ "the requested k-safety factor of "
								+ m_configuredReplicationFactor + ".\n"
								+ "No more nodes can join.", false, null);
			}
			return partitions;
		} else {
			JSONObject topo = topologyProviderFactory.getTopo();
			try {
				return ClusterConfig.partitionsForHost(topo,
						m_messenger.getHostId());
			} catch (JSONException e) {
				throw new IllegalStateException("Cannot get cluster config", e);// TODO:
				// use
				// specialized
				// exception
			}
		}
	}

}
