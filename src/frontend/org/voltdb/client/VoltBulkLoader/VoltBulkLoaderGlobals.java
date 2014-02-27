package org.voltdb.client.VoltBulkLoader;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.voltdb.client.ClientImpl;

/**
 * Global objects shared by all VoltBulkLoader instances operating under a single Client
 */
public class VoltBulkLoaderGlobals {
	// Client instance shared by all VoltBulkLoaders
	ClientImpl m_clientImpl = null;
	// Thread dedicated to each partition and an additional thread for all MP tables
	List<Thread> m_spawnedPartitionProcessors = null;
	// Maps a table name to the list of VoltBulkLoaders that are operating on that table
	final Map<String, List<VoltBulkLoader>> m_TableNameToLoader =
			Collections.synchronizedMap(new TreeMap<String, List<VoltBulkLoader>>());
	// Array of organized by partitionId each containing the Queue of PerPartitionTables that have batches waiting
	ConcurrentLinkedQueue<PerPartitionTable>[] tableQueues = null;
	// Array of PartitionProcessors (one per partition plus one for multi-partition bulk inserts)
	PartitionProcessor[] m_partitionProcessors = null;
	// Trigger for shutting down the PartitionProcessor threads
	boolean m_shutdownPartitionProcessors = false;
	// Latch used to clean things up during shutdown
	CountDownLatch m_processor_cdl = null;
	
	public VoltBulkLoaderGlobals(ClientImpl clientImpl) {
		m_clientImpl = clientImpl;
	}
	
	public int getTableNameToLoaderCnt() {
		return m_TableNameToLoader.size();
	}
}
