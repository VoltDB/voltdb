/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

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
public class BulkLoaderState {
    // Client instance shared by all VoltBulkLoaders
    final ClientImpl m_clientImpl;
    // Thread dedicated to each partition and an additional thread for all MP tables
    List<Thread> m_spawnedPartitionProcessors = null;
    // Maps a table name to the list of VoltBulkLoaders that are operating on that table
    final Map<String, List<VoltBulkLoader>> m_TableNameToLoader =
            Collections.synchronizedMap(new TreeMap<String, List<VoltBulkLoader>>());
    // Array of organized by partitionId each containing the Queue of PerPartitionTables that have batches waiting
    ConcurrentLinkedQueue<PerPartitionTable>[] m_tableQueues = null;
    // Array of PartitionProcessors (one per partition plus one for multi-partition bulk inserts)
    PartitionProcessor[] m_partitionProcessors = null;
    // Trigger for shutting down the PartitionProcessor threads
    boolean m_shutdownPartitionProcessors = false;
    // Latch used to clean things up during shutdown
    CountDownLatch m_processor_cdl = null;

    public BulkLoaderState(ClientImpl clientImpl) {
        m_clientImpl = clientImpl;
    }

    void cleanupBulkLoaderState () {
        m_spawnedPartitionProcessors = null;
        m_tableQueues = null;
        m_partitionProcessors = null;
        m_processor_cdl = null;
    }

    int getTableNameToLoaderCnt() {
        return m_TableNameToLoader.size();
    }
}
