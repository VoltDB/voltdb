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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process partition specific data. If the table is not partitioned only one instance of this processor will be used
 */
public class PartitionProcessor implements Runnable {
    // Global object used for testing when to exit the thread
    final BulkLoaderState m_vblGlobals;
    //Partition for which this processor thread is processing.
    final int m_partitionId;
    //This is just so we can identity thread name.
    final String m_processorName;

    // Latch used for process cleanup.
    final CountDownLatch m_processor_cdl;
    // Queue of tables submitting batches.
    final LinkedBlockingQueue<PerPartitionTable> m_PendingTables;
    // When true indicates that this is the Multi-Partition row insert processor.
    final boolean m_isMP;

    public PartitionProcessor(int partitionId, boolean isMP, BulkLoaderState vblGlobals) {
        m_partitionId = partitionId;
        m_vblGlobals = vblGlobals;
        if (isMP) {
            m_processorName = "MP-PartitionProcessor";
        }
        else {
            m_processorName = "PartitionProcessor-" + partitionId;
        }

        m_PendingTables = new LinkedBlockingQueue<PerPartitionTable>();
        m_isMP = isMP;
        m_processor_cdl = m_vblGlobals.m_processor_cdl;
   }

    //This is to keep track of when to report how many rows inserted, shared by all processors.
    static AtomicLong lastMultiple = new AtomicLong(0);

    // while there are pending tables m_shutdownPartitionProcessors is false keep inserting bulk tables.
    private void processLoadTable() {
        PerPartitionTable nextPartitionTable;
        if (m_isMP) {
            try {
                while (true) {
                    nextPartitionTable = m_PendingTables.take();
                    if (m_vblGlobals.m_shutdownPartitionProcessors)
                        break;
                    nextPartitionTable.processMpNextTable();
                }
            } catch (InterruptedException e) {
            }
        }
        else {
            try {
                while (true) {
                    nextPartitionTable = m_PendingTables.take();
                    if (m_vblGlobals.m_shutdownPartitionProcessors)
                        break;
                    nextPartitionTable.processSpNextTable();
                }
            } catch (InterruptedException e) {
            }
        }
    }


    @Override
    public void run() {

        try {
            //Process the Partition queue.
            processLoadTable();

            m_PendingTables.clear();
        } finally {
            m_processor_cdl.countDown();
        }
    }

}
