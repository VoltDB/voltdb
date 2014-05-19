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

/**
 * Encapsulation of the applications insert request (also used for passing internal notifications across processors)
 */
class VoltBulkLoaderRow {
    final VoltBulkLoader m_loader;
    Object m_rowHandle;
    final Object[] m_rowData;

    interface BulkLoaderNotification {
        void setBatchCount(int cnt);        // Called to initialize the row to the number of batches that need to be processed
        void notifyOfPendingInsert();       // Called when the Notification is about to force a batch send
        void notifyOfClientResponse();      // Called when ClientResponse has been generated for this batch
        CountDownLatch getLatch();
    }

    // Used when a VoltBulkLoader instance requests a synchronous Drain()
    class DrainNotificationCallBack implements BulkLoaderNotification {
        private final CountDownLatch drainLatch;
        int batchCnt;

        DrainNotificationCallBack(CountDownLatch latch) {
            drainLatch = latch;
            m_rowHandle = this;
        }

        @Override
        public void setBatchCount(int cnt) {
            batchCnt = cnt;
        }

        @Override
        public CountDownLatch getLatch() {
            return drainLatch;
        }

        @Override
        public void notifyOfPendingInsert() {
        }

        @Override
        public void notifyOfClientResponse() {
            if (--batchCnt <= 0)
                drainLatch.countDown();
        }
    }

    // Used when a VoltBulkLoader instance requests a synchronous Close()
    class CloseNotificationCallBack implements BulkLoaderNotification {
        PerPartitionTable m_tableForClosedBulkLoader;
        final CountDownLatch m_closeCompleteLatch;
        int batchCnt;
        CloseNotificationCallBack(PerPartitionTable tableForClosedBulkLoader, CountDownLatch latch) {
            m_tableForClosedBulkLoader = tableForClosedBulkLoader;
            m_closeCompleteLatch = latch;
            m_rowHandle = this;
        }

        @Override
        public void setBatchCount(int cnt) {
            batchCnt = cnt;
        }

        @Override
        public CountDownLatch getLatch() {
            return m_closeCompleteLatch;
        }

        // In this call we are synchronized by the PerPartitionTable object
        @Override
        public void notifyOfPendingInsert() {
            // First prevent the BulkLoader from accepting new RowInserts
            m_loader.m_partitionTable[m_tableForClosedBulkLoader.m_partitionId] = null;
        }

        @Override
        public void notifyOfClientResponse() {
            if (--batchCnt <= 0) {
                if (m_tableForClosedBulkLoader.m_partitionQueuedRowCnt.get() >  0) {
                    // A row squeaked through so reinsert this row to repeat the close attempt
                    try {
                        m_tableForClosedBulkLoader.drainTableQueue(VoltBulkLoaderRow.this);
                    }
                    catch (InterruptedException e) {}
                }
                else {
                    m_closeCompleteLatch.countDown();
                }
            }
        }
    }

    VoltBulkLoaderRow(VoltBulkLoader bulkLoader) {
        m_loader = bulkLoader;
        this.m_rowData = null;
    }

    VoltBulkLoaderRow(VoltBulkLoader bulkLoader, Object rowHandle, Object... rowData) {
        m_loader = bulkLoader;
        this.m_rowHandle = rowHandle;
        this.m_rowData = rowData;
    }

    boolean isNotificationRow()
    {
        // Null objectLists are not allowed using VoltBulkLoader.insertRow()
        return (m_rowData == null);
    }
}
