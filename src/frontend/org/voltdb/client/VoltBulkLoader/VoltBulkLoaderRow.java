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
    final VoltBulkLoader loader;
    Object rowHandle;
    final Object[] objectList;

    interface BulkLoaderNotification {
        void setBatchCount(int cnt);        // Called to initialize the row to the number of batches that need to be processed
        void notifyOfPendingInsert();       // Called when the Notification is about to force a batch send
        void notifyOfClientResponse();      // Called when ClientResponse has been generated for this batch
        CountDownLatch getLatch();
    }

    class DrainNotificationCallBack implements BulkLoaderNotification {
        private final CountDownLatch drainLatch;
        int batchCnt;

        DrainNotificationCallBack(CountDownLatch latch) {
            drainLatch = latch;
            rowHandle = this;
        }

        public void setBatchCount(int cnt) {
            batchCnt = cnt;
        }

        public CountDownLatch getLatch() {
            return drainLatch;
        }

        public void notifyOfPendingInsert() {
        }

        public void notifyOfClientResponse() {
            if (--batchCnt <= 0)
                drainLatch.countDown();
        }
    }

    class CloseNotificationCallBack implements BulkLoaderNotification {
        PerPartitionTable m_tableForClosedBulkLoader;
        final CountDownLatch m_closeCompleteLatch;
        int batchCnt;
        CloseNotificationCallBack(PerPartitionTable tableForClosedBulkLoader, CountDownLatch latch) {
            m_tableForClosedBulkLoader = tableForClosedBulkLoader;
            m_closeCompleteLatch = latch;
            rowHandle = this;
        }

        public void setBatchCount(int cnt) {
            batchCnt = cnt;
        }

        public CountDownLatch getLatch() {
            return m_closeCompleteLatch;
        }

        // In this call we are synchronized by the PerPartitionTable object
        public void notifyOfPendingInsert() {
            // First prevent the BulkLoader from accepting new RowInserts
            loader.m_partitionTable[m_tableForClosedBulkLoader.m_partitionId] = null;
        }

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
        loader = bulkLoader;
        this.objectList = null;
    }

    VoltBulkLoaderRow(VoltBulkLoader bulkLoader, Object rowHandle, Object... objectList) {
        loader = bulkLoader;
        this.rowHandle = rowHandle;
        this.objectList = objectList;
    }

    boolean isNotificationRow()
    {
        // Null objectLists are not allowed using VoltBulkLoader.insertRow()
        return (objectList == null);
    }
}
