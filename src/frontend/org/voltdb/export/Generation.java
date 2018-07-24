/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
package org.voltdb.export;

import java.nio.ByteBuffer;
import java.util.Map;

import org.voltcore.messaging.HostMessenger;

/**
 * Export data from a single catalog version and database instance.
 *
 */
public interface Generation {

    public void acceptMastership(int partitionId);
    public void close(final HostMessenger messenger);

    public long getQueuedExportBytes(int partitionId, String signature);
    public void onSourceDone(int partitionId, String signature);

    public void pushExportBuffer(int partitionId, String signature, long uso, ByteBuffer buffer, boolean sync);
    public void pushEndOfStream(int partitionId, String signature);
    public void truncateExportToTxnId(long snapshotTxnId, long[] perPartitionTxnIds);

    public Map<Integer, Map<String, ExportDataSource>> getDataSourceByPartition();
}
