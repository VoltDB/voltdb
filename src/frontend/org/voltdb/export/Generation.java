/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.List;
import java.util.Map;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.export.ExportDataSource.StreamStartAction;

/**
 * Export data from a single catalog version and database instance.
 *
 */
public interface Generation {

    public void becomeLeader(int partitionId);
    public void shutdown();

    public List<ExportStatsRow> getStats(boolean interval);
    public void onSourceDrained(int partitionId, String tableName);

    public void pushExportBuffer(int partitionId, String signature, long seqNo, long committedSeqNo,
            int tupleCount, long uniqueId, BBContainer container);

    public void updateInitialExportStateToSeqNo(int partitionId, String signature, StreamStartAction action,
            Map<Integer, ExportSnapshotTuple> sequenceNumberPerPartition);

    public void updateDanglingExportStates(StreamStartAction action,
            Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers);

    public Map<Integer, Map<String, ExportDataSource>> getDataSourceByPartition();

    public int getCatalogVersion();

    public void updateGenerationId(long genId);

    // FIXME: review if needed
    public void sync();
}
