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

import java.util.List;
import java.util.Map;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.export.ExportDataSource.StreamStartAction;

/**
 * Export data from a single catalog version and database instance.
 *
 */
public interface Generation {

    public void acceptMastership(int partitionId);
    public void close();

    public List<ExportStatsRow> getStats(boolean interval);
    public void onSourceDone(int partitionId, String signature);

    public void pushExportBuffer(int partitionId, String signature, long seqNo, int tupleCount,
                                 long uniqueId, BBContainer cont, boolean sync);
    public void updateInitialExportStateToSeqNo(int partitionId, String signature,
                                                StreamStartAction action,
                                                Map<Integer, Pair<Long, Long>> sequenceNumberPerPartition);
    public void updateDanglingExportStates(StreamStartAction action,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers);

    public Map<Integer, Map<String, ExportDataSource>> getDataSourceByPartition();
}
