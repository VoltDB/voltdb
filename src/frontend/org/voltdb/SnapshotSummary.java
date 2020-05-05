/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
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

package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.voltcore.utils.Pair;
import org.voltdb.SnapshotStatus.SnapshotTypeChecker;
import org.voltdb.SnapshotStatus.StatusIterator;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot.SnapshotScanner;

public class SnapshotSummary extends StatsSource {

    private enum SnapshotResult {
        SUCCESS,
        FAILURE;
    }

    SnapshotTypeChecker m_typeChecker = new SnapshotTypeChecker();

    public SnapshotSummary(String truncationSnapshotPath, String autoSnapshotPath) {
        super(false);
        m_typeChecker.setSnapshotPath(truncationSnapshotPath, autoSnapshotPath);
    }

    // if any node has different result, that's a failure
    static VoltTable[] summarize(VoltTable perHostStats) {
        if (perHostStats.getRowCount() == 0) {
            return new VoltTable[] { perHostStats };
        }
        Map<String, VoltTableRow> snapshotMap = new TreeMap<>();
        perHostStats.resetRowPosition();
        while (perHostStats.advanceRow()) {
            String nonce = perHostStats.getString("NONCE");
            String result = perHostStats.getString("RESULT");
            VoltTableRow row = snapshotMap.get(nonce);
            if (row == null) {
                snapshotMap.put(nonce, perHostStats.cloneRow());
            } else {
                String res = row.getString("RESULT");
                if (!res.equalsIgnoreCase(result)) {
                    VoltTable newRow = new VoltTable(perHostStats.getTableSchema());
                    newRow.addRow(row.getString("NONCE"), row.getLong("TXNID"),
                            row.getLong("START_TIME"), row.getLong("END_TIME"),
                            row.getLong("DURATION"), SnapshotResult.FAILURE.toString(),
                            row.getString("TYPE"));
                    // advance to first row of table
                    newRow.advanceRow();
                    snapshotMap.put(nonce, newRow);
                }
            }
        }

        perHostStats.clearRowData();
        for (Map.Entry<String, VoltTableRow> e : snapshotMap.entrySet()) {
            perHostStats.add(e.getValue());
        }
        return new VoltTable[] { perHostStats };
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        columns.add(new ColumnInfo("NONCE", VoltType.STRING));
        columns.add(new ColumnInfo("TXNID", VoltType.BIGINT));
        columns.add(new ColumnInfo("START_TIME", VoltType.BIGINT));
        columns.add(new ColumnInfo("END_TIME", VoltType.BIGINT));
        columns.add(new ColumnInfo("DURATION", VoltType.BIGINT));
        columns.add(new ColumnInfo("RESULT", VoltType.STRING));
        columns.add(new ColumnInfo("TYPE", VoltType.STRING));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        Pair<Snapshot, Boolean> p = (Pair<Snapshot, Boolean>) rowKey;
        Snapshot s = p.getFirst();
        double duration = 0;
        Boolean hasError = p.getSecond();
        if (s.timeFinished != 0) {
            duration =
                (s.timeFinished - s.timeStarted) / 1000.0;
        }

        rowValues[columnNameToIndex.get("NONCE")] = s.nonce;
        rowValues[columnNameToIndex.get("TXNID")] = s.txnId;
        rowValues[columnNameToIndex.get("START_TIME")] = s.timeStarted;
        rowValues[columnNameToIndex.get("END_TIME")] = s.timeFinished;
        rowValues[columnNameToIndex.get("DURATION")] = duration;
        rowValues[columnNameToIndex.get("RESULT")] =
                hasError ? SnapshotResult.FAILURE.toString() : SnapshotResult.SUCCESS.toString();
        rowValues[columnNameToIndex.get("TYPE")] = m_typeChecker.getSnapshotType(s.path);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new StatusIterator<Boolean>(
                SnapshotRegistry.getSnapshotHistory().iterator(),
                new SnapshotScanner<Boolean>() {
                    public List<Boolean> flatten(Snapshot s) {
                        return s.iterateTableErrors();
                    }
                });
    }

}
