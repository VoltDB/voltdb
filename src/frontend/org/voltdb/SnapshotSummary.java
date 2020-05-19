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
import org.voltdb.SnapshotStatus.SnapshotResult;
import org.voltdb.SnapshotStatus.SnapshotTypeChecker;
import org.voltdb.SnapshotStatus.StatusIterator;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot.SnapshotScanner;

public class SnapshotSummary extends StatsSource {

    private enum ColumnName {
        NONCE,
        TXNID,
        START_TIME,
        END_TIME,
        DURATION,
        PROGRESS_PCT,
        RESULT,
        TYPE;
    }

    private static class StatsRow {
        public VoltTableRow statsRow;
        public String result;
        public float progressPct;

        public StatsRow(VoltTableRow row, String result, float progressPct) {
            this.statsRow = row;
            this.result = result;
            this.progressPct = progressPct;
        }
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
        Map<String, List<StatsRow>> snapshotMap = new TreeMap<>();
        perHostStats.resetRowPosition();
        while (perHostStats.advanceRow()) {
            String nonce = perHostStats.getString(ColumnName.NONCE.toString());
            String result = perHostStats.getString(ColumnName.RESULT.toString());
            float progressPct = (float)perHostStats.getDouble(ColumnName.PROGRESS_PCT.toString());
            List<StatsRow> statsRows = snapshotMap.get(nonce);
            if (statsRows == null) {
                statsRows = new ArrayList<StatsRow>();
                snapshotMap.put(nonce, statsRows);
            }
            StatsRow row = new StatsRow(perHostStats.cloneRow(), result, progressPct);
            statsRows.add(row);
        }

        VoltTable resultTable = new VoltTable(perHostStats.getTableSchema());
        for (Map.Entry<String, List<StatsRow>> e : snapshotMap.entrySet()) {
            float avgProgress = 0;
            SnapshotResult sr;
            long startTime = Long.MAX_VALUE;
            long endTime = 0;
            StatsRow lastRow = null;
            double duration = 0;
            // 1-failure, 2-in_progress, 3-success
            int lowestOrdinal = SnapshotResult.SUCCESS.ordinal();
            for (StatsRow row : e.getValue()) {
                avgProgress += row.progressPct;
                sr = SnapshotResult.valueOf(row.result);
                lowestOrdinal = Math.min(sr.ordinal(), lowestOrdinal);
                long tmpStartTime = row.statsRow.getLong(ColumnName.START_TIME.toString());
                if (tmpStartTime < startTime) {
                    startTime = tmpStartTime;
                }
                long tmpEndTime = row.statsRow.getLong(ColumnName.END_TIME.toString());
                if (tmpEndTime > endTime) {
                    endTime = tmpEndTime;
                }
                lastRow = row;
            }
            avgProgress /= e.getValue().size();
            if (endTime != 0) {
                duration = (endTime - startTime) / 1000.0; // in seconds
            }
            resultTable.addRow(lastRow.statsRow.getString(ColumnName.NONCE.toString()),
                    lastRow.statsRow.getLong(ColumnName.TXNID.toString()),
                    lastRow.statsRow.getString(ColumnName.TYPE.toString()),
                    startTime,
                    endTime,
                    duration,
                    Math.round(avgProgress * 10.0) / 10.0, // round to 1 decimal places
                    SnapshotResult.values()[lowestOrdinal].toString());
        }
        return new VoltTable[] { resultTable };
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        columns.add(new ColumnInfo(ColumnName.NONCE.toString(), VoltType.STRING));
        columns.add(new ColumnInfo(ColumnName.TXNID.toString(), VoltType.BIGINT));
        columns.add(new ColumnInfo(ColumnName.TYPE.toString(), VoltType.STRING));
        columns.add(new ColumnInfo(ColumnName.START_TIME.toString(), VoltType.BIGINT));
        columns.add(new ColumnInfo(ColumnName.END_TIME.toString(), VoltType.BIGINT));
        columns.add(new ColumnInfo(ColumnName.DURATION.toString(), VoltType.BIGINT));
        columns.add(new ColumnInfo(ColumnName.PROGRESS_PCT.toString(), VoltType.FLOAT));
        columns.add(new ColumnInfo(ColumnName.RESULT.toString(), VoltType.STRING));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        Pair<Snapshot, SnapshotResult> p = (Pair<Snapshot, SnapshotResult>) rowKey;
        Snapshot s = p.getFirst();
        SnapshotResult sr = p.getSecond();

        rowValues[columnNameToIndex.get(ColumnName.NONCE.toString())] = s.nonce;
        rowValues[columnNameToIndex.get(ColumnName.TXNID.toString())] = s.txnId;
        rowValues[columnNameToIndex.get(ColumnName.TYPE.toString())] = m_typeChecker.getSnapshotType(s.path);
        rowValues[columnNameToIndex.get(ColumnName.START_TIME.toString())] = s.timeStarted;
        rowValues[columnNameToIndex.get(ColumnName.END_TIME.toString())] = s.timeFinished;
        rowValues[columnNameToIndex.get(ColumnName.PROGRESS_PCT.toString())] = (float)s.progress();
        rowValues[columnNameToIndex.get(ColumnName.RESULT.toString())] = sr.toString();
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new StatusIterator<SnapshotResult>(
                SnapshotRegistry.getSnapshotHistory().iterator(),
                new SnapshotScanner<SnapshotResult>() {
                    public List<SnapshotResult> flatten(Snapshot s) {
                        return s.iterateTableErrors();
                    }
                });
    }

}
