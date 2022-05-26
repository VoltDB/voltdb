/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import org.voltdb.SnapshotStatus.SnapshotType;
import org.voltdb.SnapshotStatus.SnapshotTypeChecker;
import org.voltdb.SnapshotStatus.StatusIterator;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot.SnapshotScanner;

public class SnapshotSummary extends StatsSource {

    public enum SnapshotSummaryCols {
        NONCE                   (VoltType.STRING),
        TXNID                   (VoltType.BIGINT),
        TYPE                    (VoltType.STRING),
        PATH                    (VoltType.STRING),
        START_TIME              (VoltType.BIGINT),
        END_TIME                (VoltType.BIGINT),
        DURATION                (VoltType.BIGINT),
        PROGRESS_PCT            (VoltType.FLOAT),
        RESULT                  (VoltType.STRING);

        public final VoltType m_type;
        SnapshotSummaryCols(VoltType type) { m_type = type; }
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
            String nonce = perHostStats.getString(SnapshotSummaryCols.NONCE.name());
            String result = perHostStats.getString(SnapshotSummaryCols.RESULT.name());
            String path = perHostStats.getString(SnapshotSummaryCols.PATH.name());
            float progressPct = (float)perHostStats.getDouble(SnapshotSummaryCols.PROGRESS_PCT.name());
            List<StatsRow> statsRows = snapshotMap.get(nonce + path);
            if (statsRows == null) {
                statsRows = new ArrayList<StatsRow>();
                snapshotMap.put(nonce + path, statsRows);
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
                long tmpStartTime = row.statsRow.getLong(SnapshotSummaryCols.START_TIME.name());
                if (tmpStartTime < startTime) {
                    startTime = tmpStartTime;
                }
                long tmpEndTime = row.statsRow.getLong(SnapshotSummaryCols.END_TIME.name());
                if (tmpEndTime > endTime) {
                    endTime = tmpEndTime;
                }
                lastRow = row;
            }
            avgProgress /= e.getValue().size();
            if (endTime != 0) {
                duration = (endTime - startTime) / 1000.0; // in seconds
            }
            resultTable.addRow(lastRow.statsRow.getString(SnapshotSummaryCols.NONCE.name()),
                    lastRow.statsRow.getLong(SnapshotSummaryCols.TXNID.name()),
                    lastRow.statsRow.getString(SnapshotSummaryCols.TYPE.name()),
                    lastRow.statsRow.getString(SnapshotSummaryCols.PATH.name()),
                    startTime,
                    endTime,
                    duration,
                    Math.round(avgProgress * 10.0) / 10.0, // round to 1 decimal places
                    SnapshotResult.values()[lowestOrdinal].name());
        }
        return new VoltTable[] { resultTable };
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        for (SnapshotSummaryCols col : SnapshotSummaryCols.values()) {
            columns.add(new VoltTable.ColumnInfo(col.name(), col.m_type));
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        Pair<Snapshot, SnapshotResult> p = (Pair<Snapshot, SnapshotResult>) rowKey;
        Snapshot s = p.getFirst();
        SnapshotType type = m_typeChecker.getSnapshotType(s.path, s.nonce);
        SnapshotResult sr = p.getSecond();

        rowValues[SnapshotSummaryCols.NONCE.ordinal()] = s.nonce;
        rowValues[SnapshotSummaryCols.TXNID.ordinal()] = s.txnId;
        rowValues[SnapshotSummaryCols.TYPE.ordinal()] = type.name();
        rowValues[SnapshotSummaryCols.PATH.ordinal()] = s.path;
        rowValues[SnapshotSummaryCols.START_TIME.ordinal()] = s.timeStarted;
        rowValues[SnapshotSummaryCols.END_TIME.ordinal()] = s.timeFinished;
        rowValues[SnapshotSummaryCols.PROGRESS_PCT.ordinal()] = (float)s.progress();
        rowValues[SnapshotSummaryCols.RESULT.ordinal()] = sr.name();
        return SnapshotSummaryCols.values().length;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {

        return new StatusIterator<SnapshotResult>(
                SnapshotRegistry.getSnapshotHistory().iterator(),
                new SnapshotScanner<SnapshotResult>() {
                    public List<SnapshotResult> flatten(Snapshot s) {
                        // Ignore join and index snapshot
                        List<SnapshotResult> result  = new ArrayList<>();
                        SnapshotType type = m_typeChecker.getSnapshotType(s.path, s.nonce);
                        if (type == SnapshotType.ELASTIC) {
                            return result;
                        }
                        result.add(s.result);
                        return result;
                    }
                });
    }

}
