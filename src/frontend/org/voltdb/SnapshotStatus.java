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

package org.voltdb;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.voltcore.utils.Pair;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot.SnapshotScanner;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot.Table;

public class SnapshotStatus extends StatsSource {

    // Item order matters, SnapshotSummary manipulates the result by checking the ordinal
    public enum SnapshotResult {
        FAILURE,
        IN_PROGRESS,
        SUCCESS;
    }

    enum SnapshotType {
        AUTO,
        MANUAL,
        COMMANDLOG,
        REJOIN,
        ELASTIC;
    };

    public enum SnapshotStatusCols {
        TABLE                   (VoltType.STRING),
        PATH                    (VoltType.STRING),
        FILENAME                (VoltType.STRING),
        NONCE                   (VoltType.STRING),
        TXNID                   (VoltType.BIGINT),
        START_TIME              (VoltType.BIGINT),
        END_TIME                (VoltType.BIGINT),
        SIZE                    (VoltType.BIGINT),
        DURATION                (VoltType.BIGINT),
        THROUGHPUT              (VoltType.FLOAT),
        RESULT                  (VoltType.STRING),
        TYPE                    (VoltType.STRING);

        public final VoltType m_type;
        SnapshotStatusCols(VoltType type) { m_type = type; }
    }

    static class SnapshotTypeChecker {
        File m_truncationSnapshotPath = null;
        File m_autoSnapshotPath = null;

        public void setSnapshotPath(String truncationSnapshotPathStr, String autoSnapshotPathStr) {
            m_truncationSnapshotPath = new File(truncationSnapshotPathStr);
            m_autoSnapshotPath = new File(autoSnapshotPathStr);
        }

        SnapshotType getSnapshotType(String path, String nonce) {
            File thisSnapshotPath = new File(path);
            if (m_truncationSnapshotPath.equals(thisSnapshotPath)) {
                if (nonce.startsWith("JOIN")) {
                    return SnapshotType.ELASTIC;
                } else {
                    return SnapshotType.COMMANDLOG;
                }
            } else if (m_autoSnapshotPath.equals(thisSnapshotPath)) {
                return SnapshotType.AUTO;
            } else if (path.equals("") && nonce.startsWith("Rejoin")) {
                return SnapshotType.REJOIN;
            }
            return SnapshotType.MANUAL;
        }
    }
    SnapshotTypeChecker m_typeChecker = new SnapshotTypeChecker();


    /**
     * Since there are multiple tables inside a Snapshot object, and we cannot
     * get a copy of the tables directly, flattens the tables in a Snapshot
     * object into a flat list.
     */
    static class StatusIterator<T> implements Iterator<Object> {
        private final List<Pair<Snapshot, T>> m_snapshots;
        private final Iterator<Pair<Snapshot, T>> m_iter;

        public StatusIterator(Iterator<Snapshot> i, SnapshotScanner<T> sc) {
            m_snapshots = new LinkedList<Pair<Snapshot, T>>();

            while (i.hasNext()) {
                final Snapshot s = i.next();
                List<T> objs = sc.flatten(s);
                for (T t : objs) {
                    m_snapshots.add(Pair.of(s, t));
                }
            }

            m_iter = m_snapshots.iterator();
        }

        @Override
        public boolean hasNext() {
            return m_iter.hasNext();
        }

        @Override
        public Object next() {
            return m_iter.next();
        }

        @Override
        public void remove() {
            m_iter.remove();
        }
    }

    public SnapshotStatus(String truncationSnapshotPathStr, String autoSnapshotPathStr) {
        super(false);
        m_typeChecker.setSnapshotPath(truncationSnapshotPathStr, autoSnapshotPathStr);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, SnapshotStatusCols.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        Pair<Snapshot, Table> p = (Pair<Snapshot, Table>) rowKey;
        Snapshot s = p.getFirst();
        Table t = p.getSecond();
        double duration = 0;
        double throughput = 0;
        long timeStarted = s.timeStarted;
        if (s.timeFinished != 0) {
            duration =
                (s.timeFinished - timeStarted) / 1000.0;
            throughput = (s.bytesWritten / (1024.0 * 1024.0)) / duration;
        }

        rowValues[offset + SnapshotStatusCols.TABLE.ordinal()] = t.name;
        rowValues[offset + SnapshotStatusCols.PATH.ordinal()] = s.path;
        rowValues[offset + SnapshotStatusCols.FILENAME.ordinal()] = t.filename;
        rowValues[offset + SnapshotStatusCols.NONCE.ordinal()] = s.nonce;
        rowValues[offset + SnapshotStatusCols.TXNID.ordinal()] = s.txnId;
        rowValues[offset + SnapshotStatusCols.START_TIME.ordinal()] = timeStarted;
        rowValues[offset + SnapshotStatusCols.END_TIME.ordinal()] = s.timeFinished;
        rowValues[offset + SnapshotStatusCols.SIZE.ordinal()] = t.size;
        rowValues[offset + SnapshotStatusCols.DURATION.ordinal()] = duration;
        rowValues[offset + SnapshotStatusCols.THROUGHPUT.ordinal()] = throughput;
        String result;
        if (t.writeExp == null && t.serializationExp == null) {
            if (s.result == SnapshotResult.SUCCESS) {
                result = SnapshotResult.SUCCESS.toString();
            } else {
                result = SnapshotResult.IN_PROGRESS.toString();
            }
        } else {
            result = SnapshotResult.FAILURE.toString();
        }
        rowValues[offset + SnapshotStatusCols.RESULT.ordinal()] = result;
        rowValues[offset + SnapshotStatusCols.TYPE.ordinal()] = m_typeChecker.getSnapshotType(s.path, s.nonce).name();
        return offset + SnapshotStatusCols.values().length;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new StatusIterator<Table>(
                SnapshotRegistry.getSnapshotHistory().iterator(),
                new SnapshotScanner<Table>() {
                    public List<Table> flatten(Snapshot s) {
                        return s.iterateTables();
                    }
                });
    }

}
