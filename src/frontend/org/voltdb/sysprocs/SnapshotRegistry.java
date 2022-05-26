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

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotStatus.SnapshotResult;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

/**
 * The snapshot registry contains information about snapshots that executed
 * while the system was running.
 *
 */
public class SnapshotRegistry {
    private static final int m_maxStatusHistory = 10;

    private static final TreeSet<Snapshot> m_snapshots = new TreeSet<Snapshot>(
            new java.util.Comparator<Snapshot>() {

                @Override
                public int compare(Snapshot o1, Snapshot o2) {
                    return Long.compare(o1.txnId, o2.txnId);
                }

            });

    public static class Snapshot {
        public final long txnId;
        public final long timeStarted;
        public final long timeFinished;

        public final String path;
        public final String nonce;
        public SnapshotResult result;

        public final long bytesWritten;

        public int finishedTasks = 0;
        public int totalTasks = 0;

        private final HashMap< String, Table> tables = new HashMap< String, Table>();

        private Snapshot(long txnId, long timeStarted, int hostId, String path, String nonce,
                SnapshotFormat format, List<SnapshotTableInfo> tables) {
            this.txnId = txnId;
            this.timeStarted = timeStarted;
            this.path = path;
            this.nonce = nonce;
            timeFinished = 0;
            synchronized (this.tables) {
                for (SnapshotTableInfo table : tables) {
                    String filename =
                        SnapshotUtil.constructFilenameForTable(
                                table,
                                nonce,
                                format,
                                hostId);
                    this.tables.put(table.getName(), new Table(table.getName(), filename));
                }
            }
            result = SnapshotResult.IN_PROGRESS;
            bytesWritten = 0;
        }

        private Snapshot(Snapshot incomplete, long timeFinished) {
            txnId = incomplete.txnId;
            timeStarted = incomplete.timeStarted;
            path = incomplete.path;
            nonce = incomplete.nonce;
            totalTasks = incomplete.totalTasks;
            finishedTasks = incomplete.finishedTasks;
            this.timeFinished = timeFinished;
            synchronized (tables) {
                tables.putAll(incomplete.tables);
            }
            long bytesWritten = 0;
            boolean success = true;
            for (Table t : tables.values()) {
                bytesWritten += t.size;
                if (t.writeExp != null || t.serializationExp != null) {
                    success = false;
                }
            }
            this.bytesWritten = bytesWritten;
            this.result = success ? SnapshotResult.SUCCESS : SnapshotResult.FAILURE;
        }

        public void setTotalTasks(int total) {
            totalTasks = total;
        }

        public synchronized void taskFinished() {
            finishedTasks++;
        }

        public double progress() {
            return totalTasks == 0 ? 100 : (finishedTasks * 100.0) / totalTasks;
        }

        public interface TableUpdater {
            public Table update(Table t);
        }

        public interface SnapshotScanner<T> {
            public List<T> flatten(Snapshot s);
        }

        public List<Table> iterateTables() {
            List<Table> snapshotTables = new ArrayList<>();
            synchronized (tables) {
                snapshotTables.addAll(tables.values());
            }
            return snapshotTables;
        }

        public void updateTable(String name, TableUpdater tu) {
            synchronized (tables) {
                assert(tables.get(name) != null);
                tables.put(name, tu.update(tables.get(name)));
            }
        }

        public Table removeTable(String name) {
            synchronized (tables) {
                return tables.remove(name);
            }
        }

        public class Table {
            public final String name;
            public final String filename;
            public final long size;
            public final Throwable writeExp;
            public final Throwable serializationExp;

            private Table(String name, String filename) {
                this.name = name;
                this.filename = filename;
                size = 0;
                writeExp = null;
                serializationExp = null;
            }

            public Table(Table t, long size, Throwable writeExp, Throwable serializationExp) {
                this.name = t.name;
                this.filename = t.filename;
                this.size = size;
                this.writeExp = writeExp;
                this.serializationExp = serializationExp;
            }
        }
    }

    public static synchronized Snapshot startSnapshot(
            long txnId,
            int hostId,
            String path,
            String nonce,
            SnapshotFormat format,
            List<SnapshotTableInfo> tables) {
        final Snapshot s = new Snapshot(txnId, System.currentTimeMillis(),
                hostId, path, nonce, format, tables);

        m_snapshots.add(s);
        if (m_snapshots.size() > m_maxStatusHistory) {
            m_snapshots.pollFirst();
        }

        return s;
    }

    public static synchronized void discardSnapshot(Snapshot s) {
        m_snapshots.remove(s);
    }

    public static synchronized Snapshot finishSnapshot(Snapshot incomplete) {
        boolean removed = m_snapshots.remove(incomplete);
        assert(removed);
        final Snapshot completed = new Snapshot(incomplete, System.currentTimeMillis());
        m_snapshots.add(completed);
        return completed;
    }

    public static synchronized TreeSet<Snapshot> getSnapshotHistory() {
        return new TreeSet<Snapshot>(m_snapshots);
    }

    public static synchronized void clear() {
        m_snapshots.clear();
    }
}
