/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.LinkedList;
import java.util.List;

import org.voltcore.utils.Pair;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot;
import org.voltdb.sysprocs.SnapshotRegistry.Snapshot.Table;

public class SnapshotStatus extends StatsSource {

    /**
     * Since there are multiple tables inside a Snapshot object, and we cannot
     * get a copy of the tables directly, flattens the tables in a Snapshot
     * object into a flat list.
     */
    private class StatusIterator implements Iterator<Object> {
        private final List<Pair<StatusRow, Table>> m_snapshots;
        private final Iterator<Pair<StatusRow, Table>> m_iter;

        private StatusIterator(Iterator<Snapshot> i) {
            m_snapshots = new LinkedList<Pair<StatusRow, Table>>();

            while (i.hasNext()) {
                final Snapshot s = i.next();
                s.iterateTables(new Snapshot.TableIterator() {
                    @Override
                    public void next(Table t) {
                        m_snapshots.add(Pair.of(new StatusRow(s,t), t));
                    }
                });
                for (SnapshotRegistry.HardLink hl: s.hardLinks.values()) {
                    for (Table t: hl.tables.values()) {
                        m_snapshots.add(Pair.of(new StatusRow(s,t,hl.path,hl.nonce), t));
                    }
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

    public SnapshotStatus() {
        super(false);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("TABLE", VoltType.STRING));
        columns.add(new ColumnInfo("PATH", VoltType.STRING));
        columns.add(new ColumnInfo("FILENAME", VoltType.STRING));
        columns.add(new ColumnInfo("NONCE", VoltType.STRING));
        columns.add(new ColumnInfo("TXNID", VoltType.BIGINT));
        columns.add(new ColumnInfo("START_TIME", VoltType.BIGINT));
        columns.add(new ColumnInfo("END_TIME", VoltType.BIGINT));
        columns.add(new ColumnInfo("SIZE", VoltType.BIGINT));
        columns.add(new ColumnInfo("DURATION", VoltType.BIGINT));
        columns.add(new ColumnInfo("THROUGHPUT", VoltType.FLOAT));
        columns.add(new ColumnInfo("RESULT", VoltType.STRING));
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        Pair<StatusRow, Table> p = (Pair<StatusRow, Table>) rowKey;
        StatusRow s = p.getFirst();
        Table t = p.getSecond();
        double duration = 0;
        double throughput = 0;
        long timeStarted = s.timeStarted;
        if (s.timeFinished != 0) {
            duration =
                (s.timeFinished - timeStarted) / 1000.0;
            throughput = (s.size / (1024.0 * 1024.0)) / duration;
        }

        rowValues[columnNameToIndex.get("TABLE")] = t.name;
        rowValues[columnNameToIndex.get("PATH")] = s.path;
        rowValues[columnNameToIndex.get("FILENAME")] = t.filename;
        rowValues[columnNameToIndex.get("NONCE")] = s.nonce;
        rowValues[columnNameToIndex.get("TXNID")] = s.txnId;
        rowValues[columnNameToIndex.get("START_TIME")] = timeStarted;
        rowValues[columnNameToIndex.get("END_TIME")] = s.timeFinished;
        rowValues[columnNameToIndex.get("SIZE")] = t.size;
        rowValues[columnNameToIndex.get("DURATION")] = duration;
        rowValues[columnNameToIndex.get("THROUGHPUT")] = throughput;
        rowValues[columnNameToIndex.get("RESULT")] = t.error == null ? "SUCCESS" : "FAILURE";
        super.updateStatsRow(rowKey, rowValues);
    }

    public static class StatusRow {
        final public String table;
        final public String path;
        final public String filename;
        final public String nonce;
        final public long   txnId;
        final public long   size;
        final public long   timeStarted;
        final public long   timeFinished;
        final public String result;

        private StatusRow(Snapshot s, Table t) {
            this.table         = t.name;
            this.path          = s.path;
            this.filename      = t.filename;
            this.nonce         = s.nonce;
            this.size          = t.size;
            this.txnId         = s.txnId;
            this.timeStarted   = s.timeStarted;
            this.timeFinished  = s.timeFinished;
            this.result        = t.error == null ? "SUCCESS" : "FAILURE";
        }

        private StatusRow(Snapshot s, Table t, String path, String nonce) {
            this.table         = t.name;
            this.path          = path;
            this.filename      = t.filename;
            this.nonce         = nonce;
            this.size          = t.size;
            this.txnId         = s.txnId;
            this.timeStarted   = s.timeStarted;
            this.timeFinished  = s.timeFinished;
            this.result        = t.error == null ? "SUCCESS" : "FAILURE";
        }
    }


    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new StatusIterator(SnapshotRegistry.getSnapshotHistory().iterator());
    }

}
