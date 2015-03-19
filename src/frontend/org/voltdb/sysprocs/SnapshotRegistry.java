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

package org.voltdb.sysprocs;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.SnapshotFormat;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * The snapshot registry contains information about snapshots that executed
 * while the system was running.
 *
 */
public class SnapshotRegistry {
    private static final int m_maxStatusHistory = 10;

    public static final Map<String,HardLink> NO_HARDLINKS = ImmutableMap.<String,HardLink>of();

    private static final TreeSet<Snapshot> m_snapshots = new TreeSet<Snapshot>(
            new java.util.Comparator<Snapshot>() {

                @Override
                public int compare(Snapshot o1, Snapshot o2) {
                    return Long.valueOf(o1.txnId).compareTo(o2.txnId);
                }

            });

    public static class HardLink {

        public final String path;
        public final String nonce;
        public final Map<String, Snapshot.Table> tables;

        private HardLink(String path, String nonce, int hostId,
                SnapshotFormat format, org.voltdb.catalog.Table [] tables) {
            this.path = path;
            this.nonce = nonce;

            ImmutableMap.Builder<String, Snapshot.Table> mb = ImmutableMap.builder();
            for (org.voltdb.catalog.Table table: tables) {
                String filename =
                        SnapshotUtil.constructFilenameForTable(
                                table,
                                nonce,
                                format,
                                hostId);
                mb.put(table.getTypeName(), new Snapshot.Table(table.getTypeName(), filename));
            }
            this.tables = mb.build();
        }
    }

    public static class Snapshot {

        public final long txnId;
        public final long timeStarted;
        public final long timeFinished;

        public final String path;
        public final String nonce;
        public final boolean result; //true success, false failure

        public final long bytesWritten;
        public final SnapshotFormat format;

        private final Map<String,Table> tables = Collections.synchronizedMap(new LinkedHashMap<String,Table>());

        public final Map<String,HardLink> hardLinks;

        private Snapshot(long txnId, long timeStarted, int hostId, String path, String nonce,
                         SnapshotFormat format, org.voltdb.catalog.Table tables[],
                         JSONObject jsnHardLinks) {

            this.txnId = txnId;
            this.timeStarted = timeStarted;
            this.path = path;
            this.nonce = nonce;
            this.format = format;
            this.timeFinished = 0;

            Map<String,HardLink> hardLinks = NO_HARDLINKS;
            for (org.voltdb.catalog.Table table : tables) {
                String filename =
                        SnapshotUtil.constructFilenameForTable(
                                table,
                                nonce,
                                format,
                                hostId);
                this.tables.put(table.getTypeName(), new Table(table.getTypeName(), filename));
            }
            if (jsnHardLinks != null && jsnHardLinks.length() > 0) {
                ImmutableMap.Builder<String,HardLink> hardLinksBuilder = ImmutableMap.builder();
                Iterator<String> hlitr = jsnHardLinks.keys();
                while (hlitr.hasNext()) try {
                    String reqId = hlitr.next();
                    JSONObject jsnHardLink = jsnHardLinks.getJSONObject(reqId);
                    HardLink hardLink = new HardLink(
                            jsnHardLink.getString("path"),
                            jsnHardLink.getString("nonce"),
                            hostId, format, tables
                            );
                    hardLinksBuilder.put(reqId, hardLink);
                } catch (JSONException handleItByAssigninItAnEmptyMap) {
                    hardLinks = NO_HARDLINKS;
                }
                hardLinks = hardLinksBuilder.build();
            }

            this.hardLinks = hardLinks;
            this.result = false;
            this.bytesWritten = 0;
        }

        private Snapshot(Snapshot incomplete, long timeFinished) {
            this.txnId = incomplete.txnId;
            this.timeStarted = incomplete.timeStarted;
            this.path = incomplete.path;
            this.nonce = incomplete.nonce;
            this.format = incomplete.format;
            this.hardLinks = incomplete.hardLinks;
            this.timeFinished = timeFinished;
            this.tables.putAll(incomplete.tables);

            long bytesWritten = 0;
            boolean result = true;
            for (Table t : tables.values()) {
                bytesWritten += t.size;
                if (t.error != null) {
                    result = false;
                }
            }
            this.bytesWritten = bytesWritten;
            this.result = result;
        }

        public interface TableUpdater {
            public Table update(Table t);
        }

        public interface TableIterator {
            public void next(Table t);
        }

        public void iterateTables(TableIterator ti) {
            for (Table t: ImmutableList.copyOf(tables.values())) {
                ti.next(t);
            }
        }

        public void updateTable(String name, TableUpdater tu) {
            assert(tables.get(name) != null);
            tables.put(name, tu.update(tables.get(name)));
        }

        public Table removeTable(String name) {
            return tables.remove(name);
        }

        public static class Table {
            public final String name;
            public final String filename;
            public final long size;
            public final Throwable error;

            private Table(String name, String filename) {
                this.name = name;
                this.filename = filename;
                size = 0;
                error = null;
            }

            public Table(Table t, long size, Throwable error) {
                this.name = t.name;
                this.filename = t.filename;
                this.size = size;
                this.error = error;
            }
        }
    }

    public static synchronized Snapshot startSnapshot(
            long txnId,
            int hostId,
            String path,
            String nonce,
            SnapshotFormat format,
            org.voltdb.catalog.Table tables[],
            JSONObject jsnHardLinks) {
        final Snapshot s = new Snapshot(txnId, System.currentTimeMillis(),
                hostId, path, nonce, format, tables, jsnHardLinks);

        m_snapshots.add(s);
        if (m_snapshots.size() > m_maxStatusHistory) {
            Iterator<Snapshot> iter = m_snapshots.iterator();
            iter.next();
            iter.remove();
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
