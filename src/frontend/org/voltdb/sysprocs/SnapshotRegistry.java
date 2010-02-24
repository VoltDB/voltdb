/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs;

import java.util.HashMap;
import java.util.TreeSet;
import java.util.Iterator;

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
                    return new Long(o1.timeStarted).compareTo(o2.timeStarted);
                }

            });

    public static class Snapshot {
        public final long timeStarted;
        public final long timeFinished;

        public final String path;
        public final String nonce;
        public final boolean result; //true success, false failure

        private final HashMap< String, Table> tables = new HashMap< String, Table>();

        private Snapshot(long startTime, String path, String nonce, String tables[]) {
            timeStarted = startTime;
            this.path = path;
            this.nonce = nonce;
            timeFinished = 0;
            synchronized (this.tables) {
                for (String table : tables) {
                    this.tables.put( table, new Table(table, startTime));
                }
            }
            result = false;
        }

        private Snapshot(Snapshot incomplete, long timeFinished) {
            timeStarted = incomplete.timeStarted;
            path = incomplete.path;
            nonce = incomplete.nonce;
            this.timeFinished = timeFinished;
            synchronized (tables) {
                tables.putAll(incomplete.tables);
            }
            for (Table t : tables.values()) {
                if (t.error != null) {
                    result = false;
                    return;
                }
            }
            result = true;
        }

        public interface TableUpdater {
            public Table update(Table t);
        }

        public interface TableIterator {
            public void next(Table t);
        }

        public void iterateTables(TableIterator ti) {
            synchronized (tables) {
                for (Table t : tables.values()) {
                    ti.next(t);
                }
            }
        }

        public void updateTable(String name, TableUpdater tu) {
            synchronized (tables) {
                assert(tables.get(name) != null);
                tables.put(name, tu.update(tables.get(name)));
            }
        }

        public class Table {
            public final String name;
            public final long size;
            public final long timeClosed;
            public final long timeCreated;
            public final Exception error;

            private Table(String name, long timeCreated) {
                this.name = name;
                this.timeCreated = timeCreated;
                size = 0;
                timeClosed = 0;
                error = null;
            }

            public Table(Table t, long size, long timeClosed, Exception error) {
                this.name = t.name;
                this.size = size;
                this.timeClosed = timeClosed;
                this.timeCreated = t.timeCreated;
                this.error = error;
            }
        }
    }

    public static synchronized Snapshot startSnapshot(long startTime, String path, String nonce, String tables[]) {
        final Snapshot s = new Snapshot(startTime, path, nonce, tables);

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
