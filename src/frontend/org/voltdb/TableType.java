/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

/*
 * Define the different modes of operation for table streams.
 *
 * IMPORTANT: Keep this enum in sync with the C++ equivalent in types.h!
 *            Values are serialized through JNI and IPC as integers.
 */
public enum TableType {
    PERSISTENT(0),              // Regular PersistentTable
    STREAM_VIEW_ONLY(1),        // StreamTable without ExportTupleStream (Views only)
    STREAM(2),                  // StreamTable with ExportTupleStream
    PERSISTENT_MIGRATE(3),      // PersistentTable with associated Stream for migrating DELETES
    PERSISTENT_EXPORT(4);       // PersistentTable with associated Stream for linking INSERTS

    final int type;
    TableType(int type) {
        this.type = type;
    }
    public int get() {
        return type;
    }

    public static boolean isStream(int e) {
        return (e ==STREAM.get() || e == STREAM_VIEW_ONLY.get());
    }
}

