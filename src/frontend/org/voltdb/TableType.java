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

import java.util.List;

import com.google_voltpatches.common.collect.ImmutableMap;

/*
 * Define the different modes of operation for table streams.
 *
 * IMPORTANT: Keep this enum in sync with the C++ equivalent in types.h!
 *            Values are serialized through JNI and IPC as integers.
 */
public enum TableType {
    INVALID_TYPE(0),
    PERSISTENT(1),              // Regular PersistentTable
    CONNECTOR_LESS_STREAM(2),   // StreamTable without ExportTupleStream (Views only)
    STREAM(3),                  // StreamTable with ExportTupleStream
    PERSISTENT_MIGRATE(4),      // PersistentTable with associated Stream for migrating DELETES
    // PersistentTable with associated Stream for linking INSERTS
    PERSISTENT_EXPORT_INSERT(8),
    PERSISTENT_EXPORT_DELETE(16),
    PERSISTENT_EXPORT_UPDATEOLD(32),
    PERSISTENT_EXPORT_UPDATENEW(64),
    // MIND THE MATH FOR VALUES BELOW !
    PERSISTENT_EXPORT_INSERT_DELETE(8+16),
    PERSISTENT_EXPORT_INSERT_UPDATEOLD(8+32),
    PERSISTENT_EXPORT_DELETE_UPDATEOLD(16+32),
    PERSISTENT_EXPORT_INSERT_DELETE_UPDATEOLD(8+16+32),
    PERSISTENT_EXPORT_INSERT_UPDATENEW(8+64),
    PERSISTENT_EXPORT_DELETE_UPDATENEW(16+64),
    PERSISTENT_EXPORT_INSERT_DELETE_UPDATENEW(8+16+64),
    PERSISTENT_EXPORT_UPDATE(32+64),
    PERSISTENT_EXPORT_INSERT_UPDATE(8+32+64),
    PERSISTENT_EXPORT_DELETE_UPDATE(16+32+64),
    PERSISTENT_EXPORT_INSERT_DELETE_UPDATE(8+16+32+64);

    public static final ImmutableMap<Integer, String> typeToString;
    static {
        ImmutableMap.Builder<Integer, String> builder = ImmutableMap.builder();
        builder.put(PERSISTENT_EXPORT_INSERT.get(), "INSERT");
        builder.put(PERSISTENT_EXPORT_DELETE.get(), "DELETE");
        builder.put(PERSISTENT_EXPORT_UPDATEOLD.get(), "UPDATE_OLD");
        builder.put(PERSISTENT_EXPORT_UPDATENEW.get(), "UPDATE_NEW");
        builder.put(PERSISTENT_EXPORT_INSERT_DELETE.get(), "INSERT,DELETE");
        builder.put(PERSISTENT_EXPORT_INSERT_UPDATEOLD.get(), "INSERT,UPDATE_OLD");
        builder.put(PERSISTENT_EXPORT_DELETE_UPDATEOLD.get(), "DELETE,UPDATE_OLD");
        builder.put(PERSISTENT_EXPORT_INSERT_DELETE_UPDATEOLD.get(), "INSERT,DELETE,UPDATE_OLD");
        builder.put(PERSISTENT_EXPORT_INSERT_UPDATENEW.get(), "INSERT,UPDATE_NEW");
        builder.put(PERSISTENT_EXPORT_DELETE_UPDATENEW.get(), "DELETE,UPDATE_NEW");
        builder.put(PERSISTENT_EXPORT_INSERT_DELETE_UPDATENEW.get(), "INSERT,DELETE,UPDATE_NEW");
        builder.put(PERSISTENT_EXPORT_UPDATE.get(), "UPDATE");
        builder.put(PERSISTENT_EXPORT_INSERT_UPDATE.get(), "INSERT,UPDATE");
        builder.put(PERSISTENT_EXPORT_DELETE_UPDATE.get(), "DELETE,UPDATE");
        builder.put(PERSISTENT_EXPORT_INSERT_DELETE_UPDATE.get(), "INSERT,DELETE,UPDATE");
        typeToString = builder.build();
    }
    final int type;
    TableType(int type) {
        this.type = type;
    }
    public int get() {
        return type;
    }

    public static boolean isConnectorLessStream(int e) {
        return (e == CONNECTOR_LESS_STREAM.get());
    }

    /**
     * @param e table type
     * @return {@code true} if this type is {@link #STREAM} or {@link #CONNECTOR_LESS_STREAM}
     */
    public static boolean isStream(int e) {
        return (e == STREAM.get() || isConnectorLessStream(e));
    }

    public static boolean isPersistentMigrate(int e) {
        return (e == PERSISTENT_MIGRATE.get());
    }

    // return true if this is a persistent table associated with an export stream
    public static boolean needsShadowStream(int e) {
        return (e >= PERSISTENT_MIGRATE.get() && e <= PERSISTENT_EXPORT_INSERT_DELETE_UPDATE.get());
    }

    public static boolean isPersistentExport(int e) {
        return (e >= PERSISTENT_EXPORT_INSERT.get() && e <= PERSISTENT_EXPORT_INSERT_DELETE_UPDATE.get());
    }

    // return true if the table type needs a {@code ExportDataSource} instance
    public static boolean needsExportDataSource(int e) {
        return (e == STREAM.get() || needsShadowStream(e));
    }

    public static boolean isInvalidType(int e) {
        return e == INVALID_TYPE.type;
    }

    public static int getPersistentExportTrigger(List<String> triggers) {
        int tableType = 0;
        for (String trigger : triggers) {
            if(trigger.equalsIgnoreCase("INSERT")) {
                tableType += TableType.PERSISTENT_EXPORT_INSERT.get();
            } else if(trigger.equalsIgnoreCase("DELETE")) {
                tableType += TableType.PERSISTENT_EXPORT_DELETE.get();
            } else if(trigger.equalsIgnoreCase("UPDATE_OLD")) {
                tableType += TableType.PERSISTENT_EXPORT_UPDATEOLD.get();
            } else if(trigger.equalsIgnoreCase("UPDATE_NEW")) {
                tableType += TableType.PERSISTENT_EXPORT_UPDATENEW.get();
            } else if (trigger.equalsIgnoreCase("UPDATE")) {
                tableType += TableType.PERSISTENT_EXPORT_UPDATE.get();
            }
        }
        assert typeToString.get(tableType) != null : String.format("Bad type %d for triggers %s", tableType, triggers);
        return tableType;
    }

    public static String toPersistentExportString(int tableType) {
        if (isPersistentExport(tableType)) {
            return typeToString.get(tableType);
        }
        return "";
    }
}

