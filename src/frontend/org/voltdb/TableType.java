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

import java.util.Collections;
import java.util.List;

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
    PERSISTENT_EXPORT_INSERT_DELETE(24),
    PERSISTENT_EXPORT_INSERT_UPDATEOLD(40),
    PERSISTENT_EXPORT_DELETE_UPDATEOLD(50),
    PERSISTENT_EXPORT_INSERT_DELETE_UPDATEOLD(60),
    PERSISTENT_EXPORT_INSERT_UPDATENEW(72),
    PERSISTENT_EXPORT_DELETE_UPDATENEW(80),
    PERSISTENT_EXPORT_INSERT_DELETE_UPDATENEW(88),
    PERSISTENT_EXPORT_UPDATE(96),
    PERSISTENT_EXPORT_INSERT_UPDATE(104),
    PERSISTENT_EXPORT_DELETE_UPDATE(112),
    PERSISTENT_EXPORT_INSERT_DELETE_UPDATE(120);

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

    public static boolean isStream(int e) {
        return (e == STREAM.get() || e == CONNECTOR_LESS_STREAM.get());
    }

    public static boolean isPersistentMigrate(int e) {
        return (e == PERSISTENT_MIGRATE.get());
    }

    public static boolean needsShadowStream(int e) {
        return (e >= PERSISTENT_MIGRATE.get() && e <= PERSISTENT_EXPORT_INSERT_DELETE_UPDATE.get());
    }

    public static boolean isPersistentExport(int e) {
        return (e >= PERSISTENT_EXPORT_INSERT.get() && e <= PERSISTENT_EXPORT_INSERT_DELETE_UPDATE.get());
    }

    public static boolean isInvalidType(int e) {
        return e == INVALID_TYPE.type;
    }

    public static int getPeristentExportTrigger(List<String> triggers) {
        int tableType = 0;
        Collections.sort(triggers);
        for (String trigger : triggers) {
            if(trigger.equals("INSERT")) {
                tableType += TableType.PERSISTENT_EXPORT_INSERT.get();
            } else if(trigger.equals("DELETE")) {
                tableType += TableType.PERSISTENT_EXPORT_DELETE.get();
            } else if(trigger.equals("UPDATE_OLD")) {
                tableType += TableType.PERSISTENT_EXPORT_UPDATEOLD.get();
            } else if(trigger.equals("UPDATE_NEW")) {
                tableType += TableType.PERSISTENT_EXPORT_UPDATENEW.get();
            } else if (trigger.equals("UPDATE")) {
                tableType += TableType.PERSISTENT_EXPORT_UPDATE.get();
            }
        }
        return tableType;
    }

    public static String toPersistentExportString(int tableType) {
        if (tableType== PERSISTENT_EXPORT_INSERT.get()){
            return "INSERT";
        } else if (tableType == PERSISTENT_EXPORT_DELETE.get()) {
            return "DELETE";
        } else if (tableType == PERSISTENT_EXPORT_UPDATEOLD.get()) {
            return "UPDATE_OLD";
        } else if (tableType == PERSISTENT_EXPORT_UPDATENEW.get()) {
            return "UPDATE_NEW";
        } else if (tableType == PERSISTENT_EXPORT_INSERT_DELETE.get()) {
            return "INSERT,DELETE";
        } else if (tableType == PERSISTENT_EXPORT_INSERT_UPDATEOLD.get()) {
            return "INSERT,UPDATE_OLD";
        } else if (tableType == PERSISTENT_EXPORT_DELETE_UPDATEOLD.get()) {
            return "DELETE,UPDATE_OLD";
        } else if (tableType == PERSISTENT_EXPORT_INSERT_DELETE_UPDATEOLD.get()) {
            return "INSERT,DELETE,UPFDATE_OLD";
        } else if (tableType == PERSISTENT_EXPORT_INSERT_UPDATENEW.get()) {
            return "INSERT,UPDATE_NEW";
        } else if (tableType == PERSISTENT_EXPORT_DELETE_UPDATENEW.get()) {
            return "DELETE,UPDATE_NEW";
        } else if (tableType == PERSISTENT_EXPORT_INSERT_DELETE_UPDATENEW.get()) {
            return "INSERT,DELETE,UPDATE_NEW";
        } else if (tableType == PERSISTENT_EXPORT_UPDATE.get()) {
            return "UPDATE";
        } else if (tableType == PERSISTENT_EXPORT_INSERT_UPDATE.get()) {
            return "INSERT,UPDATE";
        } else if (tableType == PERSISTENT_EXPORT_DELETE_UPDATE.get()) {
            return "DELETE,UPDATE";
        } else if (tableType == PERSISTENT_EXPORT_INSERT_DELETE_UPDATE.get()) {
            return "INSERT,DELETE,UPDATE";
        }
        return "";
    }
}

