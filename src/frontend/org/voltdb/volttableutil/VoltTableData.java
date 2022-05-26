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

package org.voltdb.volttableutil;

import com.google.common.collect.ImmutableList;
import org.voltdb.VoltTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The utility class that represent the Schemas and Tables in VoltTable.
 * @author Chao Zhou
 */
public final class VoltTableData {
    private VoltTableData() {
    }

    public static final Map<String, Database> SCHEMA = new ConcurrentHashMap<>();

    public static class Database {
        public final List<Table> tables;
        Database(List<Table> tables) {
            this.tables = ImmutableList.copyOf(tables);
        }
    }

    public static class Table {
        public final String tableName;
        public final VoltTable table;
        public Table(String tableName, VoltTable voltTable) {
            this.tableName = tableName.toUpperCase();
            table = voltTable;
        }
    }
}
