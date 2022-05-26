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

import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * Schema mapped onto a {@link VoltTableUtil#executeSql(String, Object...)} call.
 */
public class VoltTableSchema extends AbstractSchema {
    private String m_databaseId;

    public VoltTableSchema(String databaseId) {
        this.m_databaseId = databaseId;
    }

    @Override
    public Map<String, Table> getTableMap() {
        Map<String, Table> tables = new HashMap<>();
        VoltTableData.Database database = VoltTableData.SCHEMA.get(this.m_databaseId);
        if (database == null)
            return tables;
        for (VoltTableData.Table table : database.tables) {
            tables.put(table.tableName, new VoltScannableTable(table.table));
        }
        return tables;
    }
}
