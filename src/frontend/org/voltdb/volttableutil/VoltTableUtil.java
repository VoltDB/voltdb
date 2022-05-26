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

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.jdbc.Driver;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.voltdb.VoltTable;
import org.voltdb.plannerv2.ColumnTypes;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * The utility class that allow user to run SQL query on {@link VoltTable}.
 * @author Chao Zhou
 */
public final class VoltTableUtil {
    private VoltTableUtil() {
    }

    /**
     * Run a SQL query on {@link VoltTable}.
     * Exp:
     * <code>VoltTable vt = VoltTableUtil.executeSql(
     * "select stations.station_id, count(activity.card_id)
     * from stations, activity where stations.station_id = activity.station_id
     * group by stations.station_id",
     * "stations", stations, "activity", activity);</code>
     *
     * @param sql  The SQL string.
     * @param tableNames a list of {@link String} table names.
     * @param tables a list of {@link VoltTable} volt tables. Then sizes of tables and tableNames must equal.
     * @return A result set represented in {@link VoltTable}.
     */
    public static VoltTable executeSql(String sql, List<String> tableNames, List<VoltTable> tables) throws Exception {
        return run(sql, new VoltTableData.Database(asTable(tableNames, tables)));
    }

    public static VoltTable executeSql(String sql, String tableName, VoltTable table) throws Exception {
        return run(sql, new VoltTableData.Database(
                Collections.singletonList(new VoltTableData.Table(tableName, table))));
    }

    private static VoltTable run(String sql, VoltTableData.Database db) throws Exception {
        final String uuid = UUID.randomUUID().toString();
        VoltTableData.SCHEMA.put(uuid, db);
        final Properties info = new Properties();

        info.setProperty("schemaFactory", "org.voltdb.volttableutil.VoltTableSchemaFactory");
        info.setProperty("schema.id", uuid);
        try {       // mimic what a transaction block does: clean schema afterwards
            return asVoltTable(new Driver().connect("jdbc:calcite:", info).createStatement().executeQuery(sql));
        } finally {
            VoltTableData.SCHEMA.remove(uuid);
        }
    }

    private static List<VoltTableData.Table> asTable(List<String> tableNames, List<VoltTable> tables) {
        Preconditions.checkArgument(tables.size() == tableNames.size(),
                "Length of table names must equal to length of tables");
        ImmutableList.Builder<VoltTableData.Table> builder = ImmutableList.builder();
        for (int i = 0; i < tableNames.size(); ++i) {
            builder.add(new VoltTableData.Table(tableNames.get(i), tables.get(i)));
        }
        return builder.build();
    }

    private static VoltTable asVoltTable(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return null;
        } else {
            try {
                final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                VoltTable.ColumnInfo[] columnInfoArray = new VoltTable.ColumnInfo[resultSetMetaData.getColumnCount()];
                for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                    // TODO: verify the type convert
                    columnInfoArray[i] = new VoltTable.ColumnInfo(resultSetMetaData.getColumnName(i + 1),
                            ColumnTypes.getVoltType(SqlTypeName.getNameForJdbcType(resultSetMetaData.getColumnType(i + 1))));
                }
                final VoltTable vt = new VoltTable(columnInfoArray);
                while (resultSet.next()) {
                    Object[] row = new Object[resultSetMetaData.getColumnCount()];
                    for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                        Object value = resultSet.getObject(i + 1);
                        if (resultSet.wasNull()) {
                            value = ColumnTypes.getVoltType(SqlTypeName.getNameForJdbcType(
                                    resultSetMetaData.getColumnType(i + 1)))
                                    .getNullValue();
                        }
                        row[i] = value;
                    }
                    vt.addRow(row);
                }
                return vt;
            } finally {
                resultSet.close();
            }
        }
    }
}
