/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.voltcore.utils.Pair;
import org.voltdb.VoltTable;
import org.voltdb.calciteadapter.ColumnType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public final class VoltTableUtil {
    private VoltTableUtil() {
    }

    public static VoltTable executeSql(String sql, Object... args) {
        if (args.length % 2 == 1) {
            throw new IllegalArgumentException("Argument number not correct");
        }
        List<Pair<String, VoltTable>> tables = new LinkedList<>();
        for (int i = 0; i < args.length; i += 2) {
            tables.add(new Pair<>((String) args[i], (VoltTable) args[i + 1], false));
        }
        return executeSql(sql, tables);
    }

    private static VoltTable executeSql(String sql, List<Pair<String, VoltTable>> tables) {
        VoltTableData.Database db = new VoltTableData.Database();
        ImmutableList.Builder<VoltTableData.Table> builder = ImmutableList.builder();
        tables.forEach(pair -> builder.add(new VoltTableData.Table(pair)));
        db.tables = builder.build();

        String uuid = UUID.randomUUID().toString();
        VoltTableData.SCHEMA.put(uuid, db);

        String connectString = String.format("jdbc:calcite:schemaFactory=org.voltdb.volttableutil.VoltTableSchemaFactory;schema.id=%s",
                uuid);

        try (Connection connection =
                     DriverManager.getConnection(connectString)) {

            Statement st = connection.createStatement();
            ResultSet result = st.executeQuery(sql);
            return resultSetToVoltTable(result);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            VoltTableData.SCHEMA.remove(uuid);
        }
        return null;
    }

    private static VoltTable resultSetToVoltTable(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return null;
        }
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        VoltTable.ColumnInfo[] columnInfoArray = new VoltTable.ColumnInfo[resultSetMetaData.getColumnCount()];
        for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
            // TODO: verify the type convert
            columnInfoArray[i] = new VoltTable.ColumnInfo(resultSetMetaData.getColumnName(i + 1),
                    ColumnType.getVoltType(SqlTypeName.getNameForJdbcType(resultSetMetaData.getColumnType(i + 1))));
        }
        VoltTable vt = new VoltTable(columnInfoArray);
        while (resultSet.next()) {
            Object[] row = new Object[resultSetMetaData.getColumnCount()];
            for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                Object value = resultSet.getObject(i + 1);
                if (resultSet.wasNull()) {
                    value = ColumnType.getVoltType(SqlTypeName.getNameForJdbcType(resultSetMetaData.getColumnType(i + 1)))
                            .getNullValue();
                }
                row[i] = value;
            }
            vt.addRow(row);
        }
        resultSet.close();
        return vt;
    }
}
