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

import org.apache.calcite.jdbc.Driver;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.jdbc.CalciteConnection;
import org.voltcore.utils.Pair;
import org.voltdb.VoltTable;
import org.voltdb.plannerv2.ColumnTypes;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * The utility class that allow user to run SQL query on {@link VoltTable}.
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
     * @param args a list of {@link String} table_name and {@link VoltTable}
     *             volt_table pairs. The table_name is always followed by
     *             the volt_table it assigned to.
     * @return A result set represented in {@link VoltTable}.
     */
    public static VoltTable executeSql(String sql, Object... args) throws Exception {
        if (args.length % 2 == 1) {
            throw new IllegalArgumentException("Argument number not correct");
        }
        List<Pair<String, VoltTable>> tables = new LinkedList<>();
        for (int i = 0; i < args.length; i += 2) {
            tables.add(new Pair<>((String) args[i], (VoltTable) args[i + 1], false));
        }
        return executeSql(sql, tables);
    }

    public static VoltTable executeSql(String sql, List<Pair<String, VoltTable>> tables) throws Exception {
        VoltTableData.Database db = new VoltTableData.Database();
        ImmutableList.Builder<VoltTableData.Table> builder = ImmutableList.builder();
        tables.forEach(pair -> builder.add(new VoltTableData.Table(pair)));
        db.tables = builder.build();

        String uuid = UUID.randomUUID().toString();
        VoltTableData.SCHEMA.put(uuid, db);

        String connectString = String.format("jdbc:calcite:");

        Properties info = new Properties();

        info.setProperty("schemaFactory", "org.voltdb.volttableutil.VoltTableSchemaFactory");
        info.setProperty("schema.id", uuid);

        try {
            final Driver driver = new Driver();
            CalciteConnection connection = (CalciteConnection)
                driver.connect(connectString, info);

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
                    ColumnTypes.getVoltType(SqlTypeName.getNameForJdbcType(resultSetMetaData.getColumnType(i + 1))));
        }
        VoltTable vt = new VoltTable(columnInfoArray);
        while (resultSet.next()) {
            Object[] row = new Object[resultSetMetaData.getColumnCount()];
            for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                Object value = resultSet.getObject(i + 1);
                if (resultSet.wasNull()) {
                    value = ColumnTypes.getVoltType(SqlTypeName.getNameForJdbcType(resultSetMetaData.getColumnType(i + 1)))
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
