/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTypeUtil;

/**
 * Set of utility methods to make writing test code with VoltTables easier.
 */
public class TableHelper {

    /** Used for unique constraint checking, mostly pkeys */
    static class Tuple {
        final Object[] values;

        Tuple(int size) {
            values = new Object[size];
        }

        @Override
        public boolean equals(Object obj) {
            if ((obj instanceof Tuple) == false) {
                return false;
            }
            Tuple other = (Tuple) obj;
            return Arrays.deepEquals(values, other.values);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(values);
        }
    }

    /** Get a table from shorthand using TableShorthand */
    public static VoltTable quickTable(String shorthand) {
        return TableShorthand.tableFromShorthand(shorthand);
    }

    /**
     * Get the DDL for a table.
     * Only works with tables created with TableHelper.quickTable(..) above.
     */
    public static String ddlForTable(VoltTable table) {
        assert(table.m_originalColumnInfos != null);

        // for each column, one line
        String[] colLines = new String[table.m_originalColumnInfos.length];
        for (int i = 0; i < table.m_originalColumnInfos.length; i++) {
            VoltTable.ColumnInfo colInfo = table.m_originalColumnInfos[i];
            String col = colInfo.name + " " + colInfo.type.toSQLString().toUpperCase();
            if ((colInfo.type == VoltType.STRING) || (colInfo.type == VoltType.VARBINARY)) {
                col += String.format("(%d)", colInfo.size);
            }
            if (colInfo.nullable == false) {
                col += " NOT NULL";
            }
            if (colInfo.unique == true) {
                col += " UNIQUE";
            }
            if (colInfo.defaultValue != VoltTable.ColumnInfo.NO_DEFAULT_VALUE) {
                col += " DEFAULT ";
                if (colInfo.type.isNumber()) {
                    col += colInfo.defaultValue;
                }
                else {
                    col += "'" + colInfo.defaultValue + "'";
                }
            }
            colLines[i] = col;
        }

        String s = "CREATE TABLE " + table.m_name + " (\n  ";
        s += StringUtils.join(colLines, ",\n  ");

        // pkey line
        int[] pkeyIndexes = table.getPkeyColumnIndexes();
        if (pkeyIndexes.length > 0) {
            s += ",\n  PRIMARY KEY (";
            String[] pkeyColNames = new String[pkeyIndexes.length];
            for (int i = 0; i < pkeyColNames.length; i++) {
                pkeyColNames[i] = table.getColumnName(pkeyIndexes[i]);
            }
            s += StringUtils.join(pkeyColNames, ",");
            s += ")";
        }

        s += "\n);";

        return s;
    }

    static Object[] randomRow(VoltTable table, int maxStringSize, Random rand) {
        Object[] row = new Object[table.getColumnCount()];
        for (int col = 0; col < table.getColumnCount(); col++) {
            boolean allowNulls = table.getColumnNullable(col);
            int size = table.getColumnMaxSize(col);
            if (size > maxStringSize) size = maxStringSize;
            double nullFraction = allowNulls ? 0.05 : 0.0;
            row[col] = VoltTypeUtil.getRandomValue(table.getColumnType(col), size, nullFraction, rand);
        }
        return row;
    }

    /**
     * Fill a table with random values.
     * If created with TableHelper.quickTable(..), then it will respect
     * unique columns, pkey uniqueness, column widths and nullability
     *
     */
    public static void randomFill(VoltTable table, int rowCount, int maxStringSize, Random rand) {
        int[] pkeyIndexes = table.getPkeyColumnIndexes();

        Set<Tuple> pkeyValues = new HashSet<Tuple>();

        // figure out which columns must have unique values
        Map<Integer, Set<Object>> uniqueValues = new TreeMap<Integer, Set<Object>>();
        for (int col = 0; col < table.getColumnCount(); col++) {
            if (table.getColumnUniqueness(col)) {
                uniqueValues.put(col, new HashSet<Object>());
            }
        }

        for (int i = 0; i < rowCount; i++) {
            Object[] row;
            Tuple pkey = new Tuple(pkeyIndexes.length);
            // build the row
            boolean success = false;
            trynewrow:
            while (!success) {
                // create a candidate row
                row = randomRow(table, maxStringSize, rand);

                // store pkey values for row
                for (int col = 0; col < table.getColumnCount(); col++) {
                    int pkeyIndex = ArrayUtils.indexOf(pkeyIndexes, col);
                    if (pkeyIndex != -1) {
                        pkey.values[pkeyIndex] = row[col];
                    }
                }

                // check pkey
                if (pkeyIndexes.length > 0) {
                    if (pkeyValues.contains(pkey)) {
                        //System.err.println("randomFill: skipping tuple because of pkey violation");
                        continue trynewrow;
                    }
                }

                // check unique cols
                for (int col = 0; col < table.getColumnCount(); col++) {
                    Set<Object> uniqueColValues = uniqueValues.get(col);
                    if (uniqueColValues != null) {
                        if (uniqueColValues.contains(row[col])) {
                            //System.err.println("randomFill: skipping tuple because of uniqe col violation");
                            continue trynewrow;
                        }
                    }
                }

                // update pkey
                if (pkeyIndexes.length > 0) {
                    pkeyValues.add(pkey);
                }

                // update unique cols
                for (int col = 0; col < table.getColumnCount(); col++) {
                    Set<Object> uniqueColValues = uniqueValues.get(col);
                    if (uniqueColValues != null) {
                        uniqueColValues.add(row[col]);
                    }
                }

                // add the row
                table.addRow(row);
                success = true;
            }
        }
    }

    /**
     * Java version of table schema change.
     * - Supports adding columns with default values (or null if none specified)
     * - Supports dropping columns.
     * - Supports widening of columns.
     *
     * Note, this might fail in wierd ways if you ask it to do more than what
     * the EE version can do. It's not really set up to test the negative
     * cases.
     */
    public static void migrateTable(VoltTable source, VoltTable dest) throws Exception {
        Map<Integer, Integer> indexMap = new TreeMap<Integer, Integer>();

        for (int i = 0; i < dest.getColumnCount(); i++) {
            String destColName = dest.getColumnName(i);
            for (int j = 0; j < source.getColumnCount(); j++) {
                String srcColName = source.getColumnName(j);
                if (srcColName.equals(destColName)) {
                    indexMap.put(i, j);
                }
            }
        }

        assert(dest.getRowCount() == 0);

        source.resetRowPosition();
        while (source.advanceRow()) {
            Object[] row = new Object[dest.getColumnCount()];
            // get the values from the source table or defaults
            for (int i = 0; i < dest.getColumnCount(); i++) {
                if (indexMap.containsKey(i)) {
                    int sourcePos = indexMap.get(i);
                    row[i] = source.get(sourcePos, source.getColumnType(sourcePos));
                }
                else {
                    row[i] = dest.getColumnDefaultValue(i);
                }
                // make the values the core types of the target table
                VoltType destColType = dest.getColumnType(i);
                Class<?> descColClass = destColType.classFromType();
                row[i] = ParameterConverter.tryToMakeCompatible(false, false, descColClass, null, row[i]);
            }

            dest.addRow(row);
        }
    }

    public static void fillTableWithFirstColIntegerPkey(VoltTable t, int mb, Client client, Random rand) throws Exception {
        final AtomicInteger outstanding = new AtomicInteger(0);

        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                outstanding.decrementAndGet();
                assert(clientResponse.getStatus() == ClientResponse.SUCCESS);
            }
        };

        int i = 0;
        while (MiscUtils.getMBRss(client) < mb) {
            System.out.println("Loading 10000 rows");
            for (int j = 0; j < 10000; j++) {
                Object[] row = randomRow(t, Integer.MAX_VALUE, rand);
                row[0] = i++;
                outstanding.incrementAndGet();
                client.callProcedure(callback, t.m_name.toUpperCase() + ".insert", row);
            }
            while (outstanding.get() > 0) {
                Thread.yield();
            }
        }
    }
}
