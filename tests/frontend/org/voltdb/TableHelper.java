/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.voltdb.utils.VoltTypeUtil;

public class TableHelper {

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

    public static VoltTable quickTable(String shorthand) {
        return TableShorthand.tableFromShorthand(shorthand);
    }

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
            Object[] row = new Object[table.getColumnCount()];
            Tuple pkey = new Tuple(pkeyIndexes.length);
            // build the row
            boolean success = false;
            trynewrow:
            while (!success) {
                // create a candidate row
                for (int col = 0; col < table.getColumnCount(); col++) {
                    boolean allowNulls = table.getColumnNullable(col);
                    int size = table.getColumnMaxSize(col);
                    if (size > maxStringSize) size = maxStringSize;
                    double nullFraction = allowNulls ? 0.05 : 0.0;
                    row[col] = VoltTypeUtil.getRandomValue(table.getColumnType(col), size, nullFraction, rand);
                    int pkeyIndex = ArrayUtils.indexOf(pkeyIndexes, col);
                    if (pkeyIndex != -1) {
                        pkey.values[pkeyIndex] = row[col];
                    }
                }

                // check pkey
                if (pkeyIndexes.length > 0) {
                    if (pkeyValues.contains(pkey)) {
                        System.err.println("randomFill: skipping tuple because of pkey violation");
                        continue trynewrow;
                    }
                }

                // check unique cols
                for (int col = 0; col < table.getColumnCount(); col++) {
                    Set<Object> uniqueColValues = uniqueValues.get(col);
                    if (uniqueColValues != null) {
                        if (uniqueColValues.contains(row[col])) {
                            System.err.println("randomFill: skipping tuple because of uniqe col violation");
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
}
