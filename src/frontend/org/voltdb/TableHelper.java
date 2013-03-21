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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.Encoder;
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

    /**
     * Index representation for schema building purposes.
     * Not presently used much, but will be part of expanded schema change tests.
     */
    static class IndexRep {
        public final VoltTable table;
        public final String indexName;
        public final Integer[] columns;
        public boolean unique = false;

        public IndexRep(VoltTable table, String indexName, Integer... columns) {
            this.table = table;
            this.indexName = indexName;
            this.columns = columns;
        }

        public String ddl(String indexName) {
            String ddl = "CREATE ";
            ddl += unique ? "UNIQUE " : "";
            ddl += "INDEX " + indexName + " ON ";
            ddl += table.m_name + " (";
            String[] colNames = new String[columns.length];
            for (int i = 0; i < columns.length; i++) {
                colNames[i] = table.getColumnName(columns[i]);
            }
            ddl += StringUtils.join(colNames, ", ") + ");";
            return ddl;
        }
    }

    /**
     * Package together a VoltTable with indexes for testing.
     * Not presently used much, but will be part of expanded schema change tests.
     */
    class IndexedTable {
        public VoltTable table;
        public ArrayList<IndexRep> indexes = new ArrayList<IndexRep>();

        public String ddl() {
            String ddl = TableHelper.ddlForTable(table) + "\n";
            for (int i = 0; i < indexes.size(); i++) {
                ddl += indexes.get(i).ddl("IDX" + String.valueOf(i)) + "\n";
            }
            return ddl;
        }
    }

    /** Get a table from shorthand using TableShorthand */
    public static VoltTable quickTable(String shorthand) {
        return TableShorthand.tableFromShorthand(shorthand);
    }

    /**
     * Get a sorted copy of a VoltTable. This is not guaranteed to be in any
     * particular order. It's also rather slow, as implementations go. The constraint
     * is that if you sort two tables with the same rows, but in different orders,
     * the two sorted tables will have identical contents. Useful for tests
     * more than for production.
     *
     * @param table Input table.
     * @return A new table containing the data from the old table in sorted order.
     */
    public static VoltTable sortTable(VoltTable table) {
        // get all of the rows of the source table as a giant array
        Object[][] rows = new Object[table.getRowCount()][];
        table.resetRowPosition();
        int row = 0;
        while (table.advanceRow()) {
            rows[row] = new Object[table.getColumnCount()];
            for (int column = 0; column < table.getColumnCount(); column++) {
                rows[row][column] = table.get(column, table.getColumnType(column));
                if (table.wasNull()) {
                    rows[row][column] = null;
                }
            }
            row++;
        }

        // sort the rows of the table
        Arrays.sort(rows, new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
                for (int i = 0; i < o1.length; i++) {
                    // normally bad, but here this should be true
                    assert(o1.length == o2.length);

                    // handle both null or very lucky otherwise
                    if (o1[i] == o2[i]) {
                        continue;
                    }

                    // handle one is null
                    if (o1[i] == null) {
                        return -1;
                    }
                    if (o2[i] == null) {
                        return 1;
                    }
                    // assume neither null
                    int cmp;

                    // handle varbinary comparisons
                    if (o1[i] instanceof byte[]) {
                        assert(o2[i] instanceof byte[]);
                        String hex1 = Encoder.hexEncode((byte[]) o1[i]);
                        String hex2 = Encoder.hexEncode((byte[]) o2[i]);
                        cmp = hex1.compareTo(hex2);
                    }
                    // generic case
                    else {
                        cmp = o1[i].toString().compareTo(o2[i].toString());
                    }

                    if (cmp != 0) {
                        return cmp;
                    }
                }

                // they're equal
                return 0;
            }
        });

        // clone the table
        VoltTable.ColumnInfo columns[] = new VoltTable.ColumnInfo[table.getColumnCount()];
        for (int column = 0; column < table.getColumnCount(); column++) {
            columns[column] = new VoltTable.ColumnInfo(table.getColumnName(column),
                    table.getColumnType(column));
        }
        VoltTable retval = new VoltTable(columns);

        // add the sorted rows to the new table
        for (Object[] rowArray : rows) {
            retval.addRow(rowArray);
        }
        return retval;
    }

    /**
     * Compare two tables using the data inside them, rather than simply comparing the underlying
     * buffers. This is slightly more tolerant of floating point issues than {@link VoltTable#hasSameContents(VoltTable)}.
     * It's also much slower than comparing buffers.
     *
     * Note, this will reset the row position of both tables.
     *
     * @param t1 {@link VoltTable} 1
     * @param t2 {@link VoltTable} 2
     * @return true if the tables are equal.
     * @see TableHelper#deepEquals(VoltTable, VoltTable) deepEquals
     */
    public static boolean deepEquals(VoltTable t1, VoltTable t2) {
        return deepEqualsWithErrorMsg(t1, t2, null);
    }

    /**
     * <p>Compare two tables using the data inside them, rather than simply comparing the underlying
     * buffers. This is slightly more tolerant of floating point issues than {@link VoltTable#hasSameContents(VoltTable)}.
     * It's also much slower than comparing buffers.</p>
     *
     * <p>This will also add a specific error message to the provided {@link StringBuilder} that explains how
     * the tables are different, printing out values if needed.</p>
     *
     * @param t1 {@link VoltTable} 1
     * @param t2 {@link VoltTable} 2
     * @param sb A {@link StringBuilder} to append the error message to.
     * @return true if the tables are equal.
     * @see TableHelper#deepEquals(VoltTable, VoltTable) deepEquals
     */
    public static boolean deepEqualsWithErrorMsg(VoltTable t1, VoltTable t2, StringBuilder sb) {
        // allow people to pass null without guarding everything with if statements
        if (sb == null) {
            sb = new StringBuilder();
        }

        // this behaves like an equals method should, but feels wrong here... alas...
        if ((t1 == null) && (t2 == null)) {
            return true;
        }

        // handle when one side is null
        if (t1 == null) {
            sb.append("t1 == NULL\n");
            return false;
        }
        if (t2 == null) {
            sb.append("t2 == NULL\n");
            return false;
        }

        if (t1.getRowCount() != t2.getRowCount()) {
            sb.append(String.format("Row count %d != %d\n", t1.getRowCount(), t2.getRowCount()));
            return false;
        }
        if (t1.getColumnCount() != t2.getColumnCount()) {
            sb.append(String.format("Col count %d != %d\n", t1.getColumnCount(), t2.getColumnCount()));
            return false;
        }
        for (int col = 0; col < t1.getColumnCount(); col++) {
            if (t1.getColumnType(col) != t2.getColumnType(col)) {
                sb.append(String.format("Column %d: type %s != %s\n", col,
                        t1.getColumnType(col).toString(), t2.getColumnType(col).toString()));
                return false;
            }
            if (t1.getColumnName(col).equals(t2.getColumnName(col)) == false) {
                sb.append(String.format("Column %d: name %s != %s\n", col,
                        t1.getColumnName(col), t2.getColumnName(col)));
                return false;
            }
        }

        t1.resetRowPosition();
        t2.resetRowPosition();
        for (int row = 0; row < t1.getRowCount(); row++) {
            t1.advanceRow();
            t2.advanceRow();

            for (int col = 0; col < t1.getColumnCount(); col++) {
                Object obj1 = t1.get(col, t1.getColumnType(col));
                if (t1.wasNull()) {
                    obj1 = null;
                }

                Object obj2 = t2.get(col, t2.getColumnType(col));
                if (t2.wasNull()) {
                    obj2 = null;
                }

                if ((obj1 == null) && (obj2 == null)) {
                    continue;
                }

                if ((obj1 == null) || (obj2 == null)) {
                    sb.append(String.format("Row,Col-%d,%d of type %s: %s != %s\n", row, col,
                            t1.getColumnType(col).toString(), String.valueOf(obj1), String.valueOf(obj2)));
                    return false;
                }

                if (t1.getColumnType(col) == VoltType.VARBINARY) {
                    byte[] array1 = (byte[]) obj1;
                    byte[] array2 = (byte[]) obj2;
                    if (Arrays.equals(array1, array2) == false) {
                        sb.append(String.format("Row,Col-%d,%d of type %s: %s != %s\n", row, col,
                                t1.getColumnType(col).toString(),
                                Encoder.hexEncode(array1),
                                Encoder.hexEncode(array2)));
                        return false;
                    }
                }
                else {
                    if (obj1.equals(obj2) == false) {
                        sb.append(String.format("Row,Col-%d,%d of type %s: %s != %s\n", row, col,
                                t1.getColumnType(col).toString(), obj1.toString(), obj2.toString()));
                        return false;
                    }
                }
            }
        }

        // true means we made it through the gaundlet and the tables are, fwiw, identical
        return true;
    }

    /**
     * Helper function for getTotallyRandomTable that makes random columns.
     */
    protected static VoltTable.ColumnInfo getRandomColumn(String name, Random rand) {
        VoltType[] allTypes = { VoltType.BIGINT, VoltType.DECIMAL, VoltType.FLOAT,
                VoltType.INTEGER, VoltType.SMALLINT, VoltType.STRING,
                VoltType.TIMESTAMP, VoltType.TINYINT, VoltType.VARBINARY };

        // random type
        VoltTable.ColumnInfo column = new VoltTable.ColumnInfo(name, allTypes[rand.nextInt(allTypes.length)]);

        // random sizes
        column.size = 0;
        if ((column.type == VoltType.VARBINARY) || (column.type == VoltType.STRING)) {
            // pick a column size with 50% inline and 50% out of line
            if (rand.nextBoolean()) {
                // pick a random number between 1 and 63 inclusive
                column.size = rand.nextInt(63) + 1;
            }
            else {
                // gaussian with stddev on 1024 (though offset by 64) and max of 1mb
                column.size = Math.min(64 + (int) (Math.abs(rand.nextGaussian()) * (1024 - 64)), 1024 * 1024);
            }
        }

        // nullable or default valued?
        Object defaultValue = null;
        if (rand.nextBoolean()) {
            column.nullable = true;
            defaultValue = VoltTypeUtil.getRandomValue(column.type, Math.max(column.size % 128, 1), 0.8, rand);
        }
        else {
            column.nullable = false;
            defaultValue = VoltTypeUtil.getRandomValue(column.type, Math.max(column.size % 128, 1), 0.0, rand);
            // no uniques for now, as the random fill becomes too slow
            //column.unique = (r.nextDouble() > 0.3); // 30% of non-nullable cols unique (15% total)
        }
        if (defaultValue != null) {
            column.defaultValue = String.valueOf(defaultValue);
        }
        else {
            column.defaultValue = null;
        }

        // these two columns need to be nullable with no default value
        if ((column.type == VoltType.VARBINARY) || (column.type == VoltType.DECIMAL)) {
            column.defaultValue = null;
            column.nullable = true;
        }

        assert(column.name != null);
        assert(column.size >= 0);
        if((column.type == VoltType.STRING) || (column.type == VoltType.VARBINARY)) {
            assert(column.size >= 0);
        }

        return column;
    }

    /**
     * Generate a totally random (valid) schema.
     * One constraint is that it will have a single bigint pkey somewhere.
     * For now, no non-pkey unique columns.
     */
    public static VoltTable getTotallyRandomTable(String name, Random rand) {
        // pick a number of cols between 1 and 1000, with most tables < 25 cols
        int numColumns = Math.max(1, Math.min(Math.abs((int) (rand.nextGaussian() * 25)), 1000));

        // make random columns
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[numColumns];
        for (int i = 0; i < numColumns; i++) {
            columns[i] = getRandomColumn(String.format("C%d", i), rand);
        }

        // pick pkey and make it a bigint
        int pkeyIndex = rand.nextInt(numColumns);
        columns[pkeyIndex] = new VoltTable.ColumnInfo("PKEY", VoltType.BIGINT);
        columns[pkeyIndex].pkeyIndex = 0;
        columns[pkeyIndex].size = 0;
        columns[pkeyIndex].nullable = false;
        columns[pkeyIndex].unique = true;

        // return the table from the columns
        VoltTable t = new VoltTable(columns);
        t.m_name = name;
        return t;
    }

    /**
     * Helper method for mutateTable
     */
    private static VoltTable.ColumnInfo growColumn(VoltTable.ColumnInfo oldCol) {
        VoltTable.ColumnInfo newCol = null;
        switch (oldCol.type) {
        case TINYINT:
            newCol = new VoltTable.ColumnInfo(oldCol.name, VoltType.SMALLINT);
            break;
        case SMALLINT:
            newCol = new VoltTable.ColumnInfo(oldCol.name, VoltType.INTEGER);
            break;
        case INTEGER:
            newCol = new VoltTable.ColumnInfo(oldCol.name, VoltType.BIGINT);
            break;
        case VARBINARY: case STRING:
            if (oldCol.size < 63) {
                newCol = new VoltTable.ColumnInfo(oldCol.name, oldCol.type);
                newCol.size = oldCol.size + 1;
            }
            // skip size 63 for now due to a bug
            if ((oldCol.size > 63) && (oldCol.size < VoltType.MAX_VALUE_LENGTH)) {
                newCol = new VoltTable.ColumnInfo(oldCol.name, oldCol.type);
                newCol.size = oldCol.size + 1;
            }
            break;
        default:
            // do nothing
            break;
        }

        if (newCol != null) {
            newCol.defaultValue = oldCol.defaultValue;
            newCol.nullable = oldCol.nullable;
            newCol.unique = oldCol.unique;
        }

        return newCol;
    }

    /**
     * Support method for mutateTable
     */
    private static int getNextColumnIndex(VoltTable table) {
        int max = 0;

        for (int i = 0; i < table.getColumnCount(); i++) {
            String name = table.getColumnName(i);
            if (name.startsWith("NEW")) {
                name = name.substring(3);
                int index = Integer.parseInt(name);
                if (index > max) {
                    max = index;
                }
            }
        }

        return max + 1;
    }

    /**
     * Given a VoltTable with schema metadata, return a new VoltTable with schema
     * metadata that had been changed slightly.
     *
     * Four kinds of changes will be aplied:
     * 1. Dropping columns.
     * 2. Adding columns.
     * 3. Widening columns.
     * 4. Re-ordering columns.
     */
    public static VoltTable mutateTable(VoltTable table, boolean allowIdenty, Random rand) {
        int totalMutations = 0;
        int columnDrops;
        int columnAdds;
        int columnGrows;
        int columnReorders;

        // pick values for the various kinds of mutations
        // don't allow all zeros unless allowIdentidy == true
        do {
            columnDrops =    Math.min((int) (Math.abs(rand.nextGaussian()) * 1.5), table.m_colCount);
            columnAdds =     Math.min((int) (Math.abs(rand.nextGaussian()) * 1.5), table.m_colCount);
            columnGrows =    Math.min((int) (Math.abs(rand.nextGaussian()) * 1.5), table.m_colCount);
            columnReorders = Math.min((int) (Math.abs(rand.nextGaussian()) * 1.5), table.m_colCount);
            totalMutations = columnDrops + columnAdds + columnGrows + columnReorders;
        }
        while ((allowIdenty == false) && (totalMutations == 0));

        System.out.printf("Mutations: %d %d %d %d\n", columnDrops, columnAdds, columnGrows, columnReorders);

        ArrayList<VoltTable.ColumnInfo> columns = new ArrayList<VoltTable.ColumnInfo>();
        for (int i = 0; i < table.m_originalColumnInfos.length; i++) {
            columns.add(table.m_originalColumnInfos[i].clone());
        }

        // limit tries to prevent looping forever
        int tries = columns.size() * 2;
        while ((columnDrops > 0) && (tries-- > 0)) {
            int indexToRemove = rand.nextInt(columns.size());
            VoltTable.ColumnInfo toRemove = columns.get(indexToRemove);
            if (toRemove.pkeyIndex == -1) {
                columnDrops--;
                columns.remove(indexToRemove);
            }
        }

        while (columnReorders > 0) {
            if (columns.size() > 1) {
                int srcIndex = rand.nextInt(columns.size());
                int destIndex;
                do {
                    destIndex = rand.nextInt(columns.size());
                } while (destIndex == srcIndex);
                columns.add(destIndex, columns.remove(srcIndex));
            }
            columnReorders--;
        }

        int newColIndex = getNextColumnIndex(table);
        while (columnAdds > 0) {
            int indexToAdd = rand.nextInt(columns.size());
            VoltTable.ColumnInfo toAdd = getRandomColumn(String.format("NEW%d", newColIndex++), rand);
            columnAdds--;
            columns.add(indexToAdd, toAdd);
        }

        // limit tries to prevent looping forever
        tries = columns.size() * 2;
        while ((columnGrows > 0) && (tries-- > 0)) {
            int indexToGrow = rand.nextInt(columns.size());
            VoltTable.ColumnInfo toGrow = columns.get(indexToGrow);
            if (toGrow.pkeyIndex != -1) continue;
            toGrow = growColumn(toGrow);
            if (toGrow != null) {
                columns.remove(indexToGrow);
                columns.add(indexToGrow, toGrow);
                columnGrows--;
            }
        }

        VoltTable t2 = new VoltTable(columns.toArray(new VoltTable.ColumnInfo[0]));
        t2.m_name = table.m_name;
        return t2;
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
            if (colInfo.defaultValue != VoltTable.ColumnInfo.NO_DEFAULT_VALUE) {
                col += " DEFAULT ";
                if (colInfo.defaultValue == null) {
                    col += "NULL";
                }
                else if (colInfo.type.isNumber()) {
                    col += colInfo.defaultValue;
                }
                else {
                    col += "'" + colInfo.defaultValue + "'";
                }
            }
            if (colInfo.nullable == false) {
                col += " NOT NULL";
            }
            if (colInfo.unique == true) {
                col += " UNIQUE";
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

    /**
     * Helper method for RandomFill
     */
    protected static Object[] randomRow(VoltTable table, int maxStringSize, Random rand) {
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
     * Fill a Java VoltTable with random values.
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
                    // handle no default specified
                    if (row[i] == TableShorthand.ColMeta.NO_DEFAULT_VALUE) {
                        if (dest.getColumnNullable(i)) {
                            row[i] = null;
                        }
                        else {
                            throw new RuntimeException(
                                    String.format("New column %s needs a default value in migration",
                                            dest.getColumnName(i)));
                        }
                    }
                }
                // make the values the core types of the target table
                VoltType destColType = dest.getColumnType(i);
                Class<?> descColClass = destColType.classFromType();
                row[i] = ParameterConverter.tryToMakeCompatible(false, descColClass.isArray(), descColClass,
                        descColClass.getComponentType(), row[i]);
            }

            dest.addRow(row);
        }
    }

    /**
     * Public access to the package-private metadata.
     */
    public static String getTableName(VoltTable table) {
        return table.m_name;
    }

    /**
     * Get the column index of the single bigint primary key column,
     * assuming the table metadata specified this.
     * Return -1 if not.
     */
    public static int getBigintPrimaryKeyIndexIfExists(VoltTable table) {
        // find the primary key
        int pkeyColIndex = 0;
        if (table.m_originalColumnInfos != null) {
            for (int i = 0; i < table.getColumnCount(); i++) {
                if (table.m_originalColumnInfos[i].pkeyIndex == 0) {
                    pkeyColIndex = i;
                }
                // no multi-column pkeys
                if (table.m_originalColumnInfos[i].pkeyIndex > 0) {
                    return -1;
                }
            }
            // bigint columns only for now
            if (table.m_originalColumnInfos[pkeyColIndex].type == VoltType.BIGINT) {
                return pkeyColIndex;
            }
        }
        return -1;
    }

    /**
     * Load random data into a partitioned table in VoltDB that has a biging pkey.
     *
     * If the VoltTable indicates which column is its pkey, then it will use it, but otherwise it will
     * assume the first column is the bigint pkey. Note, this works with other integer keys, but
     * your keyspace is pretty small.
     *
     * If mb == 0, then maxRows is used. If maxRows == 0, then mb is used.
     *
     * @param table Table with or without schema metadata.
     * @param mb Target RSS (approximate)
     * @param maxRows Target maximum rows
     * @param client To load with.
     * @param rand To generate random data with.
     * @param offset Generated pkey values start here.
     * @param jump Generated pkey values increment by this value.
     * @throws Exception
     */
    public static void fillTableWithBigintPkey(VoltTable table, int mb,
            long maxRows, final Client client, Random rand,
            long offset, long jump) throws Exception
    {
        // make sure some kind of limit is set
        assert((maxRows > 0) || (mb > 0));
        assert(maxRows >= 0);
        assert(mb >= 0);
        final int mbTarget = mb > 0 ? mb : Integer.MAX_VALUE;
        if (maxRows == 0) {
            maxRows = Long.MAX_VALUE;
        }

        System.out.printf("Filling table %s with rows starting with pkey id %d (every %d rows) until either RSS=%dmb or rowcount=%d\n",
                table.m_name, offset, jump, mbTarget, maxRows);

        // find the primary key, assume first col if not found
        int pkeyColIndex = getBigintPrimaryKeyIndexIfExists(table);
        if (pkeyColIndex == -1) {
            pkeyColIndex = 0;
            assert(table.getColumnType(0).isInteger());
        }

        final AtomicLong rss = new AtomicLong(0);

        ProcedureCallback insertCallback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    System.out.println("Error in loader callback:");
                    System.out.println(((ClientResponseImpl)clientResponse).toJSONString());
                    assert(false);
                }
            }
        };

        // update the rss value asynchronously
        final AtomicBoolean rssThreadShouldStop = new AtomicBoolean(false);
        Thread rssThread = new Thread() {
            @Override
            public void run() {
                long tempRss = rss.get();
                long rssPrev = tempRss;
                while (!rssThreadShouldStop.get()) {
                    tempRss = MiscUtils.getMBRss(client);
                    if (tempRss != rssPrev) {
                        rssPrev = tempRss;
                        rss.set(tempRss);
                        System.out.printf("RSS=%dmb\n", tempRss);
                        // bail when done
                        if (tempRss > mbTarget) {
                            return;
                        }
                    }
                    try { Thread.sleep(2000); } catch (Exception e) {}
                }
            }
        };

        // load rows until RSS goal is met (status print every 100k)
        long i = offset;
        long rows = 0;
        rssThread.start();
        final String insertProcName = table.m_name.toUpperCase() + ".insert";
        while (rss.get() < mbTarget) {
            Object[] row = randomRow(table, Integer.MAX_VALUE, rand);
            row[pkeyColIndex] = i;
            client.callProcedure(insertCallback, insertProcName, row);
            rows++;
            if ((rows % 100000) == 0) {
                System.out.printf("Loading 100000 rows. %d inserts sent (%d max id).\n", rows, i);
            }
            // if row limit is set, break if it's hit
            if (rows >= maxRows) {
                break;
            }
            i += jump;
        }
        rssThreadShouldStop.set(true);
        client.drain();
        rssThread.join();

        System.out.printf("Filled table %s with %d rows and now RSS=%dmb\n",
                table.m_name, rows, rss.get());
    }

    /**
     * Delete rows in a VoltDB table that has a bigint pkey where pkey values are odd.
     * Works best when pkey values are contiguous and start around 0.
     *
     * Exists mostly to force compaction on tables loaded with fillTableWithBigintPkey.
     * Though if you have an even number of sites, this won't work. It'll need to be
     * updated to delete some other pattern that's a bit more generic. Right now it
     * works great for my one-site testing.
     *
     */
    public static long deleteEveryNRows(VoltTable table, Client client, int n) throws Exception {
        // find the primary key, assume first col if not found
        int pkeyColIndex = getBigintPrimaryKeyIndexIfExists(table);
        if (pkeyColIndex == -1) {
            pkeyColIndex = 0;
            assert(table.getColumnType(0).isInteger());
        }
        String pkeyColName = table.getColumnName(pkeyColIndex);

        VoltTable result = client.callProcedure("@AdHoc",
                String.format("select %s from %s order by %s desc limit 1;",
                        pkeyColName, TableHelper.getTableName(table), pkeyColName)).getResults()[0];
        long maxId = result.getRowCount() > 0 ? result.asScalarLong() : 0;
        System.out.printf("Deleting odd rows with pkey ids in the range 0-%d\n", maxId);

        // track outstanding responses so 10k can be out at a time
        final AtomicInteger outstanding = new AtomicInteger(0);
        final AtomicLong deleteCount = new AtomicLong(0);

        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                outstanding.decrementAndGet();
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    System.out.println("Error in deleter callback:");
                    System.out.println(((ClientResponseImpl)clientResponse).toJSONString());
                    assert(false);
                }
                VoltTable result = clientResponse.getResults()[0];
                long modified = result.asScalarLong();
                assert(modified <= 1);
                deleteCount.addAndGet(modified);
            }
        };

        // delete 100k rows at a time until nothing comes back
        long deleted = 0;
        final String deleteProcName = table.m_name.toUpperCase() + ".delete";
        for (int i = 1; i <= maxId; i += n) {
            client.callProcedure(callback, deleteProcName, i);
            outstanding.incrementAndGet();
            deleted++;
            if ((deleted % 100000) == 0) {
                System.out.printf("Sent %d total delete invocations (%.1f%% of range).\n",
                        deleted, (i * 100.0) / maxId);
            }
            // block while 1000 txns are outstanding
            while (outstanding.get() >= 1000) {
                Thread.yield();
            }
        }
        // block until all calls have returned
        while (outstanding.get() > 0) {
            Thread.yield();
        }
        System.out.printf("Deleted %d odd rows\n", deleteCount.get());

        return deleteCount.get();
    }
}
