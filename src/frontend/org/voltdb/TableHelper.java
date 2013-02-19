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
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
     * Helper function for getTotallyRandomTable that makes random columns.
     */
    protected static VoltTable.ColumnInfo getRandomColumn(String name, Random r) {
        VoltType[] allTypes = { VoltType.BIGINT, VoltType.DECIMAL, VoltType.FLOAT,
                VoltType.INTEGER, VoltType.SMALLINT, VoltType.STRING,
                VoltType.TIMESTAMP, VoltType.TINYINT, VoltType.VARBINARY };

        // random type
        VoltTable.ColumnInfo column = new VoltTable.ColumnInfo(name, allTypes[r.nextInt(allTypes.length)]);

        // random sizes
        column.size = 0;
        if ((column.type == VoltType.VARBINARY) || (column.type == VoltType.STRING)) {
            // pick a column size with 50% inline and 50% out of line
            if (r.nextBoolean()) {
                // pick a random number between 1 and 63 inclusive
                column.size = r.nextInt(63) + 1;
            }
            else {
                // gaussian with stddev on 1024 (though offset by 64) and max of 1mb
                column.size = Math.min(64 + (int) (Math.abs(r.nextGaussian()) * (1024 - 64)), 1024 * 1024);
            }
        }

        // nullable or default valued?
        Object defaultValue = null;
        if (r.nextBoolean()) {
            column.nullable = true;
            defaultValue = VoltTypeUtil.getRandomValue(column.type, column.size % 128, 0.8, r);
        }
        else {
            column.nullable = false;
            defaultValue = VoltTypeUtil.getRandomValue(column.type, column.size % 128, 0.0, r);
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
     *
     * @param name
     * @param r
     * @return
     */
    public static VoltTable getTotallyRandomTable(String name, Random r) {
        // pick a number of cols between 1 and 1000, with most tables < 25 cols
        int numColumns = Math.max(1, Math.min(Math.abs((int) (r.nextGaussian() * 25)), 1000));

        // make random columns
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[numColumns];
        for (int i = 0; i < numColumns; i++) {
            columns[i] = getRandomColumn(String.format("C%d", i), r);
        }

        // pick pkey and make it a bigint
        int pkeyIndex = r.nextInt(numColumns);
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
     * Helper method for m
     */
    static VoltTable.ColumnInfo growColumn(VoltTable.ColumnInfo oldCol) {
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

    static int getNextColumnIndex(VoltTable t) {
        int max = 0;

        for (int i = 0; i < t.getColumnCount(); i++) {
            String name = t.getColumnName(i);
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
    public static VoltTable mutateTable(VoltTable t, Random r) {
        int columnDrops =    Math.min((int) (Math.abs(r.nextGaussian()) * 1.5), t.m_colCount);
        int columnAdds =     Math.min((int) (Math.abs(r.nextGaussian()) * 1.5), t.m_colCount);
        int columnGrows =    Math.min((int) (Math.abs(r.nextGaussian()) * 1.5), t.m_colCount);
        int columnReorders = Math.min((int) (Math.abs(r.nextGaussian()) * 1.5), t.m_colCount);

        System.out.printf("Mutations: %d %d %d %d\n", columnDrops, columnAdds, columnGrows, columnReorders);

        ArrayList<VoltTable.ColumnInfo> columns = new ArrayList<VoltTable.ColumnInfo>();
        for (int i = 0; i < t.m_originalColumnInfos.length; i++) {
            columns.add(t.m_originalColumnInfos[i].clone());
        }

        // limit tries to prevent looping forever
        int tries = columns.size() * 2;
        while ((columnDrops > 0) && (tries-- > 0)) {
            int indexToRemove = r.nextInt(columns.size());
            VoltTable.ColumnInfo toRemove = columns.get(indexToRemove);
            if (toRemove.pkeyIndex == -1) {
                columnDrops--;
                columns.remove(indexToRemove);
            }
        }

        while (columnReorders > 0) {
            if (columns.size() > 1) {
                int srcIndex = r.nextInt(columns.size());
                int destIndex;
                do {
                    destIndex = r.nextInt(columns.size());
                } while (destIndex == srcIndex);
                columns.add(destIndex, columns.remove(srcIndex));
            }
            columnReorders--;
        }

        int newColIndex = getNextColumnIndex(t);
        while (columnAdds > 0) {
            int indexToAdd = r.nextInt(columns.size());
            VoltTable.ColumnInfo toAdd = getRandomColumn(String.format("NEW%d", newColIndex++), r);
            columnAdds--;
            columns.add(indexToAdd, toAdd);
        }

        // limit tries to prevent looping forever
        tries = columns.size() * 2;
        while ((columnGrows > 0) && (tries-- > 0)) {
            int indexToGrow = r.nextInt(columns.size());
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
        t2.m_name = t.m_name;
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
    public static String getTableName(VoltTable t) {
        return t.m_name;
    }

    /**
     * Get the column index of the single bigint primary key column,
     * assuming the table metadata specified this.
     * Return -1 if not.
     */
    public static int getBigintPrimaryKeyIndexIfExists(VoltTable t) {
        // find the primary key
        int pkeyColIndex = 0;
        if (t.m_originalColumnInfos != null) {
            for (int i = 0; i < t.getColumnCount(); i++) {
                if (t.m_originalColumnInfos[i].pkeyIndex == 0) {
                    pkeyColIndex = i;
                }
                // no multi-column pkeys
                if (t.m_originalColumnInfos[i].pkeyIndex > 0) {
                    return -1;
                }
            }
            // bigint columns only for now
            if (t.m_originalColumnInfos[pkeyColIndex].type == VoltType.BIGINT) {
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
     * @param t Table with or without schema metadata.
     * @param mb Target RSS (approximate)
     * @param client To load with.
     * @param rand To generate random data with.
     * @param offet Generated pkey values start here.
     * @param jump Generated pkey values increment by this value.
     * @throws Exception
     */
    public static void fillTableWithBigintPkey(VoltTable t, final int mb, final Client client, Random rand, long offset, long jump) throws Exception {
        System.out.printf("Filling table %s with rows starting with pkey id %d (every %d rows) until RSS=%dmb\n",
                t.m_name, offset, jump, mb);

        // find the primary key, assume first col if not found
        int pkeyColIndex = getBigintPrimaryKeyIndexIfExists(t);
        if (pkeyColIndex == -1) {
            pkeyColIndex = 0;
            assert(t.getColumnType(0).isInteger());
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
        Thread rssThread = new Thread() {
            @Override
            public void run() {
                long tempRss = rss.get();
                long rssPrev = tempRss;
                while (true) {
                    tempRss = MiscUtils.getMBRss(client);
                    if (tempRss != rssPrev) {
                        rssPrev = tempRss;
                        rss.set(tempRss);
                        System.out.printf("RSS=%dmb\n", tempRss);
                        // bail when done
                        if (tempRss > mb) {
                            return;
                        }
                    }
                    try { Thread.sleep(2000); } catch (Exception e) {}
                }
            }
        };

        // load 100k rows at a time until RSS goal is met
        long i = offset;
        long rows = 0;
        rssThread.start();
        while (rss.get() < mb) {
            Object[] row = randomRow(t, Integer.MAX_VALUE, rand);
            row[pkeyColIndex] = i;
            i += jump;
            client.callProcedure(insertCallback, t.m_name.toUpperCase() + ".insert", row);
            rows++;
            if ((rows % 100000) == 0) {
                System.out.printf("Loading 100000 rows. %d inserts sent (%d max id).\n", rows, i - 1);
            }
        }
        client.drain();
        rssThread.join();

        System.out.printf("Filled table %s with %d rows and now RSS=%dmb\n",
                t.m_name, rows, rss.get());
    }

    /**
     * Delete rows in a VoltDB table that has a bigint pkey where pkey values are odd.
     * Works best when pkey values are contiguous and start around 0.
     *
     */
    public static void deleteOddRows(VoltTable t, Client client) throws Exception {
        // find the primary key, assume first col if not found
        int pkeyColIndex = getBigintPrimaryKeyIndexIfExists(t);
        if (pkeyColIndex == -1) {
            pkeyColIndex = 0;
            assert(t.getColumnType(0) == VoltType.BIGINT);
        }

        VoltTable result = client.callProcedure("@AdHoc",
                String.format("select max(pkey) from %s;", TableHelper.getTableName(t))).getResults()[0];
        long maxId = result.asScalarLong();
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
        for (int i = 1; i <= maxId; i += 2) {
            client.callProcedure(callback, t.m_name.toUpperCase() + ".delete", i);
            outstanding.incrementAndGet();
            deleted++;
            if ((deleted % 100000) == 0) {
                System.out.printf("Deleting 100000 pkeys. %d total deleted (%.1f%% of range).\n",
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
    }
}
