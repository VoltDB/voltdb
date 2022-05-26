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

package org.voltdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
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
 *
 * The static methods provide general table access and manipulation.
 *
 * The instance methods can do configurable deterministic mutations. The optional
 * Configuration object has properties that control schema generation and
 * mutation behavior, including an optional user-supplied randomizer.
 *
 * Static methods may become instance methods to support configuration options.
 */
public class TableHelper {

    private final Configuration m_config;
    private final Random m_rand;

    public TableHelper(Configuration config) {
        m_config = config != null ? config : new Configuration();
        m_rand = m_config.rand != null ? m_config.rand : new Random(m_config.seed);
    }

    public TableHelper() {
        m_config = new Configuration();
        m_rand = new Random(m_config.seed);
    }

    public enum RandomPartitioning {
        // 50% partitioned if a table is flagged partitionable
        RANDOM,
        // Caller handles partitioning, but informs when a table is partitioned
        CALLER
    }

    /**
     * Configuration properties to modify TableHelper instance behavior.
     */
    public static class Configuration {
        // Optional caller-supplied randomizer.
        public Random rand = null;
        // Random seed for locally-created Random object if no randomizer is provided.
        public int seed = 0;
        // Number of extra columns, e.g. to receive unique data and support unique indexes.
        public int numExtraColumns = 0;
        // Prefix given to extra columns.
        public String extraColumnPrefix = "CX";
        // Randomly partition in getTotallyRandomTable()
        public RandomPartitioning randomPartitioning = RandomPartitioning.RANDOM;
    }

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
     * Represents a simple materialized view used for test purposes.
     * For now, has one sum column, a count* and a single group by column.
     * No provision for manual creation, only the random creation from a
     * source table.
     */
    public static class ViewRep {
        public final String viewName;
        public final String sumColName;
        public final String groupColName;
        public final String srcTableName;

        protected ViewRep(String name, String sumColName, String groupColName, String srcTableName) {
            this.viewName = name;
            this.sumColName = sumColName;
            this.groupColName = groupColName;
            this.srcTableName = srcTableName;
        }

        public String ddlForView() {
            return String.format("CREATE VIEW %s (col1,col2,col3) AS " +
                    "SELECT %s, COUNT(*), SUM(%s) FROM %s GROUP BY %s;",
                    viewName, groupColName, sumColName, srcTableName, groupColName);
        }

        /**
         * Check if the view could apply to the provided table unchanged.
         */
        public boolean compatibleWithTable(VoltTable table) {
            String candidateName = getTableName(table);
            // table can't have the same name as the view
            if (candidateName.equals(viewName)) {
                return false;
            }
            // view is for a different table
            if (candidateName.equals(srcTableName) == false) {
                return false;
            }

            try {
                // ignore ret value here - just looking to not throw
                int groupColIndex = table.getColumnIndex(groupColName);
                VoltType groupColType = table.getColumnType(groupColIndex);
                if (groupColType == VoltType.DECIMAL) {
                    // no longer a good type to group
                    return false;
                }

                // check the sum col is still value
                int sumColIndex = table.getColumnIndex(sumColName);
                VoltType sumColType = table.getColumnType(sumColIndex);
                if ((sumColType == VoltType.TINYINT) ||
                        (sumColType == VoltType.SMALLINT) ||
                        (sumColType == VoltType.INTEGER)) {
                    return true;
                }
                else {
                    // no longer a good type to sum
                    return false;
                }
            }
            catch (IllegalArgumentException e) {
                // column index is bad
                return false;
            }
        }
    }

    /**
     * Create a random view based on a given table, or return null if no
     * good view is possible to create.
     */
    public ViewRep viewRepForTable(String name, VoltTable table) {
        String sumColName = null;
        String groupColName = null;

        // pick a sum column
        for (int colIndex = 0; colIndex < table.getColumnCount(); colIndex++) {
            VoltType type = table.getColumnType(colIndex);
            if ((type == VoltType.TINYINT) || (type == VoltType.SMALLINT) || (type == VoltType.INTEGER)) {
                sumColName = table.getColumnName(colIndex);
            }
        }
        if (sumColName == null) {
            return null;
        }

        // find all potential group by columns
        List<String> potentialGroupByCols = new ArrayList<String>();
        for (int colIndex = 0; colIndex < table.getColumnCount(); colIndex++) {
            String colName = table.getColumnName(colIndex);
            // skip the sum col
            if (colName.equals(sumColName)) {
                continue;
            }
            potentialGroupByCols.add(colName);
        }

        // no potential group by cols
        if (potentialGroupByCols.size() == 0) {
            return null;
        }

        // pick a random non-summing col to group on
        // could pick more than one to make this better in the future
        groupColName = potentialGroupByCols.get(m_rand.nextInt(potentialGroupByCols.size()));

        return new ViewRep(name, sumColName, groupColName, getTableName(table));
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
            ddl += table.m_extraMetadata.name + " (";
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

        public String ddl(boolean isStream) {
            String ddl = TableHelper.ddlForTable(table, isStream) + "\n";
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
    protected VoltTable.ColumnInfo getRandomColumn(String name) {
        VoltType[] allTypes = { VoltType.BIGINT, VoltType.DECIMAL, VoltType.FLOAT,
                VoltType.INTEGER, VoltType.SMALLINT, VoltType.STRING,
                VoltType.TIMESTAMP, VoltType.TINYINT, VoltType.VARBINARY };

        // random type
        VoltType type = allTypes[m_rand.nextInt(allTypes.length)];

        // random sizes
        int size = 0;
        if ((type == VoltType.VARBINARY) || (type == VoltType.STRING)) {
            // pick a column size with 50% inline and 50% out of line
            if (m_rand.nextBoolean()) {
                // pick a random number between 1 and 63 inclusive
                size = m_rand.nextInt(63) + 1;
            }
            else {
                // gaussian with stddev on 1024 (though offset by 64) and max of 1mb
                size = Math.min(64 + (int) (Math.abs(m_rand.nextGaussian()) * (1024 - 64)), 1024 * 1024);
            }
        }

        // nullable or default valued?
        Object defaultValue = null;
        boolean nullable = false;
        if (m_rand.nextBoolean()) {
            nullable = true;
            defaultValue = VoltTypeUtil.getRandomValue(type, Math.max(size % 128, 1), 0.8, m_rand);
        }
        else {
            nullable = false;
            defaultValue = VoltTypeUtil.getRandomValue(type, Math.max(size % 128, 1), 0.0, m_rand);
            // no uniques for now, as the random fill becomes too slow
            //column.unique = (r.nextDouble() > 0.3); // 30% of non-nullable cols unique (15% total)
        }
        if (defaultValue != null) {
            defaultValue = String.valueOf(defaultValue);
        }
        else {
            defaultValue = null;
        }

        // these two columns need to be nullable with no default value
        if ((type == VoltType.VARBINARY) || (type == VoltType.DECIMAL)) {
            defaultValue = null;
            nullable = true;
        }

        assert(name != null);
        assert(size >= 0);
        if((type == VoltType.STRING) || (type == VoltType.VARBINARY)) {
            assert(size >= 0);
        }

        return new VoltTable.ColumnInfo(name, type, size, nullable, false, (String) defaultValue);
    }

    /**
     * Generated table with extra information to help with testing against the table.
     */
    public static class RandomTable {

        public VoltTable table;
        // the PK bigint column is randomly chosen from set of random columns
        public int bigintPrimaryKey;
        public int numRandomColumns;
        // the extra columns immediately follow the random/PK columns
        public int numExtraColumns;

        public RandomTable() {
            this.table = null;
            this.bigintPrimaryKey = -1;
            this.numRandomColumns = 0;
            this.numExtraColumns = 0;
        }

        public RandomTable(VoltTable table, int bigintPrimaryKey, int numRandomColumns, int numExtraColumns) {
            this.table = table;
            this.bigintPrimaryKey = bigintPrimaryKey;
            this.numRandomColumns = numRandomColumns;
            this.numExtraColumns = numExtraColumns;
        }

        public RandomTable(final RandomTable other) {
            this.table = other.table;
            this.bigintPrimaryKey = other.bigintPrimaryKey;
            this.numRandomColumns = other.numRandomColumns;
            this.numExtraColumns = other.numExtraColumns;
        }

        public String getTableName() {
            return this.table.m_extraMetadata.name;
        }
    }

    /**
     * Generate a totally random (valid) schema.
     * One constraint is that it will have a single bigint pkey somewhere.
     * Generates extra BIGINT column(s) if enabled on non-partitioned tables.
     * See overloaded getTotallyRandomTable() for more info on partitioning.
     */
    public RandomTable getTotallyRandomTable(String name) {
        return getTotallyRandomTable(name, true);
    }

    /**
     * Generate a totally random (valid) schema.
     * One constraint is that it will have a single bigint pkey somewhere.
     * Generates extra BIGINT column(s) if enabled on non-partitioned tables.
     *
     * The partitioning logic has a couple of variations. It can be a 50%
     * random chance of being partitioned or decided by the caller. Randomly
     * partitioned VoltTable's are fully initialized based on the partitioning
     * choice. Caller-partitioned tables are set up as replicated tables,
     * which can be changed by the caller later. It does make sure to only add
     * extra unique columns when the table isn't or won't be partitioned.
     */
    public RandomTable getTotallyRandomTable(String name, boolean partition) {

        // pick a number of cols between 1 and 1000, with most tables < 25 cols
        int numRandomColumns = Math.max(1, Math.min(Math.abs((int) (m_rand.nextGaussian() * 25)), 1000));

        // partitioning is either random or handled by the caller (e.g. SchemaChangeClient)
        boolean partitioned = false;
        boolean partitionMetadata = true;
        if (partition) {
            switch(m_config.randomPartitioning) {
            case CALLER:
                partitioned = true;
                partitionMetadata = false;
                break;
            case RANDOM:
                partitioned = m_rand.nextBoolean();
                break;
            }
        }

        /*
         * Only add more column(s) if requested and the table is not partitioned,
         * because unique columns are complicated by partitioned tables.
         */
        int numExtraColumns = partitioned ? 0 : m_config.numExtraColumns;

        // make random columns
        int numColumnsTotal = numRandomColumns + numExtraColumns;
        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[numColumnsTotal];
        for (int i = 0; i < numRandomColumns; i++) {
            columns[i] = getRandomColumn(String.format("C%d", i));
        }

        // add optional extra column(s) (only BIGINT for now) for possible use as alternate keys.
        for (int i = 0; i < numExtraColumns; i++) {
            columns[numRandomColumns+i] = new VoltTable.ColumnInfo(
                    String.format("%s%d", m_config.extraColumnPrefix, i), VoltType.BIGINT, 20, false, false, null);
        }

        // pick pkey and make it a bigint
        int bigintPrimaryKey = m_rand.nextInt(numRandomColumns);
        columns[bigintPrimaryKey] = new VoltTable.ColumnInfo("PKEY",
                                                             VoltType.BIGINT,
                                                             0,
                                                             false,
                                                             true,
                                                             "0");
        int[] pkeyIndexes = new int[] { bigintPrimaryKey };

        // if partitionable and random partitioning is enabled, flip a coin
        int partitionColumn = partitioned && partitionMetadata ? pkeyIndexes[0] : -1;

        // return the table wrapped in a TableRep from the columns
        VoltTable.ExtraMetadata extraMetadata = new VoltTable.ExtraMetadata(name,
                                                                            partitionColumn,
                                                                            pkeyIndexes,
                                                                            columns);
        VoltTable table = new VoltTable(extraMetadata, columns, columns.length);
        return new RandomTable(table, bigintPrimaryKey, numRandomColumns, numExtraColumns);
    }

    /**
     * Helper method for mutateTable
     */
    private static VoltTable.ColumnInfo growColumn(VoltTable.ColumnInfo oldCol) {
        VoltTable.ColumnInfo newCol = null;
        switch (oldCol.type) {
        case TINYINT:
            newCol = new VoltTable.ColumnInfo(oldCol.name,
                                              VoltType.SMALLINT,
                                              oldCol.size,
                                              oldCol.nullable,
                                              oldCol.unique,
                                              oldCol.defaultValue);
            break;
        case SMALLINT:
            newCol = new VoltTable.ColumnInfo(oldCol.name,
                                              VoltType.INTEGER,
                                              oldCol.size,
                                              oldCol.nullable,
                                              oldCol.unique,
                                              oldCol.defaultValue);
            break;
        case INTEGER:
            newCol = new VoltTable.ColumnInfo(oldCol.name,
                                              VoltType.BIGINT,
                                              oldCol.size,
                                              oldCol.nullable,
                                              oldCol.unique,
                                              oldCol.defaultValue);
        case VARBINARY: case STRING:
            // skip size 63 for now due to a bug
            if ((oldCol.size != 63) && (oldCol.size < VoltType.MAX_VALUE_LENGTH)) {
                newCol = new VoltTable.ColumnInfo(oldCol.name,
                                                  oldCol.type,
                                                  oldCol.size + 1,
                                                  oldCol.nullable,
                                                  oldCol.unique,
                                                  oldCol.defaultValue);
            }
            break;
        default:
            // do nothing
            break;
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

    public static String getAlterTableDDLToMigrate(VoltTable t1, VoltTable t2) {
        assert(t1.m_extraMetadata.name.equals(t2.m_extraMetadata.name));

        StringBuilder ddl = new StringBuilder();

        // look for column type changes
        for (VoltTable.ColumnInfo t1Column : t1.m_extraMetadata.originalColumnInfos) {
            boolean found = false;
            for (VoltTable.ColumnInfo t2Column : t2.m_extraMetadata.originalColumnInfos) {
                // same column, even if position is different
                if (t1Column.name.equals(t2Column.name)) {
                    found = true;
                    if (!t1Column.equals(t2Column)) {
                        // DDL to change this column
                        ddl.append(String.format("ALTER TABLE %s ALTER COLUMN %s;\n", t1.m_extraMetadata.name, getDDLColumnDefinition(t2, t2Column)));
                    }
                }
            }
            if (!found) {
                ddl.append(String.format("ALTER TABLE %s DROP %s;\n", t1.m_extraMetadata.name, t1Column.name));
            }
        }

        for (int i = t2.m_extraMetadata.originalColumnInfos.length - 1; i >=0 ; i--) {
            VoltTable.ColumnInfo t2Column = t2.m_extraMetadata.originalColumnInfos[i];
            boolean found = false;
            for (VoltTable.ColumnInfo t1Column : t1.m_extraMetadata.originalColumnInfos) {
                // same column, even if position is different
                if (t1Column.name.equals(t2Column.name)) {
                    found = true;
                }
            }

            if (!found) {
                // DDL to add this column
                ddl.append(String.format("ALTER TABLE %s ADD COLUMN %s", t1.m_extraMetadata.name, getDDLColumnDefinition(t2, t2Column)));
                // if not the last column, add it before the next column
                if (i != t2.m_extraMetadata.originalColumnInfos.length - 1) {
                    VoltTable.ColumnInfo nextCol = t2.m_extraMetadata.originalColumnInfos[i + 1];
                    ddl.append(String.format(" BEFORE %s", nextCol.name));
                }
                ddl.append(";\n");
            }
        }

        return ddl.toString();
    }

    /** Is this column a member of the primary key? */
    static boolean isAPkeyColumn(VoltTable table, VoltTable.ColumnInfo column) {
        assert(table.m_extraMetadata != null);
        for (int pkeyIndex : table.m_extraMetadata.pkeyIndexes) {
            VoltTable.ColumnInfo indexColumn = table.m_extraMetadata.originalColumnInfos[pkeyIndex];
            if (indexColumn.name.equals(column.name)) {
                return true;
            }
        }
        return false;
    }

    /** Is this an extra column possibly used for alternate keys? */
    boolean isAnExtraColumn(VoltTable table, VoltTable.ColumnInfo column) {
        return column.name.startsWith(m_config.extraColumnPrefix);
    }

    /** Check if a unique column should be ASSUMEUNIQUE or UNIQUE */
    static boolean needsAssumeUnique(VoltTable table, VoltTable.ColumnInfo column) {
        // stupid safety
        if (column.unique == false) return false;

        // replicated tables can use UNIQUE
        if (table.m_extraMetadata.partitionColIndex == -1) {
            return false;
        }

        // find the index of this column in the table
        int colIndex = -1;
        for (int i = 0; i < table.m_extraMetadata.originalColumnInfos.length; i++) {
            if (column.equals(table.m_extraMetadata.originalColumnInfos[i])) {
                colIndex = i;
            }
        }
        assert(colIndex >= 0);

        // can use UNIQUE if the column is the partition column
        if (colIndex == table.m_extraMetadata.partitionColIndex) {
            return false;
        }

        boolean pkeyContainsPartitionColumn = false;
        boolean pkeyContainsThisColumn = false;
        for (int pkeyColIndex : table.m_extraMetadata.pkeyIndexes) {
            if (pkeyColIndex == table.m_extraMetadata.partitionColIndex) {
                pkeyContainsPartitionColumn = true;
            }
            if (pkeyColIndex == colIndex) {
                pkeyContainsThisColumn = true;
            }
        }
        // can use unique if this column is in the pkey and the pkey contains partition col
        if (pkeyContainsPartitionColumn && pkeyContainsThisColumn) {
            return false;
        }

        // needs to be ASSUMEUNIQUE
        return true;
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
    public VoltTable mutateTable(VoltTable table, boolean allowIdenty) {
        int totalMutations = 0;
        int columnDrops;
        int columnAdds;
        int columnGrows;

        int[] pkeyIndexes = table.m_extraMetadata.pkeyIndexes.clone();
        int partitionColIndex = table.m_extraMetadata.partitionColIndex;

        // pick values for the various kinds of mutations
        // don't allow all zeros unless allowIdentidy == true
        do {
            columnDrops =    Math.min((int) (Math.abs(m_rand.nextGaussian()) * 1.5), table.m_colCount);
            columnAdds =     Math.min((int) (Math.abs(m_rand.nextGaussian()) * 1.5), table.m_colCount);
            columnGrows =    Math.min((int) (Math.abs(m_rand.nextGaussian()) * 1.5), table.m_colCount);
            totalMutations = columnDrops + columnAdds + columnGrows;
        }
        while ((allowIdenty == false) && (totalMutations == 0));

        System.out.printf("Mutations: %d %d %d\n", columnDrops, columnAdds, columnGrows);

        ArrayList<VoltTable.ColumnInfo> columns = new ArrayList<VoltTable.ColumnInfo>();
        for (int i = 0; i < table.m_extraMetadata.originalColumnInfos.length; i++) {
            columns.add(table.m_extraMetadata.originalColumnInfos[i].clone());
        }

        //////////////////
        // DROP COLUMNS //

        // limit tries to prevent looping forever
        int tries = columns.size() * 2;
        while ((columnDrops > 0) && (tries-- > 0)) {
            // don't drop extra columns because they're used differently
            int indexToRemove = m_rand.nextInt(columns.size());
            VoltTable.ColumnInfo toRemove = columns.get(indexToRemove);
            if (isAPkeyColumn(table, toRemove)) continue;
            // don't drop extra columns used as alternate keys
            if (isAnExtraColumn(table, toRemove)) continue;
            columnDrops--;
            columns.remove(indexToRemove);

            if ((partitionColIndex >= 0) && (partitionColIndex > indexToRemove)) {
                partitionColIndex--;
            }
            for (int i = 0; i < pkeyIndexes.length; i++) {
                if (pkeyIndexes[i] > indexToRemove) {
                    pkeyIndexes[i]--;
                }
            }
        }

        /////////////////
        // ADD COLUMNS //

        int newColIndex = getNextColumnIndex(table);
        while (columnAdds > 0) {
            int indexToAdd = m_rand.nextInt(columns.size());
            VoltTable.ColumnInfo toAdd = getRandomColumn(String.format("NEW%d", newColIndex++));
            columnAdds--;
            columns.add(indexToAdd, toAdd);

            if ((partitionColIndex >= 0) && (partitionColIndex >= indexToAdd)) {
                partitionColIndex++;
            }
            for (int i = 0; i < pkeyIndexes.length; i++) {
                if (pkeyIndexes[i] >= indexToAdd) {
                    pkeyIndexes[i]++;
                }
            }
        }

        ///////////////////
        // WIDEN COLUMNS //

        // limit tries to prevent looping forever
        tries = columns.size() * 2;
        while ((columnGrows > 0) && (tries-- > 0)) {
            int indexToGrow = m_rand.nextInt(columns.size());
            VoltTable.ColumnInfo toGrow = columns.get(indexToGrow);
            if (isAPkeyColumn(table, toGrow)) continue;
            // don't change extra columns used as alternate keys
            if (isAnExtraColumn(table, toGrow)) continue;
            toGrow = growColumn(toGrow);
            if (toGrow != null) {
                columns.remove(indexToGrow);
                columns.add(indexToGrow, toGrow);
                columnGrows--;
            }
        }

        VoltTable.ColumnInfo[] columnArray = columns.toArray(new VoltTable.ColumnInfo[0]);
        VoltTable.ExtraMetadata extraMetadata = new VoltTable.ExtraMetadata(
                table.m_extraMetadata.name,
                partitionColIndex,
                pkeyIndexes,
                columnArray);

        return new VoltTable(extraMetadata, columnArray, columnArray.length);
    }

    /**
     * Get the DDL description for a column that can be used for CREATE TABLE
     * or ALTER TABLE
     */
    static String getDDLColumnDefinition(final VoltTable table, final VoltTable.ColumnInfo colInfo) {
        assert(colInfo != null);

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
            if (needsAssumeUnique(table, colInfo)) {
                col += " ASSUMEUNIQUE";
            }
            else {
                col += " UNIQUE";
            }
        }
        return col;
    }

    /**
     * Get the DDL for a table.
     * Only works with tables created with TableHelper.quickTable(..) above.
     */
    public static String ddlForTable(VoltTable table, boolean isStream) {
        assert(table.m_extraMetadata != null);

        // for each column, one line
        String[] colLines = new String[table.m_extraMetadata.originalColumnInfos.length];
        for (int i = 0; i < table.m_extraMetadata.originalColumnInfos.length; i++) {
            colLines[i] = getDDLColumnDefinition(table, table.m_extraMetadata.originalColumnInfos[i]);
        }

        String s = (isStream ? "CREATE STREAM " : "CREATE TABLE ") + table.m_extraMetadata.name;
        if (isStream) {
            // partition this table if need be
            if (table.m_extraMetadata.partitionColIndex != -1) {
                s += String.format("PARTITION ON COLUMN %s",
                        table.m_extraMetadata.name,
                        table.m_extraMetadata.originalColumnInfos[table.m_extraMetadata.partitionColIndex].name);
            }
        }
        s += " (\n  ";
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

        if (!isStream) {
            // partition this table if need be
            if (table.m_extraMetadata.partitionColIndex != -1) {
                s += String.format("\nPARTITION TABLE %s ON COLUMN %s;",
                        table.m_extraMetadata.name,
                        table.m_extraMetadata.originalColumnInfos[table.m_extraMetadata.partitionColIndex].name);
            }
        }

        return s;
    }

    public RandomRowMaker createRandomRowMaker(
                VoltTable table,
                int maxStringSize,
                boolean loadPrimaryKeys,
                boolean loadUniqueColumns) {
        String extraColumnPrefix = m_config.numExtraColumns > 0 ? m_config.extraColumnPrefix : null;
        return new RandomRowMaker(table, maxStringSize, m_rand, loadPrimaryKeys, loadUniqueColumns, extraColumnPrefix);
    }

    /**
     * Object to generate random row data that optionally satisfies uniqueness constraints.
     */
    public static class RandomRowMaker {
        final VoltTable table;
        final int maxStringSize;
        final Random rand;
        final int[] pkeyIndexes;
        final Set<Tuple> pkeyValues;
        final Map<Integer, Set<Object>> uniqueValues;

        /**
         * Row maker constructor
         * @param table provides column metadata
         * @param maxStringSize limit to string size
         * @param rand pre-seeded random number generator
         * @param loadPrimaryKeys if true handles PK uniqueness constraints
         * @param loadUniqueColumns if true handles other unique columns' uniqueness constraints
         */
        RandomRowMaker(
                VoltTable table,
                int maxStringSize,
                Random rand,
                boolean loadPrimaryKeys,
                boolean loadUniqueColumns,
                String extraColumnPrefix) {

            this.table = table;
            this.maxStringSize = maxStringSize;
            this.rand = rand;

            if (loadPrimaryKeys) {
                this.pkeyIndexes = this.table.getPkeyColumnIndexes();
                this.pkeyValues = new HashSet<Tuple>();
            }
            else {
                this.pkeyIndexes = null;
                this.pkeyValues = null;
            }

            // figure out which columns must have unique values
            if (loadUniqueColumns) {
                this.uniqueValues = new TreeMap<Integer, Set<Object>>();
                for (int col = 0; col < this.table.getColumnCount(); col++) {
                    // treat extra columns as unique for loading since they become alternate keys
                    if (this.table.getColumnUniqueness(col) ||
                        (extraColumnPrefix != null && this.table.getColumnName(col).startsWith(extraColumnPrefix))) {
                        this.uniqueValues.put(col, new HashSet<Object>());
                    }
                }
            }
            else {
                this.uniqueValues = null;
            }
        }

        /**
         * Generate purely random row data without accounting for uniqueness requirements.
         * @return row data
         */
        private Object[] randomRowData() {
            Object[] row = new Object[this.table.getColumnCount()];
            for (int col = 0; col < this.table.getColumnCount(); col++) {
                boolean allowNulls = this.table.getColumnNullable(col);
                int size = this.table.getColumnMaxSize(col);
                if (size > this.maxStringSize) {
                    size = this.maxStringSize;
                }
                double nullFraction = allowNulls ? 0.05 : 0.0;
                row[col] = VoltTypeUtil.getRandomValue(this.table.getColumnType(col), size, nullFraction, this.rand);
            }
            return row;
        }

        /**
         * Attempt to unique-ify the primary key if the feature is enabled.
         * @param row row data
         * @return true if successful or false if the key is not unique
         */
        private boolean handlePrimaryKey(Object[] row) {

            if (this.pkeyIndexes != null) {

                Tuple pkey = new Tuple(this.pkeyIndexes.length);

                for (int col = 0; col < this.table.getColumnCount(); col++) {
                    int pkeyIndex = ArrayUtils.indexOf(this.pkeyIndexes, col);
                    if (pkeyIndex != -1) {
                        pkey.values[pkeyIndex] = row[col];
                    }
                }

                // check pkey
                if (this.pkeyIndexes.length > 0) {
                    if (this.pkeyValues.contains(pkey)) {
                        //System.err.println("RandomRowFiller.handlePrimaryKey: skipping tuple because of pkey violation");
                        return false;
                    }
                }

                // update pkey
                if (this.pkeyIndexes.length > 0) {
                    this.pkeyValues.add(pkey);
                }
            }

            return true;
        }

        /**
         * Attempt to unique-ify the unique columns if the feature is enabled.
         * @param row row data
         * @return true if successful or false if a value is not unique
         */
        private boolean handleUniqueColumns(Object[] row) {

            if (this.uniqueValues != null) {

                // check unique cols
                for (int col = 0; col < this.table.getColumnCount(); col++) {
                    Set<Object> uniqueColValues = this.uniqueValues.get(col);
                    if (uniqueColValues != null) {
                        if (uniqueColValues.contains(row[col])) {
                            //System.err.println("RandomRowFiller.handleUniqueColumns: skipping tuple because of unique col violation");
                            return false;
                        }
                    }
                }

                // update unique cols
                for (int col = 0; col < this.table.getColumnCount(); col++) {
                    Set<Object> uniqueColValues = this.uniqueValues.get(col);
                    if (uniqueColValues != null) {
                        uniqueColValues.add(row[col]);
                    }
                }
            }

            return true;
        }

        /**
         * Fill a Java VoltTable row with random values.
         * If created with TableHelper.quickTable(..), then it will respect
         * unique columns, pkey uniqueness, column widths and nullability
         * The caller may handle loading the primary key.
         */
        public Object[] randomRow() {
            // build the row and retry until the PK and other unique columns have unique values
            while (true) {
                // create a candidate row.
                Object[] row = this.randomRowData();
                // make sure the PK and unique columns are satisfied
                if (this.handlePrimaryKey(row) && this.handleUniqueColumns(row)) {
                    // success
                    return row;
                }
            }
        }
    }

    /**
     * Fill a Java VoltTable with random values.
     * If created with TableHelper.quickTable(..), then it will respect
     * unique columns, pkey uniqueness, column widths and nullability
     *
     */
    public void randomFill(VoltTable table, int rowCount, int maxStringSize) {
        // add the requested number of random rows generated using a filler
        RandomRowMaker filler = createRandomRowMaker(table, maxStringSize, true, true);
        for (int i = 0; i < rowCount; i++) {
            table.addRow(filler.randomRow());
        }
    }

    /**
     * Java version of table schema change.
     * - Supports adding columns with default values (or null if none specified)
     * - Supports dropping columns.
     * - Supports widening of columns.
     *
     * Note, this might fail in weird ways if you ask it to do more than what
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
                    if (row[i] == VoltTable.ColumnInfo.NO_DEFAULT_VALUE) {
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
                row[i] = ParameterConverter.tryToMakeCompatible(descColClass, row[i]);
                // check the result type in an assert
                assert(ParameterConverter.verifyParameterConversion(row[i], descColClass));
            }

            dest.addRow(row);
        }
    }

    /**
     * Public access to the package-private metadata.
     */
    public static String getTableName(VoltTable table) {
        return table.m_extraMetadata.name;
    }

    /**
     * Get the column index of the single bigint primary key column,
     * assuming the table metadata specified this.
     * Return -1 if not.
     */
    public static int getBigintPrimaryKeyIndexIfExists(VoltTable table) {
        // find the primary key
        if (table.m_extraMetadata != null) {
            int[] pkeyIndexes = table.m_extraMetadata.pkeyIndexes;
            if (pkeyIndexes != null) {
                if (pkeyIndexes.length > 0) {
                    VoltTable.ColumnInfo column = table.m_extraMetadata.originalColumnInfos[pkeyIndexes[0]];
                    if (column.type == VoltType.BIGINT) {
                        return pkeyIndexes[0];
                    }
                }
             }
        }
        return -1;
    }

    /**
     * Load random data into a partitioned table in VoltDB that has a bigint pkey.
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
     * @param offset Generated pkey values start here.
     * @param jump Generated pkey values increment by this value.
     * @throws Exception
     */
    public void fillTableWithBigintPkey(VoltTable table, int mb,
            long maxRows, final Client client,
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
                table.m_extraMetadata.name, offset, jump, mbTarget, maxRows);

        // find the primary key, assume first col if not found
        int pkeyColIndex = getBigintPrimaryKeyIndexIfExists(table);
        if (pkeyColIndex == -1) {
            pkeyColIndex = 0;
            assert(table.getColumnType(0).isBackendIntegerType());
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
        final String insertProcName = table.m_extraMetadata.name.toUpperCase() + ".insert";
        RandomRowMaker filler = createRandomRowMaker(table, Integer.MAX_VALUE, false, false);
        while (rss.get() < mbTarget) {
            Object[] row = filler.randomRow();
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
                table.m_extraMetadata.name, rows, rss.get());
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
            assert(table.getColumnType(0).isBackendIntegerType());
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
        final String deleteProcName = table.m_extraMetadata.name.toUpperCase() + ".delete";
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

    /**
     * A fairly straighforward loader for tables with metadata and rows. Maybe this could
     * be faster or have better error messages? Meh.
     *
     * @param client Client connected to a VoltDB instance containing a table with same name
     * and schema as the VoltTable parameter named "t".
     * @param t A table with extra metadata and presumably some data in it.
     * @throws Exception
     */
    public static void loadTable(Client client, VoltTable t) throws Exception {
        // ensure table is annotated
        assert(t.m_extraMetadata != null);

        // replicated tables
        if (t.m_extraMetadata.partitionColIndex == -1) {
            client.callProcedure("@LoadMultipartitionTable", t.m_extraMetadata.name, (byte) 0, t); // using insert here
        }

        // partitioned tables
        else {
            final AtomicBoolean failed = new AtomicBoolean(false);
            final CountDownLatch latch = new CountDownLatch(t.getRowCount());
            int columns = t.getColumnCount();
            String procedureName = t.m_extraMetadata.name.toUpperCase() + ".insert";

            // callback for async row insertion tracks response count + failure
            final ProcedureCallback insertCallback = new ProcedureCallback() {
                @Override
                public void clientCallback(ClientResponse clientResponse) throws Exception {
                    latch.countDown();
                    if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                        failed.set(true);
                    }
                }
            };

            // async insert all the rows
            t.resetRowPosition();
            while (t.advanceRow()) {
                Object params[] = new Object[columns];
                for (int i = 0; i < columns; ++i) {
                    params[i] = t.get(i, t.getColumnType(i));
                }
                client.callProcedure(insertCallback, procedureName, params);
            }

            // block until all inserts are done
            latch.await();

            // throw a generic exception if anything fails
            if (failed.get()) {
                throw new RuntimeException("TableHelper.load failed.");
            }
        }
    }

    public static ByteBuffer getBackedBuffer(VoltTable table) {
        // This is an unsafe version of VoltTable.getBuffer() because it does not instantiate a new ByteBuffer
        table.m_buffer.position(0);
        table.m_readOnly = true;
        assert(table.m_buffer.remaining() == table.m_buffer.limit());
        return table.m_buffer;
    }

    public static VoltTable[] convertBackedBufferToTables(ByteBuffer fullBacking, int batchSize) {
        final VoltTable[] results = new VoltTable[batchSize];
        for (int i = 0; i < batchSize; ++i) {
            final int numdeps = fullBacking.getInt(); // number of dependencies for this frag
            assert(numdeps == 1);
            @SuppressWarnings("unused")
            final
            int depid = fullBacking.getInt(); // ignore the dependency id
            final int tableSize = fullBacking.getInt();
            // reasonableness check
            assert(tableSize < 50000000);
            final ByteBuffer tableBacking = fullBacking.slice();
            fullBacking.position(fullBacking.position() + tableSize);
            tableBacking.limit(tableSize);

            results[i] = PrivateVoltTableFactory.createVoltTableFromBuffer(tableBacking, true);
        }
        return results;
    }
}
