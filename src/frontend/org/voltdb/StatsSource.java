/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb;
import org.voltdb.utils.EstTime;
import org.voltdb.VoltTable.ColumnInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Abstract superclass of all sources of statistical information inside the Java frontend.
 */
public abstract class StatsSource {

    /**
     * Name of this source of statistical information
     */
    private final String name;

    /**
     * Column schema for statistical result rows
     */
    private final ArrayList<ColumnInfo> columns = new ArrayList<ColumnInfo>();

    /**
     * Map from the name of a column to its index in the a result row. In order to decouple the contributions of each class
     * in the inheritance hierarchy from the integer index in the result row this map is used for lookups instead of hard coding an index.
     */
    protected final HashMap<String, Integer> columnNameToIndex = new HashMap<String, Integer>();

    /**
     * Initialize this source of statistical information with the specified name. Populate the column schema by calling populateColumnSchema
     * on the derived class and use it to populate the columnNameToIndex map.
     * @param name
     */
    public StatsSource(String name) {
        populateColumnSchema(columns);

        for (int ii = 0; ii < columns.size(); ii++) {
            columnNameToIndex.put(columns.get(ii).name, ii);
        }

        this.name = name;
    }

    /**
     * Called from the constructor to generate the column schema at run time. Derived classes need to override this method in order
     * to specify the columns they will be adding. The first line must always be a call the superclasses version of populateColumnSchema
     * in order to ensure the columns are add to the list in the right order.
     * @param columns Output list for the column schema.
     */
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        columns.add(new ColumnInfo("STAT_TIME", VoltType.BIGINT));
    }

    /**
     * Retrieve the already constructed column schema
     * @return List of column names and types
     */
    public ArrayList<ColumnInfo> getColumnSchema() {
        return columns;
    }

    /**
     * Get the name of this source of statistical information
     * @return Name of this source
     */
    public String getName() {
        return name;
    }

    /**
     * Get the latest stat values as an array of arrays of objects suitable for insertion into an VoltTable
     * @return Array of Arrays of objects containing the latest values
     */
    public Object[][] getStatsRows() {
        Iterator<Object> i = getStatsRowKeyIterator();
        ArrayList<Object[]> rows = new ArrayList<Object[]>();
        while (i.hasNext()) {
            Object rowKey = i.next();
            Object rowValues[] = new Object[columns.size()];
            updateStatsRow(rowKey, rowValues);
            rows.add(rowValues);
        }
        return rows.toArray(new Object[0][]);
    }

    /**
     * Update the parameter array with the latest values. This is similar to populateColumnSchema in that it must be overriden by
     * derived classes and the derived class implementation must call the super classes implementation.
     * @param rowKey Key identifying the specific row to be populated
     * @param rowValues Output parameter. Array of values to be updated.
     */
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        rowValues[0] = EstTime.currentTimeMillis();
    }

    /**
     * Retrieve an iterator that iterates over the keys identifying all unique stats rows available for retreival from the stats source
     * @return Iterator of Objects representing keys that identify unique stats rows
     */
    abstract protected Iterator<Object> getStatsRowKeyIterator();
}
