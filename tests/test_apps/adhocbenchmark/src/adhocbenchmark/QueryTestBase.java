/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package adhocbenchmark;

import adhocbenchmark.QueryTestHelper;

/**
 * Base class for query test configuration.
 */
public abstract class QueryTestBase {

    /// Table name prefix
    public final String tablePrefix;

    /// Column name prefix
    public final String columnPrefix;

    /// Number of non-key columns
    public final int nColumns;

    /// Quantity of random numbers to shuffle
    public final int nRandomNumbers;

    /**
     * Constructor
     * @param tablePrefix  table name prefix
     * @param columnPrefix  column name prefix
     * @param nColumns  number of non-key columns
     * @param nRandomNumbers quantity of random numbers to shuffle
     */
    public QueryTestBase(final String tablePrefix, final String columnPrefix,
                         final int nColumns, final int nRandomNumbers) {
        this.tablePrefix = tablePrefix;
        this.columnPrefix = columnPrefix;
        this.nColumns = nColumns;
        this.nRandomNumbers = nRandomNumbers;
    }

    /**
     * Required override to get next query string.
     * @param iQuery  query number (zero-based)
     * @return  query string
     */
    public abstract String getQuery(final int iQuery, QueryTestHelper helper);
}