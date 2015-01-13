/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package adhocbenchmark;


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

    protected interface Factory {
        public QueryTestBase make(final String tablePrefix, int nVariations, final String columnPrefix,
                final int nColumns, final int nRandomNumbers);
    }

}