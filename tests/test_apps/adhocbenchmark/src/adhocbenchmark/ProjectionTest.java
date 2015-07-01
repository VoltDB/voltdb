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
 * Configuration that determines what the projection queries look like.
 */
class ProjectionTest extends QueryTestBase {
    private final String m_idColumn;
    private ProjectionTest(final String tableName,
                           final String columnPrefix, int nColumns, String idColumn) {
        super(tableName, columnPrefix, nColumns, nColumns);
        m_idColumn = idColumn;
    }

    @Override
    public String getQuery(int iQuery, QueryTestHelper helper) {
        // Build the projected column list.
        StringBuilder query = new StringBuilder("SELECT ");
        for (int iColumn = 0; iColumn < this.nColumns; ++iColumn) {
            if (iColumn > 0) {
                query.append(", ");
            }
            query.append(helper.columnName(helper.getShuffledNumber(iColumn)));
        }
        // Complete the query.
        // Enough cache-thrashing complexity comes fom the random column list,
        // so keep the where clause simple --
        // an equality filter with a random constant enables parallel SP processing
        // when the table is partitioned on that column (ID).
        // For a replicated table, or an MP query against partitioned data,
        // this where clause (against a non-partitioning column) doesn't greatly complicate the plan,
        // so keep it.
        query.append(" FROM " + this.tablePrefix + "_1 WHERE " + m_idColumn + " = " + helper.getShuffledNumber(0));
        return query.toString();
    }

    public static class Factory implements QueryTestBase.Factory {
        @Override
        public QueryTestBase make(final String tablePrefix, int nVariations, final String columnPrefix,
                final int nColumns, final int nRandomNumbers) {
            return new ProjectionTest(tablePrefix, columnPrefix, nColumns, "ID");
        }
    }

    public static class MPFactory implements QueryTestBase.Factory {
        @Override
        public QueryTestBase make(final String tablePrefix, int nVariations, final String columnPrefix,
                final int nColumns, final int nRandomNumbers) {
            return new ProjectionTest(tablePrefix, columnPrefix, nColumns, "PARENT_ID");
        }
    }

}