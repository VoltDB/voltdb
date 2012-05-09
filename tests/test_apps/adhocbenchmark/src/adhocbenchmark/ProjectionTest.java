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

package adhocbenchmark;

import adhocbenchmark.QueryTestBase;
import adhocbenchmark.QueryTestHelper;

/**
 * Configuration that determines what the projection queries look like.
 */
class ProjectionTest extends QueryTestBase {

    public ProjectionTest(final String tableName,
                          final String columnPrefix, int nColumns) {
        super(tableName, columnPrefix, nColumns, nColumns);
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
        query.append(" FROM ")
             .append(this.tablePrefix)
             .append(" WHERE ")
             .append(helper.columnName(helper.getShuffledNumber(0)))
             .append(" = 'abc'");
        return query.toString();
    }
}