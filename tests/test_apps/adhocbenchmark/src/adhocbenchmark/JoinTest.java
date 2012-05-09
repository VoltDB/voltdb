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
 * Configuration that determines what the generated join queries look like.
 */
class JoinTest extends QueryTestBase {

    // Table count for constructing joins
    public final int nTables;
    // Number of join levels
    public final int nLevels;

    public JoinTest(final String tablePrefix, final int nTables,
                    final String columnPrefix, final int nColumns,
                    int nLevels) {
        super(tablePrefix, columnPrefix, nColumns, nTables);
        this.nTables = nTables;
        this.nLevels = nLevels;
    }

    @Override
    public String getQuery(int iQuery, QueryTestHelper helper) {
        // Generate table lists by grabbing n sequential numbers at a time (wrap around).
        int iStart = iQuery * this.nLevels;
        StringBuilder query = new StringBuilder("SELECT * FROM ");
        for (int i = 0; i < this.nLevels; i++) {
            if (i > 0) {
                query.append(", ");
            }
            query.append(helper.tableName(helper.getShuffledNumber(iStart + i)));
        }
        // The where clause uses a foreign key/primary key pair.
        query.append(" WHERE ");
        for (int i = 0; i < this.nLevels - 1; i++) {
            if (i > 0) {
                query.append(" AND ");
            }
            query.append(helper.tableColumnName(helper.getShuffledNumber(iStart + i + 1), 1))
                 .append(" = ")
                 .append(helper.tableColumnName(helper.getShuffledNumber(iStart + i), 0));
        }
        return query.toString();
    }
}