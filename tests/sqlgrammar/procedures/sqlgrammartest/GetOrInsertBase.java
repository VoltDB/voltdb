/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package sqlgrammartest;

import org.voltdb.*;

/**
 * Base class for various test stored procedures that, for a particular table,
 * select the minimum or maximum ID value, or an entire row (with specified ID);
 * or insert a new row, either with specified ID, or with ID just below the
 * minimum or just above the maximum.
 */
public class GetOrInsertBase extends VoltProcedure {

    protected VoltTable[] selectMinOrMaxId(final SQLStmt getMinOrMaxIdQuery) {
        // Select the minium or maximum ID, for the table (may be null)
        voltQueueSQL(getMinOrMaxIdQuery);
        return voltExecuteSQL();
    }

    private VoltTableRow getMinOrMaxRow(final SQLStmt getMinOrMaxIdQuery) {
        // Get a row containing the minium or maximum ID, for the table (may be null)
        VoltTable[] queryResults = selectMinOrMaxId(getMinOrMaxIdQuery);
        VoltTable result = queryResults[0];
        return result.fetchRow(0);
    }

    private long getMinOrMaxId(final SQLStmt getMinOrMaxIdQuery) {
        // Get the minium or maximum ID value, for the table (may be null)
        VoltTableRow row = getMinOrMaxRow(getMinOrMaxIdQuery);
        return row.getLong(0);
    }

    protected long insertRow(final SQLStmt insertQuery, long id) {
        // Insert a new row into the table, using the specified ID
        // and related values
        voltQueueSQL(insertQuery,
                id, id/10, id*10, id*100, id*1000, id*1.1, id*10.1,
                "abc"+id, "ABC"+id, "XYZ"+id, "xyz"+id, "{jsonValue:"+id+"}",
                null, null,
                null, null,
                null, null, null, null);
        voltExecuteSQL();
        return 1;  // 1 row inserted
    }

    protected long insertMinRow(final SQLStmt getMinIdQuery, final SQLStmt insertQuery) {
        VoltTableRow row = getMinOrMaxRow(getMinIdQuery);
        long minId = row.getLong(0);
        if (row.wasNull()) {  // there are no rows yet
                minId = 1;
        }
        return insertRow(insertQuery, minId-1);
    }

    protected long insertMaxRow(final SQLStmt getMaxIdQuery, final SQLStmt insertQuery) {
        VoltTableRow row = getMinOrMaxRow(getMaxIdQuery);
        long maxId = row.getLong(0);
        if (row.wasNull()) {  // there are no rows yet
            maxId = -1;
        }
        return insertRow(insertQuery, maxId+1);
    }

    protected VoltTable[] selectRow(final SQLStmt selectByIdQuery, long id) {
        // Select a row from the table, having the specified ID
        voltQueueSQL(selectByIdQuery, id);
        return voltExecuteSQL();
    }

}
