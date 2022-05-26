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

package org.voltdb_testprocs.regressionsuites.sqlfeatureprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class TruncateTable extends VoltProcedure {

    public final SQLStmt truncateRTable = new SQLStmt("DELETE FROM RTABLE;");
    public final SQLStmt checkRTable = new SQLStmt("SELECT COUNT(*) FROM RTABLE;");
    public final SQLStmt truncatePTable = new SQLStmt("DELETE FROM PTABLE;");
    public final SQLStmt checkPTable = new SQLStmt("SELECT COUNT(*) FROM PTABLE;");

    public final SQLStmt insertRTable = new SQLStmt("INSERT INTO RTABLE VALUES(6,  30,  1.1, 'Jedi',  'Winchester');");

    public long run() {

        voltQueueSQL(truncateRTable);
        voltQueueSQL(checkRTable);
        voltQueueSQL(truncatePTable);
        voltQueueSQL(checkPTable);

        VoltTable[] results = voltExecuteSQL();
        if (results.length != 4 ||
                results[1].asScalarLong() != 0 ||
                results[3].asScalarLong() != 0) {
            throw new VoltAbortException(
                    "A table truncation behaved unexpectedly PRIOR to the intentional violation.");
        }

        // Force a rollback after a constraint violation
        voltQueueSQL(insertRTable);
        voltQueueSQL(insertRTable);
        voltExecuteSQL();
        return 0;
    }

}
