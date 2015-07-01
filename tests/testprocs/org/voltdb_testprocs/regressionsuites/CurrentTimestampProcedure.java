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

package org.voltdb_testprocs.regressionsuites;

import java.util.Date;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
        singlePartition = false
        )

public class CurrentTimestampProcedure extends VoltProcedure {

    public final SQLStmt insertStmt = new SQLStmt(
            "INSERT INTO P_TIME (ID, C1) VALUES (?, ?);");

    public final SQLStmt selectStmt = new SQLStmt(
            "SELECT ID, C1, T_NOW, T_CURRENT_TIME, NOW, CURRENT_TIMESTAMP FROM P_TIME ORDER BY 1;");

    public final SQLStmt updateStmt = new SQLStmt(
            "UPDATE P_TIME SET C1 = ?, T1 = NOW, T2 = CURRENT_TIMESTAMP WHERE C1 = ?;");

    public final SQLStmt groupbyStmt = new SQLStmt(
            "SELECT T1, T2, COUNT(*) FROM P_TIME GROUP BY T1, T2;");

    private Date time = null;
    private long epsilonMicros = 100 * 1000;

    public long run() {

        time = new Date();
        long starttime = time.getTime()*1000;

        // TEST TIMESTAMP function in DEFAULT and Select.
        voltQueueSQL(insertStmt, 1, 1);
        voltQueueSQL(insertStmt, 2, 1);
        voltQueueSQL(insertStmt, 3, 1);
        voltQueueSQL(insertStmt, 4, 1);
        voltExecuteSQL();

        voltQueueSQL(selectStmt);
        VoltTable vt = voltExecuteSQL()[0];
        time = new Date();
        long endtime = time.getTime()*1000;

        long timeValue = -1;
        long id = 0;

        if(vt.getRowCount() != 4) {
            throw new VoltAbortException("bad row count");
        }

        while(vt.advanceRow()) {
            ++id;
            if (vt.getLong(0) != id) {
                throw new VoltAbortException("bad row ID " + id);
            }
            if (vt.getLong(1) != 1) {
                throw new VoltAbortException("bad row C1");
            }

            if (timeValue == -1) {
                timeValue = vt.getTimestampAsLong(2);
            }
            if (vt.getTimestampAsLong(2) != timeValue || vt.getTimestampAsLong(3) != timeValue ||
                    vt.getTimestampAsLong(4) != timeValue || vt.getTimestampAsLong(5) != timeValue) {
                throw new VoltAbortException("bad inconsistent current_timestamp value from different partitions");
            }
        }

        if( Math.abs(timeValue - starttime) > epsilonMicros) {
            throw new VoltAbortException(
                "bad current_timestamp, off too much from previous system time. Difference(usec): " +
                (timeValue - starttime));
        }

        if( timeValue > endtime + epsilonMicros) {
            throw new VoltAbortException(
                "bad current_timestamp, off too much from later system time. Difference(usec): " +
                (endtime - timeValue));
        }

        // Test Update
        voltQueueSQL(updateStmt, 2, 1);
        voltQueueSQL(groupbyStmt);

        vt = voltExecuteSQL()[1];

        assert(vt.advanceRow());
        if(vt.getRowCount() != 1) {
            throw new VoltAbortException("bad row count");
        }
        if (vt.getTimestampAsLong(0) != timeValue || vt.getTimestampAsLong(1) != timeValue) {
            throw new VoltAbortException("bad inconsistent current_timestamp value from different partitions");
        }
        if (vt.getLong(2) != 4) {
            throw new VoltAbortException("bad group by row count");
        }

        if( Math.abs(timeValue - starttime) > epsilonMicros) {
            throw new VoltAbortException(
                "bad current_timestamp, off too much from previous system time. Difference(usec): " +
                (timeValue - starttime));
        }

        if( timeValue > endtime + epsilonMicros) {
            throw new VoltAbortException(
                "bad current_timestamp, off too much from later system time. Difference(usec): " +
                (endtime - timeValue));
        }

        return 1;
    }


}
