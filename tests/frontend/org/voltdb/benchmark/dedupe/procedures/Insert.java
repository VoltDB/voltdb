/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.benchmark.dedupe.procedures;

import java.util.Calendar;
import java.util.TimeZone;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

@ProcInfo(
        partitionInfo = "ARCHIVED.COLUMN1: 0",
        singlePartition = true
)

public class Insert extends VoltProcedure {
    // check if row already archived today
    public final SQLStmt checkArchived = new SQLStmt("select column1 from archived where column1 = ? and column2 = ? and column3 = ? and column4 = ? and checktime >= ? and checktime <= ?;");

    // get checktime from unarchived (if they are in there)
    public final SQLStmt checkUnArchived = new SQLStmt("select checktime from unarchived where column1 = ? and column2 = ? and column3 = ? and column4 = ? and checktime >= ? and checktime <= ?;");

    // update checktime in unarchived
    public final SQLStmt updateUnArchived = new SQLStmt("update unarchived set checktime = ? where column1 = ? and column2 = ? and column3 = ? and column4 = ? and checktime >= ? and checktime <= ?;");

    // insert into unarchived
    public final SQLStmt insertUnArchived = new SQLStmt("insert into unarchived (column1, column2, column3, column4, checktime) values (?, ?, ?, ?, ?);");

    private final TimeZone tz = TimeZone.getTimeZone("US/Eastern");
    private final Calendar calTemp = Calendar.getInstance(tz);

    public VoltTable[] run(
            long column1,
            long column2,
            long column3,
            long column4,
            long checkTimeMillis,
            long called_time_milliseconds
    ) {
        calTemp.setTimeInMillis(checkTimeMillis);
        calTemp.set(Calendar.HOUR_OF_DAY, 0);
        calTemp.set(Calendar.MINUTE, 0);
        calTemp.set(Calendar.SECOND, 0);
        calTemp.set(Calendar.MILLISECOND, 0);

        long checkTimeStartOfDayMillis = calTemp.getTimeInMillis();

        calTemp.set(Calendar.HOUR_OF_DAY, 23);
        calTemp.set(Calendar.MINUTE, 59);
        calTemp.set(Calendar.SECOND, 59);
        calTemp.set(Calendar.MILLISECOND, 999);

        long checkTimeEndOfDayMillis = calTemp.getTimeInMillis();

        voltQueueSQL(checkArchived, column1, column2, column3, column4, checkTimeStartOfDayMillis, checkTimeEndOfDayMillis);
        voltQueueSQL(checkUnArchived, column1, column2, column3, column4, checkTimeStartOfDayMillis, checkTimeEndOfDayMillis);

        VoltTable results1[] = voltExecuteSQL();
        int sqlStatements = 0;

        if (results1[0].getRowCount() == 0) {
            // row is not yet archived

            if (results1[1].getRowCount() == 1) {
                // update unarchived row if older checktime
                if (results1[1].fetchRow(0).getLong(0) > checkTimeMillis) {
                    voltQueueSQL(updateUnArchived, checkTimeMillis, column1, column2, column3, column4, checkTimeStartOfDayMillis, checkTimeEndOfDayMillis);
                    sqlStatements++;
                }
            } else {
                // insert unarchived row
                voltQueueSQL(insertUnArchived, column1, column2, column3, column4, checkTimeMillis);
                sqlStatements++;
            }

            if (sqlStatements > 0) {
                voltExecuteSQL(true);
            }
        }

        VoltTable vtLoad = new VoltTable(new VoltTable.ColumnInfo("called_time_milliseconds",VoltType.BIGINT));
        Object row[] = new Object[1];
        row[0] = called_time_milliseconds;
        vtLoad.addRow(row);

        final VoltTable[] vtReturn = {vtLoad};

        return vtReturn;
    }
}
