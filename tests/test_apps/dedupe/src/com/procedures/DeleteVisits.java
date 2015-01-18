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

package com.procedures;

import org.voltdb.*;
import java.util.Calendar;
import java.util.TimeZone;

@ProcInfo(
    partitionInfo = "VISIT_ARCHIVED.USERID: 0",
    singlePartition = true
)

public class DeleteVisits extends VoltProcedure {
    // grab <x> archived visits for deletion
    public final SQLStmt getArchived = new SQLStmt("select userId, field1, field2, field3, visitTime from visit_archived where visitTime < ? order by visitTime, userId, field1, field2, field3 limit ?;");

    // delete an archived visit
    public final SQLStmt deleteArchived = new SQLStmt("delete from visit_archived where userId = ? and field1 = ? and field2 = ? and field3 = ? and visitTime = ?;");

    private final TimeZone tz = TimeZone.getTimeZone("US/Eastern");
    private final Calendar calBegin = Calendar.getInstance(tz);

    public VoltTable[] run(
            long partitionId,
            long numDeletes,
            long called_time_milliseconds
    ) {
        calBegin.setTimeInMillis(called_time_milliseconds);
        calBegin.set(Calendar.HOUR_OF_DAY, 0);
        calBegin.set(Calendar.MINUTE, 0);
        calBegin.set(Calendar.SECOND, 0);
        calBegin.set(Calendar.MILLISECOND, 0);

        long timeStartOfDay = calBegin.getTimeInMillis();

        voltQueueSQL(getArchived, timeStartOfDay, numDeletes);

        VoltTable results1[] = voltExecuteSQL();

        int rowCount = results1[0].getRowCount();

        int sqlStatements = 0;

        if (rowCount > 0) {
            // delete rows
            for (int ii = 0; ii < rowCount; ii++) {
                VoltTableRow row = results1[0].fetchRow(ii);
                long userId = row.getLong(0);
                long field1 = row.getLong(1);
                long field2 = row.getLong(2);
                long field3 = row.getLong(3);
                long visitTimeLong = row.getLong(4);

                voltQueueSQL(deleteArchived, userId, field1, field2, field3, visitTimeLong);

                sqlStatements++;

                if (sqlStatements > 900) {
                    // limit of 1000 sql statements
                    VoltTable results2[] = voltExecuteSQL();
                    sqlStatements = 0;
                }
            }

            if (sqlStatements > 0) {
                VoltTable results3[] = voltExecuteSQL(true);
            }
        }

        VoltTable vtDeleted = new VoltTable(new VoltTable.ColumnInfo("rows_deleted",VoltType.INTEGER));
        Object row1[] = new Object[1];
        row1[0] = rowCount;
        vtDeleted.addRow(row1);

        VoltTable vtLoad = new VoltTable(new VoltTable.ColumnInfo("called_time_milliseconds",VoltType.BIGINT));
        Object row[] = new Object[1];
        row[0] = called_time_milliseconds;
        vtLoad.addRow(row);

        final VoltTable[] vtReturn = {vtDeleted,vtLoad};

        return vtReturn;
    }
}
