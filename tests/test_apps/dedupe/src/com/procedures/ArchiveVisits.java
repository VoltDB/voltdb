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

import java.util.Date;


@ProcInfo(
    partitionInfo = "VISIT_ARCHIVED.USERID: 0",
    singlePartition = true
)

public class ArchiveVisits extends VoltProcedure {
    // grab <x> visits to archive
    public final SQLStmt getUnArchived = new SQLStmt("select userId, field1, field2, field3, visitTime from visit_unarchived where visitTime < ? order by visitTime, userId, field1, field2, field3 limit ?;");

    // insert an archived visit
    public final SQLStmt insertArchived = new SQLStmt("insert into visit_archived (userId, field1, field2, field3, visitTime) values (?, ?, ?, ?, ?);");

    // delete an unarchived visit
    public final SQLStmt deleteUnArchived = new SQLStmt("delete from visit_unarchived where userId = ? and field1 = ? and field2 = ? and field3 = ? and visitTime = ?;");


    public VoltTable[] run(
            long partitionId,
            long numMoves,
            long called_time_milliseconds
    ) {
        long timeFiveMinutesAgoMillis = called_time_milliseconds - (1000l * 60l * 5l);

        VoltTable vtMoved = new VoltTable(new VoltTable.ColumnInfo("userId",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("field1",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("field2",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("field3",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("visitTime",VoltType.BIGINT));
        Object rowMoved[] = new Object[5];

        voltQueueSQL(getUnArchived, timeFiveMinutesAgoMillis, numMoves);

        VoltTable results1[] = voltExecuteSQL();

        int rowCount = results1[0].getRowCount();

        int sqlStatements = 0;

        if (rowCount > 0) {
            // move from unarchived to archived
            for (int ii = 0; ii < rowCount; ii++) {
                VoltTableRow row = results1[0].fetchRow(ii);
                long userId = row.getLong(0);
                long field1 = row.getLong(1);
                long field2 = row.getLong(2);
                long field3 = row.getLong(3);
                long visitTimeLong = row.getLong(4);

                voltQueueSQL(insertArchived, userId, field1, field2, field3, visitTimeLong);
                voltQueueSQL(deleteUnArchived, userId, field1, field2, field3, visitTimeLong);

                sqlStatements += 2;

                if (sqlStatements > 900) {
                    // limit of 1000 sql statements
                    VoltTable results2[] = voltExecuteSQL();
                    sqlStatements = 0;
                }

                rowMoved[0] = userId;
                rowMoved[1] = field1;
                rowMoved[2] = field2;
                rowMoved[3] = field3;
                rowMoved[4] = visitTimeLong;
                vtMoved.addRow(rowMoved);
            }

            if (sqlStatements > 0) {
                VoltTable results3[] = voltExecuteSQL(true);
            }
        }

        VoltTable vtMetric = new VoltTable(new VoltTable.ColumnInfo("called_time_milliseconds",VoltType.BIGINT),
                                           new VoltTable.ColumnInfo("rows_moved",VoltType.BIGINT));
        Object rowMetric[] = new Object[1];
        rowMetric[0] = called_time_milliseconds;
        vtMetric.addRow(rowMetric);

        final VoltTable[] vtReturn = {vtMoved,vtMetric};

        return vtReturn;
    }
}
