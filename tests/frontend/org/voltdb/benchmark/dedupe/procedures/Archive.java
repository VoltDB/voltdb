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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

@ProcInfo(
    partitionInfo = "ARCHIVED.COLUMN1: 0",
    singlePartition = true
)

public class Archive extends VoltProcedure {
    // grab <x> rows to archive
    public final SQLStmt getUnArchived = new SQLStmt("select column1, column2, column3, column4, checktime from unarchived where checktime < ? order by checktime, column1, column2, column3, column4 limit ?;");

    // insert an archived row
    public final SQLStmt insertArchived = new SQLStmt("insert into archived (column1, column2, column3, column4, checktime) values (?, ?, ?, ?, ?);");

    // delete an unarchived row
    public final SQLStmt deleteUnArchived = new SQLStmt("delete from unarchived where column1 = ? and column2 = ? and column3 = ? and column4 = ? and checktime = ?;");


    public VoltTable[] run(
            long partitionId,
            long numMoves,
            long called_time_milliseconds
    ) {
        long timeFiveMinutesAgoMillis = called_time_milliseconds - (1000l * 60l * 5l);

        VoltTable vtMoved = new VoltTable(new VoltTable.ColumnInfo("column1",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("column2",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("column3",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("column4",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("checktime",VoltType.BIGINT));
        Object rowMoved[] = new Object[5];

        voltQueueSQL(getUnArchived, timeFiveMinutesAgoMillis, numMoves);

        VoltTable results1[] = voltExecuteSQL();

        int rowCount = results1[0].getRowCount();

        int sqlStatements = 0;

        if (rowCount > 0) {
            // move row from unarchived to archived
            for (int ii = 0; ii < rowCount; ii++) {
                VoltTableRow row = results1[0].fetchRow(ii);
                long column1 = row.getLong(0);
                long column2 = row.getLong(1);
                long column3 = row.getLong(2);
                long column4 = row.getLong(3);
                long checktime = row.getLong(4);

                voltQueueSQL(insertArchived, column1, column2, column3, column4, checktime);
                voltQueueSQL(deleteUnArchived, column1, column2, column3, column4, checktime);

                sqlStatements += 2;

                if (sqlStatements > 900) {
                    // limit of 1000 sql statements
                    voltExecuteSQL();
                    sqlStatements = 0;
                }

                rowMoved[0] = column1;
                rowMoved[1] = column2;
                rowMoved[2] = column3;
                rowMoved[3] = column4;
                rowMoved[4] = checktime;
                vtMoved.addRow(rowMoved);
            }

            if (sqlStatements > 0) {
                voltExecuteSQL(true);
            }
        }

        VoltTable vtMetric = new VoltTable(new VoltTable.ColumnInfo("called_time_milliseconds",VoltType.BIGINT));
        Object rowMetric[] = new Object[1];
        rowMetric[0] = called_time_milliseconds;
        vtMetric.addRow(rowMetric);

        final VoltTable[] vtReturn = {vtMoved,vtMetric};

        return vtReturn;
    }
}
