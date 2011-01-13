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

// 1a. Insert into TABLE1
// 1b. Update TABLE2
// 1c. Update TABLE3
// 1d. Execute 1a - 1c
// 2a. If update (1b) failed, insert into TABLE2
// 2b. If update (1c) failed, insert into TABLE3
// 2c. Execute (2a) and/or (2b) if either is needed

package org.voltdb.benchmark.twentyindex.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

@ProcInfo(
        partitionInfo = "TABLE1.MAINID: 0",
        singlePartition = true
)

public class Insert extends VoltProcedure {

    public final SQLStmt insertTable1 = new SQLStmt("INSERT INTO TABLE1 VALUES (?, ?, ?, ?, ?);"); //mainId, eventTime, eventId, flag1, flag2

    public final SQLStmt updateTable2 = new SQLStmt("UPDATE TABLE2 SET EVENTID = ? WHERE MAINID = ? AND EVENTID = ?;"); //eventId, mainId, eventId

    public final SQLStmt insertTable2 = new SQLStmt("INSERT INTO TABLE2 VALUES (?, ?);"); //mainId, eventId

    public final SQLStmt insertTable3 = new SQLStmt("INSERT INTO TABLE3 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
       // mainId
       // field1 ... field20

    public final SQLStmt updateTable3 = new SQLStmt("UPDATE TABLE3 SET FIELD1 = ?, FIELD2 = ?, FIELD3 = ?, FIELD4 = ?, FIELD5 = ?, " +
                                                    "                  FIELD6 = ?, FIELD7 = ?, FIELD8 = ?, FIELD9 = ?, FIELD10 = ?, FIELD11 = ?, FIELD12 = ?, FIELD13 = ?, FIELD14 = ?, " +
                                                    "                  FIELD15 = ?, FIELD16 = ?, FIELD17 = ?, FIELD18 = ?, FIELD19 = ?, FIELD20 = ?, UPDATE_COUNTER = UPDATE_COUNTER + 1 "+
                                                    " WHERE MAINID = ?;");

    public VoltTable[] run(
            long mainId,
            TimestampType eventTime,
            long eventId,
            long flag1,
            long flag2,
            String field1,
            double field2,
            double field3,
            double field4,
            double field5,
            String field6,
            String field7,
            String field8,
            String field9,
            String field10,
            long field11,
            long field12,
            long field13,
            long field14,
            double field15,
            double field16,
            double field17,
            double field18,
            double field19,
            double field20,
            long called_time_milliseconds
    ) {
        voltQueueSQL(insertTable1, mainId, eventTime, eventId, flag1, flag2);
        voltQueueSQL(updateTable2, eventId, mainId, eventId);
        voltQueueSQL(updateTable3, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11, field12, field13, field14, field15, field16, field17, field18, field19, field20, mainId);
        final VoltTable results1[] = voltExecuteSQL();

        int secondSql = 0;
        if (results1[1].asScalarLong() == 0) {
            // no existing row in Table2
            voltQueueSQL(insertTable2, mainId, eventId);
            secondSql = 1;
        }

        if (results1[2].asScalarLong() == 0) {
            // no existing row in Table3
            voltQueueSQL(insertTable3, mainId, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11, field12, field13, field14, field15, field16, field17, field18, field19, field20, 1l);
            secondSql = 1;
        }

        if (secondSql == 1) {
            voltExecuteSQL();
        }

        VoltTable vtLoad = new VoltTable(new VoltTable.ColumnInfo("called_time_milliseconds",VoltType.BIGINT));
        Object row[] = new Object[1];
        row[0] = called_time_milliseconds;
        vtLoad.addRow(row);

        final VoltTable[] vtReturn = {vtLoad};

        return vtReturn;
    }
}
