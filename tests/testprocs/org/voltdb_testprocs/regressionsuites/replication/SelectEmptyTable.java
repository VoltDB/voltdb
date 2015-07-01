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

package org.voltdb_testprocs.regressionsuites.replication;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

@ProcInfo(
          partitionInfo = "P1.ID: 0",
          singlePartition = true)

public class SelectEmptyTable extends VoltProcedure
{
    public final SQLStmt makeReadWrite =
        new SQLStmt("insert into P1 VALUES (?, 'bung', 1, 1.0);");

    public VoltTable[] run(int id)
    {

        VoltTable empty1 = new VoltTable(new VoltTable.ColumnInfo("column1",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("column2",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("column3",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("column4",VoltType.BIGINT),
                                          new VoltTable.ColumnInfo("checktime",VoltType.BIGINT));

        VoltTable empty2 = new VoltTable(new VoltTable.ColumnInfo("called_time_milliseconds",VoltType.BIGINT));
        final VoltTable[] vtReturn = {empty1, empty2};
        return vtReturn;
    }
}
