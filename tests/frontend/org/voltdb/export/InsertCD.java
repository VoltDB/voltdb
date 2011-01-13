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

package org.voltdb.export;
import org.voltdb.*;

@ProcInfo (
   partitionInfo = "C.C_CLIENT: 0",
   singlePartition = true
)
public class InsertCD extends VoltProcedure {

    public final SQLStmt insertC = new SQLStmt("INSERT INTO C VALUES (?, ?, ?);");

    public final SQLStmt insertD = new SQLStmt("INSERT INTO D VALUES (?, ?, ?);");

    public VoltTable[] run (long clientnum, long id_cd, long item_cd) throws VoltAbortException {
        voltQueueSQL(insertC, clientnum, id_cd, item_cd);

        // insert every 10,000 id into table D.
        if (id_cd % 10000 == 0)
            voltQueueSQL(insertD, clientnum, id_cd, item_cd);

        return voltExecuteSQL();
    }
}
