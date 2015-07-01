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

package org.voltdb_testprocs.regressionsuites.failureprocs;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
    partitionInfo = "WIDE.P: 0",
    singlePartition = true
)
public class InsertLotsOfData extends VoltProcedure {

    public final SQLStmt insertBigString = new SQLStmt("INSERT INTO WIDE VALUES (" +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? " +
            ");");

    public long run(int p, long initialId) {
        final int STRLEN = 60;

        int totalBytes = 0;
        final int MB75Bytes = 1024 * 1024 * 5;

        /**
         * String is inlined in the tuple so the space is taken up either way. No need to
         * transmit all the extra bytes to the IPC client.
         */
        String theString = "";

        while (totalBytes < MB75Bytes) {
            voltQueueSQL(insertBigString, initialId++, p,
                    theString, theString, theString, theString, theString, theString, theString, theString, theString, theString,
                    theString, theString, theString, theString, theString, theString, theString, theString, theString, theString,
                    theString, theString, theString, theString, theString, theString, theString, theString, theString, theString,
                    theString, theString, theString, theString, theString, theString, theString, theString, theString, theString);
            long tuplesChanged = voltExecuteSQL()[0].asScalarLong();
            assert(tuplesChanged == 1);
            totalBytes += (STRLEN * 40);
        }
        return initialId;
    }
}
