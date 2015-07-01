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

// Put stored procedure
//
//   Puts the given Key-Value pair


package voltkvqa.procedures_withexport;

import java.nio.ByteBuffer;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

//@ProcInfo
//(
//  partitionInfo   = "store.key:0"
//, singlePartition = true
//)

public class PutMp extends VoltProcedure
{
    // Checks if key exists
    public final SQLStmt checkStmt = new SQLStmt("SELECT key,value FROM store WHERE key = ?;");

    // Updates a key/value pair
    public final SQLStmt updateStmt = new SQLStmt("UPDATE store SET value = ? WHERE key = ?;");

    // Logs update to export table
    public final SQLStmt exportStmt = new SQLStmt("INSERT INTO store_export VALUES ( ?, ?, ?, ?, ?)");

    // Inserts a key/value pair
    public final SQLStmt insertStmt = new SQLStmt("INSERT INTO store (key, value) VALUES (?, ?);");

    public VoltTable[] run(String key, byte[] value)
    {
        long putCounter = 0;

        // Check whether the pair exists
        voltQueueSQL(checkStmt, key);
        VoltTable[] queryresults = voltExecuteSQL();

        //Stuff an incrementing putCounter into the 1st long of the payload
        //This is clearly going to mess up compressed data.

        // Insert new or update existing key depending on result
        if (queryresults[0].getRowCount() == 0) {
            //New key gets putCounter  set to 0
            for (int i = 0; i < 8; i++) {
                value[i] = 0;
            }
            voltQueueSQL(insertStmt, key, value);
        }
        else {
            // Get the old count from 1st 8 bytes, increment it, stuff it
            // back in
            queryresults[0].advanceRow();
            ByteBuffer bb = ByteBuffer.wrap(queryresults[0].getVarbinary(1));
            putCounter = bb.getLong(0);
            putCounter++;
            bb.putLong(0, putCounter);
            voltQueueSQL(updateStmt, bb.array(), key);
            voltQueueSQL(exportStmt, queryresults[0].getString(0), queryresults[0].getVarbinary(1), getTransactionTime(), getVoltPrivateRealTransactionIdDontUseMe(), getSeededRandomNumberGenerator().nextDouble());
        }
        voltExecuteSQL(true);
        VoltTable t[] = new VoltTable[1];
        t[0] = new VoltTable(new VoltTable.ColumnInfo("counter",VoltType.BIGINT));
        t[0].addRow(putCounter);
        return t ;
    }
}
