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

// Put stored procedure
//
//   Puts the given Key-Value pair


package com.procedures;

import org.voltdb.*;

@ProcInfo(
        partitionInfo = "KEY_VALUE.KEY_COLUMN: 0",
        singlePartition = true
)

public class Put extends VoltProcedure {
    // check if key exists
    public final SQLStmt checkKey = new SQLStmt("select key_column from key_value where key_column = ?;");

    // update key/value
    public final SQLStmt updateKeyValue = new SQLStmt("update key_value set value_column = ? where key_column = ?;");

    // insert key/value
    public final SQLStmt insertKeyValue = new SQLStmt("insert into key_value (key_column, value_column) values (?, ?);");

    public VoltTable[] run(
            String strKey,
            byte[] baValue
    ) {
        voltQueueSQL(checkKey, strKey);

        VoltTable results1[] = voltExecuteSQL();

        if (results1[0].getRowCount() == 0) {
            // key does not exist, insert
            voltQueueSQL(insertKeyValue, strKey, baValue);
        } else {
            // key exists, update
            voltQueueSQL(updateKeyValue, baValue, strKey);
        }

        return voltExecuteSQL(true);
    }
}
