/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Scan extends VoltProcedure {

    public final SQLStmt getBddStmt = new SQLStmt("SELECT value FROM Store WHERE keyspace = ? AND key >= ? LIMIT ?");
    public final SQLStmt getUnbddStmt = new SQLStmt("SELECT value FROM Store WHERE keyspace = ? LIMIT ?");

    public VoltTable[] run(byte[] keyspace, String partKey, byte[] rangeMin, int count) throws Exception
    {
        if (rangeMin != null)
        {
            voltQueueSQL(getBddStmt, keyspace, new String(rangeMin, "UTF-8"), count);
        }
        else
        {
            voltQueueSQL(getUnbddStmt, keyspace, count);
        }
        return voltExecuteSQL(true);
    }
}
