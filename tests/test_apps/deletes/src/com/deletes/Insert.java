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
package com.deletes;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Insert extends VoltProcedure {

    public final SQLStmt SQL = new SQLStmt(
            "INSERT INTO big_table (fullname, age, weight, desc1, desc2, addr1, addr2, addr3, text1, text2, sig, ts, seconds, company, co_addr, deceased) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] run(String fullname, long age, long weight,
                           String desc1, String desc2,
                           String addr1, String addr2, String addr3,
                           String text1, String text2,
                           String sig,
                           long ts,
                           String company, String co_addr,
                           byte deceased) throws VoltAbortException
    {
        // execute query
        voltQueueSQL(SQL, fullname, age, weight, desc1, desc2,
                     addr1, addr2, addr3, text1, text2, sig, ts,
                     ts / 1000, company, co_addr, deceased);
        return voltExecuteSQL(true);
    }
}
