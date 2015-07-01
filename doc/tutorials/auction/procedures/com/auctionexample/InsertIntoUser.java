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
package com.auctionexample;

import org.voltdb.*;

/**
 *
 *
 */
@ProcInfo(
    singlePartition = false
)
public class InsertIntoUser extends VoltProcedure {

    public final SQLStmt insert = new SQLStmt("INSERT INTO USER VALUES (?, ?, ?, ?, ?, ?, ?, ?);");

    /**
     *
     * @param userid
     * @param lastname
     * @param firstname
     * @param streetaddress
     * @param city
     * @param state
     * @param zip
     * @param email
     * @return The number of rows affected.
     * @throws VoltAbortException
     */
    public VoltTable[] run(int userid, String lastname, String firstname, String streetaddress, String city, String state, String zip, String email) throws VoltAbortException {
        voltQueueSQL(insert, userid, lastname, firstname, streetaddress, city, state, zip, email);
        return voltExecuteSQL();
    }
}
