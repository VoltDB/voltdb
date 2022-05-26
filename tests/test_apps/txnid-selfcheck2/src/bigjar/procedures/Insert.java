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


//
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voter (phone number of the caller) is not above the
// number of allowed votes.
//

package bigjar.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import java.lang.RuntimeException;

public class Insert extends VoltProcedure {

    public final SQLStmt insert_p1 = new SQLStmt ("INSERT INTO P1 VALUES (?, ?, ?, ?);");
    public final SQLStmt insert_r1 = new SQLStmt ("INSERT INTO R1 VALUES (?, ?, ?, ?);");

    public VoltTable[] run(String tablename, long minid, long maxid) {
        if (tablename.equals("P1")) {
            for (long i=minid; i<=maxid; i++) {
                voltQueueSQL(insert_p1, i, "str"+i, -i, (i + Double.valueOf(i-1)/maxid));
            }
        } else if (tablename.equals("R1")) {
            for (long i=minid; i<=maxid; i++) {
                voltQueueSQL(insert_r1, i, "str"+i, -i, (i + Double.valueOf(i-1)/maxid));
            }
        } else {
            throw new RuntimeException("Unknown Table Name: "+tablename);
        }
        return voltExecuteSQL();
    }
}
