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

package org.voltdb.benchmark.multisite.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
    partitionInfo = "RESERVATIONS.FID: 0",
    singlePartition = true
)
public class ChangeSeat extends VoltProcedure {

    public final SQLStmt CHECK_SEAT =
        new SQLStmt("select RID from RESERVATIONS where FID = ? and SEAT = ?");

    public final SQLStmt CHECK_CID =
        new SQLStmt("select RID from RESERVATIONS where FID = ? and CID = ?");

    public final SQLStmt RESERVE_SEAT =
        new SQLStmt("update RESERVATIONS set SEAT = ? where FID = ? and CID = ?");


    public VoltTable[] run(int fid, int cid, long seatnum) throws VoltAbortException {

        // check if the seat is occupied
        // check if the customer has multiple seats on this flight
        voltQueueSQL(CHECK_SEAT, fid, seatnum);
        voltQueueSQL(CHECK_CID, fid, cid);
        final VoltTable[] results = voltExecuteSQL();

        assert(results.length == 2);
        if (results[0].getRowCount() > 0) {
            throw new VoltAbortException("Seat reservation conflict");
        }
        if (results[1].getRowCount() > 1) {
            throw new VoltAbortException("Customer owns multiple reservations");
        }

        // update the seat reservation for the customer
        voltQueueSQL(RESERVE_SEAT, seatnum, fid, cid);
        VoltTable[] updates = voltExecuteSQL();
        assert(updates.length == 1);

        return updates;
    }
}
