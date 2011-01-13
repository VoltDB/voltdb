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

import org.voltdb.*;

@ProcInfo(
    partitionInfo = "RESERVATIONS.FID: 0",
    singlePartition = true
)
public class FindOpenSeats extends VoltProcedure {

    public final SQLStmt GET_SEATS =
        new SQLStmt("select FID, RID, SEAT from RESERVATIONS where FID = ?");

    public VoltTable[] run(int fid) {

        // 150 seats
        final long seatmap[] = new long[]
          {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};

        final VoltTable retarray[] = new VoltTable[1];
        retarray[0] = new VoltTable(
                new VoltTable.ColumnInfo("FID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("SEAT", VoltType.INTEGER)
        );
        //retarray[0].ensureRowCapacity(150);

        voltQueueSQL(GET_SEATS, fid);
        final VoltTable[] results = voltExecuteSQL();
        assert (results.length == 1);

        while (results[0].advanceRow()) {
            // System.out.printf("row fid %d rid %d seat %d\n", row.getLong(0), row.getLong(1), row.getLong(2));
            seatmap[(int) (results[0].getLong(2)-1)] = results[0].getLong(1);
        }

        for (int i=0; i < 150; ++i) {
            if (seatmap[i] != -1) {
                Object[] row = new Object[] {new Integer((int)fid), new Integer(i+1)};
                retarray[0].addRow(row);
            }
        }
        return retarray;
    }

}
