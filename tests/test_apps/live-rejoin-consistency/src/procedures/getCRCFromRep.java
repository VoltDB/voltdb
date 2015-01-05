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

//
// Returns the heat map data (winning contestant by state) for display on nthe Live Statistics dashboard.
//

package LiveRejoinConsistency.procedures;

import java.util.zip.CRC32;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;


@ProcInfo (
        partitionInfo = "joiner.id:0",
        singlePartition = true
        )

public class getCRCFromRep extends VoltProcedure {

    // get Counter
    public final SQLStmt Stmt = new SQLStmt(
            "SELECT j.id as id, c.counter as counter FROM joiner j, like_counters_rep c WHERE j.id=c.id and c.id = ? order by 1;");

    public long run(int id) {

        CRC32 crc = new CRC32();

        voltQueueSQL(Stmt, id);
        VoltTable[] result = voltExecuteSQL(true);

        while (result[0].advanceRow()) {
            long counter = result[0].getLong("counter");

            byte [] b = new byte[8];
            for(int i= 0; i < 8; i++) {
                b[7 - i] = (byte)(counter >>> (i * 8));
            }

            crc.update(b);
        }
        return crc.getValue();
    }
}
