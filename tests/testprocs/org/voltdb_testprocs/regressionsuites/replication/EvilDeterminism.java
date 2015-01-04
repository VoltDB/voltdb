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

package org.voltdb_testprocs.regressionsuites.replication;

import java.util.Date;
import java.util.Random;

import org.voltdb.ProcInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

@ProcInfo(
        partitionInfo = "P1.ID: 0",
        singlePartition = true
)
public class EvilDeterminism extends VoltProcedure {

    public VoltTable run(int id)
    {
        // Get a deterministic date and a non-deterministic date and
        // verify they're within ten seconds of each other as a basic
        // sanity check
        long nonDeterDate = new Date().getTime();
        long deterDate = getTransactionTime().getTime();
        long diffInMS = nonDeterDate - deterDate;
        if (diffInMS > 10000) {
            String msg = "VoltProcedure time is to far from real time: " + String.valueOf(diffInMS) + " ms";
            throw new VoltAbortException(msg);
        }

        // Get a deterministically-generated random number
        Random rand = getSeededRandomNumberGenerator();
        long randNo = rand.nextLong();

        // Return the deterministic values.
        // Replication should check to make sure they're the same
        // from all replicas.
        VoltTable retval = new VoltTable(
                new ColumnInfo("date", VoltType.BIGINT),
                new ColumnInfo("rand", VoltType.BIGINT));
        retval.addRow(deterDate, randNo);
        return retval;
    }
}
