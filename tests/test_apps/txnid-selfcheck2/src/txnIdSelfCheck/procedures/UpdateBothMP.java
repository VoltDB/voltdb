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

package txnIdSelfCheck.procedures;

import org.voltdb.VoltTable;
import org.voltdb.utils.MiscUtils;
import org.voltdb.VoltProcedure;

public class UpdateBothMP extends ReplicatedUpdateBaseProc {

    public VoltTable[] run(byte cid, long rid, byte[] value, byte rollback) {
        VoltTable[] results1 = doWork(p_getCIDData, p_cleanUp, p_insert, p_export, p_getAdhocData,
                cid, rid, value, rollback);
        VoltTable[] results2 = doWork(r_getCIDData, r_cleanUp, r_insert, r_export, r_getAdhocData,
                cid, rid, value, rollback);

        assert(results1.length == 3);
        assert(results2.length == 3);

        // make sure the partitioned and local results are the same

        long checksumR1a = MiscUtils.cheesyBufferCheckSum(results1[0].getBuffer());
        long checksumR1b = MiscUtils.cheesyBufferCheckSum(results1[1].getBuffer());
        long checksumR1c = MiscUtils.cheesyBufferCheckSum(results1[2].getBuffer());

        long checksumR2a = MiscUtils.cheesyBufferCheckSum(results2[0].getBuffer());
        long checksumR2b = MiscUtils.cheesyBufferCheckSum(results2[1].getBuffer());
        long checksumR2c = MiscUtils.cheesyBufferCheckSum(results2[2].getBuffer());

        assert(checksumR1a == checksumR2a);
        assert(checksumR1b == checksumR2b);
        assert(checksumR1c == checksumR2c);

        VoltTable[] combined = doSummaryAndCombineResults(results2);

        return combined;
    }

}
