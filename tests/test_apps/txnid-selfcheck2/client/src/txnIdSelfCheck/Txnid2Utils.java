/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package txnIdSelfCheck;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;

public class Txnid2Utils {

    /* This method does the same thing in the client that is done in the
     * server in UpdateBaseProc.java method of the same name.
     */

    public static void validateCIDData(VoltTable data, String callerId) throws Exception {
        // empty tables are lamely valid
        if (data.getRowCount() == 0) return;

        byte cid = (byte) data.fetchRow(0).getLong("cid");

        data.resetRowPosition();
        long prevCnt = 0;
        while (data.advanceRow()) {
            // check that the inner join of partitioned and replicated tables
            // produce the expected result
            byte desc = (byte) data.getLong("desc");
            if (desc != cid) {
                throw new Exception(callerId +
                        " desc value " + desc +
                        " not equal to cid value " + cid);
            }
            // make sure all cnt values are consecutive
            long cntValue = data.getLong("cnt");
            if ((prevCnt > 0) && ((prevCnt - 1) != cntValue)) {
                throw new Exception(callerId +
                        " cnt values are not consecutive" +
                        " for cid " + cid + ". Got " + cntValue +
                        ", prev was: " + prevCnt);
            }
            prevCnt = cntValue;
        }
    }
}
