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

package matviewbenchmark;

import matviewbenchmark.MaterializedViewBenchmark.MatViewConfig;
import org.voltdb.client.Client;
import org.voltdb.client.NullCallback;

public class JoinedTableViewPhase extends BenchmarkPhase {

    static boolean joinedViewSrc2Populated = false;
    int m_minMaxRecalcFreq;

    public JoinedTableViewPhase(Client client, MatViewConfig config, int minMaxRecalcFreq) throws Exception {
        super(client, "2tables view freq" + minMaxRecalcFreq,
                      "2t freq" + minMaxRecalcFreq, "joinedViewSrc1", true);
        // The number of min/max recalculation out of ten delete operations.
        m_minMaxRecalcFreq = minMaxRecalcFreq;
        if (! joinedViewSrc2Populated) {
            System.out.println("Preparing joinViewIdxSrc2...");
            for (int i = 1; i <= config.group; i++) {
                m_client.callProcedure(new NullCallback(), "joinedViewSrc2_insert", i);
            }
            joinedViewSrc2Populated = true;
        }
    }

    @Override
    public void insert(int txnid, int grp) throws Exception {
        m_client.callProcedure(new NullCallback(),
                               m_insertProcStr,
                               txnid,
                               grp, grp,
                               txnid, getSkewedMinColValue(txnid, m_minMaxRecalcFreq), txnid);
    }
}
