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

import org.voltdb.client.Client;
import org.voltdb.client.NullCallback;

// Defines the calls to finish one phase of the benchmark.
public abstract class BenchmarkPhase {

    // Strings used to label the benchmark phase in the report.
    final String m_systemStr;
    final String m_csvStr;

    final boolean m_isMinMatViewCase;
    final Client m_client;

    final String m_insertProcStr;
    final String m_updateGroupProcStr;
    final String m_updateValueProcStr;
    final String m_deleteProcStr;

    public BenchmarkPhase(Client client, String systemStr, String csvStr,
                             String procStr, boolean isMinMatViewCase) {
        // apprunner has a file name length limit
        assert (csvStr.length() <= 9);
        m_client = client;
        m_systemStr = systemStr;
        m_csvStr = csvStr;
        m_isMinMatViewCase = isMinMatViewCase;
        m_insertProcStr = procStr + "_insert";
        m_updateGroupProcStr = procStr + "_group_id_update";
        m_updateValueProcStr = procStr + "_value_update";
        m_deleteProcStr = procStr + "_delete";
    }

    public void insert(int txnid, int grp) throws Exception {
        m_client.callProcedure(new NullCallback(),
                               m_insertProcStr,
                               txnid,
                               grp,
                               txnid);
    }

    public void delete(int txnid) throws Exception {
        m_client.callProcedure(new NullCallback(),
                               m_deleteProcStr,
                               txnid);
    }

    public void updateGroup(int grp, int txnid) throws Exception {
        m_client.callProcedure(new NullCallback(),
                               m_updateGroupProcStr,
                               grp,
                               txnid);
    }

    public void updateValue(int newValue, int oldValue) throws Exception {
        m_client.callProcedure(new NullCallback(),
                               m_updateGroupProcStr,
                               newValue,
                               oldValue);
    }

    public void warmUp(int warmUpCount, boolean streamview) throws Exception {
        // insert first then delete.
        for (int i = 0; i < warmUpCount; i++) {
            insert(i, i);
        }
        m_client.drain();
        if (streamview) {
            return;
        }
        for (int i = 0; i < warmUpCount; i++) {
            delete(i);
        }
        m_client.drain();
    }

    public final String getSystemString() {
        return m_systemStr;
    }

    public final String getCSVString() {
        return m_csvStr;
    }

    public final String getInsertProcString() {
        return m_insertProcStr;
    }

    public final String getUpdateGroupProcString() {
        return m_updateGroupProcStr;
    }

    public final String getUpdateValueProcString() {
        return m_updateValueProcStr;
    }

    public final String getDeleteProcString() {
        return m_deleteProcStr;
    }

    public final boolean isMinMatViewCase() {
        return m_isMinMatViewCase;
    }

    // minMaxRecalcFreq = 2, nrow = 100
    // idx      1  2  3  4  5  6  7  8  9 10 11 ...
    // minCol   1  2 10  9  8  7  6  5  4  3 11...
    // maxCol 100 99 91 92 93 94 95 96 97 98 90 ...
    // Delete from left to right, then min / max will only be updated for 20% of the time.
    static int getSkewedMinColValue(int txnid, int minMaxRecalcFreq) {
        if (minMaxRecalcFreq == 0) {
            return -txnid;
        }
        int txnidm1 = txnid - 1;
        if (txnidm1 % 10 < minMaxRecalcFreq) {
            return txnid;
        }
        else {
            return (txnidm1 / 10 + 1) * 10 + minMaxRecalcFreq - txnidm1 % 10;
        }
    }

    static int getSkewedMaxColValue(int txn, int txnid, int minMaxRecalcFreq) {
        if (minMaxRecalcFreq == 0) {
            return txnid;
        }
        int txnidm1 = txnid - 1;
        if (txnidm1 % 10 < minMaxRecalcFreq) {
            return txn - txnidm1;
        }
        else {
            return txn - (txnidm1 / 10 + 1) * 10 + txnidm1 % 10 - 1;
        }
    }
}
