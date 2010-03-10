/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
package org.voltdb.dtxn;

import org.voltdb.StoredProcedureInvocation;

import junit.framework.TestCase;

public class TestPendingTxnList extends TestCase
{
    InFlightTxnState createTxnState(long txnId, int coordId)
    {
        return new InFlightTxnState(txnId, coordId, new int[]{}, false, true,
                                    new StoredProcedureInvocation(), new Object(),
                                    0, 0, 0);
    }

    public void testBasicOps()
    {
        PendingTxnList dut = new PendingTxnList();
        // Add a single transaction state
        dut.addTxn(1, 2, createTxnState(1, 2));
        assertEquals(1, dut.size());
        assertEquals(1, dut.getTxnIdSize(1));
        // now add a state for the same TXN id but different coordinator ID
        dut.addTxn(1, 3, createTxnState(1, 3));
        assertEquals(1, dut.size());
        assertEquals(2, dut.getTxnIdSize(1));
        InFlightTxnState result = dut.getTxn(1, 2);
        assertEquals(1, result.txnId);
        assertEquals(2, result.coordinatorId);
        assertEquals(1, dut.size());
        assertEquals(1, dut.getTxnIdSize(1));
        // add a state for a new TXN id
        dut.addTxn(2, 2, createTxnState(2, 2));
        assertEquals(2, dut.size());
        result = dut.getTxn(1, 3);
        dut.removeTxnId(1);
        assertEquals(1, dut.size());
    }

    public void testSiteRemoval()
    {
        PendingTxnList dut = new PendingTxnList();
        dut.addTxn(1, 2, createTxnState(1, 2));
        dut.addTxn(1, 3, createTxnState(1, 3));
        dut.addTxn(2, 1, createTxnState(2, 1));
        dut.addTxn(2, 2, createTxnState(2, 2));
        dut.addTxn(2, 4, createTxnState(2, 4));
        dut.addTxn(3, 2, createTxnState(3, 2));
        dut.addTxn(4, 1, createTxnState(4, 1));
        assertEquals(4, dut.size());
        assertEquals(2, dut.getTxnIdSize(1));
        assertEquals(3, dut.getTxnIdSize(2));
        assertEquals(1, dut.getTxnIdSize(3));
        assertEquals(1, dut.getTxnIdSize(4));
        dut.removeSite(2);
        assertEquals(4, dut.size());
        assertEquals(1, dut.getTxnIdSize(1));
        assertEquals(2, dut.getTxnIdSize(2));
        assertEquals(0, dut.getTxnIdSize(3));
        assertEquals(1, dut.getTxnIdSize(4));
    }
}
