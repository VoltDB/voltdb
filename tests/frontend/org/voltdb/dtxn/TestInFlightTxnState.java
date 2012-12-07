/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.Random;

import junit.framework.TestCase;

import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

public class TestInFlightTxnState extends TestCase
{
    InFlightTxnState createTxnState(long txnId, int[] coordIds) {
        return createTxnState(txnId, coordIds, false, false);
    }

    InFlightTxnState createTxnState(long txnId, int[] coordIds, boolean readOnly, boolean allowMismatched) {
        InFlightTxnState retval = new InFlightTxnState(txnId, coordIds[0], null, new long[]{},
                readOnly, true, new StoredProcedureInvocation(), new Object(), 0, 0, 0, "", false, allowMismatched);
        for (int i = 1; i < coordIds.length; i++) {
            retval.addCoordinator(coordIds[i]);
        }
        return retval;
    }

    public void testBasicOps()
    {
        // Add a single transaction state
        InFlightTxnState state1 =  createTxnState(1, new int[] {2,3,1});
        assertEquals(3, state1.m_outstandingCoordinators.size());
        assertEquals(3, state1.m_outstandingResponses);
        assertFalse(state1.hasAllResponses());

        InFlightTxnState state2 = createTxnState(2, new int[] {2});
        assertEquals(1, state2.m_outstandingCoordinators.size());
        assertEquals(1, state2.m_outstandingResponses);
        assertFalse(state2.hasAllResponses());

        ClientResponseImpl cri = new ClientResponseImpl();
        state1.addFailedOrRecoveringResponse(3);
        assertEquals(2, state1.m_outstandingCoordinators.size());
        assertEquals(2, state1.m_outstandingResponses);
        assertFalse(state1.hasAllResponses());

        state1.addResponse(2, cri);
        assertEquals(1, state1.m_outstandingCoordinators.size());
        assertEquals(1, state1.m_outstandingResponses);
        assertFalse(state1.hasAllResponses());

        state1.addResponse(1, cri);
        assertEquals(0, state1.m_outstandingCoordinators.size());
        assertEquals(0, state1.m_outstandingResponses);
        assertTrue(state1.hasAllResponses());
        assertTrue(state1.hasSentResponse());
    }

    public void testRandomOps()
    {
        Random r = new Random();
        ClientResponseImpl cri = new ClientResponseImpl();

        for (int i = 0; i < 10; i++) {
            int coordCount = r.nextInt(10) + 1; // between 1 and 10 inclusive
            ArrayList<Integer> coords = new ArrayList<Integer>();
            int[] coordsArray = new int[coordCount];
            for (int j = 0; j < coordCount; j++) {
                coords.add(j + 1);
                coordsArray[j] = j + 1;
            }
            InFlightTxnState state = createTxnState(1, coordsArray);

            boolean hasOneValidResponse = false;
            while (coords.size() > 0) {
                int coord = coords.remove(r.nextInt(coords.size()));

                if (r.nextBoolean()) {
                    state.addResponse(coord, cri);
                    hasOneValidResponse = true;
                }
                else {
                    state.addFailedOrRecoveringResponse(coord);
                }

                assertEquals(state.countOutstandingResponses(), coords.size());
            }

            assertTrue(state.hasAllResponses());
            if (hasOneValidResponse) {
                assertTrue(state.hasSentResponse());
            }
        }

    }

    public void testHashDeterminismChecks() {
        VoltDB.ignoreCrash = true;

        // Test hash mismatch on read-only
        InFlightTxnState state =  createTxnState(1, new int[] {3,1}, true, true);
        ClientResponseImpl cri1 = new ClientResponseImpl();
        ClientResponseImpl cri2 = new ClientResponseImpl();
        cri1.setHash(0);
        cri2.setHash(1);
        state.addResponse(1, cri1);
        try {
            state.addResponse(3, cri2);
            fail("Mismatched hash in response should have failed");
        }
        catch (AssertionError e) {
            // success
        }
        VoltDB.wasCrashCalled = false;

        // Test hash mismatch on read-only with no mismatches
        state =  createTxnState(1, new int[] {3,1}, true, false);
        cri1 = new ClientResponseImpl();
        cri2 = new ClientResponseImpl();
        cri1.setHash(0);
        cri2.setHash(1);
        state.addResponse(1, cri1);
        try {
            state.addResponse(3, cri2);
            fail("Mismatched hash in response should have failed");
        }
        catch (AssertionError e) {
            // success
        }
        VoltDB.wasCrashCalled = false;

        // Test hash mismatch on read-write with no mismatches
        state =  createTxnState(1, new int[] {3,1}, false, false);
        cri1 = new ClientResponseImpl();
        cri2 = new ClientResponseImpl();
        cri1.setHash(0);
        cri2.setHash(1);
        state.addResponse(1, cri1);
        try {
            state.addResponse(3, cri2);
            fail("Mismatched hash in response should have failed");
        }
        catch (AssertionError e) {
            // success
        }
        VoltDB.wasCrashCalled = false;

        // Test hash match on non-failing case
        state =  createTxnState(1, new int[] {3,1}, false, false);
        cri1 = new ClientResponseImpl();
        cri2 = new ClientResponseImpl();
        cri1.setHash(1);
        cri2.setHash(1);
        state.addResponse(1, cri1);
        state.addResponse(3, cri2);
        assertFalse(VoltDB.wasCrashCalled);

        VoltDB.ignoreCrash = false;
    }

    public void testContentDeterminsimChecks() {
        VoltDB.ignoreCrash = true;

        VoltTable t1 = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT));
        t1.addRow(7);
        VoltTable t2 = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.BIGINT));
        t2.addRow(6);

        // Test hash mismatch on read-only
        InFlightTxnState state =  createTxnState(1, new int[] {3,1}, true, true);
        ClientResponseImpl cri1 = new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { t1 }, "");
        ClientResponseImpl cri2 = new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { t2 }, "");
        state.addResponse(1, cri1);
        state.addResponse(3, cri2);
        assertFalse(VoltDB.wasCrashCalled);

        // Test hash mismatch on read-only with no mismatches
        state =  createTxnState(1, new int[] {3,1}, true, false);
        cri1 = new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { t1 }, "");
        cri2 = new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { t2 }, "");
        state.addResponse(1, cri1);
        try {
            state.addResponse(3, cri2);
            fail("Mismatched hash in response should have failed");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        catch (AssertionError e) {
            // success
        }
        VoltDB.wasCrashCalled = false;

        // Test hash mismatch on read-write with no mismatches
        state =  createTxnState(1, new int[] {3,1}, false, false);
        cri1 = new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { t1 }, "");
        cri2 = new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { t2 }, "");
        state.addResponse(1, cri1);
        try {
            state.addResponse(3, cri2);
            fail("Mismatched hash in response should have failed");
        }
        catch (AssertionError e) {
            // success
        }
        VoltDB.wasCrashCalled = false;

        // Test hash match on non-failing case
        state =  createTxnState(1, new int[] {3,1}, false, false);
        cri1 = new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { t1 }, "");
        cri2 = new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { t1 }, "");
        state.addResponse(1, cri1);
        state.addResponse(3, cri2);
        assertFalse(VoltDB.wasCrashCalled);

        VoltDB.ignoreCrash = false;
    }
}
