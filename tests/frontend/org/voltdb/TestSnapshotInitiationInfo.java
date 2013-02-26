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

package org.voltdb;

import static org.mockito.Mockito.*;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;

import org.junit.Test;

import junit.framework.TestCase;

public class TestSnapshotInitiationInfo extends TestCase {
    // Most of the previously possible error conditions are tested in
    // TestSnapshotDaemon.  Could get moved here at some point

    @Test
    public void testInvalidService() throws JSONException
    {
        SnapshotInitiationInfo dut;
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("service").value("pwnme");
        stringer.endObject();
        Object[] params = new Object[1];
        params[0] = stringer.toString();
        try {
            dut = new SnapshotInitiationInfo(params);
        }
        catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Unknown snapshot save service"));
        }
    }

    @Test
    public void testJSONWithTruncation() throws Exception
    {
        // Need to mock VoltDB.instance().getCommandLog()
        VoltDBInterface mockVolt = mock(VoltDBInterface.class);
        CommandLog cl = mock(CommandLog.class);
        when(cl.isEnabled()).thenReturn(false);
        when(mockVolt.getCommandLog()).thenReturn(cl);
        VoltDB.replaceVoltDBInstanceForTest(mockVolt);

        // Start off w/ command log disabled
        SnapshotInitiationInfo dut;
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("service").value("log_truncation");
        stringer.endObject();
        Object[] params = new Object[1];
        params[0] = stringer.toString();
        try {
            dut = new SnapshotInitiationInfo(params);
        }
        catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("command logging is not present or enabled"));
        }

        // now turn it on and try again
        when(cl.isEnabled()).thenReturn(true);
        dut = new SnapshotInitiationInfo(params);
        assertTrue(dut.isTruncationRequest());
    }
}
