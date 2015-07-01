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

package org.voltdb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestStartAction {
    private MockVoltDB m_mvoltdb;

    @Before
    public void setUp() throws Exception {
        m_mvoltdb = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(m_mvoltdb);
    }

    @After
    public void tearDown() throws Exception {
        m_mvoltdb.shutdown(null);
        VoltDB.replaceVoltDBInstanceForTest(null);
    }

    @Test
    public void testCreateAndJoin() throws Exception {
        m_mvoltdb.createStartActionNode(0, StartAction.JOIN);
        m_mvoltdb.createStartActionNode(1, StartAction.CREATE);

        try {
            VoltDB.ignoreCrash = true;
            m_mvoltdb.validateStartAction();
            VoltDB.ignoreCrash = false;
        } catch (AssertionError e) {}

        assertTrue(VoltDB.wasCrashCalled);
        VoltDB.wasCrashCalled = false;
    }

    @Test
    public void testCreateAndRejoin() throws Exception {
        m_mvoltdb.createStartActionNode(0, StartAction.REJOIN);
        m_mvoltdb.createStartActionNode(1, StartAction.CREATE);

        try {
            VoltDB.ignoreCrash = true;
            m_mvoltdb.validateStartAction();
            VoltDB.ignoreCrash = false;
        } catch (AssertionError e) {}

        assertTrue(VoltDB.wasCrashCalled);
        VoltDB.wasCrashCalled = false;
    }

    @Test
    public void testCreateAndLiveRejoin() throws Exception {
        m_mvoltdb.createStartActionNode(0, StartAction.LIVE_REJOIN);
        m_mvoltdb.createStartActionNode(1, StartAction.CREATE);

        try {
            VoltDB.ignoreCrash = true;
            m_mvoltdb.validateStartAction();
            VoltDB.ignoreCrash = false;
        } catch (AssertionError e) {}

        assertTrue(VoltDB.wasCrashCalled);
        VoltDB.wasCrashCalled = false;
    }

    @Test
    public void testCreateAndCreate() throws Exception {
        m_mvoltdb.createStartActionNode(0, StartAction.CREATE);
        m_mvoltdb.createStartActionNode(1, StartAction.CREATE);

        try {
            VoltDB.ignoreCrash = true;
            m_mvoltdb.validateStartAction();
            VoltDB.ignoreCrash = false;
        } catch (AssertionError e) {}

        assertFalse(VoltDB.wasCrashCalled);
        VoltDB.wasCrashCalled = false;
    }

}
