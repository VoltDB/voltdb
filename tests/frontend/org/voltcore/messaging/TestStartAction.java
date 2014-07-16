/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltcore.messaging;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.StartAction;
import org.voltdb.VoltDB;

public class TestStartAction {
    private static final ArrayList<HostMessenger> createdMessengers = new ArrayList<HostMessenger>();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        for (HostMessenger hm : createdMessengers) {
            hm.shutdown();
        }
        createdMessengers.clear();
    }

    private HostMessenger createHostMessenger(int index, StartAction action, boolean start) throws Exception {
        HostMessenger.Config config = new HostMessenger.Config();
        config.internalPort = config.internalPort + index;
        config.zkInterface = "127.0.0.1:" + (2181 + index);
        config.startAction = action;
        HostMessenger hm = new HostMessenger(config);
        createdMessengers.add(hm);
        if (start) {
            hm.start();
        }
        return hm;
    }

    @Test
    public void testCreateAndJoin() throws Exception {
        final HostMessenger hm1 = createHostMessenger(0, StartAction.CREATE, false);
        final HostMessenger hm2 = createHostMessenger(1, StartAction.JOIN, false);

        try {
            hm1.start();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            VoltDB.ignoreCrash = true;
            hm2.start();
            VoltDB.ignoreCrash = false;
        } catch (AssertionError e) {}

        assertTrue(VoltDB.wasCrashCalled);
        VoltDB.wasCrashCalled = false;
    }

    @Test
    public void testCreateAndRejoin() throws Exception {
        final HostMessenger hm1 = createHostMessenger(0, StartAction.CREATE, false);
        final HostMessenger hm2 = createHostMessenger(1, StartAction.REJOIN, false);

        try {
            hm1.start();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            VoltDB.ignoreCrash = true;
            hm2.start();
            VoltDB.ignoreCrash = false;
        } catch (AssertionError e) {}

        assertTrue(VoltDB.wasCrashCalled);
        VoltDB.wasCrashCalled = false;
    }

    @Test
    public void testCreateAndLiveRejoin() throws Exception {
        final HostMessenger hm1 = createHostMessenger(0, StartAction.CREATE, false);
        final HostMessenger hm2 = createHostMessenger(1, StartAction.LIVE_REJOIN, false);

        try {
            hm1.start();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            VoltDB.ignoreCrash = true;
            hm2.start();
            VoltDB.ignoreCrash = false;
        } catch (AssertionError e) {}

        assertTrue(VoltDB.wasCrashCalled);
        VoltDB.wasCrashCalled = false;
    }

    @Test
    public void testCreateAndCreate() throws Exception {
        final HostMessenger hm1 = createHostMessenger(0, StartAction.CREATE, false);
        final HostMessenger hm2 = createHostMessenger(1, StartAction.CREATE, false);

        try {
            hm1.start();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        try {
            VoltDB.ignoreCrash = true;
            hm2.start();
            VoltDB.ignoreCrash = false;
        } catch (AssertionError e) {}

        assertFalse(VoltDB.wasCrashCalled);
        hm1.waitForGroupJoin(2);
        hm2.waitForGroupJoin(2);
        VoltDB.wasCrashCalled = false;
    }

}
