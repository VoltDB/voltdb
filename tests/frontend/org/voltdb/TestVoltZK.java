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

package org.voltdb;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.zk.CoreZK;
import org.voltcore.zk.ZKTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestVoltZK extends ZKTestBase {
    private ZooKeeper m_zk = null;

    @Before
    public void setUp() throws Exception
    {
        VoltDB.ignoreCrash = false;
        VoltDB.wasCrashCalled = false;
        setUpZK(1);
        m_zk = getClient(0);
        VoltZK.createPersistentZKNodes(m_zk);
    }

    @After
    public void tearDown() throws Exception
    {
        if (m_zk != null) {
            m_zk.close();
        }
        tearDownZK();
    }

    @Test
    public void testRejoinBlocker()
    {
        // Create a rejoin blocker for host 0
        assertEquals(-1, CoreZK.createRejoinNodeIndicator(m_zk, 0));
        // Try to create a blocker for host 1 while host 0 is still in progress, should fail
        assertEquals(0, CoreZK.createRejoinNodeIndicator(m_zk, 1));
        // Try removing the blocker for host 1, which doesn't hold the blocker, no-op
        assertFalse(CoreZK.removeRejoinNodeIndicatorForHost(m_zk, 1));
        // Remove host 0's blocker
        assertTrue(CoreZK.removeRejoinNodeIndicatorForHost(m_zk, 0));
        // Should be able to create another blocker now
        assertEquals(-1, CoreZK.createRejoinNodeIndicator(m_zk, 2));
        assertTrue(CoreZK.removeRejoinNodeIndicatorForHost(m_zk, 2));
        // Removing the same hostId twice should be okay
        assertTrue(CoreZK.removeRejoinNodeIndicatorForHost(m_zk, 2));
    }
}
