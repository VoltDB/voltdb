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

package org.voltdb.elt;

import org.voltdb.*;
import org.voltdb.elt.processors.RawProcessor;
import org.voltdb.messaging.*;

import junit.framework.TestCase;

public class TestELTDataSource extends TestCase {

    private static class MockHostMessenger extends HostMessenger {
        public MockHostMessenger() {
            super(null, // VoltNetwork
                  null, // Coordinator IP
                  1,    // expected hosts,
                  0,    // catalogCRC
                  null); // hostLog);
        }

        @Override
        public void send(final int siteId, final int mailboxId, final VoltMessage msg) {
            this.siteId = siteId;
            this.mailboxId = mailboxId;
            this.msg = msg;
        }

        public int siteId = -1;
        public int mailboxId = -1;
        public VoltMessage msg = null;
    }

    public void testELTDataSource() {
        ELTDataSource s = new ELTDataSource("database", "table", 1,  2, 3);
        assertEquals("database", s.getDatabase());
        assertEquals("table", s.getTableName());
        assertEquals(1, s.getPartitionId());
        assertEquals(2, s.getSiteId());
        assertEquals(3, s.getTableId());
    }


    public void testPoll() throws MessagingException {
        MockHostMessenger hm = new MockHostMessenger();
        MockVoltDB mockvolt = new MockVoltDB();
        mockvolt.setHostMessenger(hm);
        mockvolt.addHost(10);
        mockvolt.addPartition(20);
        mockvolt.addSite(30, 10, 20, true);
        VoltDB.replaceVoltDBInstanceForTest(mockvolt);

        ELTProtoMessage m = new ELTProtoMessage(1, 3);
        RawProcessor.ELTInternalMessage pair = new RawProcessor.ELTInternalMessage(null, m);
        ELTDataSource s = new ELTDataSource("database", "table", 1, 2, 3);
        s.poll(pair);

        assertEquals(2, hm.siteId);
        assertEquals(0, hm.mailboxId);
        assertTrue(hm.msg instanceof RawProcessor.ELTInternalMessage);
        assertEquals(pair, hm.msg);
    }
}
