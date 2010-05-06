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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.voltdb.*;
import org.voltdb.catalog.Table;
import org.voltdb.elt.processors.RawProcessor;
import org.voltdb.messaging.*;

import junit.framework.TestCase;

public class TestELTDataSource extends TestCase {

    private static class MockHostMessenger extends HostMessenger {
        public MockHostMessenger() throws UnknownHostException {
            super(null, // VoltNetwork
                  InetAddress.getLocalHost(), // Coordinator IP
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

    MockVoltDB m_mockVoltDB = new MockVoltDB();
    int m_host = 0;
    int m_site = 1;
    int m_part = 2;

    @Override
    public void setUp() {
        m_mockVoltDB.addHost(m_host);
        m_mockVoltDB.addPartition(m_part);
        m_mockVoltDB.addSite(m_site, m_host, m_part, true);
        m_mockVoltDB.addTable("TableName", false);
        m_mockVoltDB.addColumnToTable("TableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("TableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);
    }

    public void testELTDataSource() {
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ELTDataSource s = new ELTDataSource("database",
                                            table.getTypeName(),
                                            m_part,
                                            m_site,
                                            table.getRelativeIndex(),
                                            table.getColumns());

        assertEquals("database", s.getDatabase());
        assertEquals("TableName", s.getTableName());
        assertEquals(m_part, s.getPartitionId());
        assertEquals(m_site, s.getSiteId());
        assertEquals(table.getRelativeIndex(), s.getTableId());
        assertEquals(2, s.m_columnNames.size());
        assertEquals(2, s.m_columnTypes.size());
        assertEquals("COL1", s.m_columnNames.get(0));
        assertEquals("COL2", s.m_columnNames.get(1));
        assertEquals(VoltType.INTEGER.ordinal(), s.m_columnTypes.get(0).intValue());
        assertEquals(VoltType.STRING.ordinal(), s.m_columnTypes.get(1).intValue());
    }


    public void testPoll() throws MessagingException, UnknownHostException {
        MockHostMessenger hm = new MockHostMessenger();
        m_mockVoltDB.setHostMessenger(hm);
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ELTDataSource s = new ELTDataSource("database",
                                            table.getTypeName(),
                                            m_part,
                                            m_site,
                                            table.getRelativeIndex(),
                                            table.getColumns());

        ELTProtoMessage m = new ELTProtoMessage(m_part, table.getRelativeIndex());
        RawProcessor.ELTInternalMessage pair = new RawProcessor.ELTInternalMessage(null, m);

        s.poll(pair);

        assertEquals(m_site, hm.siteId);
        assertEquals(0, hm.mailboxId);
        assertTrue(hm.msg instanceof RawProcessor.ELTInternalMessage);
        assertEquals(pair, hm.msg);
    }
}
