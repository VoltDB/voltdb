/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.export;

import java.net.UnknownHostException;

import junit.framework.TestCase;

import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.export.processors.RawProcessor;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.VoltMessage;

public class TestExportDataSource extends TestCase {

    private static class MockHostMessenger extends HostMessenger {
        public MockHostMessenger() throws UnknownHostException {
            super(null, // VoltNetwork
                  ConnectionUtil.getLocalAddress(), // Coordinator IP
                  1,    // expected hosts,
                  0,    // catalogCRC
                  0,    // deploymentCRC
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
        m_mockVoltDB.addTable("RepTableName", true);
        m_mockVoltDB.addColumnToTable("RepTableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("RepTableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);
    }

    public void testExportDataSource()
    {
        String[] tables = {"TableName", "RepTableName"};
        for (String table_name : tables)
        {
            Table table = m_mockVoltDB.getCatalogContext().database.getTables().get(table_name);
            ExportDataSource s = new ExportDataSource("database",
                                                table.getTypeName(),
                                                table.getIsreplicated(),
                                                m_part,
                                                m_site,
                                                table.getRelativeIndex(),
                                                table.getColumns());

            assertEquals("database", s.getDatabase());
            assertEquals(table_name, s.getTableName());
            assertEquals((table_name.equals("RepTableName") ? 1 : 0), s.getIsReplicated());
            assertEquals(m_part, s.getPartitionId());
            assertEquals(m_site, s.getSiteId());
            assertEquals(table.getRelativeIndex(), s.getTableId());
            // There are 6 additional Export columns added
            assertEquals(2 + 6, s.m_columnNames.size());
            assertEquals(2 + 6, s.m_columnTypes.size());
            assertEquals("VOLT_TRANSACTION_ID", s.m_columnNames.get(0));
            assertEquals("VOLT_EXPORT_OPERATION", s.m_columnNames.get(5));
            assertEquals("COL1", s.m_columnNames.get(6));
            assertEquals("COL2", s.m_columnNames.get(7));
            assertEquals(VoltType.INTEGER.ordinal(), s.m_columnTypes.get(6).intValue());
            assertEquals(VoltType.STRING.ordinal(), s.m_columnTypes.get(7).intValue());
        }
    }

    public void testPoll() throws MessagingException, UnknownHostException {
        MockHostMessenger hm = new MockHostMessenger();
        m_mockVoltDB.setHostMessenger(hm);
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new ExportDataSource("database",
                                            table.getTypeName(),
                                            table.getIsreplicated(),
                                            m_part,
                                            m_site,
                                            table.getRelativeIndex(),
                                            table.getColumns());

        ExportProtoMessage m = new ExportProtoMessage(m_part, table.getRelativeIndex());
        RawProcessor.ExportInternalMessage pair = new RawProcessor.ExportInternalMessage(null, m);

        s.exportAction(pair);

        assertEquals(m_site, hm.siteId);
        assertEquals(0, hm.mailboxId);
        assertTrue(hm.msg instanceof RawProcessor.ExportInternalMessage);
        assertEquals(pair, hm.msg);
    }
}
