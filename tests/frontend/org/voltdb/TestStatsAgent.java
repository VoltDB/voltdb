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
package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import org.voltcore.network.*;
import org.voltdb.client.ClientResponse;

public class TestStatsAgent {

    private MockVoltDB m_mvoltdb;

    private StatsAgent m_secondAgent;
    private final LinkedBlockingQueue<ClientResponseImpl> responses = new LinkedBlockingQueue<ClientResponseImpl>();

    private final Connection m_mockConnection = new MockConnection() {

        @Override
        public WriteStream writeStream() {
            return new MockWriteStream() {
                @Override
                public void enqueue(ByteBuffer buf) {
                    ClientResponseImpl cri = new ClientResponseImpl();
                    buf.clear();
                    buf.position(4);
                    try {
                        cri.initFromBuffer(buf);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    responses.offer(cri);
                }
            };
        }
    };

    @Before
    public void setUp() throws Exception {
        m_mvoltdb = new MockVoltDB();
        m_mvoltdb.addHost(0);
        m_mvoltdb.addHost(1);
        m_mvoltdb.addSite( 1, 0, 0, false);
        m_mvoltdb.addSite( 2, 1, 0, false);
        VoltDB.replaceVoltDBInstanceForTest(m_mvoltdb);
        m_secondAgent = new StatsAgent();
        m_secondAgent.getMailbox(VoltDB.instance().getHostMessenger(), 2);
    }

    @After
    public void tearDown() throws Exception {
        MockStatsSource.delay = 0;
        StatsAgent.STATS_COLLECTION_TIMEOUT = 60 * 1000;
        m_mvoltdb.shutdown(null);
        VoltDB.replaceVoltDBInstanceForTest(null);
        m_secondAgent.shutdown();
    }

    private void createAndRegisterStats() throws Exception {
        List<VoltTable.ColumnInfo> partitionColumns = Arrays.asList(new VoltTable.ColumnInfo[] {
                new VoltTable.ColumnInfo( "p1", VoltType.INTEGER),
                new VoltTable.ColumnInfo( "p2", VoltType.STRING)
        });
        MockStatsSource.columns = partitionColumns;
        MockStatsSource partitionSource = new MockStatsSource(new Object[][] {
                { 42, "42" },
                { 43, "43" }
        });
        m_mvoltdb.getStatsAgent().registerStatsSource(SysProcSelector.WANPARTITION, 0, partitionSource);

        List<VoltTable.ColumnInfo> nodeColumns = Arrays.asList(new VoltTable.ColumnInfo[] {
                new VoltTable.ColumnInfo( "c1", VoltType.STRING),
                new VoltTable.ColumnInfo( "c2", VoltType.INTEGER)
        });
        MockStatsSource.columns = nodeColumns;
        MockStatsSource nodeSource = new MockStatsSource(new Object[][] {
                { "43", 43 },
                { "42", 43 }
        });
        m_mvoltdb.getStatsAgent().registerStatsSource(SysProcSelector.WANNODE, 0, nodeSource);

        MockStatsSource.columns = partitionColumns;
        partitionSource = new MockStatsSource(new Object[][] {
                { 44, "44" },
                { 45, "45" }
        });
        m_secondAgent.registerStatsSource(SysProcSelector.WANPARTITION, 0, partitionSource);

        MockStatsSource.columns = nodeColumns;
        nodeSource = new MockStatsSource(new Object[][] {
                { "45", 44 },
                { "44", 44 }
        });
        m_secondAgent.registerStatsSource(SysProcSelector.WANNODE, 0, nodeSource);
    }

    @Test
    public void testCollectWANStats() throws Exception {
        createAndRegisterStats();
        m_mvoltdb.getStatsAgent().collectStats( m_mockConnection, 32, "WAN");
        ClientResponseImpl response = responses.take();

        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        VoltTable results[] = response.getResults();
        System.out.println(results[0]);
        System.out.println(results[1]);
        verifyResults(response);
    }

    @Test
    public void testCollectUnsupportedStats() throws Exception {
        m_mvoltdb.getStatsAgent().collectStats( m_mockConnection, 32, "WAN");
        ClientResponseImpl response = responses.take();

        assertEquals(ClientResponse.GRACEFUL_FAILURE, response.getStatus());
        VoltTable results[] = response.getResults();
        assertEquals(0, results.length);
        assertTrue(
                "Requested statistic \"WAN\" is not supported in the current configuration".equals(
                response.getStatusString()));
    }

    @Test
    public void testCollectionTimeout() throws Exception {
        createAndRegisterStats();
        StatsAgent.STATS_COLLECTION_TIMEOUT = 300;
        MockStatsSource.delay = 200;
        m_mvoltdb.getStatsAgent().collectStats( m_mockConnection, 32, "WAN");
        ClientResponseImpl response = responses.take();

        assertEquals(ClientResponse.GRACEFUL_FAILURE, response.getStatus());
        VoltTable results[] = response.getResults();
        assertEquals(0, results.length);
        System.out.println(response.getStatusString());
        assertTrue(
                "Stats request hit sixty second timeout before all responses were received".equals(
                response.getStatusString()));
    }

    @Test
    public void testBackpressure() throws Exception {
        createAndRegisterStats();
        MockStatsSource.delay = 20;

        /*
         * Generate a bunch of requests, should get backpressure on some of them
         */
        for (int ii = 0; ii < 12; ii++) {
            m_mvoltdb.getStatsAgent().collectStats( m_mockConnection, 32, "WAN");
        }

        boolean hadBackpressure = false;
        for (int ii = 0; ii < 12; ii++) {
            ClientResponseImpl response = responses.take();

            if (response.getStatus() == ClientResponse.GRACEFUL_FAILURE) {
                assertTrue(
                        "Too many pending stat requests".equals(
                        response.getStatusString()));
                hadBackpressure = true;
            }
        }
        assertTrue(hadBackpressure);

        /*
         * Now having recieved all responses, it should be possible to collect the stats
         */
        m_mvoltdb.getStatsAgent().collectStats( m_mockConnection, 32, "WAN");
        ClientResponseImpl response = responses.take();
        verifyResults(response);
    }

    private void verifyResults(ClientResponseImpl response) {
        VoltTable results[] = response.getResults();
        assertEquals(2, results.length);

        Set<Integer> pValues = new HashSet<Integer>();
        pValues.add(45);
        pValues.add(44);
        pValues.add(43);
        pValues.add(42);

        while (results[0].advanceRow()) {
            assertTrue(pValues.contains((int)results[0].getLong(0)));
            assertTrue(pValues.contains(Integer.valueOf(results[0].getString(1))));
        }

       Set<Integer> c1Values = pValues;
       Set<Integer> c2Values = new HashSet<Integer>();
       c2Values.add(43);
       c2Values.add(44);

       while (results[1].advanceRow()) {
           assertTrue(c1Values.contains(Integer.valueOf(results[1].getString(0))));
           assertTrue(c2Values.contains((int)results[1].getLong(1)));
       }
    }
}
