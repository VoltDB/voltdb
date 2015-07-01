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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

import org.voltcore.network.*;
import org.voltdb.client.ClientResponse;

public class TestStatsAgent {

    private MockVoltDB m_mvoltdb;

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
        VoltDB.replaceVoltDBInstanceForTest(m_mvoltdb);
    }

    @After
    public void tearDown() throws Exception {
        MockStatsSource.delay = 0;
        StatsAgent.OPS_COLLECTION_TIMEOUT = 60 * 1000;
        m_mvoltdb.shutdown(null);
        VoltDB.replaceVoltDBInstanceForTest(null);
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
        m_mvoltdb.getStatsAgent().registerStatsSource(StatsSelector.DRPARTITION, 0, partitionSource);

        List<VoltTable.ColumnInfo> nodeColumns = Arrays.asList(new VoltTable.ColumnInfo[] {
                new VoltTable.ColumnInfo( "c1", VoltType.STRING),
                new VoltTable.ColumnInfo( "c2", VoltType.INTEGER)
        });
        MockStatsSource.columns = nodeColumns;
        MockStatsSource nodeSource = new MockStatsSource(new Object[][] {
                { "43", 43 },
                { "42", 43 }
        });
        m_mvoltdb.getStatsAgent().registerStatsSource(StatsSelector.DRNODE, 0, nodeSource);

        List<VoltTable.ColumnInfo> snapshotStatusColumns = Arrays.asList(new VoltTable.ColumnInfo[] {
            new VoltTable.ColumnInfo("c1", VoltType.STRING),
            new VoltTable.ColumnInfo("c2", VoltType.STRING)
        });
        MockStatsSource.columns = snapshotStatusColumns;
        MockStatsSource snapshotSource = new MockStatsSource(new Object[][] {
            {"RYANLOVES", "THEYANKEES"},
            {"NOREALLY", "ASKHIM"}
        });
        m_mvoltdb.getStatsAgent().registerStatsSource(StatsSelector.SNAPSHOTSTATUS, 0, snapshotSource);
    }

    private ParameterSet subselect(String subselector, int interval)
    {
        Object[] blah = new Object[2];
        blah[0] = subselector;
        blah[1] = interval;
        return ParameterSet.fromArrayWithCopy(blah);
    }

    @Test
    public void testInvalidStatisticsSubselector() throws Exception {
        createAndRegisterStats();
        m_mvoltdb.getStatsAgent().performOpsAction(m_mockConnection, 32,
                OpsSelector.STATISTICS, subselect("CRAZY", 0));
        ClientResponseImpl response = responses.take();
        assertEquals(ClientResponse.GRACEFUL_FAILURE, response.getStatus());
        assertEquals("First argument to @Statistics must be a valid STRING selector, instead was CRAZY",
                response.getStatusString());
        System.out.println(response.toJSONString());
    }

    @Test
    public void testCollectDRStats() throws Exception {
        createAndRegisterStats();
        m_mvoltdb.getStatsAgent().performOpsAction(m_mockConnection, 32, OpsSelector.STATISTICS, subselect("DR", 0));
        ClientResponseImpl response = responses.take();

        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        VoltTable results[] = response.getResults();
        System.out.println(results[0]);
        System.out.println(results[1]);
        verifyResults(response);
    }

    @Test
    public void testCollectSnapshotStatusStats() throws Exception {
        createAndRegisterStats();
        m_mvoltdb.getStatsAgent().performOpsAction( m_mockConnection, 32, OpsSelector.STATISTICS,
                subselect("SNAPSHOTSTATUS", 0));
        ClientResponseImpl response = responses.take();

        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        VoltTable results[] = response.getResults();
        System.out.println(results[0]);
        while (results[0].advanceRow()) {
            String c1 = results[0].getString("c1");
            String c2 = results[0].getString("c2");
            if (c1.equalsIgnoreCase("RYANLOVES")) {
                assertEquals("THEYANKEES", c2);
            }
            else if (c1.equalsIgnoreCase("NOREALLY")) {
                assertEquals("ASKHIM", c2);
            }
            else {
                fail("Unexpected row in results: c1: " + c1 + ", c2: " + c2);
            }
        }
    }

    @Test
    public void testCollectUnavailableStats() throws Exception {
        for (StatsSelector selector : StatsSelector.values()) {
            m_mvoltdb.getStatsAgent().performOpsAction(m_mockConnection, 32, OpsSelector.STATISTICS,
                    subselect(selector.name(), 0));
            ClientResponseImpl response = responses.take();
            assertEquals(ClientResponse.GRACEFUL_FAILURE, response.getStatus());
            VoltTable results[] = response.getResults();
            assertEquals(0, results.length);
            assertEquals(
                    "Requested info \"" + selector.name() + "\" is not yet available or not " +
                    "supported in the current configuration.",
                    response.getStatusString());
        }
    }

    @Test
    public void testCollectionTimeout() throws Exception {
        createAndRegisterStats();
        StatsAgent.OPS_COLLECTION_TIMEOUT = 300;
        MockStatsSource.delay = 200;
        m_mvoltdb.getStatsAgent().performOpsAction(m_mockConnection, 32, OpsSelector.STATISTICS, subselect("DR", 0));
        ClientResponseImpl response = responses.take();

        assertEquals(ClientResponse.GRACEFUL_FAILURE, response.getStatus());
        VoltTable results[] = response.getResults();
        assertEquals(0, results.length);
        System.out.println(response.getStatusString());
        assertEquals("OPS request hit sixty second timeout before all responses were received",
                response.getStatusString());
    }

    @Test
    public void testBackpressure() throws Exception {
        createAndRegisterStats();
        MockStatsSource.delay = 20;

        /*
         * Generate a bunch of requests, should get backpressure on some of them
         */
        for (int ii = 0; ii < 30; ii++) {
            m_mvoltdb.getStatsAgent().performOpsAction(m_mockConnection, 32, OpsSelector.STATISTICS, subselect("DR", 0));
        }

        boolean hadBackpressure = false;
        for (int ii = 0; ii < 30; ii++) {
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
        m_mvoltdb.getStatsAgent().performOpsAction(m_mockConnection, 32, OpsSelector.STATISTICS, subselect("DR", 0));
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
