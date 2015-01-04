/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.util.Date;

import junit.framework.TestCase;

import org.voltcore.utils.CoreUtils;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

public class TestVoltProcedure extends TestCase {
    static class DateProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(Date arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static Date arg;
    }

    static class TimestampProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(TimestampType arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static TimestampType arg;
    }

    static class StringProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(String arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static String arg;
    }

    static class DecimalProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(BigDecimal arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static BigDecimal arg;
    }

    static class ByteProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(byte arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static byte arg;
    }

    static class ShortProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(short arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static short arg;
    }

    static class IntegerProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(int arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static int arg;
    }

    static class LongProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(long arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static long arg;
    }

    static class DoubleProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(double arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static double arg;
    }

    static class BoxedByteProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(Byte arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static Byte arg;
    }

    static class BoxedShortProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(Short arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static Short arg;
    }

    static class BoxedIntegerProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(Integer arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static Integer arg;
    }

    static class BoxedLongProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(Long arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static Long arg;
    }

    static class BoxedDoubleProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(Double arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static Double arg;
    }

    static class LongArrayProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(long[] arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static long[] arg;
    }

    static class NPEProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(String arg) {
            return new VoltTable[arg.length()];
        }
    }

    // See ENG-807
    static class UnexpectedFailureFourProcedure extends NullProcedureWrapper
    {
        public static VoltTable[] run(String arg)
        {
            String[] haha = {"Amusingly", "Horrible", "Message"};
            String boom = haha[4];
            return new VoltTable[boom.length()];
        }
    }

    static class NullProcedureWrapper extends VoltProcedure {
        VoltTable runQueryStatement(SQLStmt stmt, Object... params) {
            assert false;
            return null;
        }
        long runDMLStatement(SQLStmt stmt, Object... params) {
            assert false;
            return -1;
        }
        void addQueryStatement(SQLStmt stmt, Object... args) {
            assert false;
        }
        void addDMLStatement(SQLStmt stmt, Object... args) {
            assert false;
        }
        VoltTable[] executeQueryBatch() {
            assert false;
            return null;
        }
        long[] executeDMLBatch() {
            assert false;
            return null;
        }
    }

    MockVoltDB manager;
    SiteProcedureConnection site;
    MockStatsAgent agent;
    ParameterSet nullParam;
    private long executionSiteId;

    @Override
    public void setUp()
    {
        manager = new MockVoltDB();

        final long executionSiteId = CoreUtils.getHSIdFromHostAndSite( 0, 42);
        this.executionSiteId = executionSiteId;
        manager.addSite(executionSiteId, 0);
        agent = new MockStatsAgent();
        manager.setStatsAgent(agent);
        VoltDB.replaceVoltDBInstanceForTest(manager);
        manager.addProcedureForTest(DateProcedure.class.getName());
        manager.addProcedureForTest(TimestampProcedure.class.getName());
        manager.addProcedureForTest(StringProcedure.class.getName());
        manager.addProcedureForTest(DecimalProcedure.class.getName());
        manager.addProcedureForTest(ByteProcedure.class.getName());
        manager.addProcedureForTest(ShortProcedure.class.getName());
        manager.addProcedureForTest(IntegerProcedure.class.getName());
        manager.addProcedureForTest(LongProcedure.class.getName());
        manager.addProcedureForTest(DoubleProcedure.class.getName());
        manager.addProcedureForTest(BoxedByteProcedure.class.getName());
        manager.addProcedureForTest(BoxedShortProcedure.class.getName());
        manager.addProcedureForTest(BoxedIntegerProcedure.class.getName());
        manager.addProcedureForTest(BoxedLongProcedure.class.getName());
        manager.addProcedureForTest(BoxedDoubleProcedure.class.getName());
        manager.addProcedureForTest(LongArrayProcedure.class.getName());
        manager.addProcedureForTest(NPEProcedure.class.getName());
        manager.addProcedureForTest(UnexpectedFailureFourProcedure.class.getName());
        site = mock(SiteProcedureConnection.class);
        doReturn(42).when(site).getCorrespondingPartitionId();
        doReturn(executionSiteId).when(site).getCorrespondingSiteId();
        nullParam = ParameterSet.fromArrayNoCopy(new Object[]{null});
    }

    @Override
    public void tearDown() throws Exception {
        manager.shutdown(null);
    }

    /**
     * XXX (Ning) I'm not sure this test is still useful since we don't support
     * Java Date object anymore.
     */
    public void testNullDate() {
        ClientResponse r = call(DateProcedure.class);
        assertEquals(null, DateProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullTimestamp() {
        ClientResponse r = call(TimestampProcedure.class);
        assertEquals(null, TimestampProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullString() {
        ClientResponse r = call(StringProcedure.class);
        assertEquals(null, StringProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullDecimal() {
        ClientResponse r = call(DecimalProcedure.class);
        assertEquals(null, DecimalProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullByte() {
        ClientResponse r = call(ByteProcedure.class);
        assertEquals(VoltType.NULL_TINYINT, ByteProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullShort() {
        ClientResponse r = call(ShortProcedure.class);
        assertEquals(VoltType.NULL_SMALLINT, ShortProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullInteger() {
        ClientResponse r = call(IntegerProcedure.class);
        assertEquals(VoltType.NULL_INTEGER, IntegerProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullLong() {
        ClientResponse r = call(LongProcedure.class);
        assertEquals(VoltType.NULL_BIGINT, LongProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullDouble() {
        ClientResponse r = call(DoubleProcedure.class);
        assertEquals(VoltType.NULL_FLOAT, DoubleProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullBoxedByte() {
        ClientResponse r = call(BoxedByteProcedure.class);
        assertEquals(null, BoxedByteProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullBoxedShort() {
        ClientResponse r = call(BoxedShortProcedure.class);
        assertEquals(null, BoxedShortProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullBoxedInteger() {
        ClientResponse r = call(BoxedIntegerProcedure.class);
        assertEquals(null, BoxedIntegerProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullBoxedLong() {
        ClientResponse r = call(BoxedLongProcedure.class);
        assertEquals(null, BoxedLongProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullBoxedDouble() {
        ClientResponse r = call(BoxedDoubleProcedure.class);
        assertEquals(null, BoxedDoubleProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullLongArray() {
        ClientResponse r = call(LongArrayProcedure.class);
        assertEquals(null, LongArrayProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullPointerException() {
        ClientResponse r = call(NPEProcedure.class);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.getStatus());
        System.out.println(r.getStatusString());
        assertTrue(r.getStatusString().contains("java.lang.NullPointerException"));
    }

    public void testNegativeWiderType() {
        ClientResponse r = callWithArgs(LongProcedure.class, Integer.valueOf(-1000));
        assertEquals(-1000L, LongProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testUnexpectedFailureFour() {
        ClientResponse r = call(UnexpectedFailureFourProcedure.class);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.getStatus());
        System.out.println(r.getStatusString());
        assertTrue(r.getStatusString().contains("java.lang.ArrayIndexOutOfBoundsException"));
    }

    public void testProcedureStatsCollector() {
        NullProcedureWrapper wrapper = new LongProcedure();
        ProcedureRunner runner = new ProcedureRunner(
                wrapper, site, null,
                VoltDB.instance().getCatalogContext().database.getProcedures().get(LongProcedure.class.getName()), null);

        ParameterSet params = ParameterSet.fromArrayNoCopy(1L);
        assertNotNull(agent.m_selector);
        assertNotNull(agent.m_source);
        assertEquals(agent.m_selector, StatsSelector.PROCEDURE);
        assertEquals(agent.m_catalogId,
                     executionSiteId);
        Object statsRow[][] = agent.m_source.getStatsRows(false, 0L);
        assertNotNull(statsRow);
        assertEquals( 0, statsRow.length);
        for (int ii = 1; ii < 200; ii++) {
            runner.setupTransaction(null);
            runner.call(params.toArray());
            statsRow = agent.m_source.getStatsRows(false, 0L);
            assertEquals(statsRow[0][6], new Long(ii));
        }
        assertTrue(((Long)statsRow[0][6]).longValue() > 0L);
        assertTrue(((Long)statsRow[0][7]).longValue() > 0L);
        assertFalse(statsRow[0][8].equals(0));
        assertFalse(statsRow[0][9].equals(0));
        assertTrue(((Long)statsRow[0][9]) > 0L);
    }

    private ClientResponse call(Class<? extends NullProcedureWrapper> procedure) {
        return callWithArgs(procedure, (Object) null);
    }

    private ClientResponse callWithArgs(Class<? extends NullProcedureWrapper> procedure, Object... args) {
        NullProcedureWrapper wrapper = null;
        try {
            wrapper = procedure.newInstance();
        }  catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        ProcedureRunner runner = new ProcedureRunner(
                wrapper, site, null,
                VoltDB.instance().getCatalogContext().database.getProcedures().get(LongProcedure.class.getName()), null);

        runner.setupTransaction(null);
        return runner.call(args);
    }

    private class MockStatsAgent extends StatsAgent {
        public StatsSource m_source = null;
        public StatsSelector m_selector = null;
        public long m_catalogId = 0;

        @Override
        public void registerStatsSource(StatsSelector selector, long catalogId, StatsSource source) {
            m_source = source;
            m_selector = selector;
            m_catalogId = catalogId;
        }
    }
}
