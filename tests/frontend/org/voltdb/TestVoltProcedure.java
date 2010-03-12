/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

import java.util.Date;

import junit.framework.TestCase;

import org.voltdb.catalog.Catalog;
import org.voltdb.client.ClientResponse;

public class TestVoltProcedure extends TestCase {
    static class DateProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(Date arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static Date arg;
    }

    static class LongProcedure extends NullProcedureWrapper {
        public static VoltTable[] run(long arg1) {
            arg = arg1;
            return new VoltTable[0];
        }

        public static long arg;
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

    static class NullProcedureWrapper extends VoltProcedure {
        //NullProcedureWrapper(ExecutionSite site, Class<?> c) {
        //  super(site, new Procedure(), c);
        //}

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
    MockExecutionSite site;
    MockStatsAgent agent;
    ParameterSet nullParam;

    @Override
    public void setUp()
    {
        manager = new MockVoltDB();
        manager.addHost(0);
        manager.addPartition(0);
        manager.addSite(1, 0, 0, true);
        agent = new MockStatsAgent();
        manager.setStatsAgent(agent);
        VoltDB.replaceVoltDBInstanceForTest(manager);
        manager.addProcedureForTest(DateProcedure.class.getName()).setClassname(DateProcedure.class.getName());
        manager.addProcedureForTest(LongProcedure.class.getName()).setClassname(LongProcedure.class.getName());
        manager.addProcedureForTest(LongArrayProcedure.class.getName()).setClassname(LongArrayProcedure.class.getName());
        manager.addProcedureForTest(NPEProcedure.class.getName()).setClassname(NPEProcedure.class.getName());
        site = new MockExecutionSite(1, VoltDB.instance().getCatalogContext().catalog.serialize());
        nullParam = new ParameterSet();
        nullParam.setParameters(new Object[]{null});
    }

    public void testNullDate() {
        ClientResponse r = call(DateProcedure.class);
        assertEquals(null, DateProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullLong() {
        ClientResponse r = call(LongProcedure.class);
        assertEquals(ClientResponse.GRACEFUL_FAILURE, r.getStatus());
        assertTrue(r.getExtra().contains("cannot be null"));
    }

    public void testNullLongArray() {
        ClientResponse r = call(LongArrayProcedure.class);
        assertEquals(null, LongArrayProcedure.arg);
        assertEquals(ClientResponse.SUCCESS, r.getStatus());
    }

    public void testNullPointerException() {
        ClientResponse r = call(NPEProcedure.class);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, r.getStatus());
        System.out.println(r.getExtra());
        assertTrue(r.getExtra().contains("Null Pointer Exception"));
    }

    public void testProcedureStatsCollector() {
        NullProcedureWrapper wrapper = new LongProcedure();
        wrapper.init(site, site.m_context.database.getProcedures().get(LongProcedure.class.getName()), BackendTarget.NATIVE_EE_JNI, null, null);
        ParameterSet params = new ParameterSet();
        params.m_params = new Object[1];
        params.m_params[0] = new Long(1);
        assertNotNull(agent.m_selector);
        assertNotNull(agent.m_source);
        assertEquals(agent.m_selector, SysProcSelector.PROCEDURE);
        assertEquals(agent.m_catalogId, Integer.parseInt(site.m_context.cluster.getSites().get(Integer.toString(site.siteId)).getTypeName()));
        Object statsRow[][] = agent.m_source.getStatsRows(false, 0L);
        assertNotNull(statsRow);
        assertEquals( 0, statsRow.length);
        for (int ii = 1; ii < 200; ii++) {
            wrapper.call(params.m_params);
            statsRow = agent.m_source.getStatsRows(false, 0L);
            assertEquals(statsRow[0][4], new Long(ii));
        }
        assertTrue(((Long)statsRow[0][4]).longValue() > 0L);
        assertTrue(((Long)statsRow[0][5]).longValue() > 0L);
        assertFalse(statsRow[0][6].equals(0));
        assertFalse(statsRow[0][7].equals(0));
        assertTrue(((Long)statsRow[0][8]) > 0L);
    }

    private ClientResponse call(Class<? extends NullProcedureWrapper> procedure) {
        NullProcedureWrapper wrapper = null;
        try {
            wrapper = procedure.newInstance();
        }  catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        wrapper.init(site, site.m_context.database.getProcedures().get(procedure.getName()), BackendTarget.NATIVE_EE_JNI, null, null);
        return wrapper.call((Object) null);
    }

    private class MockExecutionSite extends ExecutionSite {
        public MockExecutionSite(int siteId, String serializedCatalog) {
            this.siteId = siteId;

            // get some catalog shortcuts ready
            catalog = new Catalog();
            catalog.execute(serializedCatalog);
            m_context = new CatalogContext(catalog, CatalogContext.NO_PATH);
            site = m_context.cluster.getSites().get(Integer.toString(siteId));
            //host = cluster.getHosts().get("host");
            //site = host.getSites().get(String.valueOf(siteId));
        }
    }

    private class MockStatsAgent extends StatsAgent {
        public StatsSource m_source = null;
        public SysProcSelector m_selector = null;
        public int m_catalogId = 0;

        @Override
        public void registerStatsSource(SysProcSelector selector, int catalogId, StatsSource source) {
            m_source = source;
            m_selector = selector;
            m_catalogId = catalogId;
        }
    }
}
