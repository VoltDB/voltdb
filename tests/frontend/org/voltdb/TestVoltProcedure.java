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

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientResponse;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.impl.HostMessenger;
import org.voltdb.network.VoltNetwork;

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
        agent = new MockStatsAgent();
        manager.setStatsAgent(agent);
        VoltDB.replaceVoltDBInstanceForTest(manager);
        manager.addProcedureForTest(DateProcedure.class.getName()).setClassname(DateProcedure.class.getName());
        manager.addProcedureForTest(LongProcedure.class.getName()).setClassname(LongProcedure.class.getName());
        manager.addProcedureForTest(LongArrayProcedure.class.getName()).setClassname(LongArrayProcedure.class.getName());
        manager.addProcedureForTest(NPEProcedure.class.getName()).setClassname(NPEProcedure.class.getName());
        manager.addSiteForTest("1");
        site = new MockExecutionSite(1, VoltDB.instance().getCatalog().serialize());
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
        wrapper.init(site, site.database.getProcedures().get(LongProcedure.class.getName()), BackendTarget.NATIVE_EE_JNI, null, null);
        ParameterSet params = new ParameterSet();
        params.m_params = new Object[1];
        params.m_params[0] = new Long(1);
        assertNotNull(agent.m_selector);
        assertNotNull(agent.m_source);
        assertEquals(agent.m_selector, SysProcSelector.PROCEDURE);
        assertEquals(agent.m_catalogId, Integer.parseInt(site.cluster.getSites().get(Integer.toString(site.siteId)).getTypeName()));
        Object statsRow[][] = agent.m_source.getStatsRows();
        assertNotNull(statsRow);
        assertEquals(statsRow[0][1], new Long(site.siteId));
        assertEquals(statsRow[0][2], LongProcedure.class.getName());
        assertEquals(statsRow[0][3], 0L); //Starts with 0 invocations
        assertEquals(statsRow[0][4], 0L); //Starts with 0 timed invocations time
        assertEquals(statsRow[0][5], (long)0); //Starts with 0 min execution time
        assertEquals(statsRow[0][6], (long)0); //Starts with 0 max execution time
        assertEquals(statsRow[0][7], 0L); //Average invocation length is 0 to start
        for (int ii = 1; ii < 200; ii++) {
            wrapper.call(params.m_params);
            statsRow = agent.m_source.getStatsRows();
            assertEquals(statsRow[0][3], new Long(ii));
        }
        assertTrue(((Long)statsRow[0][3]).longValue() > 0L);
        assertTrue(((Long)statsRow[0][4]).longValue() > 0L);
        assertFalse(statsRow[0][5].equals(0));
        assertFalse(statsRow[0][6].equals(0));
        assertTrue(((Long)statsRow[0][7]) > 0L);
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

        wrapper.init(site, site.database.getProcedures().get(procedure.getName()), BackendTarget.NATIVE_EE_JNI, null, null);
        return wrapper.call((Object) null);
    }

    private class MockVoltDB implements VoltDBInterface
    {
        private Catalog m_catalog = new Catalog();

        private CatalogMap<Procedure> m_procedures;
        private CatalogMap<Site> m_sites;
        private StatsAgent m_statsAgent;

        MockVoltDB()
        {
            m_catalog.execute("add / clusters cluster\nadd /clusters[cluster] databases database\n");
            m_procedures = m_catalog.getClusters().get("cluster").getDatabases().get("database").getProcedures();
            m_sites = m_catalog.getClusters().get("cluster").getSites();
        }

        public Procedure addProcedureForTest(String name) {
            Procedure retval = m_procedures.add(name);
            retval.setHasjava(true);
            return retval;
        }

        public Site addSiteForTest(String name) {
            // The tests that use this function conflate partitions and sites.
            m_catalog.getClusters().get("cluster").getPartitions().add(name);
            return m_sites.add(name);
        }

        @Override
        public Class<?> classForProcedure(String procedureClassName)
        throws ClassNotFoundException
        {
            return null;
        }

        @Override
        public AuthSystem getAuthSystem()
        {
            return null;
        }

        @Override
        public String getBuildString()
        {
            return null;
        }

        @Override
        public Catalog getCatalog()
        {
            return m_catalog;
        }

        @Override
        public ArrayList<ClientInterface> getClientInterfaces()
        {
            return null;
        }

        @Override
        public Cluster getCluster()
        {
            return null;
        }

        @Override
        public Configuration getConfig()
        {
            return null;
        }

        @Override
        public HostMessenger getHostMessenger()
        {
            return null;
        }

        @Override
        public Hashtable<Integer, ExecutionSite> getLocalSites()
        {
            return null;
        }

        @Override
        public Messenger getMessenger()
        {
            return null;
        }

        @Override
        public VoltNetwork getNetwork()
        {
            return null;
        }

        @Override
        public int getNumberOfNodes()
        {
            return 0;
        }

        @Override
        public int getNumberOfPartitions()
        {
            return 0;
        }

        @Override
        public int getNumberOfExecSites()
        {
            return 0;
        }

        @Override
        public CatalogMap<Procedure> getProcedures()
        {
            return null;
        }

        @Override
        public CatalogMap<Site> getSites()
        {
            return null;
        }

        @Override
        public StatsAgent getStatsAgent()
        {
            return m_statsAgent;
        }

        @Override
        public String getVersionString()
        {
            return null;
        }

        @Override
        public SiteTracker getSiteTracker()
        {
            return null;
        }

        @Override
        public void initialize(Configuration config)
        {
        }

        @Override
        public boolean isRunning()
        {
            return false;
        }

        @Override
        public void readBuildInfo()
        {
        }

        @Override
        public void run()
        {
        }

        public void setStatsAgent(StatsAgent statsAgent)
        {
            m_statsAgent = statsAgent;
        }

        @Override
        public void shutdown(Thread mainSiteThread) throws InterruptedException
        {
        }

        @Override
        public void startSampler()
        {
        }
    }

    private class MockExecutionSite extends ExecutionSite {
        public MockExecutionSite(int siteId, String serializedCatalog) {
            this.siteId = siteId;

            // get some catalog shortcuts ready
            catalog = new Catalog();
            catalog.execute(serializedCatalog);
            cluster = catalog.getClusters().get("cluster");
            site = cluster.getSites().get(Integer.toString(siteId));
            //host = cluster.getHosts().get("host");
            //site = host.getSites().get(String.valueOf(siteId));
            database = cluster.getDatabases().get("database");
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
