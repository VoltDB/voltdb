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

package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.Hashtable;

import org.voltdb.AuthSystem;
import org.voltdb.ClientInterface;
import org.voltdb.ExecutionSite;
import org.voltdb.StatsAgent;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.dtxn.WorkUnit;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.messaging.impl.HostMessenger;
import org.voltdb.network.VoltNetwork;

import junit.framework.TestCase;

public class TestSimpleWorkUnit extends TestCase {

    // TODO: this dupes a bunch of functionality in CatalogCreatorTestHelper
    // ponder a way to reuse/fold/eliminate the other.
    public class MockVoltDB implements VoltDBInterface
    {
        Catalog m_catalog = new Catalog();
        final String m_clusterName = "cluster";
        final String m_databaseName = "database";
        int m_execSiteCount = 0;

        MockVoltDB()
        {
            m_catalog.execute("add / clusters " + m_clusterName);
            m_catalog.execute("add " + getCluster().getPath() + " databases " +
                              m_databaseName);
        }

        public void addHost(int hostId)
        {
            m_catalog.execute("add " + getCluster().getPath() + " hosts " + hostId);
        }

        public void addPartition(int partitionId)
        {
            m_catalog.execute("add " + getCluster().getPath() + " partitions " +
                              partitionId);
        }

        public void addSite(int siteId, int hostId, int partitionId, boolean isExec)
        {
            m_catalog.execute("add " + getCluster().getPath() + " sites " + siteId);
            m_catalog.execute("set " + getSite(siteId).getPath() + " host " +
                              getHost(hostId).getPath());
            m_catalog.execute("set " + getSite(siteId).getPath() + " isexec " +
                              isExec);
            String partition_path = "null";
            if (isExec)
            {
                partition_path = getPartition(partitionId).getPath();
                m_execSiteCount++;
            }
            m_catalog.execute("set " + getSite(siteId).getPath() + " partition " +
                    partition_path);
        }

        Host getHost(int hostId)
        {
            return getCluster().getHosts().get(String.valueOf(hostId));
        }

        Partition getPartition(int partitionId)
        {
            return getCluster().getPartitions().get(String.valueOf(partitionId));
        }

        Site getSite(int siteId)
        {
            return getCluster().getSites().get(String.valueOf(siteId));
        }

        @Override
        public Class<?> classForProcedure(String procedureClassName)
                                                                    throws ClassNotFoundException
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public AuthSystem getAuthSystem()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getBuildString()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Catalog getCatalog()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ArrayList<ClientInterface> getClientInterfaces()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Cluster getCluster()
        {
            return m_catalog.getClusters().get(m_clusterName);
        }

        @Override
        public Configuration getConfig()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public HostMessenger getHostMessenger()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Hashtable<Integer, ExecutionSite> getLocalSites()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Messenger getMessenger()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public VoltNetwork getNetwork()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int getNumberOfExecSites()
        {
            return m_execSiteCount;
        }

        @Override
        public int getNumberOfPartitions()
        {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int getNumberOfNodes()
        {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public CatalogMap<Procedure> getProcedures()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public SiteTracker getSiteTracker()
        {
            return new SiteTracker(getSites());
        }

        @Override
        public CatalogMap<Site> getSites()
        {
            // TODO Auto-generated method stub
            return getCluster().getSites();
        }

        @Override
        public StatsAgent getStatsAgent()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getVersionString()
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void initialize(Configuration config)
        {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean isRunning()
        {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public void readBuildInfo()
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void run()
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void shutdown(Thread mainSiteThread) throws InterruptedException
        {
            // TODO Auto-generated method stub

        }

        @Override
        public void startSampler()
        {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean ignoreCrash() {
            return false;
        }

    }

    static final VoltMessage work = VoltMessage.createNewMessage(VoltMessage.INITIATE_RESPONSE_ID);
    VoltTable t1;
    VoltTable t2;
    MockVoltDB m_voltdb;

    // automatically generate a limited set of cluster topologies
    public void setUpSites(int numHosts, int numParts, int replicas)
    {
        assert (numHosts >= replicas);
        assert(((numParts * replicas) % numHosts) == 0);
        int sites_per_host = (numParts * replicas) / numHosts;
        assert(numHosts % replicas == 0);
        for (int i = 0; i < numHosts; i++)
        {
            m_voltdb.addHost(i);
        }
        for (int i = 0; i < numParts; i++)
        {
            m_voltdb.addPartition(i);
        }
        for (int i = 0; i < numParts * replicas; i++)
        {
            m_voltdb.addSite(i, i / sites_per_host, i % replicas, true);
        }
    }

    @Override
    public void setUp()
    {
        m_voltdb = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(m_voltdb);

        VoltTable.ColumnInfo[] cols1 =
        { new VoltTable.ColumnInfo("name", VoltType.STRING) };

        VoltTable.ColumnInfo[] cols2 =
        { new VoltTable.ColumnInfo("age", VoltType.INTEGER) };

        t1 = new VoltTable(cols1, 1);
        t1.addRow("dude");
        t2 = new VoltTable(cols2, 1);
        t2.addRow(10);
    }

    public void testNoDependenciesNoReplicas() {
        setUpSites(1, 2, 1);
        WorkUnit w = new WorkUnit(work, new int[]{}, false);
        assertTrue(w.allDependenciesSatisfied());
        assertEquals(work, w.getPayload());
        assertNull(w.getDependencies());
        assertNull(w.getDependency(0));

        w = new WorkUnit(work, null, false);
        assertTrue(w.allDependenciesSatisfied());
    }

    public void testDependenciesNoReplicas() {
        setUpSites(1, 2, 1);
        WorkUnit w = new WorkUnit(work, new int[]{ 4, 5 }, false);
        assertFalse(w.allDependenciesSatisfied());
        assertEquals(w.getDependency(4).size(), 0);
        assertEquals(w.getDependency(5).size(), 0);
        w.putDependency(4, 0, t1);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(5, 0, t2);
        assertTrue(w.allDependenciesSatisfied());
        assertEquals(t1, w.getDependency(4).get(0));
        assertEquals(t2, w.getDependency(5).get(0));
    }

    public void testBadPutDependencyNoReplicas() {
        setUpSites(1, 2, 1);
        WorkUnit w = new WorkUnit(work, new int[]{ 4, 5 }, false);

        // Put a dependency that does not exist
        try {
            w.putDependency(0, 0, t1);
            fail("assertion expected");
        } catch (AssertionError e) {}

        // Put a dependency with a null value
        try {
            w.putDependency(4, 0, null);
            fail("assertion expected");
        } catch (AssertionError e) {}

        // Put a dependency twice
        w.putDependency(4, 0, t1);
        try {
            w.putDependency(4, 0, t1);
            fail("assertion expected");
        } catch (AssertionError e) {}
    }

    public void testDependenciesWithReplicas()
    {
        setUpSites(2, 2, 1);
        int multi_dep = 5 | DtxnConstants.MULTIPARTITION_DEPENDENCY;
        WorkUnit w = new WorkUnit(work, new int[]{ 4, multi_dep }, false);
        assertFalse(w.allDependenciesSatisfied());
        assertEquals(w.getDependency(4).size(), 0);
        assertEquals(w.getDependency(5).size(), 0);
        w.putDependency(4, 0, t1);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, 0, t2);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, 1, t2);
        assertTrue(w.allDependenciesSatisfied());
        assertEquals(1, w.getDependency(4).size());
        assertEquals(t1, w.getDependency(4).get(0));
        assertEquals(1, w.getDependency(multi_dep).size());
        assertEquals(t2, w.getDependency(multi_dep).get(0));
    }

    public void testReplicaDependencyWithMismatchedResults()
    {
        VoltTable.ColumnInfo[] cols2 =
        { new VoltTable.ColumnInfo("age", VoltType.INTEGER) };

        VoltTable t3 = new VoltTable(cols2, 1);
        t3.addRow(11);

        setUpSites(2, 2, 1);
        int multi_dep = 5 | DtxnConstants.MULTIPARTITION_DEPENDENCY;
        WorkUnit w = new WorkUnit(work, new int[]{ 4, multi_dep }, false);
        assertFalse(w.allDependenciesSatisfied());
        assertEquals(w.getDependency(4).size(), 0);
        assertEquals(w.getDependency(5).size(), 0);
        w.putDependency(4, 0, t1);
        assertFalse(w.allDependenciesSatisfied());
        w.putDependency(multi_dep, 0, t2);
        assertFalse(w.allDependenciesSatisfied());
        boolean threw = false;
        try
        {
            w.putDependency(multi_dep, 1, t3);
        }
        catch (RuntimeException e)
        {
            threw = true;
        }
        assertTrue(threw);
    }
}
