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
package org.voltdb;

import java.util.ArrayList;
import java.util.Hashtable;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.impl.HostMessenger;
import org.voltdb.network.VoltNetwork;

public class MockVoltDB implements VoltDBInterface
{
    private final Catalog m_catalog;
    private CatalogContext m_context;
    final String m_clusterName = "cluster";
    final String m_databaseName = "database";
    int m_execSiteCount = 0;
    StatsAgent m_statsAgent = null;

    public MockVoltDB()
    {
        m_catalog = new Catalog();
        m_catalog.execute("add / clusters " + m_clusterName);
        m_catalog.execute("add " + m_catalog.getClusters().get(m_clusterName).getPath() + " databases " +
                          m_databaseName);
    }

    public Procedure addProcedureForTest(String name)
    {
        Procedure retval = getCluster().getDatabases().get(m_databaseName).getProcedures().add(name);
        retval.setHasjava(true);
        return retval;
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

    Cluster getCluster()
    {
        return m_catalog.getClusters().get(m_clusterName);
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
    public String getBuildString()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CatalogContext getCatalogContext()
    {
        m_context = new CatalogContext(m_catalog, CatalogContext.NO_PATH);
        return m_context;
    }

    @Override
    public ArrayList<ClientInterface> getClientInterfaces()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Configuration getConfig()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FaultDistributor getFaultDistributor()
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

    public void setStatsAgent(StatsAgent agent)
    {
        m_statsAgent = agent;
    }

    @Override
    public StatsAgent getStatsAgent()
    {
        // TODO Auto-generated method stub
        return m_statsAgent;
    }

    @Override
    public String getVersionString()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean ignoreCrash()
    {
        // TODO Auto-generated method stub
        return false;
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
    public void catalogUpdate(String diffCommands, String newCatalogURL, int expectedCatalogVersion)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Object[] getInstanceId() {
        // TODO Auto-generated method stub
        return null;
    }

}
