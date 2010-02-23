/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb;

import java.util.ArrayList;
import java.util.Hashtable;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.impl.HostMessenger;
import org.voltdb.network.VoltNetwork;

public interface VoltDBInterface
{
    /**
     * Whether calls to crashVoltDB should result in a crash or be ignored
     * @return
     */
    public boolean ignoreCrash();

    public void readBuildInfo();

    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    public void initialize(VoltDB.Configuration config);

    /**
     * Start all the site's event loops. That's it.
     */
    public void run();

    /**
     * Try to shut everything down so they system is ready to call
     * initialize again.
     * @param mainSiteThread The thread that m_inititalized the VoltDB or
     * null if called from that thread.
     */
    public void shutdown(Thread mainSiteThread) throws InterruptedException;

    /**
     * Given a class name in the catalog jar, loads it from the jar, even if the
     * jar is served from a url and isn't in the classpath.
     *
     * @param procedureClassName The name of the class to load.
     * @return A java Class variable assocated with the class.
     * @throws ClassNotFoundException if the class is not in the jar file.
     */
    public Class<?> classForProcedure(String procedureClassName) throws ClassNotFoundException;

    public void startSampler();

    public VoltDB.Configuration getConfig();
    public AuthSystem getAuthSystem();
    public int getNumberOfExecSites();
    public int getNumberOfPartitions();
    public int getNumberOfNodes();
    public String getBuildString();
    public String getVersionString();
    public Messenger getMessenger();
    public HostMessenger getHostMessenger();
    public ArrayList<ClientInterface> getClientInterfaces();
    public Hashtable<Integer, ExecutionSite> getLocalSites();
    public Catalog getCatalog();
    public Cluster getCluster();
    public VoltNetwork getNetwork();
    public CatalogMap<Procedure> getProcedures();
    public CatalogMap<Site> getSites();
    public StatsAgent getStatsAgent();
    public SiteTracker getSiteTracker();

    /**
     * Tells if the VoltDB is running. m_isRunning needs to be set to true
     * when the run() method is called, and set to false when shutting down.
     *
     * @return true if the VoltDB is running.
     */
    public boolean isRunning();
}
