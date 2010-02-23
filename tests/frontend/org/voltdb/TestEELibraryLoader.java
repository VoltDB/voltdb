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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Hashtable;

import org.voltdb.EELibraryLoader;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.impl.HostMessenger;
import org.voltdb.network.VoltNetwork;

import org.junit.*;

public class TestEELibraryLoader {

    private class Interface implements VoltDBInterface {
        @Override
        public Class<?> classForProcedure(String procedureClassName)
                throws ClassNotFoundException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public AuthSystem getAuthSystem() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getBuildString() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Catalog getCatalog() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ArrayList<ClientInterface> getClientInterfaces() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Cluster getCluster() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Configuration getConfig() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public HostMessenger getHostMessenger() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Hashtable<Integer, ExecutionSite> getLocalSites() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Messenger getMessenger() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public VoltNetwork getNetwork() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int getNumberOfExecSites() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int getNumberOfNodes() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int getNumberOfPartitions() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public CatalogMap<Procedure> getProcedures() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public SiteTracker getSiteTracker() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CatalogMap<Site> getSites() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public StatsAgent getStatsAgent() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getVersionString() {
            return "foobar";
        }

        @Override
        public void initialize(Configuration config) {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean isRunning() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public void readBuildInfo() {
            // TODO Auto-generated method stub

        }

        @Override
        public void run() {
            // TODO Auto-generated method stub

        }

        @Override
        public void shutdown(Thread mainSiteThread)
                throws InterruptedException {
            // TODO Auto-generated method stub

        }

        @Override
        public void startSampler() {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean ignoreCrash() {
            m_crash = true;
            return true;
        }

        private boolean m_crash = false;
        private boolean m_loadLibVoltDB = true;
    }
    @Test
    public void testLoader() {
        final VoltDB.Configuration configuration = new VoltDB.Configuration();
        configuration.m_noLoadLibVOLTDB = true;
        Interface intf = new Interface();

        VoltDB.replaceVoltDBInstanceForTest(intf);

        assertFalse(EELibraryLoader.loadExecutionEngineLibrary(false));
        assertFalse(intf.m_crash);
        EELibraryLoader.reset();
        assertFalse(EELibraryLoader.loadExecutionEngineLibrary(true));
        assertTrue(intf.m_crash);
        VoltDB.initialize(configuration);
        intf.m_crash = false;
        assertFalse(EELibraryLoader.loadExecutionEngineLibrary(true));
        assertFalse(intf.m_crash);
    }
}
