/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.voltcore.common.Constants;
import org.voltcore.network.LoopbackAddress;
import org.voltcore.utils.InstanceId;
import org.voltdb.client.ClientFactory;
import org.voltdb.probe.MeshProber;
import org.voltdb.utils.MiscUtils;

import org.apache.commons.io.FileUtils;

/**
 * Wraps VoltDB in a Thread
 */
public class ServerThread extends Thread {
    VoltDB.Configuration m_config;

    static {
        ClientFactory.preserveResources();
    }

    public ServerThread(VoltDB.Configuration config) {
        m_config = config;
        if (m_config.m_pathToLicense == null) {
            m_config.m_pathToLicense = getTestLicensePath();
        }
        if (m_config.m_leader == null) {
            m_config.m_leader = "";
        }
        if (m_config.m_coordinators == null || m_config.m_coordinators.isEmpty()) {
            m_config.m_coordinators = MeshProber.hosts(m_config.m_internalPort);
        }
        if (m_config.m_startAction != StartAction.PROBE) {
            m_config.m_hostCount = VoltDB.UNDEFINED;
        }

        if (!m_config.validate()) {
            System.exit(-1);
        }

        // Disable loading the EE if running against HSQL.
        m_config.m_noLoadLibVOLTDB = m_config.m_backend == BackendTarget.HSQLDB_BACKEND;
        m_config.m_forceVoltdbCreate = true;
        if (config.m_startAction == StartAction.INITIALIZE || config.m_startAction == StartAction.GET) {
            VoltDB.ignoreCrash = true;
        }
        setName("ServerThread");
    }

    public ServerThread(String pathToCatalog, BackendTarget target) {
        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = pathToCatalog;
        m_config.m_backend = target;
        if (m_config.m_pathToLicense == null) {
            m_config.m_pathToLicense = getTestLicensePath();
        }
        m_config.m_leader = "";
        m_config.m_coordinators = MeshProber.hosts(m_config.m_internalPort);
        VoltDB.instance().setMode(OperationMode.INITIALIZING);


        // Disable loading the EE if running against HSQL.
        m_config.m_noLoadLibVOLTDB = m_config.m_backend == BackendTarget.HSQLDB_BACKEND;
        m_config.m_forceVoltdbCreate = true;

        setName("ServerThread");
    }

    public ServerThread(String pathToCatalog, String pathToDeployment, BackendTarget target) {
        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = pathToCatalog;
        m_config.m_pathToDeployment = pathToDeployment;
        m_config.m_backend = target;
        if (m_config.m_pathToLicense == null) {
            m_config.m_pathToLicense = getTestLicensePath();
        }
        m_config.m_leader = "";
        m_config.m_coordinators = MeshProber.hosts(m_config.m_internalPort);
        VoltDB.instance().setMode(OperationMode.INITIALIZING);


        // Disable loading the EE if running against HSQL.
        m_config.m_noLoadLibVOLTDB = m_config.m_backend == BackendTarget.HSQLDB_BACKEND;
        m_config.m_forceVoltdbCreate = true;

        if (!m_config.validate()) {
            System.exit(-1);
        }

        setName("ServerThread");
    }

    public ServerThread(String pathToCatalog,
            String pathToDeployment,
            int internalPort,
            int zkPort,
            BackendTarget target) {
        this(pathToCatalog, pathToDeployment, Constants.DEFAULT_INTERNAL_PORT, internalPort, zkPort, target);
    }

    private ServerThread(String pathToCatalog,
                        String pathToDeployment,
                        int leaderPort,
                        int internalPort,
                        int zkPort,
                        BackendTarget target)
    {
        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = pathToCatalog;
        m_config.m_pathToDeployment = pathToDeployment;
        m_config.m_backend = target;
        if (m_config.m_pathToLicense == null) {
            m_config.m_pathToLicense = getTestLicensePath();
        }
        m_config.m_leader = MiscUtils.getHostnameColonPortString("localhost", leaderPort);
        m_config.m_coordinators = MeshProber.hosts(internalPort);
        m_config.m_internalPort = internalPort;
        m_config.m_zkInterface = LoopbackAddress.get();
        m_config.m_zkPort = zkPort;
        VoltDB.instance().setMode(OperationMode.INITIALIZING);

        // Disable loading the EE if running against HSQL.
        m_config.m_noLoadLibVOLTDB = m_config.m_backend == BackendTarget.HSQLDB_BACKEND;
        m_config.m_forceVoltdbCreate = true;

        if (!m_config.validate()) {
            System.exit(-1);
        }

        setName("ServerThread");
    }

    @Override
    public void run() {
        VoltDB.resetSingletonsForTest();
        VoltDB.initialize(m_config, true);
        VoltDB.instance().run();
    }

    //Call this if you are doing init only or action GET
    public void initialize() {
        VoltDB.initialize(m_config, true);
    }

    //Call this if you are doing init only or action GET
    public void cli() {
        VoltDB.cli(m_config);
    }

    public void waitForInitialization() {
        // Wait until the server has actually started running.
        while (!VoltDB.instance().isRunning() ||
               VoltDB.instance().getMode() == OperationMode.INITIALIZING) {
            Thread.yield();
        }
    }

    public void waitForRejoin() {
        while (!VoltDB.instance().isRunning() ||
               VoltDB.instance().getMode() == OperationMode.INITIALIZING ||
               VoltDB.instance().rejoining()) {
            Thread.yield();
        }
    }

    public void waitForClientInterface() {
        while (!VoltDB.instance().isRunning() ||
                VoltDB.instance().getClientInterface() == null ||
                !VoltDB.instance().getClientInterface().isAcceptingConnections()) {
            Thread.yield();
        }
    }

    public void shutdown() throws InterruptedException {
        assert Thread.currentThread() != this;
        VoltDB.instance().shutdown(this);
        VoltDB.replaceVoltDBInstanceForTest(new RealVoltDB());
        this.join();
        while (VoltDB.instance().isRunning()) {
            Thread.sleep(1);
        }
    }

    // For TestStartActionWithLicenseOption only, that test want to validate given
    // license file instead of the default file.
    public void ignoreDefaultLicense() {
        m_config.m_pathToLicense = null;
    }

    /**
     * For tests only, mostly with ServerThread or LocalCluster:
     *
     * Provide a valid license in the case where license checking
     * is enabled.
     *
     * Outside tests, the license file probably won't exist.
     */
    public static String getTestLicensePath() {
        // magic license stored in the voltdb enterprise code
        URL resource = ServerThread.class.getResource("v3_general_test_license.xml");

        // in the community edition, any non-empty string
        // should work fine here, as it won't be checked
        if (resource == null) return "[community]";

        // return the filesystem path
        File licxml = new File(resource.getFile());
        return licxml.getPath();
    }

    public InstanceId getInstanceId()
    {
        return VoltDB.instance().getHostMessenger().getInstanceId();
    }

    /**
     * Convenient cleanup code for tests.
     * A very big hammer.
     */
    public static void resetUserTempDir() {
        try {
            File dir = new File("/tmp/" + System.getProperty("user.name"));
            FileUtils.deleteDirectory(dir);
            dir.mkdirs();
        }
        catch (IOException ex) {
            // ignored
        }
    }
}
