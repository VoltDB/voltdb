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

import java.io.File;
import java.net.URL;

import org.voltcore.utils.InstanceId;
import org.voltdb.utils.MiscUtils;

/**
 * Wraps VoltDB in a Thread
 */
public class ServerThread extends Thread {
    VoltDB.Configuration m_config;
    boolean initialized = false;

    public ServerThread(VoltDB.Configuration config) {
        m_config = config;
        m_config.m_pathToLicense = getTestLicensePath();
        if (m_config.m_leader == null) {
            m_config.m_leader = "";
        }

        if (!m_config.validate()) {
            System.exit(-1);
        }

        // Disable loading the EE if running against HSQL.
        m_config.m_noLoadLibVOLTDB = m_config.m_backend == BackendTarget.HSQLDB_BACKEND;

        setName("ServerThread");
    }

    public ServerThread(String pathToCatalog, BackendTarget target) {
        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = pathToCatalog;
        m_config.m_backend = target;
        m_config.m_pathToLicense = getTestLicensePath();
        m_config.m_leader = "";
        VoltDB.instance().setMode(OperationMode.INITIALIZING);


        // Disable loading the EE if running against HSQL.
        m_config.m_noLoadLibVOLTDB = m_config.m_backend == BackendTarget.HSQLDB_BACKEND;

        setName("ServerThread");
    }

    public ServerThread(String pathToCatalog, String pathToDeployment, BackendTarget target) {
        m_config = new VoltDB.Configuration();
        m_config.m_pathToCatalog = pathToCatalog;
        m_config.m_pathToDeployment = pathToDeployment;
        m_config.m_backend = target;
        m_config.m_pathToLicense = getTestLicensePath();
        m_config.m_leader = "";
        VoltDB.instance().setMode(OperationMode.INITIALIZING);


        // Disable loading the EE if running against HSQL.
        m_config.m_noLoadLibVOLTDB = m_config.m_backend == BackendTarget.HSQLDB_BACKEND;

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
        this(pathToCatalog, pathToDeployment, VoltDB.DEFAULT_INTERNAL_PORT, internalPort, zkPort, target);
    }

    public ServerThread(String pathToCatalog,
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
        m_config.m_pathToLicense = getTestLicensePath();
        m_config.m_leader = MiscUtils.getHostnameColonPortString("localhost", leaderPort);
        m_config.m_internalPort = internalPort;
        m_config.m_zkInterface = "127.0.0.1:" + zkPort;
        VoltDB.instance().setMode(OperationMode.INITIALIZING);

        // Disable loading the EE if running against HSQL.
        m_config.m_noLoadLibVOLTDB = m_config.m_backend == BackendTarget.HSQLDB_BACKEND;

        if (!m_config.validate()) {
            System.exit(-1);
        }

        setName("ServerThread");
    }

    @Override
    public void run() {
        VoltDB.initialize(m_config);
        VoltDB.instance().run();
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

    public void shutdown() throws InterruptedException {
        assert Thread.currentThread() != this;
        VoltDB.instance().shutdown(this);
        this.join();
        while (VoltDB.instance().isRunning()) {
            Thread.sleep(1);
        }
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
        URL resource = ServerThread.class.getResource("valid_subscription.xml");

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
}
