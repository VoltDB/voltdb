/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.regressionsuites;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.StartAction;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;

/**
 * Implementation of a VoltServerConfig for the simplest case:
 * the single-process VoltServer that's so easy to use.
 *
 * Edit: Please don't use this.
 * Use ServerThread or single-node, in-process LocalCluster.
 *
 */
@Deprecated
public abstract class LocalSingleProcessServer extends VoltServerConfig {

    public final String m_jarFileName;
    public int m_siteCount;

    ServerThread m_server = null;
    boolean m_compiled = false;
    protected String m_pathToDeployment;
    private File m_pathToVoltRoot = null;
    private EEProcess m_siteProcess = null;
    private int m_adminPort = -1;
    private boolean m_paused = false;

    LocalSingleProcessServer(String jarFileName, int siteCount, BackendTarget target) {
        assert(jarFileName != null);
        assert(siteCount > 0);
        m_jarFileName = Configuration.getPathToCatalogForTest(jarFileName);
        m_siteCount = siteCount;
        if (LocalCluster.isMemcheckDefined() && target.isValgrindable) {
            m_target = BackendTarget.NATIVE_EE_VALGRIND_IPC;
        } else {
            m_target = target;
        }
    }

    @Override
    public boolean isUsingCalcite() {
        return true;
    }

    @Override
    public void setCallingMethodName(String name) {
        // do nothing yet
    }

    @Override
    public boolean compile(VoltProjectBuilder builder) {
        if (m_compiled) {
            return true;
        } else {
            m_compiled = builder.compile(m_jarFileName, m_siteCount, 1, 0);
            m_pathToDeployment = builder.getPathToDeployment();
            m_pathToVoltRoot = builder.getPathToVoltRoot();
            return m_compiled;
        }
    }

    @Override
    public boolean compileWithPartitionDetection(VoltProjectBuilder builder, String snapshotPath, String ppdPrefix) {
        // this doesn't really make a lot of sense, in that you can't partition a single node,
        // but I suppose it is still feasible user configuration
        int hostCount = 1;
        int replication = 0;

        if (m_compiled) {
            return true;
        } else {
            m_compiled = builder.compile(m_jarFileName, m_siteCount, hostCount, replication,
                    null, 0, true, snapshotPath, ppdPrefix);
            m_pathToDeployment = builder.getPathToDeployment();
            m_pathToVoltRoot = builder.getPathToVoltRoot();
            return m_compiled;
        }
    }

    @Override
    public boolean compileWithAdminMode(VoltProjectBuilder builder, int adminPort, boolean adminOnStartup) {
        int hostCount = 1;
        int replication = 0;

        if (m_compiled) {
            return true;
        } else {
            m_adminPort = adminPort;
            m_paused = adminOnStartup;
            m_initialCatalog = builder.compile(m_jarFileName, m_siteCount, hostCount, replication, 0);
            m_compiled = m_initialCatalog != null;
            m_pathToDeployment = builder.getPathToDeployment();
            return m_compiled;
        }
    }

    @Override
    public int getListenerCount() {
        return 1;
    }

    @Override
    public List<String> getListenerAddresses() {
        // return just "localhost"
        if (m_server == null) {
            return null;
        } else {
            ArrayList<String> listeners = new ArrayList<>();
            listeners.add("localhost");
            return listeners;
        }
    }

    @Override
    public String getListenerAddress(int hostId) {
        if (m_server == null) {
            return null;
        } else {
            return "localhost";
        }
    }

    @Override
    public String getAdminAddress(int hostId) {
        if (m_server == null) {
            return null;
        } else {
            return "localhost:" + m_adminPort;
        }
    }

    @Override
    public String getName() {
        // name is combo of the classname and the parameters

        String retval = "localSingleProcess-";
        retval += String.valueOf(m_siteCount);
        if (m_target == BackendTarget.HSQLDB_BACKEND) {
            retval += "-HSQL";
        } else if (m_target == BackendTarget.NATIVE_EE_IPC) {
            retval += "-IPC";
        } else {
            retval += "-JNI";
        }
        return retval;
    }

    @Override
    public int getNodeCount() {
        return 1;
    }

    @Override
    public void shutDown() throws InterruptedException {
        m_server.shutdown();
        File valgrindOutputFile = m_siteProcess.waitForShutdown();
        LocalCluster.failIfValgrindErrors(valgrindOutputFile);
        VoltServerConfig.removeInstance(this);
    }

    @Override
    public void startUp(boolean clearLocalDataDirectories) {
        VoltServerConfig.addInstance(this);
        if (clearLocalDataDirectories) {
            File exportOverflow = new File( m_pathToVoltRoot, "export_overflow");
            if (exportOverflow.exists()) {
                assert(exportOverflow.isDirectory());
                for (File f : exportOverflow.listFiles()) {
                    if (f.isFile() && f.getName().endsWith(".pbd") || f.getName().endsWith(".ad")) {
                        f.delete();
                    }
                }
            }
        }

        Configuration config = new Configuration();
        config.m_backend = m_target;
        config.m_noLoadLibVOLTDB = (m_target == BackendTarget.HSQLDB_BACKEND);
        // m_jarFileName is already prefixed with test output path.
        config.m_pathToCatalog = m_jarFileName;
        config.m_pathToDeployment = m_pathToDeployment;
        config.m_startAction = StartAction.CREATE;
        config.m_isPaused = m_paused;
        if (m_adminPort != -1) {
            config.m_adminPort = m_adminPort;
        }
        m_siteProcess = new EEProcess(m_target, m_siteCount, "LocalSingleProcessServer.log");
        config.m_ipcPort = m_siteProcess.port();
        m_server = new ServerThread(config);
        m_server.start();
        m_server.waitForInitialization();
    }

    @Override
    public boolean isHSQL() {
        return m_target == BackendTarget.HSQLDB_BACKEND;
    }

    @Override
    public boolean isValgrind() {
        return m_target == BackendTarget.NATIVE_EE_VALGRIND_IPC;
    }

    @Override
    public boolean isDebug() {
        return LocalCluster.isDebugDefined();
    }

    @Override
    public void startUp() {
        startUp(true);
    }
    @Override
    public void createDirectory(File path) {
        throw new UnsupportedOperationException();
    }
    @Override
    public void deleteDirectory(File path) {
        throw new UnsupportedOperationException();
    }
    @Override
    public List<File> listFiles(File path) {
        throw new UnsupportedOperationException();
    }
    @Override
    public File[] getPathInSubroots(File path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getLogicalPartitionCount() {
        return 1;
    }

    @Override
    public int getKfactor() {
        return 0;
    }
}
