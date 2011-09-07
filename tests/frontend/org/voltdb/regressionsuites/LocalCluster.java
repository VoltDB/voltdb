/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.VoltFile;

/**
 * Implementation of a VoltServerConfig for a multi-process
 * cluster. All cluster processes run locally (keep this in
 * mind if building memory or load intensive tests.)
 */
public class LocalCluster implements VoltServerConfig {

    public enum FailureState {
        ALL_RUNNING,
        ONE_FAILURE,
        ONE_RECOVERING
    }

    // the timestamp salt for the TransactionIdManager
    // will vary between -3 and 3 uniformly
    static final int TIMESTAMP_SALT_VARIANCE = 3;

    // configuration data
    final String m_jarFileName;
    final int m_siteCount;
    final int m_hostCount;
    final int m_replication;
    final BackendTarget m_target;
    final String m_buildDir;
    int m_portOffset;
    int m_zkPortOffset;
    int m_adminPortOffset;
    boolean m_hasLocalServer = true;
    String m_pathToDeployment;
    int m_pathToDeploymentOffset;
    int m_rejoinOffset;
    FailureState m_failureState;
    int m_baseAdminPort = VoltDB.DEFAULT_ADMIN_PORT;

    // state
    boolean m_compiled = false;
    boolean m_running = false;
    ArrayList<Process> m_cluster = new ArrayList<Process>();
    //Dedicated paths in the filesystem to be used as a root for each process
    ArrayList<File> m_subRoots = new ArrayList<File>();
    ArrayList<PipeToFile> m_pipes = null;
    ServerThread m_localServer = null;

    // components
    ProcessBuilder m_procBuilder;
    private int m_debugOffset1;
    private int m_debugOffset2;

    private int m_ipcPortOffset1;
    private int m_ipcPortOffset2;
    private int m_ipcPortOffset3;

    private int m_voltFilePrefixOffset;

    private int m_timestampSaltOffset;

    private int m_licensePathOffset;

    @SuppressWarnings("unused")
    private File m_pathToVoltRoot = null;
    private final ArrayList<ArrayList<EEProcess>> m_eeProcs = new ArrayList<ArrayList<EEProcess>>();

    private final boolean m_debug;
    private int m_debugPortOffset = 8000;

    private final boolean m_isRejoinTest;

    private int m_voltStartCmdOffset;


    /* class pipes a process's output to a file name.
     * Also watches for "Server completed initialization"
     * in output - the signal of readiness!
     */
    public static class PipeToFile extends Thread {
        final static String m_initToken = "Server completed init";
        final static String m_rejoinToken = "Node recovery completed";
        final static String m_initiatorID = "Initializing initiator ID:";

        FileWriter m_writer ;
        BufferedReader m_input;
        String m_filename;

        // set m_witnessReady when the m_token byte sequence is seen.
        AtomicBoolean m_witnessedReady;
        final String m_token;
        int m_hostId = Integer.MAX_VALUE;
        long m_initTime;
        private boolean m_eof = false;

        PipeToFile(String filename, InputStream stream, String token,
                   boolean appendLog) {
            m_witnessedReady = new AtomicBoolean(false);
            m_token = token;
            m_filename = filename;
            m_input = new BufferedReader(new InputStreamReader(stream));
            try {
                m_writer = new FileWriter(filename, appendLog);
            }
            catch (IOException ex) {
                Logger.getLogger(LocalCluster.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException(ex);
            }
        }

        public int getHostId() {
            synchronized(this) {
                return m_hostId;
            }
        }
        @Override
        public void run() {
            assert(m_writer != null);
            assert(m_input != null);
            boolean initLocationFound = false;
            while (!m_eof) {
                try {
                    String data = m_input.readLine();
                    if (data == null) {
                        m_eof = true;
                        continue;
                    }

                    // look for the non-exec site id
                    if (data.contains(m_initiatorID)) {
                        // INITIALIZING INITIATOR ID: 1, SITEID: 0
                        String[] split = data.split(" ");
                        synchronized(this) {
                            try {
                                m_hostId = Integer.parseInt(split[split.length - 1]);
                            } catch (java.lang.NumberFormatException e) {
                                System.err.println("Had a number format exception processing line: '" + data + "'");
                                throw e;
                            }
                        }
                    }

                    // look for a sequence of letters matching the server ready token.
                    if (!m_witnessedReady.get() && data.contains(m_token)) {
                        synchronized (this) {
                            m_witnessedReady.set(true);
                            this.notifyAll();
                        }
                    }

                    // look for a sequence of letters matching the server ready token.
                    if (!initLocationFound && data.contains(m_initToken)) {
                        initLocationFound = true;
                        m_initTime = System.currentTimeMillis();
                    }

                    m_writer.write(data + "\n");
                    m_writer.flush();
                }
                catch (IOException ex) {
                    m_eof = true;
                }
            }
            synchronized (this) {
                notify();
            }
            try {
                m_writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public LocalCluster(String jarFileName, int siteCount,
            int hostCount, int replication, BackendTarget target) {
        this(jarFileName, siteCount,
            hostCount, replication, target,
            FailureState.ALL_RUNNING, false, false);
    }

    public LocalCluster(String jarFileName, int siteCount,
                        int hostCount, int replication, BackendTarget target,
                        boolean isRejoinTest) {
                    this(jarFileName, siteCount,
                        hostCount, replication, target,
                        FailureState.ALL_RUNNING, false, isRejoinTest);
    }

    public LocalCluster(String jarFileName, int siteCount,
                        int hostCount, int replication, BackendTarget target,
                        FailureState failureState,
                        boolean debug) {
        this(jarFileName, siteCount, hostCount, replication, target,
             failureState, debug, false);
    }

    public LocalCluster(String jarFileName, int siteCount,
                        int hostCount, int replication, BackendTarget target,
                        FailureState failureState,
                        boolean debug, boolean isRejoinTest)
    {
        System.out.println("Instantiating LocalCluster for " + jarFileName);
        System.out.println("Sites: " + siteCount + " hosts: " + hostCount
                           + " replication factor: " + replication);

        assert (jarFileName != null);
        assert (siteCount > 0);
        assert (hostCount > 0);
        assert (replication >= 0);

//      final String buildType = System.getenv().get("BUILD");
//      if (buildType == null) {
        /*
         * Disable memcheck with localcluster for now since it doesn't pass.
         */
            m_target = target;
//        } else {
//            if (buildType.startsWith("memcheck")) {
//                if (target.equals(BackendTarget.NATIVE_EE_JNI)) {
//                    m_target = BackendTarget.NATIVE_EE_VALGRIND_IPC;
//                } else {
//                    m_target = target;//For memcheck
//                }
//            } else {
//                m_target = target;
//            }
//        }

        m_jarFileName = VoltDB.Configuration.getPathToCatalogForTest(jarFileName);
        m_siteCount = siteCount;
        m_hostCount = hostCount;
        m_replication = replication;
        m_cluster.ensureCapacity(m_hostCount);
        m_debug = debug;
        m_isRejoinTest = isRejoinTest;
        String buildDir = System.getenv("VOLTDB_BUILD_DIR");  // via build.xml
        if (buildDir == null)
            m_buildDir = System.getProperty("user.dir") + "/obj/release";
        else
            m_buildDir = buildDir;
        m_failureState = failureState;
        //m_failureState = FailureState.ALL_RUNNING;

        m_failureState = failureState;
        // don't fail nodes without k-safety
        if (m_replication < 1)
            m_failureState = FailureState.ALL_RUNNING;

        String classPath = System.getProperty("java.class.path")+ ":" + m_buildDir + File.separator + m_jarFileName;
        classPath += ":" + m_buildDir + File.separator + "prod";

        // processes of VoltDBs using the compiled jar file.
        m_pipes = new ArrayList<PipeToFile>();
        m_procBuilder = new ProcessBuilder("java",
                                           "-Djava.library.path=" + m_buildDir + "/nativelibs",
                                           "-Dlog4j.configuration=log4j.xml",
                                           "-DLOG_SEGMENT_SIZE=8",
                                           "-ea",
                                           "-XX:-ReduceInitialCardMarks",
                                           "-XX:MaxDirectMemorySize=2g",
                                           "-Xmx2g",
                                           "-XX:+HeapDumpOnOutOfMemoryError",
                                           "-classpath",
                                           classPath,
                                           "org.voltdb.VoltDB",
                                           "license",
                                           ServerThread.getTestLicensePath(),
                                           "zkport",
                                           "-1",
                                           "timestampsalt",
                                           "0",
                                           "catalog",
                                           m_jarFileName,
                                           "deployment",
                                           "",
                                           "port",
                                           "-1",
                                           "adminport",
                                           "-1",
                                           "rejoinhost",
                                           "-1",
                                           "leader",
                                           "localhost");

        List<String> command = m_procBuilder.command();
        // when we actually append a port value and deployment file, these will be correct
        m_debugOffset1 = command.size() - 21;
        m_debugOffset2 = command.size() - 20;
        if (m_debug) {
            command.add(m_debugOffset1, "");
            command.add(m_debugOffset1, "");
        }

        m_voltFilePrefixOffset = command.size() - 21;
        command.add(m_voltFilePrefixOffset, "");

        m_licensePathOffset = command.size() - 17;
        m_zkPortOffset = command.size() - 15;
        m_timestampSaltOffset = command.size() - 13;
        m_pathToDeploymentOffset = command.size() - 9;
        m_portOffset = command.size() - 7;
        m_adminPortOffset = command.size() - 5;
        m_voltStartCmdOffset = command.size() - 4;
        m_rejoinOffset = command.size() - 3;

        if (m_target.isIPC) {
            command.add("");
            m_ipcPortOffset1 = command.size() - 1;
            command.add("");
            m_ipcPortOffset2 = command.size() - 1;
            command.add("");
            m_ipcPortOffset3 = command.size() - 1;
        }

        // set the working directory to obj/release/prod
        //m_procBuilder.directory(new File(m_buildDir + File.separator + "prod"));
        m_procBuilder.redirectErrorStream(true);

        Thread shutdownThread = new Thread(new ShutDownHookThread());
        java.lang.Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    public void setHasLocalServer(boolean hasLocalServer) {
        m_hasLocalServer = hasLocalServer;
    }

    public void setMaxHeap(int megabytes) {
        m_procBuilder.command().set(7, "-Xmx" + megabytes + "m");
    }

    public String getPathToDeployment() {
        return m_pathToDeployment;
    }

    @Override
    public boolean compile(VoltProjectBuilder builder) {
        if (m_compiled) {
            return true;
        }

        m_compiled = builder.compile(m_jarFileName, m_siteCount, m_hostCount, m_replication);
        m_pathToDeployment = builder.getPathToDeployment();
        m_pathToVoltRoot = builder.getPathToVoltRoot();

        return m_compiled;
    }

    @Override
    public boolean compileWithPartitionDetection(VoltProjectBuilder builder, String snapshotPath, String ppdPrefix) {
        if (m_compiled) {
            return true;
        }
        m_compiled = builder.compile(m_jarFileName, m_siteCount, m_hostCount, m_replication,
                                     null, true, snapshotPath, ppdPrefix);
        m_pathToDeployment = builder.getPathToDeployment();
        m_pathToVoltRoot = builder.getPathToVoltRoot();

        return m_compiled;
    }

    @Override
    public boolean compileWithAdminMode(VoltProjectBuilder builder, int adminPort, boolean adminOnStartup)
    {
        if (m_compiled) {
            return true;
        }
        m_baseAdminPort = adminPort;
        m_compiled = builder.compile(m_jarFileName, m_siteCount, m_hostCount, m_replication,
                                     m_baseAdminPort, adminOnStartup);
        m_pathToDeployment = builder.getPathToDeployment();
        m_pathToVoltRoot = builder.getPathToVoltRoot();

        return m_compiled;
    }

    @Override
    public void startUp() {
        startUp(true);
    }

    @Override
    public void startUp(boolean clearLocalDataDirectories) {
        assert (!m_running);
        if (m_running) {
            return;
        }

        if (clearLocalDataDirectories) {
            try {
                m_subRoots.clear();
                VoltFile.deleteAllSubRoots();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        // set to true to spew startup timing data
        boolean logtime = false;
        long startTime = 0;
        if (logtime) {
            startTime = System.currentTimeMillis();
            System.out.println("********** Starting cluster at: " + startTime);
        }

        int oopStartIndex = 0;

        for (int ii = 0; ii < m_hostCount; ii++) {
            ArrayList<EEProcess> procs = new ArrayList<EEProcess>();
            m_eeProcs.add(procs);
            for (int zz = 0; zz < m_siteCount; zz++) {
                String logfile = "LocalCluster_host_" + ii + "_site" + zz + ".log";
                procs.add(new EEProcess(m_target, logfile));
            }
        }

        m_pipes.clear();
        m_cluster.clear();

        // create the in-process server
        if (m_hasLocalServer) {
            //If the local directories are being cleared, generate a new root for the
            //in process server
            if (clearLocalDataDirectories) {
                try {
                    m_subRoots.add(VoltFile.initNewSubrootForThisProcess());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            m_cluster.add(null);
            m_pipes.add(null);
            Configuration config = new Configuration();
            config.m_backend = m_target;
            config.m_noLoadLibVOLTDB = (m_target == BackendTarget.HSQLDB_BACKEND);
            config.m_pathToCatalog = m_jarFileName;
            config.m_pathToDeployment = m_pathToDeployment;
            config.m_port = VoltDB.DEFAULT_PORT;
            config.m_adminPort = m_baseAdminPort;
            config.m_startAction = START_ACTION.CREATE;
            ArrayList<Integer> ports = new ArrayList<Integer>();
            for (EEProcess proc : m_eeProcs.get(0)) {
                ports.add(proc.port());
            }
            config.m_ipcPorts = java.util.Collections.synchronizedList(ports);
            config.m_isRejoinTest = m_isRejoinTest;

            m_localServer = new ServerThread(config);
            m_localServer.start();
            oopStartIndex++;
        }

        // create all the out-of-process servers
        for (int i = oopStartIndex; i < m_hostCount; i++) {
            startOne(i, clearLocalDataDirectories);
        }

        // spin until all the pipes see the magic "Server completed.." string.
        boolean allReady;
        do {
            if (logtime) System.out.println("********** pre witness: " + (System.currentTimeMillis() - startTime) + " ms");
            allReady = true;
            for (PipeToFile pipeToFile : m_pipes) {
                if (pipeToFile == null) {
                    continue;
                }
                synchronized(pipeToFile) {
                    if (pipeToFile.m_witnessedReady.get() != true) {
                        try {
                            pipeToFile.wait();
                        }
                        catch (InterruptedException ex) {
                            Logger.getLogger(LocalCluster.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        allReady = false;
                        break;
                    }
                }
            }
        } while (allReady == false);
        if (logtime) System.out.println("********** post witness: " + (System.currentTimeMillis() - startTime) + " ms");
        if (!allReady) {
            throw new RuntimeException("Not all processes became ready");
        }

        // Finally, make sure the local server thread is running and wait if it is not.
        if (m_hasLocalServer)
            m_localServer.waitForInitialization();
        if (logtime) System.out.println("********** DONE: " + (System.currentTimeMillis() - startTime) + " ms");
        m_running = true;

        // if supposed to kill a server, it's go time
        if (m_failureState != FailureState.ALL_RUNNING) {
            System.out.println("Killing one cluster member.");
            int procIndex = 0;
            if (m_hasLocalServer) {
                procIndex = 1;
            }

            Process proc = m_cluster.get(procIndex);
            proc.destroy();
            int retval = 0;
            try {
                retval = proc.waitFor();
                for (EEProcess eeproc : m_eeProcs.get(procIndex)) {
                    eeproc.waitForShutdown();
                }
            } catch (InterruptedException e) {
                System.out.println("External VoltDB process is acting crazy.");
            } finally {
                m_cluster.set(procIndex, null);
            }
            // exit code 143 is the forcible shutdown code from .destroy()
            if (retval != 0 && retval != 143) {
                System.out.println("External VoltDB process terminated abnormally with return: " + retval);
            }
        }

        // after killing a server, bring it back in recovery mode
        if (m_failureState == FailureState.ONE_RECOVERING) {
            int hostId = m_hasLocalServer ? 1 : 0;
            recoverOne(logtime, startTime, hostId);
        }
    }

    private void startOne(int hostId, boolean clearLocalDataDirectories) {
        try {
            // voltdb client/native ports move forward from 21212
            m_procBuilder.command().set(m_portOffset, String.valueOf(VoltDB.DEFAULT_PORT + hostId));
            // voltdb admin-mode ports move backwards from 21211
            m_procBuilder.command().set(m_adminPortOffset, String.valueOf(m_baseAdminPort - hostId));
            m_procBuilder.command().set(m_pathToDeploymentOffset, m_pathToDeployment);
            m_procBuilder.command().set(m_voltStartCmdOffset, "create");
            m_procBuilder.command().set(m_rejoinOffset, "");
            m_procBuilder.command().set(m_licensePathOffset, ServerThread.getTestLicensePath());
            m_procBuilder.command().set(m_timestampSaltOffset, String.valueOf(getRandomTimestampSalt()));
            if (m_debug) {
                m_procBuilder.command().set(m_debugOffset1, "-Xdebug");
                m_procBuilder.command().set(
                        m_debugOffset2,
                        "-agentlib:jdwp=transport=dt_socket,address="
                        + m_debugPortOffset++ + ",server=y,suspend=n");
            }
            if (m_target.isIPC) {
                m_procBuilder.command().set(m_ipcPortOffset1, "ipcports");
                String portString = "";
                for (EEProcess proc : m_eeProcs.get(hostId)) {
                    if (portString.isEmpty()) {
                        portString += Integer.valueOf(proc.port());
                    } else {
                        portString += "," + Integer.valueOf(proc.port());
                    }
                }
                m_procBuilder.command().set(m_ipcPortOffset2, portString);
                m_procBuilder.command().set(m_ipcPortOffset3, "valgrind");
            }
            m_procBuilder.command().set(m_zkPortOffset, Integer.toString(2181 + hostId));

            //If local directories are being cleared
            //Generate a new subroot, otherwise reuse the existing directory
            File subroot = null;
            if (clearLocalDataDirectories) {
                subroot = VoltFile.getNewSubroot();
                m_subRoots.add(subroot);
            } else {
                subroot = m_subRoots.get(hostId);
            }
            m_procBuilder.command().set(
                    m_voltFilePrefixOffset,
                    "-DVoltFilePrefix=" + subroot.getPath());

            StringBuilder sb = new StringBuilder();
            for (String arg : m_procBuilder.command()) {
                sb.append(arg);
                sb.append(' ');
            }

            Process proc = m_procBuilder.start();
            m_cluster.add(proc);
            // write output to obj/release/testoutput/<test name>-n.txt
            // this may need to be more unique? Also very useful to just
            // set this to a hardcoded path and use "tail -f" to debug.
            String testoutputdir = m_buildDir + File.separator + "testoutput";
            // make sure the directory exists
            File dir = new File(testoutputdir);
            if (dir.exists()) {
                assert(dir.isDirectory());
            }
            else {
                boolean status = dir.mkdirs();
                assert(status);
            }

            File dirFile = new VoltFile(testoutputdir);
            if (dirFile.listFiles() != null) {
                for (File f : dirFile.listFiles()) {
                    if (f.getName().startsWith(getName() + "-" + hostId)) {
                        f.delete();
                    }
                }
            }
            PipeToFile ptf = new PipeToFile(testoutputdir + File.separator +
                    getName() + "-" + hostId + ".txt", proc.getInputStream(),
                    PipeToFile.m_initToken, false);
            m_pipes.add(ptf);
            ptf.setName("ClusterPipe:" + String.valueOf(hostId));
            ptf.start();
        }
        catch (IOException ex) {
            System.out.println("Failed to start cluster process:" + ex.getMessage());
            Logger.getLogger(LocalCluster.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }
    }

    public boolean recoverOne(int hostId, Integer portOffset, String rejoinHost) {
        return recoverOne(false, 0, hostId, portOffset, rejoinHost);
    }

    private boolean recoverOne(boolean logtime, long startTime, int hostId) {
        return recoverOne( logtime, startTime, hostId, null, "localhost");
    }

    private boolean recoverOne(boolean logtime, long startTime, int hostId, Integer portOffset, String rejoinHost) {
        // port for the new node
        int portNo = VoltDB.DEFAULT_PORT + hostId;
        int adminPortNo = m_baseAdminPort - hostId;

        // port to connect to (not too simple, eh?)
        int portNoToRejoin = VoltDB.DEFAULT_PORT + ((hostId + 1) % getNodeCount());
        if (m_hasLocalServer) portNoToRejoin = VoltDB.DEFAULT_PORT;

        if (portOffset != null) {
            portNoToRejoin = VoltDB.DEFAULT_PORT + portOffset;
            // adminPortNo = m_baseAdminPort - portOffset;
        }
        System.out.println("Rejoining " + hostId + " to hostID: " + portOffset);


        ArrayList<EEProcess> eeProcs = m_eeProcs.get(hostId);
        for (EEProcess proc : eeProcs) {
            try {
                proc.waitForShutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        eeProcs.clear();

        for (int ii = 0; ii < m_siteCount; ii++) {
            String logfile = "LocalCluster_host_" + hostId + "_site" + ii + ".log";
            eeProcs.add(new EEProcess(m_target, logfile));
        }

        if (m_target.isIPC) {
            m_procBuilder.command().set(m_ipcPortOffset1, "ipcports");
            String portString = "";
            for (EEProcess proc : m_eeProcs.get(hostId)) {
                if (portString.isEmpty()) {
                    portString += Integer.valueOf(proc.port());
                } else {
                    portString += "," + Integer.valueOf(proc.port());
                }
            }
            m_procBuilder.command().set(m_ipcPortOffset2, portString);
            m_procBuilder.command().set(m_ipcPortOffset3, "valgrind");
        }

        //When recovering reuse the root from the original
        m_procBuilder.command().set(
                m_voltFilePrefixOffset,
                "-DVoltFilePrefix=" + m_subRoots.get(hostId).getPath());

        PipeToFile ptf = null;
        long start = 0;
        try {
            m_procBuilder.command().set(m_portOffset, String.valueOf(portNo));
            m_procBuilder.command().set(m_adminPortOffset, String.valueOf(adminPortNo));
            m_procBuilder.command().set(m_pathToDeploymentOffset, m_pathToDeployment);
            m_procBuilder.command().set(m_voltStartCmdOffset, "rejoinhost");
            m_procBuilder.command().set(m_rejoinOffset, rejoinHost + ":" + String.valueOf(portNoToRejoin));
            m_procBuilder.command().set(m_licensePathOffset, "");
            m_procBuilder.command().set(m_timestampSaltOffset, String.valueOf(getRandomTimestampSalt()));
            if (m_debug) {
                System.out.println("Debug port is " + m_debugPortOffset);
                m_procBuilder.command().set(m_debugOffset1, "-Xdebug");
                m_procBuilder.command().set(
                        m_debugOffset2,
                        "-agentlib:jdwp=transport=dt_socket,address="
                        + m_debugPortOffset++ + ",server=y,suspend=n");
            }
            m_procBuilder.command().set(m_zkPortOffset, Integer.toString(2181 + hostId));

            Process proc = m_procBuilder.start();
            start = System.currentTimeMillis();

            // write output to obj/release/testoutput/<test name>-n.txt
            // this may need to be more unique? Also very useful to just
            // set this to a hardcoded path and use "tail -f" to debug.
            String testoutputdir = m_buildDir + File.separator + "testoutput";
            // make sure the directory exists
            File dir = new File(testoutputdir);
            if (dir.exists()) {
                assert(dir.isDirectory());
            }
            else {
                boolean status = dir.mkdirs();
                assert(status);
            }

            ptf = new PipeToFile(testoutputdir + File.separator +
                    getName() + "-" + hostId + ".txt", proc.getInputStream(),
                    PipeToFile.m_rejoinToken, true);
            synchronized (this) {
                m_pipes.set(hostId, ptf);
                // replace the existing dead proc
                m_cluster.set(hostId, proc);
            }
            Thread t = new Thread(ptf);
            t.setName("ClusterPipe:" + String.valueOf(hostId));
            t.start();
        }
        catch (IOException ex) {
            System.out.println("Failed to start cluster process:" + ex.getMessage());
            Logger.getLogger(LocalCluster.class.getName()).log(Level.SEVERE, null, ex);
            assert (false);
        }

        // wait for the joining site to be ready
        synchronized (ptf) {
            while (ptf.m_witnessedReady.get() != true && !ptf.m_eof) {
                if (logtime) System.out.println("********** pre witness: " + (System.currentTimeMillis() - startTime) + " ms");
                try {
                    // wait for explicit notification
                    ptf.wait();
                }
                catch (InterruptedException ex) {
                    Logger.getLogger(LocalCluster.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        if (ptf.m_witnessedReady.get()) {
            long finish = System.currentTimeMillis();
            System.out.println(
                    "Took " + (finish - start) +
                    " milliseconds, time from init was " + (finish - ptf.m_initTime));
            return true;
        } else {
            System.out.println("Recovering process exited before recovery completed");
            try {
                silentShutdownSingleHost(hostId, true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return false;
        }
    }

    @Override
    synchronized public void shutDown() throws InterruptedException {
        // there are couple of ways to shutdown. sysproc @kill could be
        // issued to listener. this would require that the test didn't
        // break the cluster somehow.  Or ... just old fashioned kill?

        try {
            if (m_localServer != null) {
                m_localServer.shutdown();
            }
        } finally {
            m_running = false;
        }
        shutDownExternal();
    }

    public void shutDownSingleHost(int hostNum) throws InterruptedException
    {
        System.out.println("Shutting down " + hostNum);
        silentShutdownSingleHost(hostNum, false);
    }

    private void silentShutdownSingleHost(int hostNum, boolean forceKillEEProcs) throws InterruptedException {
        Process proc = null;
        //PipeToFile ptf = null;
        ArrayList<EEProcess> procs = null;
        PipeToFile ptf;
        synchronized (this) {
           proc = m_cluster.get(hostNum);
           //ptf = m_pipes.get(hostNum);
           m_cluster.set(hostNum, null);
           ptf = m_pipes.get(hostNum);
           m_pipes.set(hostNum, null);
           procs = m_eeProcs.get(hostNum);
        }

        if (ptf != null && ptf.m_filename != null) {
            //new File(ptf.m_filename).delete();
        }
        if (proc != null) {
            proc.destroy();
        }

        // if (ptf != null) {
        //     new File(ptf.m_filename).delete();
        // }

        for (EEProcess eeproc : procs) {
            if (forceKillEEProcs) {
                eeproc.destroy();
            }
            eeproc.waitForShutdown();
        }
        procs.clear();
    }

    public void shutDownExternal() throws InterruptedException {
        shutDownExternal(false);
    }

    public synchronized void shutDownExternal(boolean forceKillEEProcs) throws InterruptedException
    {
        if (m_cluster != null) {
            for (Process proc : m_cluster) {
                if (proc == null)
                    continue;
                proc.destroy();
                int retval = proc.waitFor();
                // exit code 143 is the forcible shutdown code from .destroy()
                if (retval != 0 && retval != 143)
                {
                    System.out.println("External VoltDB process terminated abnormally with return: " + retval);
                }
            }
        }

        if (m_cluster != null) m_cluster.clear();

        for (ArrayList<EEProcess> procs : m_eeProcs) {
            for (EEProcess proc : procs) {
                proc.waitForShutdown();
            }
        }

        if (m_target == BackendTarget.NATIVE_EE_VALGRIND_IPC) {
            if (!EEProcess.m_valgrindErrors.isEmpty()) {
                String failString = "";
                for (final String error : EEProcess.m_valgrindErrors) {
                    failString = failString + "\n" + error;
                }
                org.junit.Assert.fail(failString);
            }
        }
    }

    @Override
    public List<String> getListenerAddresses() {
        if (!m_running) {
            return null;
        }
        ArrayList<String> listeners = new ArrayList<String>();
        listeners.add("localhost");
        return listeners;
    }

    @Override
    public String getName() {
        String prefix = "localCluster";
        if (m_failureState == FailureState.ONE_FAILURE)
            prefix += "OneFail";
        if (m_failureState == FailureState.ONE_RECOVERING)
            prefix += "OneRecov";

        return prefix + "-" + String.valueOf(m_siteCount) + "-" +
            String.valueOf(m_hostCount) + "-" + m_target.display.toUpperCase();
    }

    @Override
    public int getNodeCount()
    {
        return m_hostCount;
    }

    public boolean areAllNonLocalProcessesDead() {
        for (Process proc : m_cluster){
            try {
                if (proc != null) {
                    proc.exitValue();
                }
            }
            catch (IllegalThreadStateException ex) {
                return false;
            }
        }
        return true;
    }

    public int getLiveNodeCount()
    {
        int count = 0;
        if (m_hasLocalServer)
        {
            count++;
        }

        if (m_cluster != null)
        {
            for (Process proc : m_cluster)
            {
                try
                {
                    if (proc != null)
                    {
                        proc.exitValue();
                    }
                }
                catch (IllegalThreadStateException ex)
                {
                    // not dead yet!
                    count++;
                }
            }
        }

        return count;
    }

    public int getBlessedPartitionDetectionProcId() {
        int currMin = Integer.MAX_VALUE;
        int currMinIdx = 0;
        for (int i = 0; i < m_pipes.size(); i++) {
            PipeToFile p = m_pipes.get(i);
            System.out.println("Index " + i + " had hostid: " + p.getHostId());
            if (p.getHostId() < currMin) {
                currMin = p.getHostId();
                currMinIdx = i;
                System.out.println("Setting index: " + i + " to blessed.");
            }
        }
        return currMinIdx;
    }

    @Override
    public void finalize() throws Throwable {
        try {
            shutDownExternal();
        }
        finally {
            super.finalize();
        }
    }

    class ShutDownHookThread implements Runnable {
        @Override
        public void run() {
            try {
                shutDownExternal(true);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean isHSQL() {
        return m_target == BackendTarget.HSQLDB_BACKEND;
    }

    @Override
    public boolean isValgrind() {
        final String buildType = System.getenv().get("BUILD");
        if (buildType == null) {
            return false;
        }
        return buildType.startsWith("memcheck");
    }

    int getRandomTimestampSalt() {
        Random r = new Random();
        // if variance is 3, get a range between 0 and 6 inclusive
        int retval = r.nextInt(TIMESTAMP_SALT_VARIANCE * 2 + 1);
        // shift that range so it goes from -3 to 3 inclusive
        retval -= TIMESTAMP_SALT_VARIANCE;
        return retval;
    }

    @Override
    public void createDirectory(File path) throws IOException {
        for (File root : m_subRoots) {
            File actualPath = new File(root, path.getPath());
            if (!actualPath.mkdirs()) {
                throw new IOException();
            }
        }
    }

    @Override
    public void deleteDirectory(File path) throws IOException {
        for (File root : m_subRoots) {
            File actualPath = new File(root, path.getPath());
            VoltFile.recursivelyDelete(actualPath);
        }
    }

    @Override
    public ArrayList<File> listFiles(File path) throws IOException {
        ArrayList<File> files = new ArrayList<File>();
        for (File root : m_subRoots) {
            File actualPath = new File(root, path.getPath());
            for (File f : actualPath.listFiles()) {
                files.add(f);
            }
        }
        return files;
    }

    public ArrayList<File> getSubRoots() {
        return m_subRoots;
    }

}
