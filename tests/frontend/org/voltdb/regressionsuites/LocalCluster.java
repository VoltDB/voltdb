/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.util.Random;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.PortGenerator;
import org.voltdb.BackendTarget;
import org.voltdb.ReplicationRole;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.CommandLine;
import org.voltdb.utils.MiscUtils;
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

    VoltLogger log = new VoltLogger("HOST");

    // the timestamp salt for the TransactionIdManager
    // will vary between -3 and 3 uniformly
    static final int TIMESTAMP_SALT_VARIANCE = 3;

    int getRandomTimestampSalt() {
        Random r = new Random();
        // if variance is 3, get a range between 0 and 6 inclusive
        int retval = r.nextInt(TIMESTAMP_SALT_VARIANCE * 2 + 1);
        // shift that range so it goes from -3 to 3 inclusive
        retval -= TIMESTAMP_SALT_VARIANCE;
        return retval;
    }

    // how long to wait for startup of external procs
    static final long PIPE_WAIT_MAX_TIMEOUT = 60 * 1000;

    String m_callingClassName = "";
    String m_callingMethodName = "";
    boolean m_compiled = false;
    int m_siteCount;
    int m_hostCount;
    int m_kfactor = 0;
    boolean m_running = false;
    private final boolean m_debug;
    FailureState m_failureState;
    ArrayList<Process> m_cluster = new ArrayList<Process>();

    // Dedicated paths in the filesystem to be used as a root for each process
    ArrayList<File> m_subRoots = new ArrayList<File>();
    public ArrayList<File> getSubRoots() {
        return m_subRoots;
    }

    boolean m_hasLocalServer = true;
    public void setHasLocalServer(boolean hasLocalServer) {
        m_hasLocalServer = hasLocalServer;
    }

    ArrayList<PipeToFile> m_pipes = null;
    ArrayList<CommandLine> m_cmdLines = null;
    ServerThread m_localServer = null;
    ProcessBuilder m_procBuilder;
    private final ArrayList<ArrayList<EEProcess>> m_eeProcs = new ArrayList<ArrayList<EEProcess>>();

    // Produce a (presumably) available IP port number.
    public final PortGenerator portGenerator = new PortGenerator();

    // The base command line - each process copies and customizes this.
    // Each local cluster process has a CommandLine instance configured
    // with the port numbers and command line parameter value specific to that
    // instance.
    private final CommandLine templateCmdLine = new CommandLine();

    public LocalCluster(String jarFileName, int siteCount,
            int hostCount, int kfactor, BackendTarget target) {
        this(jarFileName, siteCount,
             hostCount, kfactor, target,
             FailureState.ALL_RUNNING, false, false);
    }

    public LocalCluster(String jarFileName, int siteCount,
                        int hostCount, int kfactor, BackendTarget target,
                        boolean isRejoinTest) {
        this(jarFileName, siteCount,
             hostCount, kfactor, target,
             FailureState.ALL_RUNNING, false, isRejoinTest);
    }

    public LocalCluster(String jarFileName, int siteCount,
                        int hostCount, int kfactor, BackendTarget target,
                        FailureState failureState,
                        boolean debug) {
        this(jarFileName, siteCount, hostCount, kfactor, target,
             failureState, debug, false);
    }

    public LocalCluster(String jarFileName, int siteCount,
                        int hostCount, int kfactor, BackendTarget target,
                        FailureState failureState,
                        boolean debug, boolean isRejoinTest)
    {
        assert (jarFileName != null);
        assert (siteCount > 0);
        assert (hostCount > 0);

        // get the name of the calling class
        StackTraceElement[] traces = Thread.currentThread().getStackTrace();
        m_callingClassName = "UnknownClass";
        m_callingMethodName = "unknownMethod";
        //ArrayUtils.reverse(traces);
        int i;
        // skip all stack frames below this method
        for (i = 0; traces[i].getClassName().equals(getClass().getName()) == false; i++);
        // skip all stack frames from localcluster itself
        for (;      traces[i].getClassName().equals(getClass().getName()); i++);
        // skip the package name
        int dot = traces[i].getClassName().lastIndexOf('.');
        m_callingClassName = traces[i].getClassName().substring(dot + 1);
        m_callingMethodName = traces[i].getMethodName();

        System.out.println("Instantiating LocalCluster for " + jarFileName + " with class.method: " +
                m_callingClassName + "." + m_callingMethodName);
        System.out.println("Sites: " + siteCount + " hosts: " + hostCount + " replication factor: " + kfactor);

        m_cluster.ensureCapacity(hostCount);

        m_siteCount = siteCount;
        m_hostCount = hostCount;
        m_kfactor = kfactor;
        m_debug = debug;
        m_failureState = kfactor < 1 ? FailureState.ALL_RUNNING : failureState;
        m_pipes = new ArrayList<PipeToFile>();
        m_cmdLines = new ArrayList<CommandLine>();

        String buildDir = System.getenv("VOLTDB_BUILD_DIR");  // via build.xml
        if (buildDir == null) {
            buildDir = System.getProperty("user.dir") + "/obj/release";
        }

        String jzmq_dir = System.getenv("VOLTDB_JZMQ_DIR"); // via build.xml
        if (jzmq_dir == null) {
            jzmq_dir = System.getProperty("user.dir") + "/third_party/cpp/jnilib";
        }

        // set the java lib path to the one for this process - default to obj/release/nativelibs
        String java_library_path = buildDir + "/nativelibs" + ":" + jzmq_dir;
        java_library_path = System.getProperty("java.library.path", java_library_path);

        String classPath = System.getProperty("java.class.path") + ":" + buildDir
            + File.separator + jarFileName + ":" + buildDir + File.separator + "prod";

        // First try 'ant' syntax and then 'eclipse' syntax...
        String log4j = System.getProperty("log4j.configuration");
        if (log4j == null) {
            log4j = "file://" + System.getProperty("user.dir") + "/tests/log4j-allconsole.xml";
        }

        m_procBuilder = new ProcessBuilder();

        // set the working directory to obj/release/prod
        //m_procBuilder.directory(new File(m_buildDir + File.separator + "prod"));
        m_procBuilder.redirectErrorStream(true);

        Thread shutdownThread = new Thread(new ShutDownHookThread());
        java.lang.Runtime.getRuntime().addShutdownHook(shutdownThread);

        // Create the base command line that each process can makeCopy and modify
        this.templateCmdLine.
            addTestOptions(true).
            leader("").
            target(target).
            startCommand("create").
            jarFileName(VoltDB.Configuration.getPathToCatalogForTest(jarFileName)).
            buildDir(buildDir).
            javaLibraryPath(java_library_path).
            classPath(classPath).
            pathToLicense(ServerThread.getTestLicensePath()).
            log4j(log4j);
    }

    @Override
    public void setCallingMethodName(String name) {
        m_callingMethodName = name;
    }

    @Override
    public boolean compile(VoltProjectBuilder builder) {
        if (!m_compiled) {
            m_compiled = builder.compile(templateCmdLine.jarFileName(), m_siteCount, m_hostCount, m_kfactor);
            templateCmdLine.pathToDeployment(builder.getPathToDeployment());
        }
        return m_compiled;
    }

    @Override
    public boolean compileWithPartitionDetection(VoltProjectBuilder builder, String snapshotPath, String ppdPrefix) {
        if (!m_compiled) {
            m_compiled = builder.compile(templateCmdLine.jarFileName(), m_siteCount, m_hostCount, m_kfactor,
                    null, true, snapshotPath, ppdPrefix);
            templateCmdLine.pathToDeployment(builder.getPathToDeployment());
        }
        return m_compiled;
    }

    @Override
    public boolean compileWithAdminMode(VoltProjectBuilder builder, int adminPort, boolean adminOnStartup)
    {
        // ATTN: LocalCluster does not support non-default admin ports.
        // Need a way to correctly initializing the portGenerator
        // and then resetting it after tests to the usual default.
        if (adminPort != VoltDB.DEFAULT_ADMIN_PORT) {
            return false;
        }

        if (!m_compiled) {
            m_compiled = builder.compile(templateCmdLine.jarFileName(), m_siteCount, m_hostCount, m_kfactor,
                    adminPort, adminOnStartup);
            templateCmdLine.pathToDeployment(builder.getPathToDeployment());
        }
        return m_compiled;
    }

    @Override
    public void startUp() {
        startUp(true);
    }

    @Override
    public void startUp(boolean clearLocalDataDirectories) {
        startUp(clearLocalDataDirectories, ReplicationRole.NONE);
    }

    void startLocalServer(boolean clearLocalDataDirectories) {
        // Generate a new root for the in-process server if clearing directories.
        File subroot = null;
        if (clearLocalDataDirectories) {
            try {
                subroot = VoltFile.initNewSubrootForThisProcess();
                m_subRoots.add(subroot);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            subroot = m_subRoots.get(0);
        }

        // Make the local Configuration object...
        CommandLine cmdln = (templateCmdLine.makeCopy());
        cmdln.internalPort(portGenerator.next());
        cmdln.voltFilePrefix(subroot.getPath());
        cmdln.internalPort(portGenerator.next());
        cmdln.port(portGenerator.nextClient());
        cmdln.adminPort(portGenerator.nextAdmin());
        cmdln.zkport(portGenerator.next());
        // replication port and its two automatic followers.
        cmdln.drAgentStartPort(portGenerator.next());
        portGenerator.next();
        portGenerator.next();
        for (EEProcess proc : m_eeProcs.get(0)) {
            assert(proc != null);
            cmdln.ipcPort(portGenerator.next());
        }

        // rtb: why is this? this flag short-circuits an Inits worker
        // but can only be set on the local in-process server (there is no
        // command line version of it.) Consequently, in-process and out-of-
        // process VoltDBs initialize differently when this flag is set.
        // cmdln.rejoinTest(true);

        // for debug, dump the command line to a unique file.
        // cmdln.dumpToFile("/Users/rbetts/cmd_" + Integer.toString(portGenerator.next()));

        m_cluster.add(null);
        m_pipes.add(null);
        m_cmdLines.add(cmdln);
        m_localServer = new ServerThread(cmdln);
        m_localServer.start();
    }

    boolean waitForAllReady() {
        long startOfPipeWait = System.currentTimeMillis();
        boolean allReady = false;
        do {
            if ((System.currentTimeMillis() - startOfPipeWait) > PIPE_WAIT_MAX_TIMEOUT) {
                break;
            }

            allReady = true;
            for (PipeToFile pipeToFile : m_pipes) {
                if (pipeToFile == null) {
                    continue;
                }
                synchronized(pipeToFile) {
                    // if eof, then no point in waiting around
                    if (pipeToFile.m_eof.get())
                        continue;

                    // if process is dead, no point in waiting around
                    if (isProcessDead(pipeToFile.getProcess()))
                        continue;

                    // if not eof, then wait for statement of readiness
                    if (pipeToFile.m_witnessedReady.get() != true) {
                        try {
                            // use a timeout to prevent a forever hang
                            pipeToFile.wait(1000);
                        }
                        catch (InterruptedException ex) {
                            log.error(ex.toString(), ex);
                        }
                        allReady = false;
                        break;
                    }
                }
            }
        } while (allReady == false);
        return allReady;
    }

    private void printTiming(boolean logtime, String msg) {
        if (logtime) {
            System.out.println("************ " + msg);
        }
    }

    public void startUp(boolean clearLocalDataDirectories, ReplicationRole role) {
        assert (!m_running);
        if (m_running) {
            return;
        }

        // needs to be called before any call to pick a filename
        VoltDB.setDefaultTimezone();

        // set 'replica' option -- known here for the first time.
        templateCmdLine.replicaMode(role);

        // set to true to spew startup timing data
        boolean logtime = false;
        long startTime = 0;
        printTiming(logtime, "Starting cluster at: " + System.currentTimeMillis());

        // clear any logs, export or snapshot data for this run
        if (clearLocalDataDirectories) {
            try {
                m_subRoots.clear();
                VoltFile.deleteAllSubRoots();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // reset the port generator. RegressionSuite always expects
        // to find ClientInterface and Admin mode on known ports.
        portGenerator.reset();
        templateCmdLine.leaderPort(portGenerator.next());

        m_eeProcs.clear();
        for (int ii = 0; ii < m_hostCount; ii++) {
            ArrayList<EEProcess> procs = new ArrayList<EEProcess>();
            m_eeProcs.add(procs);
            for (int zz = 0; zz < m_siteCount; zz++) {
                String logfile = "LocalCluster_host_" + ii + "_site" + zz + ".log";
                procs.add(new EEProcess(templateCmdLine.target(), logfile));
            }
        }

        m_pipes.clear();
        m_cluster.clear();
        m_cmdLines.clear();
        int oopStartIndex = 0;

        // create the in-process server instance.
        if (m_hasLocalServer) {
            startLocalServer(clearLocalDataDirectories);
            ++oopStartIndex;
        }

        // create all the out-of-process servers
        for (int i = oopStartIndex; i < m_hostCount; i++) {
            startOne(i, clearLocalDataDirectories, role);
        }

        printTiming(logtime, "Pre-witness: " + (System.currentTimeMillis() - startTime) + "ms");
        boolean allReady = waitForAllReady();
        printTiming(logtime, "Post-witness: " + (System.currentTimeMillis() - startTime) + "ms");

        // verify all processes started up and count failures
        int downProcesses = 0;
        for (Process proc : m_cluster) {
            if ((proc != null) && (isProcessDead(proc))) {
                downProcesses++;
            }
        }

        // throw an exception if there were failures starting up
        if ((downProcesses > 0) || (allReady == false)) {
            // poke all the external processes to die (no guarantees)
            for (Process proc : m_cluster) {
                if (proc != null) {
                    try { proc.destroy(); } catch (Exception e) {}
                }
            }

            if (downProcesses > 0) {
                int expectedProcesses = m_hostCount - (m_hasLocalServer ? 1 : 0);
                throw new RuntimeException(
                        String.format("%d/%d external processes failed to start",
                                downProcesses, expectedProcesses));
            }
            // this error case should only be from a timeout
            if (!allReady) {
                throw new RuntimeException(
                        "One or more external processes failed to complete initialization.");
            }
        }

        // Finally, make sure the local server thread is running and wait if it is not.
        if (m_hasLocalServer) {
            m_localServer.waitForInitialization();
        }

        printTiming(logtime, "DONE: " + (System.currentTimeMillis() - startTime) + " ms");
        m_running = true;

        // if supposed to kill a server, it's go time
        if (m_failureState != FailureState.ALL_RUNNING) {
            killOne();
        }

        // after killing a server, bring it back in recovery mode
        if (m_failureState == FailureState.ONE_RECOVERING) {
            int hostId = m_hasLocalServer ? 1 : 0;
            recoverOne(logtime, startTime, hostId);
        }
    }

    private void killOne()
    {
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

    private void startOne(int hostId, boolean clearLocalDataDirectories, ReplicationRole replicaMode)
    {
        CommandLine cmdln = (templateCmdLine.makeCopy());
        try {
            cmdln.internalPort(portGenerator.next());
            // set the dragent port. it uses the start value and
            // the next two sequential port numbers - so burn those two.
            cmdln.drAgentStartPort(portGenerator.next());
            portGenerator.next();
            portGenerator.next();

            cmdln.port(portGenerator.nextClient());
            cmdln.adminPort(portGenerator.nextAdmin());
            cmdln.replicaMode(replicaMode);
            cmdln.timestampSalt(getRandomTimestampSalt());

            if (m_debug) {
                cmdln.debugPort(portGenerator.next());
            }

            if (cmdln.target().isIPC) {
                for (EEProcess proc : m_eeProcs.get(hostId)) {
                    assert(proc != null);
                    cmdln.ipcPort(portGenerator.next());
                }
            }

            cmdln.zkport(portGenerator.next());

            // If local directories are being cleared
            // generate a new subroot, otherwise reuse the existing directory
            File subroot = null;
            if (clearLocalDataDirectories) {
                subroot = VoltFile.getNewSubroot();
                m_subRoots.add(subroot);
            } else {
                subroot = m_subRoots.get(hostId);
            }
            cmdln.voltFilePrefix(subroot.getPath());

            m_cmdLines.add(cmdln);
            m_procBuilder.command().clear();
            m_procBuilder.command().addAll(cmdln.createCommandLine());

            // for debug, dump the command line to a file.
            //cmdln.dumpToFile("/tmp/izzy/cmd_" + Integer.toString(portGenerator.next()));
            System.out.println(cmdln);
            Process proc = m_procBuilder.start();
            m_cluster.add(proc);

            // write output to obj/release/testoutput/<test name>-n.txt
            // this may need to be more unique? Also very useful to just
            // set this to a hardcoded path and use "tail -f" to debug.
            String testoutputdir = cmdln.buildDir() + File.separator + "testoutput";

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

            String timestampStr = MiscUtils.getCompactStringTimestamp(System.currentTimeMillis());

            PipeToFile ptf = new PipeToFile(
                    testoutputdir +
                    File.separator +
                    "LC-" +
                    timestampStr + "-" +
                    getFileName() + "-" +
                    hostId +
                    ".txt",
                    proc.getInputStream(),
                    PipeToFile.m_initToken, false, proc);
            m_pipes.add(ptf);
            ptf.setName("ClusterPipe:" + String.valueOf(hostId));
            ptf.start();
        }
        catch (IOException ex) {
            log.error("Failed to start cluster process:" + ex.getMessage(), ex);
            assert (false);
        }
    }

    /**
     * Use the weird portable java way to figure out if a cluster is alive
     */
    private boolean isProcessDead(Process p) {
        try {
            p.exitValue();
            return true; // if no exception, process died
        }
        catch (IllegalThreadStateException e) {
            return false; // process is still alive
        }
    }

    public boolean recoverOne(int hostId, Integer portOffset, String rejoinHost) {
        return recoverOne(false, 0, hostId, portOffset, rejoinHost);
    }

    private boolean recoverOne(boolean logtime, long startTime, int hostId) {
        return recoverOne( logtime, startTime, hostId, null, "");
    }

    // Re-start a (dead) process. HostId is the enumberation of the host
    // in the cluster (0, 1, ... hostCount-1) -- not an hsid, for example.
    private boolean recoverOne(boolean logtime, long startTime, int hostId, Integer rejoinHostId, String rejoinHost) {

        // Lookup the client interface port of the rejoin host
        // I have no idea why this code ignores the user's input
        // based on other state in this class except to say that whoever wrote
        // it this way originally probably eats kittens and hates cake.
        if (rejoinHostId == null || m_hasLocalServer) {
            rejoinHostId = 0;
        }
        int portNoToRejoin = m_cmdLines.get(rejoinHostId).internalPort();
        System.out.println("Rejoining " + hostId + " to hostID: " + rejoinHostId);

        // rebuild the EE proc set.
        ArrayList<EEProcess> eeProcs = m_eeProcs.get(hostId);
        for (EEProcess proc : eeProcs) {
            try {
                proc.waitForShutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        eeProcs.clear();
        if (templateCmdLine.target().isIPC) {
            for (int ii = 0; ii < m_siteCount; ii++) {
                String logfile = "LocalCluster_host_" + hostId + "_site" + ii + ".log";
                eeProcs.add(new EEProcess(templateCmdLine.target(), logfile));
            }
        }

        PipeToFile ptf = null;
        long start = 0;
        try {
            CommandLine rejoinCmdLn = m_cmdLines.get(hostId);
            // This shouldn't collide but apparently it sucks.
            // Bump it to avoid collisions on rejoin.
            if (m_debug) {
                rejoinCmdLn.debugPort(portGenerator.next());
            }
            rejoinCmdLn.rejoinHostAndPort(rejoinHost + ":" + String.valueOf(portNoToRejoin));

            rejoinCmdLn.m_port = portGenerator.nextClient();
            rejoinCmdLn.m_adminPort = portGenerator.nextAdmin();
            rejoinCmdLn.m_zkInterface = "127.0.0.1:" + portGenerator.next();
            rejoinCmdLn.m_internalPort = portGenerator.next();
            setPortsFromConfig(hostId, rejoinCmdLn);

            m_procBuilder.command().clear();
            m_procBuilder.command().addAll(rejoinCmdLn.createCommandLine());
            Process proc = m_procBuilder.start();
            start = System.currentTimeMillis();

            // write output to obj/release/testoutput/<test name>-n.txt
            // this may need to be more unique? Also very useful to just
            // set this to a hardcoded path and use "tail -f" to debug.
            String testoutputdir = rejoinCmdLn.buildDir() + File.separator + "testoutput";
            // make sure the directory exists
            File dir = new File(testoutputdir);
            if (dir.exists()) {
                assert(dir.isDirectory());
            }
            else {
                boolean status = dir.mkdirs();
                assert(status);
            }

            String timestampStr = MiscUtils.getCompactStringTimestamp(System.currentTimeMillis());

            ptf = new PipeToFile(
                    testoutputdir +
                    File.separator +
                    "LC-" +
                    timestampStr + "-" +
                    getFileName() + "-" +
                    hostId +
                    ".rejoined.txt",
                    proc.getInputStream(),
                    PipeToFile.m_rejoinToken, true, proc);
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
            log.error("Failed to start recovering cluster process:" + ex.getMessage(), ex);
            assert (false);
        }

        // wait for the joining site to be ready
        synchronized (ptf) {
            if (logtime) System.out.println("********** pre witness: " + (System.currentTimeMillis() - startTime) + " ms");
            while (ptf.m_witnessedReady.get() != true) {
                // if eof, then no point in waiting around
                if (ptf.m_eof.get())
                    break;

                // if process is dead, no point in waiting around
                if (isProcessDead(ptf.getProcess()))
                    break;

                try {
                    // wait for explicit notification
                    ptf.wait(1000);
                }
                catch (InterruptedException ex) {
                    log.error(ex.toString(), ex);
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
        }
        catch (Exception e) {
            log.error("Failure to shutdown LocalCluster's in-process VoltDB server.", e);
        }
        finally {
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
            proc.waitFor();
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

    public synchronized void shutDownExternal(boolean forceKillEEProcs)
    {
        if (m_cluster != null) {
            // kill all procs
            for (Process proc : m_cluster) {
                if (proc == null)
                    continue;
                proc.destroy();
            }

            // join on all procs
            for (Process proc : m_cluster) {
                if (proc == null)
                    continue;
                int retval = 0;
                try {
                    retval = proc.waitFor();
                }
                catch (InterruptedException e) {
                    System.out.println("Unable to wait for Localcluster process to die: " + proc.toString());
                    log.error("Unable to wait for Localcluster process to die: " + proc.toString(), e);
                }
                // exit code 143 is the forcible shutdown code from .destroy()
                if (retval != 0 && retval != 143)
                {
                    System.out.println("External VoltDB process terminated abnormally with return: " + retval);
                    log.error("External VoltDB process terminated abnormally with return: " + retval);
                }
            }
        }

        if (m_cluster != null) m_cluster.clear();

        for (ArrayList<EEProcess> procs : m_eeProcs) {
            for (EEProcess proc : procs) {
                try {
                    proc.waitForShutdown();
                } catch (InterruptedException e) {
                    log.error("Unable to wait for EEProcess to die: " + proc.toString(), e);
                }
            }
        }

        if (templateCmdLine.target() == BackendTarget.NATIVE_EE_VALGRIND_IPC) {
            if (!EEProcess.m_valgrindErrors.isEmpty()) {
                String failString = "";
                for (final String error : EEProcess.m_valgrindErrors) {
                    failString = failString + "\n" + error;
                }
                org.junit.Assert.fail(failString);
            }
        }

        m_eeProcs.clear();
    }

    @Override
    public List<String> getListenerAddresses() {
        if (!m_running) {
            return null;
        }
        ArrayList<String> listeners = new ArrayList<String>();
        for (int i = 0; i < m_cmdLines.size(); i++) {
            CommandLine cl = m_cmdLines.get(i);
            Process p = m_cluster.get(i);
            if (p != null) {
                listeners.add("localhost:" + cl.m_port);
            }
        }
        return listeners;
    }

    @Override
    public String getName() {
        String prefix = "localCluster";
        if (m_failureState == FailureState.ONE_FAILURE)
            prefix += "OneFail";
        if (m_failureState == FailureState.ONE_RECOVERING)
            prefix += "OneRecov";

        return prefix +
            "-" + String.valueOf(m_siteCount) +
            "-" + String.valueOf(m_hostCount) +
            "-" + templateCmdLine.target().display.toUpperCase();
    }

    String getFileName() {
        String prefix = m_callingClassName + "-" + m_callingMethodName;
        if (m_failureState == FailureState.ONE_FAILURE)
            prefix += "-OneFail";
        if (m_failureState == FailureState.ONE_RECOVERING)
            prefix += "-OneRecov";

        return prefix +
            "-" + String.valueOf(m_siteCount) +
            "-" + String.valueOf(m_hostCount) +
            "-" + templateCmdLine.target().display.toUpperCase();
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
            shutDownExternal(true);
        }
    }

    @Override
    public boolean isHSQL() {
        return templateCmdLine.target() == BackendTarget.HSQLDB_BACKEND;
    }

    public void setMaxHeap(int heap) {
        templateCmdLine.setMaxHeap(heap);
    }

    public String getPathToDeployment() {
        return templateCmdLine.pathToDeployment();
    }

    public String zkinterface(int hostId) {
        return m_cmdLines.get(hostId).zkinterface();
    }

    public int drAgentStartPort(int hostId) {
        return m_cmdLines.get(hostId).drAgentStartPort();
    }

    public int internalPort(int hostId) {
        return m_cmdLines.get(hostId).internalPort();
    }

    public int port(int hostId) {
        return m_cmdLines.get(hostId).port();
    }

    public int adminPort(int hostId) {
        return m_cmdLines.get(hostId).adminPort();
    }

    public void setPortsFromConfig(int hostId, VoltDB.Configuration config) {
        CommandLine cl = m_cmdLines.get(hostId);
        assert(cl != null);
        cl.m_port = config.m_port;
        cl.m_adminPort = config.m_adminPort;
        cl.m_zkInterface = config.m_zkInterface;
        cl.m_internalPort = config.m_internalPort;
        cl.m_leaderPort = config.m_leaderPort;
    }

    @Override
    public boolean isValgrind() {
        final String buildType = System.getenv().get("BUILD");
        if (buildType == null) {
            return false;
        }
        return buildType.startsWith("memcheck");
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


}
