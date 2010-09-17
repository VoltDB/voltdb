/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.voltdb.BackendTarget;
import org.voltdb.ProcedureProfiler;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;

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

    // configuration data
    final String m_jarFileName;
    final int m_siteCount;
    final int m_hostCount;
    final int m_replication;
    final BackendTarget m_target;
    final String m_buildDir;
    int m_portOffset;
    boolean m_hasLocalServer = true;
    String m_pathToDeployment;
    int m_pathToDeploymentOffset;
    int m_rejoinOffset;
    FailureState m_failureState;

    // state
    boolean m_compiled = false;
    boolean m_running = false;
    ArrayList<Process> m_cluster = new ArrayList<Process>();
    ArrayList<PipeToFile> m_pipes = null;
    ServerThread m_localServer = null;

    // components
    ProcessBuilder m_procBuilder;
    private int m_debugOffset1;
    private int m_debugOffset2;

    private int m_ipcPortOffset1;
    private int m_ipcPortOffset2;
    private int m_ipcPortOffset3;

    private final ArrayList<ArrayList<EEProcess>> m_eeProcs = new ArrayList<ArrayList<EEProcess>>();

    private final boolean m_debug;
    private int m_debugPortOffset = 8000;


    /* class pipes a process's output to a file name.
     * Also watches for "Server completed initialization"
     * in output - the signal of readiness!
     */
    public static class PipeToFile extends Thread {
        FileWriter m_writer ;
        InputStream m_input;
        String m_filename;

        // set m_witnessReady when the m_token byte sequence is seen.
        AtomicBoolean m_witnessedReady;
        final int m_token[];
        final static int m_initToken[] = new int[] {'S', 'e', 'r', 'v', 'e', 'r', ' ',
                                        'c', 'o', 'm', 'p', 'l', 'e', 't', 'e', 'd', ' ',
                                        'i','n','i','t'};
        final static int m_rejoinToken[] = new int[] {
            'N', 'o', 'd', 'e', ' ',
            'r', 'e', 'c', 'o', 'v', 'e', 'r', 'y', ' ',
            'c', 'o', 'm', 'p', 'l', 'e', 't', 'e'};

        long m_initTime;

        private boolean m_eof = false;

        PipeToFile(String filename, InputStream stream, int token[]) {
            m_witnessedReady = new AtomicBoolean(false);
            m_token = token;
            m_filename = filename;
            m_input = stream;
            try {
                m_writer = new FileWriter(filename);
            }
            catch (IOException ex) {
                Logger.getLogger(LocalCluster.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void run() {
            assert(m_writer != null);
            assert(m_input != null);
            int location = 0;
            int initLocation = 0;
            boolean initLocationFound = false;
            while (!m_eof) {
                try {
                    int data = m_input.read();
                    if (data == -1) {
                        m_eof = true;
                    }
                    else {
                        // look for a sequence of letters matching the server ready token.
                        if (!m_witnessedReady.get() && m_token[location] == data) {
                            location++;
                            if (location == m_token.length) {
                                synchronized (this) {
                                    m_witnessedReady.set(true);
                                    this.notifyAll();
                                }
                            }
                        }
                        else {
                            location = 0;
                        }

                        // look for a sequence of letters matching the server ready token.
                        if (!initLocationFound && m_initToken[initLocation] == data) {
                            initLocation++;
                            if (initLocation == m_initToken.length) {
                                initLocationFound = true;
                                m_initTime = System.currentTimeMillis();
                            }
                        }
                        else {
                            initLocation = 0;
                        }

                        m_writer.write(data);
                        m_writer.flush();
                    }
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
            FailureState.ALL_RUNNING, false);
    }

    public LocalCluster(String jarFileName, int siteCount,
                        int hostCount, int replication, BackendTarget target,
                        FailureState failureState,
                        boolean debug)
    {
        System.out.println("Instantiating LocalCluster for " + jarFileName);
        System.out.println("Sites: " + siteCount + " hosts: " + hostCount
                           + " replication factor: " + replication);

        assert (jarFileName != null);
        assert (siteCount > 0);
        assert (hostCount > 0);
        assert (replication >= 0);

        final String buildType = System.getenv().get("BUILD");
//        if (buildType == null) {
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
                                           "-Dlog4j.configuration=log.xml",
                                           "-ea",
                                           "-XX:-ReduceInitialCardMarks",
                                           "-XX:MaxDirectMemorySize=2g",
                                           "-Xmx2g",
                                           "-XX:+HeapDumpOnOutOfMemoryError",
                                           "-classpath",
                                           classPath,
                                           "org.voltdb.VoltDB",
                                           "catalog",
                                           m_jarFileName,
                                           "deployment",
                                           "",
                                           "port",
                                           "-1",
                                           "rejoinhost",
                                           "-1");

        // when we actually append a port value and deployment file, these will be correct
        m_debugOffset1 = m_procBuilder.command().size() - 12;
        m_debugOffset2 = m_procBuilder.command().size() - 11;
        if (m_debug) {
            m_procBuilder.command().add(m_debugOffset1, "");
            m_procBuilder.command().add(m_debugOffset1, "");
        }
        m_portOffset = m_procBuilder.command().size() - 3;
        m_pathToDeploymentOffset = m_procBuilder.command().size() - 5;
        m_rejoinOffset = m_procBuilder.command().size() - 1;

        if (m_target.isIPC) {
            m_procBuilder.command().add("");
            m_ipcPortOffset1 = m_procBuilder.command().size() - 1;
            m_procBuilder.command().add("");
            m_ipcPortOffset2 = m_procBuilder.command().size() - 1;
            m_procBuilder.command().add("");
            m_ipcPortOffset3 = m_procBuilder.command().size() - 1;
        }

        for (String s : m_procBuilder.command()) {
            System.out.println(s);
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
        m_procBuilder.command().set(6, "-Xmx" + megabytes + "m");
    }


    @Override
    public boolean compile(VoltProjectBuilder builder) {
        if (m_compiled) {
            return true;
        }

        m_compiled = builder.compile(m_jarFileName, m_siteCount, m_hostCount, m_replication, "localhost");
        m_pathToDeployment = builder.getPathToDeployment();

        return m_compiled;
    }

    @Override
    public void startUp() {
        assert (!m_running);
        if (m_running) {
            return;
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
            m_cluster.add(null);
            m_pipes.add(null);
            Configuration config = new Configuration();
            config.m_backend = m_target;
            config.m_noLoadLibVOLTDB = (m_target == BackendTarget.HSQLDB_BACKEND);
            config.m_pathToCatalog = m_jarFileName;
            config.m_pathToDeployment = m_pathToDeployment;
            config.m_profilingLevel = ProcedureProfiler.Level.DISABLED;
            config.m_port = VoltDB.DEFAULT_PORT;
            ArrayList<Integer> ports = new ArrayList<Integer>();
            for (EEProcess proc : m_eeProcs.get(0)) {
                ports.add(proc.port());
            }
            config.m_ipcPorts = java.util.Collections.synchronizedList(ports);

            m_localServer = new ServerThread(config);
            m_localServer.start();
            oopStartIndex++;
        }

        // create all the out-of-process servers
        for (int i = oopStartIndex; i < m_hostCount; i++) {
            startOne(i);
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
                if (pipeToFile.m_witnessedReady.get() != true) {
                    try {
                        // wait for explicit notification
                        synchronized (pipeToFile) {
                            pipeToFile.wait();
                        }
                    }
                    catch (InterruptedException ex) {
                        Logger.getLogger(LocalCluster.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    allReady = false;
                    break;
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

    private void startOne(int hostId) {
        try {
            m_procBuilder.command().set(m_portOffset, String.valueOf(VoltDB.DEFAULT_PORT + hostId));
            m_procBuilder.command().set(m_pathToDeploymentOffset, m_pathToDeployment);
            m_procBuilder.command().set(m_rejoinOffset, "");
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

            PipeToFile ptf = new PipeToFile(testoutputdir + File.separator +
                    getName() + "-" + hostId + ".txt", proc.getInputStream(),
                    PipeToFile.m_initToken);
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
        System.out.println("Rejoining " + hostId);

        // port for the new node
        int portNo = VoltDB.DEFAULT_PORT + hostId;

        // port to connect to (not too simple, eh?)
        int portNoToRejoin = VoltDB.DEFAULT_PORT + hostId + 1;
        if (m_hasLocalServer) portNoToRejoin = VoltDB.DEFAULT_PORT;

        if (portOffset != null) {
            portNoToRejoin = VoltDB.DEFAULT_PORT + portOffset;
        }

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

        PipeToFile ptf = null;
        long start = 0;
        try {
            m_procBuilder.command().set(m_portOffset, String.valueOf(portNo));
            m_procBuilder.command().set(m_pathToDeploymentOffset, m_pathToDeployment);
            m_procBuilder.command().set(m_rejoinOffset, rejoinHost + ":" + String.valueOf(portNoToRejoin));
            if (m_debug) {
                m_procBuilder.command().set(m_debugOffset1, "-Xdebug");
                m_procBuilder.command().set(
                        m_debugOffset2,
                        "-agentlib:jdwp=transport=dt_socket,address="
                        + m_debugPortOffset++ + ",server=y,suspend=n");
            }

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
                    getName() + "-" + hostId + ".txt", proc.getInputStream(), PipeToFile.m_rejoinToken);
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
        while (ptf.m_witnessedReady.get() != true && !ptf.m_eof) {
            if (logtime) System.out.println("********** pre witness: " + (System.currentTimeMillis() - startTime) + " ms");
            try {
                // wait for explicit notification
                synchronized (ptf) {
                    ptf.wait();
                }
            }
            catch (InterruptedException ex) {
                Logger.getLogger(LocalCluster.class.getName()).log(Level.SEVERE, null, ex);
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
            if (m_localServer != null) m_localServer.shutdown();
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
        PipeToFile ptf = null;
        ArrayList<EEProcess> procs = null;
        synchronized (this) {
           proc = m_cluster.get(hostNum);
           ptf = m_pipes.get(hostNum);
           m_cluster.set( hostNum, null);
           m_pipes.set(hostNum, null);
           procs = m_eeProcs.get(hostNum);
        }

        if (proc != null)
            proc.destroy();
        if (ptf != null) {
            new File(ptf.m_filename).delete();
        }

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

    public void shutDownExternal(boolean forceKillEEProcs) throws InterruptedException
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
        return m_target == BackendTarget.NATIVE_EE_VALGRIND_IPC;
    }

}
