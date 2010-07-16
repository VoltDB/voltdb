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

    // configuration data
    final String m_jarFileName;
    final int m_siteCount;
    final int m_hostCount;
    final int m_replication;
    final BackendTarget m_target;
    final String m_buildDir;
    int m_portOffset;
    String m_pathToDeployment;
    int m_pathToDeploymentOffset;

    // state
    boolean m_compiled = false;
    boolean m_running = false;
    ArrayList<Process> m_cluster = null;
    ArrayList<PipeToFile> m_pipes = null;
    ServerThread m_localServer = null;

    // components
    ProcessBuilder m_procBuilder;

    /* class pipes a process's output to a file name.
     * Also watches for "Server completed initialization"
     * in output - the sygil of readiness!
     */
    public static class PipeToFile implements Runnable {
        FileWriter m_writer ;
        InputStream m_input;
        String m_filename;

        // set m_witnessReady when the m_token byte sequence is seen.
        AtomicBoolean m_witnessedReady;
        final int m_token[] = new int[] {'S', 'e', 'r', 'v', 'e', 'r', ' ',
                                        'c', 'o', 'm', 'p', 'l', 'e', 't', 'e', 'd', ' ',
                                        'i','n','i','t'};

        PipeToFile(String filename, InputStream stream) {
            m_witnessedReady = new AtomicBoolean(false);
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
            boolean eof = false;
            while (!eof) {
                try {
                    int data = m_input.read();
                    if (data == -1) {
                        eof = true;
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
                        m_writer.write(data);
                        m_writer.flush();
                    }
                }
                catch (IOException ex) {
                    eof = true;
                }
            }
        }
    }

    public LocalCluster(String jarFileName, int siteCount,
                        int hostCount, int replication, BackendTarget target)
    {
        System.out.println("Instantiating LocalCluster for " + jarFileName);
        System.out.println("Sites: " + siteCount + " hosts: " + hostCount
                           + " replication factor: " + replication);

        assert (jarFileName != null);
        assert (siteCount > 0);
        assert (hostCount > 0);
        assert (replication >= 0);
        m_jarFileName = VoltDB.Configuration.getPathToCatalogForTest(jarFileName);
        m_siteCount = siteCount;
        m_target = target;
        m_hostCount = hostCount;
        m_replication = replication;
        String buildDir = System.getenv("VOLTDB_BUILD_DIR");  // via build.xml
        if (buildDir == null)
            m_buildDir = System.getProperty("user.dir") + "/obj/release";
        else
            m_buildDir = buildDir;

        String classPath = System.getProperty("java.class.path")+ ":" + m_buildDir + File.separator + m_jarFileName;
        classPath += ":" + m_buildDir + File.separator + "prod";

        // processes of VoltDBs using the compiled jar file.
        m_cluster = new ArrayList<Process>();
        m_pipes = new ArrayList<PipeToFile>();
        m_procBuilder = new ProcessBuilder("java",
                                           "-Djava.library.path=" + m_buildDir + "/nativelibs",
                                           "-Dlog4j.configuration=log.xml",
                                           "-ea",
                                           "-Xmx2048m",
                                           "-XX:+HeapDumpOnOutOfMemoryError",
                                           "-classpath",
                                           classPath,
                                           "org.voltdb.VoltDB",
                                           "catalog",
                                           m_jarFileName,
                                           "deployment",
                                           "",
                                           "port",
                                           "-1");

        // when we actually append a port value and deployment file, these will be correct
        m_portOffset = m_procBuilder.command().size() - 1;
        m_pathToDeploymentOffset = m_procBuilder.command().size() - 3;

        for (String s : m_procBuilder.command()) {
            System.out.println(s);
        }

        // set the working directory to obj/release/prod
        //m_procBuilder.directory(new File(m_buildDir + File.separator + "prod"));
        m_procBuilder.redirectErrorStream(true);

        Thread shutdownThread = new Thread(new ShutDownHookThread());
        java.lang.Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    @Override
    public boolean compile(VoltProjectBuilder builder) {
        return compile(builder, false);
    }

    // TODO: remove compileDeployment after ENG-642 lands
    @Override
    public boolean compile(VoltProjectBuilder builder, boolean compileDeployment) {
        if (m_compiled) {
            return true;
        }

        if (compileDeployment) {
            m_compiled = builder.compile(m_jarFileName, m_siteCount, m_hostCount, m_replication, "localhost", true);
        } else {
            m_compiled = builder.compile(m_jarFileName, m_siteCount, m_hostCount, m_replication, "localhost", false);
            m_pathToDeployment = builder.getPathToDeployment();
        }

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

        // create the in-process server
        Configuration config = new Configuration();
        config.m_backend = m_target;
        config.m_noLoadLibVOLTDB = (m_target == BackendTarget.HSQLDB_BACKEND);
        config.m_pathToCatalog = m_jarFileName;
        config.m_pathToDeployment = m_pathToDeployment;
        config.m_profilingLevel = ProcedureProfiler.Level.DISABLED;
        config.m_port = VoltDB.DEFAULT_PORT;

        m_localServer = new ServerThread(config);
        m_localServer.start();

        // create all the out-of-process servers
        for (int i = 1; i < m_hostCount; i++) {
            try {
                m_procBuilder.command().set(m_portOffset, String.valueOf(VoltDB.DEFAULT_PORT + i));
                m_procBuilder.command().set(m_pathToDeploymentOffset, m_pathToDeployment);

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
                        getName() + "-" + i + ".txt", proc.getInputStream());
                m_pipes.add(ptf);
                Thread t = new Thread(ptf);
                t.setName("ClusterPipe:" + String.valueOf(i));
                t.start();
            }
            catch (IOException ex) {
                System.out.println("Failed to start cluster process:" + ex.getMessage());
                Logger.getLogger(LocalCluster.class.getName()).log(Level.SEVERE, null, ex);
                assert (false);
            }
        }

        // spin until all the pipes see the magic "Server completed.." string.
        boolean allReady;
        do {
            if (logtime) System.out.println("********** pre witness: " + (System.currentTimeMillis() - startTime) + " ms");
            allReady = true;
            for (PipeToFile pipeToFile : m_pipes) {
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

        // Finally, make sure the local server thread is running and wait if it is not.
        m_localServer.waitForInitialization();
        if (logtime) System.out.println("********** DONE: " + (System.currentTimeMillis() - startTime) + " ms");
        m_running = true;
    }

    @Override
    synchronized public List<String> shutDown() throws InterruptedException {
        // there are couple of ways to shutdown. sysproc @kill could be
        // issued to listener. this would require that the test didn't
        // break the cluster somehow.  Or ... just old fashioned kill?

        try {
            if (m_localServer != null) m_localServer.shutdown();
        } finally {
            m_running = false;
        }
        shutDownExternal();

        return null;
    }

    public void shutDownSingleHost(int hostNum)
    {
        Process proc = m_cluster.get(hostNum);
        proc.destroy();
        m_cluster.remove(hostNum);
    }

    public void shutDownExternal() throws InterruptedException
    {
        if (m_cluster != null) {
            for (Process proc : m_cluster) {
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
        return "localCluster-" + String.valueOf(m_siteCount) +
               "-" + String.valueOf(m_hostCount) + "-" + m_target.display.toUpperCase();
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
                shutDownExternal();
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
