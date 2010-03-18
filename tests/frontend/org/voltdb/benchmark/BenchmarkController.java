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

package org.voltdb.benchmark;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.ClusterMonitor;
import org.voltdb.benchmark.BenchmarkResults.Result;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.processtools.ProcessSetManager;
import org.voltdb.processtools.SSHTools;
import org.voltdb.processtools.ShellTools;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltLoggerFactory;

public class BenchmarkController {

    ProcessSetManager m_clientPSM = new ProcessSetManager();
    ProcessSetManager m_serverPSM = new ProcessSetManager();
    BenchmarkResults m_currentResults = null;
    Set<String> m_clients = new HashSet<String>();
    ClientStatusThread m_statusThread = null;
    Set<BenchmarkInterest> m_interested = new HashSet<BenchmarkInterest>();
    long m_maxCompletedPoll = 0;
    long m_pollCount = 0;
    AtomicBoolean m_statusThreadShouldContinue = new AtomicBoolean(true);
    AtomicInteger m_clientsNotReady = new AtomicInteger(0);
    AtomicInteger m_pollIndex = new AtomicInteger(0);

    final static String m_tpccClientClassName =
        "org.voltdb.benchmark.tpcc.TPCCClient";

    // benchmark parameters
    final BenchmarkConfig m_config;
    ResultsUploader uploader = null;

    Class<? extends ClientMain> m_clientClass = null;
    Class<? extends VoltProjectBuilder> m_builderClass = null;
    Class<? extends ClientMain> m_loaderClass = null;

    VoltProjectBuilder m_projectBuilder;
    String m_jarFileName = null;
    ServerThread m_localserver = null;
    private ClusterMonitor m_clusterMonitor;
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(BenchmarkController.class.getName(), VoltLoggerFactory.instance());
    private static final Logger benchmarkLog = Logger.getLogger("BENCHMARK", VoltLoggerFactory.instance());

    public static interface BenchmarkInterest {
        public void benchmarkHasUpdated(BenchmarkResults currentResults);
    }

    class ClientStatusThread extends Thread {

        @Override
        public void run() {
            long resultsToRead = m_pollCount * m_clients.size();

            while (resultsToRead > 0) {
                ProcessSetManager.OutputLine line = m_clientPSM.nextBlocking();
                if (line.stream == ProcessSetManager.Stream.STDERR) {
                    //System.err.printf("(%s): \"%s\"\n", line.processName, line.value);
                    continue;
                }

                // assume stdout at this point

                // split the string on commas and strip whitespace
                String[] parts = line.value.split(",");
                for (int i = 0; i < parts.length; i++)
                    parts[i] = parts[i].trim();

                // expect at least time and status
                if (parts.length < 2) {
                    if (line.value.startsWith("Listening for transport dt_socket at address:") ||
                            line.value.contains("Attempting to load") ||
                            line.value.contains("Successfully loaded native VoltDB library")) {
                        benchmarkLog.info(line.processName + ": " + line.value + "\n");
                        continue;
                    }
                    m_clientPSM.killProcess(line.processName);
                    LogKeys logkey =
                        LogKeys.benchmark_BenchmarkController_ProcessReturnedMalformedLine;
                    benchmarkLog.l7dlog( Level.ERROR, logkey.name(),
                            new Object[] { line.processName, line.value }, null);
                    continue;
                }

                long time = Long.parseLong(parts[0]);
                String status = parts[1];

                if (status.equals("READY")) {
                    LogKeys logkey = LogKeys.benchmark_BenchmarkController_GotReadyMessage;
                    benchmarkLog.l7dlog( Level.INFO, logkey.name(),
                            new Object[] { line.processName }, null);
                    benchmarkLog.info("Got ready message.");
                    m_clientsNotReady.decrementAndGet();
                }
                else if (status.equals("ERROR")) {
                    m_clientPSM.killProcess(line.processName);
                    LogKeys logkey = LogKeys.benchmark_BenchmarkController_ReturnedErrorMessage;
                    benchmarkLog.l7dlog( Level.ERROR, logkey.name(),
                            new Object[] { line.processName, parts[2] }, null);
                    benchmarkLog.error(
                            "(" + line.processName + ") Returned error message:\n"
                            + " \"" + parts[2] + "\"\n");
                    continue;
                }
                else if (status.equals("RUNNING")) {
                    //System.out.println("Got running message.");
                    HashMap<String, Long> results = new HashMap<String, Long>();
                    if ((parts.length % 2) != 0) {
                        m_clientPSM.killProcess(line.processName);
                        LogKeys logkey =
                            LogKeys.benchmark_BenchmarkController_ProcessReturnedMalformedLine;
                        benchmarkLog.l7dlog( Level.ERROR, logkey.name(),
                                new Object[] { line.processName, line.value }, null);
                        continue;
                    }
                    for (int i = 2; i < parts.length; i += 2) {
                        String txnName = parts[i];
                        long txnCount = Long.valueOf(parts[i+1]);
                        results.put(txnName, txnCount);
                    }
                    resultsToRead--;
                    setPollResponseInfo(line.processName, time, results, null);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public BenchmarkController(BenchmarkConfig config) {

        m_config = config;

        try {
            m_clientClass = (Class<? extends ClientMain>)Class.forName(m_config.benchmarkClient);
            //Hackish, client expected to have these field as a static member
            Field builderClassField = m_clientClass.getField("m_projectBuilderClass");
            Field loaderClassField = m_clientClass.getField("m_loaderClass");
            Field jarFileNameField = m_clientClass.getField("m_jarFileName");
            m_builderClass = (Class<? extends VoltProjectBuilder>)builderClassField.get(null);
            m_loaderClass = (Class<? extends ClientMain>)loaderClassField.get(null);
            m_jarFileName = (String)jarFileNameField.get(null);
        } catch (Exception e) {
            LogKeys logkey = LogKeys.benchmark_BenchmarkController_ErrorDuringReflectionForClient;
            benchmarkLog.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { m_config.benchmarkClient }, e);
            System.exit(-1);
        }

        //uploader = new ResultsUploader(m_config.benchmarkClient, config);

        try {
            m_projectBuilder = m_builderClass.newInstance();
        } catch (Exception e) {
            LogKeys logkey =
                LogKeys.benchmark_BenchmarkController_UnableToInstantiateProjectBuilder;
            benchmarkLog.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { m_builderClass.getSimpleName() }, e);
            System.exit(-1);
        }
        m_projectBuilder.addAllDefaults();

        if (config.snapshotFrequency != null
                && config.snapshotPath != null
                && config.snapshotPrefix != null
                && config.snapshotRetain > 0) {
            m_projectBuilder.setSnapshotSettings(
                    config.snapshotFrequency,
                    config.snapshotRetain,
                    config.snapshotPath,
                    config.snapshotPrefix);
        }
    }

    public void registerInterest(BenchmarkInterest interest) {
        synchronized(m_interested) {
            m_interested.add(interest);
        }
    }

    public void setupBenchmark() {
        // actually compile and write the catalog to disk
        m_projectBuilder.compile(
                m_jarFileName,
                m_config.sitesPerHost,
                m_config.hosts.length,
                m_config.k_factor,
                m_config.hosts[0]);

        // copy the catalog to the servers, but don't bother in local mode
        boolean status;
        if (m_config.localmode == false) {
            for (String host : m_config.hosts) {
                status = SSHTools.copyFromLocal(
                        new File(m_jarFileName),
                        m_config.remoteUser,
                        host,
                        m_config.remotePath);
                assert(status) :
                    "SSH copyFromLocal failed to copy "
                    + m_jarFileName + " to "
                    + m_config.remoteUser + "@" + host + ":" + m_config.remotePath;
            }
            for (String client : m_config.clients) {
                status = SSHTools.copyFromLocal(
                        new File(m_jarFileName),
                        m_config.remoteUser,
                        client,
                        m_config.remotePath);
                assert(status) :
                    "SSH copyFromLocal failed to copy "
                    + m_jarFileName + " to "
                    + m_config.remoteUser + "@" + client + ":" + m_config.remotePath;
            }


            // KILL ALL JAVA ORG.VOLTDB PROCESSES NOW
            Set<Thread> threads = new HashSet<Thread>();
            for (String host : m_config.hosts) {
                Thread t = new KillStragglers(m_config.remoteUser, host, m_config.remotePath);
                t.start();
                threads.add(t);
            }
            for (String host : m_config.clients) {
                Thread t = new KillStragglers(m_config.remoteUser, host, m_config.remotePath);
                t.start();
                threads.add(t);
            }
            for (Thread t : threads)
                try {
                    t.join();
                } catch (InterruptedException e) {
                    LogKeys logkey = LogKeys.benchmark_BenchmarkController_UnableToRunRemoteKill;
                    benchmarkLog.l7dlog(Level.FATAL, logkey.name(), e);
                    benchmarkLog.fatal("Couldn't run remote kill operation.", e);
                    System.exit(-1);
                }


            // SETUP THE CLEANUP HOOKS
            for (String host : m_config.hosts) {
                Runtime.getRuntime().addShutdownHook(
                        new KillStragglers(m_config.remoteUser, host, m_config.remotePath));
            }
            for (String client : m_config.clients) {
                Runtime.getRuntime().addShutdownHook(
                        new KillStragglers(m_config.remoteUser, client, m_config.remotePath));
            }

            // START THE SERVERS
            m_serverPSM = new ProcessSetManager();
            for (String host : m_config.hosts) {

                String debugString = "";
                if (m_config.listenForDebugger) {
                    debugString =
                        " -agentlib:jdwp=transport=dt_socket,address=8001,server=y,suspend=n ";
                }
                // -agentlib:hprof=cpu=samples,
                // depth=32,interval=10,lineno=y,monitor=y,thread=y,force=y,
                // file=" + host + "_hprof_tpcc.txt"
                String[] command = {
                        "java",
                        "-Djava.library.path=.",
                        "-Dlog4j.configuration=log.xml",
                        debugString,
                        /*
                         * The vast majority of Volt heap usage is young generation. When running the benchmark
                         * there isn't a single full GC performed. They are all young gen GCs and there isn't
                         * anywhere near enough data to fill the tenured gen.
                         */
                        "-Xmn" + String.valueOf((m_config.serverHeapSize / 4) * 3) + "m",
                        "-Xmx" + String.valueOf(m_config.serverHeapSize) + "m",
                        "-server",
                        "-cp", "\"voltdbfat.jar:vertica_4.0_jdk_5.jar:" + m_jarFileName + "\"",
                        "org.voltdb.VoltDB",
                        "catalog",
                        m_jarFileName,
                        m_config.useProfile,
                        m_config.backend};

                command = SSHTools.convert(m_config.remoteUser, host, m_config.remotePath, command);

                StringBuilder fullCommand = new StringBuilder();
                for (String s : command)
                    fullCommand.append(s).append(" ");
                //uploader.setCommandLineForHost(host, fullCommand.toString());

                benchmarkLog.debug(fullCommand.toString());

                m_serverPSM.startProcess(host, command);
            }

            // WAIT FOR SERVERS TO BE READY
            String readyMsg = "Server completed initialization.";
            ProcessSetManager.OutputLine line = m_serverPSM.nextBlocking();
            while(line.value.equals(readyMsg) == false) {
                line = m_serverPSM.nextBlocking();
            }
        }
        else {
            // START A SERVER LOCALLY IN-PROCESS
            VoltDB.Configuration localconfig = new VoltDB.Configuration();
            localconfig.m_pathToCatalog = m_jarFileName;
            m_localserver = new ServerThread(localconfig);
            m_localserver.start();
            m_localserver.waitForInitialization();
        }

        try {
            m_clusterMonitor = new ClusterMonitor(
                    m_config.applicationName == null ? m_config.benchmarkClient : m_config.applicationName,
                    m_config.subApplicationName,
                    m_config.statsTag,
                    m_config.hosts.length,
                    m_config.sitesPerHost,
                    m_config.hosts.length * m_config.sitesPerHost,
                    m_config.k_factor,
                    new ArrayList<String>(java.util.Arrays.asList(m_config.hosts)),
                    "",
                    "",
                    m_config.databaseURL,
                    m_config.interval);
            m_clusterMonitor.start();
        } catch (java.sql.SQLException e) {
            e.printStackTrace();
        }

        if (m_loaderClass != null) {
            ArrayList<String> localArgs = new ArrayList<String>();

            // set loader max heap to MAX(1M,6M) based on thread count.
            int lthreads = 2;
            if (m_config.parameters.containsKey("loadthreads")) {
                lthreads = Integer.parseInt(m_config.parameters.get("loadthreads"));
                if (lthreads < 1) lthreads = 1;
                if (lthreads > 6) lthreads = 6;
            }
            int loaderheap = 1024 * lthreads;
            benchmarkLog.debug("LOADER HEAP " + loaderheap);

            String debugString = "";
            if (m_config.listenForDebugger) {
                debugString = " -agentlib:jdwp=transport=dt_socket,address=8002,server=y,suspend=n ";
            }
            StringBuilder loaderCommand = new StringBuilder(4096);

            loaderCommand.append("java -XX:-ReduceInitialCardMarks -Xmx" + loaderheap + "m " + debugString);
            String classpath = "voltdbfat.jar" + ":" + m_jarFileName;
            if (System.getProperty("java.class.path") != null) {
                classpath = classpath + ":" + System.getProperty("java.class.path");
            }
            loaderCommand.append(" -cp \"" + classpath + "\" ");
            loaderCommand.append(m_loaderClass.getCanonicalName());
            for (String host : m_config.hosts) {
                String port = String.valueOf(VoltDB.DEFAULT_PORT);
                loaderCommand.append(" HOST=" + host + ":" + port);
                localArgs.add("HOST=" + host + ":" + port);
            }

            loaderCommand.append(" STATSDATABASEURL=" + m_config.databaseURL + " ");
            loaderCommand.append(" STATSPOLLINTERVAL=" + m_config.interval + " ");
            localArgs.add(" STATSDATABASEURL=" + m_config.databaseURL + " ");
            localArgs.add(" STATSPOLLINTERVAL=" + m_config.interval + " ");

            StringBuffer userParams = new StringBuffer(4096);
            for (Entry<String,String> userParam : m_config.parameters.entrySet()) {
                if (userParam.getKey().equals("TXNRATE")) {
                    continue;
                }
                userParams.append(" ");
                userParams.append(userParam.getKey());
                userParams.append("=");
                userParams.append(userParam.getValue());

                localArgs.add(userParam.getKey() + "=" + userParam.getValue());
            }

            loaderCommand.append(userParams);

            benchmarkLog.debug("Loader Command: " + loaderCommand.toString());

            // RUN THE LOADER
            if (m_config.localmode) {
                localArgs.add("EXITONCOMPLETION=false");
                ClientMain.main(m_loaderClass, localArgs.toArray(new String[0]), true);
            }
            else {
                String[] command = SSHTools.convert(
                        m_config.remoteUser,
                        m_config.clients[0],
                        m_config.remotePath,
                        loaderCommand.toString());
                status = ShellTools.cmdToStdOut(command);
                assert(status);
            }
        }

        //Start the clients
        // java -cp voltdbfat.jar org.voltdb.benchmark.tpcc.TPCCClient warehouses=X etc...
        ArrayList<String> clArgs = new ArrayList<String>();
        clArgs.add("java");
        if (m_config.listenForDebugger) {
            clArgs.add(""); //placeholder for agent lib
        }
        clArgs.add("-XX:-ReduceInitialCardMarks -Xmx" + String.valueOf(m_config.clientHeapSize) + "m");

        /*
         * This is needed to do database verification at the end of the run. In
         * order load the snapshot tables, we need the checksum stuff in the
         * native library.
         */
        clArgs.add("-Djava.library.path=.");

        String classpath = "voltdbfat.jar" + ":" + m_jarFileName;
        if (System.getProperty("java.class.path") != null) {
            classpath = classpath + ":" + System.getProperty("java.class.path");
        }
        clArgs.add("-cp");
        clArgs.add("\"" + classpath + "\"");

        clArgs.add(m_clientClass.getCanonicalName());
        for (Entry<String,String> userParam : m_config.parameters.entrySet()) {
            clArgs.add(userParam.getKey() + "=" + userParam.getValue());
        }

        clArgs.add("CHECKTRANSACTION=" + m_config.checkTransaction);
        clArgs.add("CHECKTABLES=" + m_config.checkTables);
        clArgs.add("STATSDATABASEURL=" + m_config.databaseURL);
        clArgs.add("STATSPOLLINTERVAL=" + m_config.interval);

        for (String host : m_config.hosts)
            clArgs.add("HOST=" + host + ":" + String.valueOf(VoltDB.DEFAULT_PORT));

        int clientIndex = 0;
        for (String client : m_config.clients) {
            for (int j = 0; j < m_config.processesPerClient; j++) {
                if (m_config.listenForDebugger) {
                    clArgs.remove(1);
                    String arg = "-agentlib:jdwp=transport=dt_socket,address="
                        + (8003 + j) + ",server=y,suspend=n ";
                    clArgs.add(1, arg);
                }
                ArrayList<String> tempCLArgs = new ArrayList<String>(clArgs);
                tempCLArgs.add("ID=" + clientIndex++);
                String[] args = tempCLArgs.toArray(new String[0]);

                args = SSHTools.convert(m_config.remoteUser, client, m_config.remotePath, args);

                StringBuilder fullCommand = new StringBuilder();
                for (String s : args)
                    fullCommand.append(s).append(" ");

                //uploader.setCommandLineForClient(
                //        client + ":" + String.valueOf(j),
                //        fullCommand.toString());
                benchmarkLog.debug("Client Commnand: " + fullCommand.toString());
                m_clientPSM.startProcess(client + ":" + String.valueOf(j), args);
            }
        }

        String[] clientNames = m_clientPSM.getProcessNames();
        for (String name : clientNames) {
            m_clients.add(name);
        }
        m_clientsNotReady.set(m_clientPSM.size());


        registerInterest(new ResultsPrinter());
        //registerInterest(uploader);
    }

    public void cleanUpBenchmark() {
        m_clientPSM.killAll();
        Client client = ClientFactory.createClient();
        try {
            client.createConnection(m_config.hosts[0], "", "");
            NullCallback cb = new NullCallback();
            client.callProcedure(cb, "@Shutdown");
        } catch (NoConnectionsException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        m_serverPSM.killAll();
    }

    public void runBenchmark() {
        m_currentResults =
            new BenchmarkResults(m_config.interval, m_config.duration, m_clients.size());
        m_statusThread = new ClientStatusThread();
        m_pollCount = m_config.duration / m_config.interval;
        m_statusThread.start();

        long nextIntervalTime = m_config.interval;

        // spin on whether all clients are ready
        while (m_clientsNotReady.get() > 0)
            Thread.yield();

        // start up all the clients
        for (String clientName : m_clients)
            m_clientPSM.writeToProcess(clientName, "START\n");

        long startTime = System.currentTimeMillis();
        nextIntervalTime += startTime;
        long nowTime = startTime;
        while(m_pollIndex.get() < m_pollCount) {

            // check if the next interval time has arrived
            if (nowTime >= nextIntervalTime) {
                m_pollIndex.incrementAndGet();

                // make all the clients poll
                for (String clientName : m_clients)
                    m_clientPSM.writeToProcess(clientName, "POLL\n");

                // get ready for the next interval
                nextIntervalTime = m_config.interval * (m_pollIndex.get() + 1) + startTime;
            }

            // wait some time
            // TODO this should probably be done with Thread.sleep(...), but for now
            // i'll test with this
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            nowTime = System.currentTimeMillis();
        }

        // shut down all the clients
        for (String clientName : m_clients)
            m_clientPSM.writeToProcess(clientName, "STOP\n");
        for (String clientName : m_clients)
            m_clientPSM.joinProcess(clientName);

        try {
            if (m_clusterMonitor != null) {
                m_clusterMonitor.stop();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            m_statusThread.join(1000);
        }
        catch (InterruptedException e) {
            benchmarkLog.warn(e);
        }
    }

    /**
     *
     * @return A ResultSet instance for the ongoing or just finished benchmark run.
     */
    public BenchmarkResults getResults() {
        assert(m_currentResults != null);
        synchronized(m_currentResults) {
            return m_currentResults.copy();
        }
    }


    void setPollResponseInfo(
            String clientName,
            long time,
            Map<String, Long> transactionCounts,
            String errMsg)
    {
        assert(m_currentResults != null);
        BenchmarkResults resultCopy = null;
        int completedCount = 0;

        synchronized(m_currentResults) {
            m_currentResults.setPollResponseInfo(
                    clientName,
                    m_pollIndex.get() - 1,
                    time,
                    transactionCounts,
                    errMsg);
            completedCount = m_currentResults.getCompletedIntervalCount();
            resultCopy = m_currentResults.copy();
        }

        if (completedCount > m_maxCompletedPoll) {
            synchronized(m_interested) {
                // notify interested parties
                for (BenchmarkInterest interest : m_interested)
                    interest.benchmarkHasUpdated(resultCopy);
            }
            m_maxCompletedPoll = completedCount;

            // get total transactions run for this segment
            long txnDelta = 0;
            for (String client : resultCopy.getClientNames()) {
                for (String txn : resultCopy.getTransactionNames()) {
                    Result[] rs = resultCopy.getResultsForClientAndTransaction(client, txn);
                    Result r = rs[rs.length - 1];
                    txnDelta += r.transactionCount;
                }
            }

            // if nothing done this segment, dump everything
//            if (txnDelta == 0) {
//                tryDumpAll();
//                System.out.println("\nDUMPING!\n");
//            }
        }


    }

    /** Call dump on each of the servers */
    public void tryDumpAll() {
        Client dumpClient = ClientFactory.createClient();
        for (String host : m_config.hosts) {
            try {
                dumpClient.createConnection(host, "program", "password");
                dumpClient.callProcedure("@dump");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ProcCallException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(final String[] vargs) {
        long interval = 10000;
        long duration = 60000;
        int hostCount = 1;
        int sitesPerHost = 2;
        int k_factor = 0;
        int clientCount = 1;
        int processesPerClient = 1;
        String remotePath = "voltbin/";
        String remoteUser = null; // null implies current local username
        String clientClassname = m_tpccClientClassName;
        boolean listenForDebugger = false;
        int serverHeapSize = 2048;
        int clientHeapSize = 1024;
        boolean localmode = false;
        String useProfile = "";
        String backend = "jni";
        String snapshotPath = null;
        String snapshotFrequency = null;
        String snapshotPrefix = null;
        int snapshotRetain = -1;
        float checkTransaction = 0;
        boolean checkTables = false;
        String databaseURL = "jdbc:mysql://hzproject.com/monitoring_agent?" +
            "user=monitoring_agent\\&password=n7s4JTchAhcMjPM7";
        String statsTag = null;
        String applicationName = null;
        String subApplicationName = null;

        LinkedHashMap<String, String> clientParams = new LinkedHashMap<String, String>();
        for (String arg : vargs) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
            } else if (parts[0].equals("CHECKTRANSACTION")) {
                /*
                 * Whether or not to check the result of each transaction.
                 */
                checkTransaction = Float.parseFloat(parts[1]);
            } else if (parts[0].equals("CHECKTABLES")) {
                /*
                 * Whether or not to check all the tables at the end.
                 */
                checkTables = Boolean.parseBoolean(parts[1]);
            } else if (parts[0].equals("USEPROFILE")) {
                useProfile = parts[1];
            } else if (parts[0].equals("LOCAL")) {
                /*
                 * The number of Volt servers to start.
                 * Can be less then the number of provided hosts
                 */
                localmode = Boolean.parseBoolean(parts[1]);
            } else if (parts[0].equals("HOSTCOUNT")) {
                /*
                 * The number of Volt servers to start.
                 * Can be less then the number of provided hosts
                 */
                hostCount = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("SITESPERHOST")) {
                /*
                 * The number of execution sites per host
                 */
                sitesPerHost = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("KFACTOR")) {
                /*
                 * The number of partition replicas (k-factor)
                 */
                k_factor = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("CLIENTCOUNT")) {
                /*
                 * The number of client hosts to place client processes on
                 */
                clientCount = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("PROCESSESPERCLIENT")) {
                /*
                 * The number of client processes per client host
                 */
                processesPerClient = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("CLIENTHEAP")) {
                /*
                 * The number of client processes per client host
                 */
                clientHeapSize = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("SERVERHEAP")) {
                /*
                 * The number of client processes per client host
                 */
                serverHeapSize = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("INTERVAL")) {
                /*
                 * The interval to poll for results in milliseconds
                 */
                interval = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("DURATION")) {
                /*
                 * Duration of the benchmark in milliseconds
                 */
                duration = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("CLIENT")) {
                /*
                 * Name of the client class for this benchmark.
                 *
                 * This is a class that extends ClientMain and has associated
                 * with it a VoltProjectBuilder implementation and possibly a
                 * Loader that also extends ClientMain
                 */
                clientClassname = parts[1];
            } else if (parts[0].equals("REMOTEPATH")) {
                /*
                 * Directory on the NFS host where the VoltDB files are stored
                 */
                remotePath = parts[1];
            } else if (parts[0].equals("REMOTEUSER")) {
                /*
                 * User that runs volt on remote client and host machines
                 */
                remoteUser =  parts[1];
            } else if (parts[0].equals("HOST") || parts[0].equals("CLIENTHOST")) {
                //Do nothing, parsed later.
            } else if (parts[0].equals("LISTENFORDEBUGGER")) {
                listenForDebugger = Boolean.parseBoolean(parts[1]);
            } else if (parts[0].equals("BACKEND")) {
                backend = parts[1];
            } else if (parts[0].equals("SNAPSHOTPATH")) {
                snapshotPath = parts[1];
            } else if (parts[0].equals("SNAPSHOTFREQUENCY")) {
                snapshotFrequency = parts[1];
            } else if (parts[0].equals("SNAPSHOTPREFIX")) {
                snapshotPrefix = parts[1];
            } else if (parts[0].equals("SNAPSHOTRETAIN")) {
                snapshotRetain = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("TXNRATE")) {
                clientParams.put(parts[0], parts[1]);
            } else if (parts[0].equals("NUMCONNECTIONS")) {
                clientParams.put(parts[0], parts[1]);
            } else if (parts[0].equals("STATSDATABASEURL")) {
                databaseURL = parts[1];
            } else if (parts[0].equals("STATSTAG")) {
                statsTag = parts[1];
            } else if (parts[0].equals("APPLICATIONNAME")) {
                applicationName = parts[1];
            } else if (parts[0].equals("SUBAPPLICATIONNAME")) {
                subApplicationName = parts[1];
            } else {
                clientParams.put(parts[0].toLowerCase(), parts[1]);
            }
        }

        if (duration < 1000) {
            System.err.println("Duration is specified in milliseconds");
            System.exit(-1);
        }

        // hack for defaults
        if (clientClassname.equals(m_tpccClientClassName)) {
            if (clientParams.containsKey("warehouses") == false)
                clientParams.put("warehouses", "4");
            if (clientParams.containsKey("loadthreads") == false)
                clientParams.put("loadthreads", "4");
        }

        ArrayList<String> hosts = new ArrayList<String>();
        ArrayList<String> clients = new ArrayList<String>();

        for (String arg : vargs) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
            }
            else if (parts[0].equals("HOST")) {
                /*
                 * Name of a host to be used for Volt servers
                 */
                String hostnport[] = parts[1].split("\\:",2);
                hosts.add(hostnport[0]);
            } else if (parts[0].equals("CLIENTHOST")) {
                /*
                 * Name of a host to be used for Volt clients
                 */
                String hostnport[] = parts[1].split("\\:",2);
                clients.add(hostnport[0]);
            }
        }

        // if no hosts given, use localhost
        if (hosts.size() == 0)
            hosts.add("localhost");
        if (clients.size() == 0)
            clients.add("localhost");

        if (clients.size() < clientCount) {
            LogKeys logkey = LogKeys.benchmark_BenchmarkController_NotEnoughClients;
            benchmarkLog.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { clients.size(), clientCount }, null);
            System.exit(-1);
        }
        if (hosts.size() < hostCount) {
            LogKeys logkey = LogKeys.benchmark_BenchmarkController_NotEnoughHosts;
            benchmarkLog.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { hosts.size(), hostCount }, null);
            benchmarkLog.fatal("Don't have enough hosts(" + hosts.size()
                    + ") for host count " + hostCount);
            System.exit(-1);
        }

        // copy the lists of hostnames into array of the right lengths
        // (this truncates the list to the right number)
        String[] hostNames = new String[hostCount];
        for (int i = 0; i < hostCount; i++)
            hostNames[i] = hosts.get(i);
        String[] clientNames = new String[clientCount];
        for (int i = 0; i < clientCount; i++)
            clientNames[i] = clients.get(i);

        // create a config object, mostly for the results uploader at this point
        BenchmarkConfig config = new BenchmarkConfig(clientClassname, backend, hostNames,
                sitesPerHost, k_factor, clientNames, processesPerClient, interval, duration,
                remotePath, remoteUser, listenForDebugger, serverHeapSize, clientHeapSize,
                localmode, useProfile, checkTransaction, checkTables, snapshotPath, snapshotPrefix,
                snapshotFrequency, snapshotRetain, databaseURL, statsTag, applicationName,
                subApplicationName);
        config.parameters.putAll(clientParams);

        // ACTUALLY RUN THE BENCHMARK
        BenchmarkController controller = new BenchmarkController(config);
        controller.setupBenchmark();
        controller.runBenchmark();
        controller.cleanUpBenchmark();
    }
}
