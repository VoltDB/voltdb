/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.utils;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.hsqldb_voltpatches.lib.tar.TarReader;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrashJVM;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrashVoltDBProc;

public class TestCollector {
    VoltProjectBuilder builder;
    LocalCluster cluster;
    String listener;
    Client client;

    String voltDbRootPath;
    String prefix = "voltdb_logs";

    @Before
    public void setUp() throws Exception {
        String simpleSchema =
                "create table blah (" +
                "ival bigint default 0 not null, " +
                "PRIMARY KEY(ival));";

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addProcedures(CrashJVM.class);
        builder.addProcedures(CrashVoltDBProc.class);

        cluster = new LocalCluster("collect.jar",
                2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        cluster.setHasLocalServer(false);
        boolean success = cluster.compile(builder);
        assert (success);
        cluster.startUp(true);

        String voltDbFilePrefix = cluster.getSubRoots().get(0).getPath();
        File voltDbRoot = new File(voltDbFilePrefix, builder.getPathToVoltRoot().getPath());
        voltDbRootPath = voltDbRoot.getPath();

        listener = cluster.getListenerAddresses().get(0);
        client = ClientFactory.createClient();
        client.createConnection(listener);
    }

    private File collect(String voltDbRootPath, boolean skipHeapDump) throws Exception {
        File collectionTgz = new File(voltDbRootPath, prefix + ".tgz");
        Collector.main(new String[]{voltDbRootPath, prefix,
                                    "", "", "", // host, username, password
                                    "true",  // noPrompt
                                    "false", // dryRun
                                    String.valueOf(skipHeapDump),
                                    "true",  // calledFromVem (set to true so that resulting collection can be easily located)
                                    "false"  // fileInfoOnly
                                    });
        assertTrue(collectionTgz.exists());

        File collectionDecompressed = new File(voltDbRootPath, prefix);
        TarReader tarReader = new TarReader(collectionTgz, TarReader.OVERWRITE_MODE, null, null, collectionDecompressed);
        tarReader.read();
        assertTrue(collectionDecompressed.exists());

        return collectionDecompressed;
    }

    private int getpid(String voltDbRootPath) throws Exception {
        File configLogDir = new File(voltDbRootPath, "config_log");
        File configInfo = new File(configLogDir, "config.json");

        JSONObject jsonObject = Collector.parseJSONFile(configInfo.getCanonicalPath());
        int pid = jsonObject.getInt("pid");

        return pid;
    }
    private String getWorkingDir(String voltDbRootPath) throws Exception {
        File configLogDir = new File(voltDbRootPath, "config_log");
        File configInfo = new File(configLogDir, "config.json");

        JSONObject jsonObject = Collector.parseJSONFile(configInfo.getCanonicalPath());
        String workingDir = jsonObject.getString("workingDir");

        return workingDir;
    }

    private List<String> getLogPaths(String voltDbRootPath) throws Exception {
        File configLogDir = new File(voltDbRootPath, "config_log");
        File configInfo = new File(configLogDir, "config.json");
        JSONObject jsonObject = Collector.parseJSONFile(configInfo.getCanonicalPath());
        List<String> logPaths = new ArrayList<String>();
        JSONArray jsonArray = jsonObject.getJSONArray("log4jDst");
        for (int i = 0; i < jsonArray.length(); i++) {
            String path = jsonArray.getJSONObject(i).getString("path");
            logPaths.add(path);
        }
        return logPaths;
    }

    /*
     * For each type of file that need to be collected, check whether it actually appears in the collection
     * currently sar data and /var/log/syslog* are ignored in testing
     * since in some cluster machines sar is not enabled and syslog* can only be read by root
     */

    @Test
    public void testCatalog() throws Exception {
        client.close();
        cluster.shutDown();

        File collectionDecompressed = collect(voltDbRootPath, true);

        File catalogJar = new File(collectionDecompressed, "catalog.jar");
        assertTrue(catalogJar.exists());
    }

    @Test
    public void testDeployment() throws Exception {
        client.close();
        cluster.shutDown();

        File collectionDecompressed = collect(voltDbRootPath, true);

        File deploymentXml = new File(collectionDecompressed, "deployment.xml");
        assertTrue(deploymentXml.exists());
    }

    @Test
    public void testDmesg() throws Exception {
        client.close();
        cluster.shutDown();

        File collectionDecompressed = collect(voltDbRootPath, true);

        File dmesgdata = new File(collectionDecompressed, "dmesgdata");
        assertTrue(dmesgdata.exists());
    }

    @Test
    public void testHeapdump() throws Exception {
        // generate heap dump
        int pid = getpid(voltDbRootPath);

        File heapdumpGenerated = new File("/tmp", "java_pid" + pid + ".hprof");
        heapdumpGenerated.deleteOnExit();

        PrintWriter writer = new PrintWriter(heapdumpGenerated.getPath());
        writer.println("fake heapdump file");
        writer.close();

        client.close();
        cluster.shutDown();

        File collectionDecompressed = collect(voltDbRootPath, false);

        File heapdumpFile = new File(collectionDecompressed, "java_pid" + pid + ".hprof");
        assertTrue(heapdumpFile.exists());
    }

    @Test
    public void testLogFiles() throws Exception {
        client.close();
        cluster.shutDown();

        File collectionDecompressed = collect(voltDbRootPath, false);

        File logDir = new File(collectionDecompressed, "log");
        assertTrue(logDir.exists());
        assertTrue(logDir.listFiles().length > 0);
        List<String> logPaths = getLogPaths(voltDbRootPath);

        for (File file: logDir.listFiles()) {
            boolean match = false;
            for (String path: logPaths) {
                if (file.getName().startsWith(new File(path).getName())) {
                    match = true;
                    break;
                }
            }
            assertTrue(match);
        }
    }

    @Test
    public void testVoltdbCrash() throws Exception {
        try {
            client.callProcedure("CrashVoltDBProc");
        }
        catch (Exception e) {

        }

        client.close();
        cluster.shutDown();

        File collectionDecompressed = collect(voltDbRootPath, true);

        File voltdbCrashDir = new File(collectionDecompressed, "voltdb_crash");
        assertTrue(voltdbCrashDir.exists());
        assertTrue(voltdbCrashDir.listFiles().length > 0);

        for (File file: voltdbCrashDir.listFiles()) {
            assertTrue(file.getName().startsWith("voltdb_crash") && file.getName().endsWith(".txt"));
        }
    }

    @Test
    public void testJvmCrash() throws Exception {
        try {
            client.callProcedure("CrashJVM");
        }
        catch (Exception e) {

        }

        client.close();
        cluster.shutDown();

        File collectionDecompressed = collect(voltDbRootPath, true);

        int pid = getpid(voltDbRootPath);
        String workingDir = getWorkingDir(voltDbRootPath);
        File jvmCrashGenerated = new File(workingDir, "hs_err_pid" + pid + ".log");
        jvmCrashGenerated.deleteOnExit();

        File jvmCrashFile = new File(collectionDecompressed, "hs_err_pid" + pid + ".log");
        assertTrue(jvmCrashFile.exists());
    }
}
