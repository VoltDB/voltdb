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
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.hsqldb_voltpatches.lib.tar.TarReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.CatalogContext;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;

import com.google.common.io.ByteStreams;

public class TestCollector {
    private static File voltDbRoot;
    private static String voltDbRootPath;

    private static String prefix = "voltdb_logs";

    private static File collectionTgz;
    private static File collectionFileDecompressed;

    private static int pid;

    @BeforeClass
    public static void setUp() throws Exception {
        // configure log4j
        PatternLayout layout = new PatternLayout("%d   %-5p [%t] %c: %m%n");
        DailyRollingFileAppender appender = new DailyRollingFileAppender(layout, "log/volt.log", "'.'yyyy-MM-dd");
        Logger.getRootLogger().addAppender(appender);

        String ddl =
                "CREATE TABLE WAREHOUSE (" +
                "W_ID INTEGER DEFAULT '0' NOT NULL, " +
                "W_NAME VARCHAR(16) DEFAULT NULL, " +
                "PRIMARY KEY (W_ID)" +
                ");";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(ddl);

        boolean success = builder.compile(Configuration.getPathToCatalogForTest("collector.jar"));
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("collector.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("collector.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("collector.xml");

        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        voltDbRoot = builder.getPathToVoltRoot();
        voltDbRootPath = voltDbRoot.getCanonicalPath();

        // generate heap dump
        pid = CLibrary.getpid();
        String[] cmd = {"jmap", "-dump:file=/tmp/java_pid" + pid + ".hprof", Integer.toString(pid)};
        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();

        /*
         * fake voltdb_crash file
         * copied from VoltDB.crashLocalVoltDB()
         * it would probably be nicer to actually force database to crash, but for now every attempt to call
         * VoltDB.crashLocalVoltDB() causes system to exit
         */
        TimestampType ts = new TimestampType(new java.util.Date());
        CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
        String root = catalogContext != null ? catalogContext.cluster.getVoltroot() + File.separator : "";
        PrintWriter writer = new PrintWriter(root + "voltdb_crash" + ts.toString().replace(' ', '-') + ".txt");
        writer.println("Time: " + ts);
        writer.close();

        // fake jvm crash file
        writer = new PrintWriter(System.getProperty("user.dir") + File.separator + "hs_err_pid" + pid + ".log");
        writer.println("#");
        writer.println("# A fatal error has been detected by the Java Runtime Environment:");
        writer.println("#");
        writer.close();

        collectionTgz = new File(voltDbRoot, prefix + ".tgz");
        Collector.main(new String[]{voltDbRootPath, prefix,
                                    "", "", "", // host, username, password
                                    "true",  // noPrompt
                                    "false", // dryRun
                                    "false", // skipHeapDump
                                    "true",  // calledFromVem (set to true so that resulting collection can be easily located)
                                    "false"  // fileInfoOnly
                                    });
        assertTrue(collectionTgz.exists());

        collectionFileDecompressed = new File(voltDbRoot, prefix);
        TarReader tarReader = new TarReader(collectionTgz, TarReader.OVERWRITE_MODE, null, null, collectionFileDecompressed);
        tarReader.read();
        assertTrue(collectionFileDecompressed.exists());
    }

    @AfterClass
    public static void teardown() throws Exception {
        collectionTgz.delete();
        FileUtils.deleteDirectory(collectionFileDecompressed);
    }

    /*
     * For each type of file that need to be collected, check whether it actually appears in the collection
     * currently sar data and /var/log/syslog* are ignored in testing
     * since in some cluster machines sar is not enabled and syslog* can only be read by root
     */

    @Test
    public void testCatalog() throws Exception {
        File catalogJar = new File(voltDbRoot + File.separator + prefix, "catalog.jar");
        assertTrue(catalogJar.exists());
    }

    @Test
    public void testDeployment() throws Exception {
        File deploymentXml = new File(voltDbRoot + File.separator + prefix, "deployment.xml");
        assertTrue(deploymentXml.exists());

        // check file content
        byte[] deploymentXmlBytes = VoltDB.instance().getHostMessenger().getZK().getData(VoltZK.deploymentBytes, false, null);
        byte[] deploymentXmlBytesInCollection = ByteStreams.toByteArray(new FileInputStream(deploymentXml));
        assertTrue(Arrays.equals(deploymentXmlBytesInCollection, deploymentXmlBytes));
    }

    @Test
    public void testVoltdbCrash() {
        File voltdbCrashDir = new File(collectionFileDecompressed, "voltdb_crash");
        assertTrue(voltdbCrashDir.exists());
        assertTrue(voltdbCrashDir.listFiles().length > 0);

        for (File file: voltdbCrashDir.listFiles()) {
            assertTrue(file.getName().startsWith("voltdb_crash") && file.getName().endsWith(".txt"));
        }
    }

    @Test
    public void testHeapdump() {
        File heapdumpFile = new File(collectionFileDecompressed, "java_pid" + pid + ".hprof");
        assertTrue(heapdumpFile.exists());
    }

    @Test
    public void testLogFiles() {
        File logDir = new File(collectionFileDecompressed, "log");
        assertTrue(logDir.exists());
        assertTrue(logDir.listFiles().length > 0);

        for (File file: logDir.listFiles()) {
            assertTrue(file.getName().startsWith("volt.log"));
        }
    }

    @Test
    public void testJvmCrash() {
        File jvmCrashFile = new File(collectionFileDecompressed, "hs_err_pid" + pid + ".log");
        assertTrue(jvmCrashFile.exists());
    }

    @Test
    public void testDmesg() {
        File dmesgdata = new File(collectionFileDecompressed, "dmesgdata");
        assertTrue(dmesgdata.exists());
    }
}
