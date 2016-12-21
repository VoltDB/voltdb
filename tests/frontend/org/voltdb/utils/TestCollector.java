/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.utils.CoreUtils;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrashJVM;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrashVoltDBProc;

import com.google_voltpatches.common.base.Charsets;

public class TestCollector extends JUnit4LocalClusterTest {
    private static final int STARTUP_DELAY = 3000;
    VoltProjectBuilder builder;
    LocalCluster cluster;
    String listener;
    Client client;

    String voltDbRootPath;
    boolean resetCurrentTime = true;

    String rootDir;

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
        File voltDbRoot;
        cluster.startUp(true);
        //Get server specific root after startup.
        if (cluster.isNewCli()) {
            voltDbRoot = new File(cluster.getServerSpecificRoot("0"));
        } else {
            String voltDbFilePrefix = cluster.getSubRoots().get(0).getPath();
            voltDbRoot = new File(voltDbFilePrefix, builder.getPathToVoltRoot().getPath());
        }
        voltDbRootPath = voltDbRoot.getPath();
        listener = cluster.getListenerAddresses().get(0);
        client = ClientFactory.createClient();
        client.createConnection(listener);
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        cluster.shutDown();
    }

    private ZipFile collect(String voltDbRootPath, boolean skipHeapDump, int days) throws Exception {
        if(resetCurrentTime) {
            Collector.m_currentTimeMillis = System.currentTimeMillis();
        }
        Collector.main(new String[]{"--voltdbroot="+voltDbRootPath, "--prefix=\"\"",
                                    "--host=\"\"", "--username=\"\"", "--password=\"\"", // host, username, password
                                    "--noprompt=true",  // noPrompt
                                    "--dryrun=false", // dryRun
                                    "--skipheapdump="+String.valueOf(skipHeapDump),
                                    "--copyToVEM=true",
                                    "--calledFromVEM=true",  // calledFromVem (set to true so that resulting collection can be easily located)
                                    "--fileInfoOnly=false",  // fileInfoOnly
                                    "--days="+String.valueOf(days),
                                    "--libPathForTest="+getWorkingDir(voltDbRootPath)+"/lib"
                                    });

        rootDir = CoreUtils.getHostnameOrAddress() + "_voltlogs_";
        File collectionFile = new File(voltDbRootPath, rootDir + ".zipfile");
        assertTrue(collectionFile.exists());

        return new ZipFile(collectionFile);
    }

    private int getpid(String voltDbRootPath) throws Exception {
        File configLogDir = new File(voltDbRootPath, Constants.CONFIG_DIR);
        File configInfo = new File(configLogDir, "config.json");

        JSONObject jsonObject = Collector.parseJSONFile(configInfo.getCanonicalPath());
        int pid = jsonObject.getInt("pid");

        return pid;
    }
    private String getWorkingDir(String voltDbRootPath) throws Exception {
        File configLogDir = new File(voltDbRootPath, Constants.CONFIG_DIR);
        File configInfo = new File(configLogDir, "config.json");

        JSONObject jsonObject = Collector.parseJSONFile(configInfo.getCanonicalPath());
        String workingDir = jsonObject.getString("workingDir");

        return workingDir;
    }

    private List<String> getLogPaths(String voltDbRootPath) throws Exception {
        File configLogDir = new File(voltDbRootPath, Constants.CONFIG_DIR);
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

    private void createLogFiles() throws Exception {

        try {
           String configInfoPath = voltDbRootPath + File.separator + Constants.CONFIG_DIR + File.separator + "config.json";;
           JSONObject jsonObject= Collector.parseJSONFile(configInfoPath);
           JSONArray jsonArray = jsonObject.getJSONArray("log4jDst");

           //maintain the file naming format
           String fileNamePrefix = "volt-junit-fulllog.txt.";
           String fileText = "This is a dummy log file.";
           String workingDir = getWorkingDir(voltDbRootPath);
           VoltFile logFolder = new VoltFile(workingDir + "/obj/release/testoutput/");
           logFolder.mkdir();

           for(File oldLogFile : logFolder.listFiles()) {
               if(oldLogFile.getName().startsWith(fileNamePrefix)) {
                   oldLogFile.delete();
               }
           }

           SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
           String[] fileDates = new String[6];
           Calendar cal, cal2;
           cal = Calendar.getInstance();
           cal2 = Calendar.getInstance();
           for(int i=-1; i < 2; i++) {
               cal.add(Calendar.DATE, -i-1);
               fileDates[i+1] = formatter.format(cal.getTime());
           }
           cal = Calendar.getInstance();
           cal.add(Calendar.YEAR, -1);
           cal2.set(cal.get(Calendar.YEAR), 11, 31);
           fileDates[3] = formatter.format(cal2.getTime());
           cal2.add(Calendar.DATE, -4);
           fileDates[4] = formatter.format(cal2.getTime());
           cal2 = Calendar.getInstance();
           cal2.set(cal2.get(Calendar.YEAR), 0, 02);
           fileDates[5] = formatter.format(cal2.getTime());

           for(String fileDate: fileDates) {
               VoltFile file = new VoltFile(logFolder, fileNamePrefix + fileDate);
               file.createNewFile();

               BufferedWriter writer = new BufferedWriter(new FileWriter(file.getAbsolutePath()));
               writer.write(fileText);
               writer.close();

               formatter.format(file.lastModified());
               file.setLastModified(formatter.parse(fileDate).getTime());

               JSONObject object = new JSONObject();
               object.put("path", file.getCanonicalPath());
               object.put("format", "'.'" + fileDate);
               jsonArray.put(object);
           }

           VoltFile repeatFileFolder = new VoltFile(logFolder, "test");
           repeatFileFolder.mkdir();
           VoltFile file = new VoltFile(repeatFileFolder, fileNamePrefix + fileDates[0]);
           file.createNewFile();

           BufferedWriter writer = new BufferedWriter(new FileWriter(file.getAbsolutePath()));
           writer.write(fileText);
           writer.close();

           JSONObject object = new JSONObject();
           object.put("path", file.getCanonicalPath());
           object.put("format", "'.'" + fileDates[0]);
           jsonArray.put(object);

           FileOutputStream fos = new FileOutputStream(configInfoPath);
           fos.write(jsonObject.toString(4).getBytes(Charsets.UTF_8));
           fos.close();
        } catch (JSONException e) {
              System.err.print(e.getMessage());
        } catch (ParseException e) {
              System.err.print(e.getMessage());
        }
    }

    /*
     * For each type of file that need to be collected, check whether it actually appears in the collection
     * currently sar data and /var/log/syslog* are ignored in testing
     * since in some cluster machines sar is not enabled and syslog* can only be read by root
     */

    @Test
    public void testBasicFilesAndCrash() throws Exception {
        //Terrible hack, wait for config logging thread to finish
        Thread.sleep(STARTUP_DELAY);

        try {
            client.callProcedure("CrashVoltDBProc");
        }
        catch (Exception e) {

        }
        client.close();
        cluster.shutDown();

        // generate heap dump
        int pid = getpid(voltDbRootPath);

        File heapdumpGenerated = new File("/tmp", "java_pid" + pid + ".hprof");
        heapdumpGenerated.deleteOnExit();

        PrintWriter writer = new PrintWriter(heapdumpGenerated.getPath());
        writer.println("fake heapdump file");
        writer.close();

        File f = new File(voltDbRootPath, "systemcheck");
        f.createNewFile();
        FileOutputStream fStream = new FileOutputStream(f);
        fStream.write("fake text for test".getBytes());
        fStream.close();

        ZipFile collectionZip = collect(voltDbRootPath, false, 50);

        String subFolderPath = rootDir + File.separator;
        ZipEntry heapdumpFile = collectionZip.getEntry(subFolderPath + "heap_dumps" + File.separator + "java_pid" + pid + ".hprof");
        assertNotNull(heapdumpFile);

        ZipEntry catalogJar = collectionZip.getEntry(subFolderPath + "voltdb_files" + File.separator + "catalog.jar");
        assertNotNull(catalogJar);

        ZipEntry deploymentXml = collectionZip.getEntry(subFolderPath + "voltdb_files" + File.separator + "deployment.xml");
        assertNotNull(deploymentXml);

        ZipEntry systemCheck = collectionZip.getEntry(subFolderPath + "system_logs" + File.separator + "systemcheck");
        assertNotNull(systemCheck);

        List<String> logPaths = getLogPaths(voltDbRootPath);
        for (String path : logPaths) {
            ZipEntry logFile = collectionZip.getEntry(subFolderPath + "voltdb_logs" + File.separator + new File(path).getName());
            assertNotNull(logFile);
        }

        InputStream systemStatsIS;
        if (System.getProperty("os.name").contains("Mac"))
            systemStatsIS = new FileInputStream(getWorkingDir(voltDbRootPath)+"/lib/macstats.properties");
        else
            systemStatsIS = new FileInputStream(getWorkingDir(voltDbRootPath)+"/lib/linuxstats.properties");
        assertNotNull(systemStatsIS);
        Properties systemStats = new Properties();
        systemStats.load(systemStatsIS);
        for (String fileName : systemStats.stringPropertyNames()) {
            ZipEntry statdata = collectionZip.getEntry(subFolderPath + "system_logs" + File.separator + fileName);
            assertNotNull(statdata);
        }

        Enumeration<? extends ZipEntry> e = collectionZip.entries();
        while (e.hasMoreElements()) {
            String pathName = e.nextElement().getName();
            if (pathName.startsWith(subFolderPath + "voltdb_crashfiles")) {
                assertTrue(pathName.startsWith(subFolderPath + "voltdb_crashfiles" + File.separator + "voltdb_crash")
                        && pathName.endsWith(".txt"));
            }
        }
        collectionZip.close();
    }

    @Test
    public void testJvmCrash() throws Exception {
        Thread.sleep(STARTUP_DELAY);
        try {
            client.callProcedure("CrashJVM");
        }
        catch (Exception e) {

        }

        client.close();
        cluster.shutDown();

        ZipFile collectionZip = collect(voltDbRootPath, true, 50);

        int pid = getpid(voltDbRootPath);
        String workingDir = getWorkingDir(voltDbRootPath);
        File jvmCrashGenerated = new File(workingDir, "hs_err_pid" + pid + ".log");
        jvmCrashGenerated.deleteOnExit();
        ZipEntry logFile = collectionZip.getEntry(rootDir + File.separator + "system_logs" + File.separator + "hs_err_pid" + pid + ".log");
        assertNotNull(logFile);
        collectionZip.close();
    }

    @Test
    public void testDaysToCollectOption() throws Exception {

        createLogFiles();

        ZipFile collectionZip = collect(voltDbRootPath, true, 3);
        int logCount = 0;
        Enumeration<? extends ZipEntry> e = collectionZip.entries();
        while (e.hasMoreElements()) {
            ZipEntry z = e.nextElement();
            if (z.getName().startsWith(rootDir + File.separator + "voltdb_logs" + File.separator))
                logCount++;
        }
        assertEquals(logCount, 4);
        collectionZip.close();
    }

    @Test
    public void testCollectFilesonYearBoundary() throws Exception {

        createLogFiles();

        //set reference date to be 1st January of the current year
        Calendar cal = Calendar.getInstance();
        cal.set(cal.get(Calendar.YEAR), 0, 01);
        Collector.m_currentTimeMillis = cal.getTimeInMillis();

        resetCurrentTime = false;
        ZipFile collectionZip = collect(voltDbRootPath, true, 4);
        int logCount = 0;
        Enumeration<? extends ZipEntry> e = collectionZip.entries();
        while (e.hasMoreElements()) {
            if (e.nextElement().getName().startsWith(rootDir + File.separator + "voltdb_logs" + File.separator))
                logCount++;
        }
        assertEquals(logCount, 1);
        resetCurrentTime = true;
        collectionZip.close();
    }

    @Test
    public void testRepeatFileName() throws Exception {

        createLogFiles();

        ZipFile collectionZip = collect(voltDbRootPath, true, 3);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        ZipEntry repeatFile = collectionZip.getEntry(rootDir + File.separator + "voltdb_logs" + File.separator +
                "volt-junit-fulllog.txt." + formatter.format(new Date()) + "(1)");
        assertNotNull(repeatFile);
    }
}
