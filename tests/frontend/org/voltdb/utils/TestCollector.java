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

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.hsqldb_voltpatches.lib.tar.TarReader;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
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

import com.google_voltpatches.common.base.Charsets;

public class TestCollector {
    private static final int STARTUP_DELAY = 3000;
    VoltProjectBuilder builder;
    LocalCluster cluster;
    String listener;
    Client client;

    String voltDbRootPath;
    String prefix = "voltdb_logs";
    boolean resetCurrentTime = true;

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

    private File collect(String voltDbRootPath, boolean skipHeapDump, int days) throws Exception {
        if(resetCurrentTime) {
            Collector.m_currentTimeMillis = System.currentTimeMillis();
        }
        Collector.main(new String[]{"--voltdbroot="+voltDbRootPath, "--prefix="+prefix,
                                    "--host=\"\"", "--username=\"\"", "--password=\"\"", // host, username, password
                                    "--noprompt=true",  // noPrompt
                                    "--dryrun=false", // dryRun
                                    "--skipheapdump="+String.valueOf(skipHeapDump),
                                    "--copyToVEM=true",
                                    "--calledFromVEM=true",  // calledFromVem (set to true so that resulting collection can be easily located)
                                    "--fileInfoOnly=false",  // fileInfoOnly
                                    "--days="+String.valueOf(days)
                                    });

        File collectionTgz = new File(voltDbRootPath, prefix + ".tgz");
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

    private void createLogFiles() throws Exception {

        try {
           String configInfoPath = voltDbRootPath + File.separator + "config_log" + File.separator + "config.json";;
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


        File collectionDecompressed = collect(voltDbRootPath, false, 50);

        String subFolderPath = "voltdb_logs"+ File.separator;
        File heapdumpFile = new File(collectionDecompressed, subFolderPath + "java_pid" + pid + ".hprof");
        assertTrue(heapdumpFile.exists());

        File catalogJar = new File(collectionDecompressed, subFolderPath + "catalog.jar");
        assertTrue(catalogJar.exists());

        File deploymentXml = new File(collectionDecompressed, subFolderPath + "deployment.xml");
        assertTrue(deploymentXml.exists());


        File dmesgdata = new File(collectionDecompressed, subFolderPath + "dmesgdata");
        assertTrue(dmesgdata.exists());

        File logDir = new File(collectionDecompressed, subFolderPath + "log");
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

        File voltdbCrashDir = new File(collectionDecompressed, subFolderPath + "voltdb_crash");
        assertTrue(voltdbCrashDir.exists());
        assertTrue(voltdbCrashDir.listFiles().length > 0);

        for (File file: voltdbCrashDir.listFiles()) {
            assertTrue(file.getName().startsWith("voltdb_crash") && file.getName().endsWith(".txt"));
        }
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

        File collectionDecompressed = collect(voltDbRootPath, true, 50);

        int pid = getpid(voltDbRootPath);
        String workingDir = getWorkingDir(voltDbRootPath);
        File jvmCrashGenerated = new File(workingDir, "hs_err_pid" + pid + ".log");
        jvmCrashGenerated.deleteOnExit();
        String subFolderPath = "voltdb_logs"+ File.separator;
        File jvmCrashFile = new File(collectionDecompressed, subFolderPath + "hs_err_pid" + pid + ".log");
        assertTrue(jvmCrashFile.exists());
    }

    @Test
    public void testDaysToCollectOption() throws Exception {

        createLogFiles();

        File logDir = getLogDir(3);
        assertTrue(logDir.exists());
        assertTrue(logDir.listFiles().length > 0);
        assertEquals(logDir.listFiles().length, 3);
    }

    @Test
    public void testCollectFilesonYearBoundary() throws Exception {

        createLogFiles();

        //set reference date to be 1st January of the current year
        Calendar cal = Calendar.getInstance();
        cal.set(cal.get(Calendar.YEAR), 0, 01);
        Collector.m_currentTimeMillis = cal.getTimeInMillis();

        resetCurrentTime = false;
        File logDir = getLogDir(4);
        assertTrue(logDir.exists());
        assertTrue(logDir.listFiles().length > 0);
        assertEquals(logDir.listFiles().length, 1);
        resetCurrentTime = true;
    }

    private File getLogDir(int daysOfFilesToCollect) throws Exception {
        File collectionDecompressed = collect(voltDbRootPath, true, daysOfFilesToCollect);
        return new File(collectionDecompressed, "voltdb_logs"+ File.separator + "log" + File.separator);
    }
}