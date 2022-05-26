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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.voltcore.utils.CoreUtils;
import org.voltdb.BackendTarget;
import org.voltdb.VoltDB.SimulatedExitException;
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
    private static final String LOG_NAME_PREFIX = "volt-junit-fulllog.txt.";

    @Rule
    public TestName name = new TestName();

    VoltProjectBuilder builder;
    LocalCluster cluster;
    String listener;
    Client client;

    String m_voltDbRootPath;
    // used for specifying output file name to cli. Also used to store
    // the output file path, after collector call, so that file can be deleted
    String m_outputFileName = "";
    String m_prefix = "";
    boolean resetCurrentTime = true;

    String m_collectBaseFolder;
    int m_pid;

    @Before
    public void setUp() throws Exception {
        String simpleSchema =
                "create table blah (" +
                "ival bigint default 0 not null, " +
                "PRIMARY KEY(ival));";

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addProcedure(CrashJVM.class);
        builder.addProcedure(CrashVoltDBProc.class);

        cluster = new LocalCluster("collect.jar",
                2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        cluster.setHasLocalServer(false);
        boolean success = cluster.compile(builder);
        assert (success);
        File voltDbRoot;
        cluster.startUp(true);
        //Get server specific root after startup.
        voltDbRoot = new File(cluster.getServerSpecificRoot("0"));

        verifyConfigFilesPresent(voltDbRoot);

        m_voltDbRootPath = voltDbRoot.getPath();
        listener = cluster.getListenerAddresses().get(0);
        client = ClientFactory.createClient();
        client.createConnection(listener);
        m_outputFileName = "";
        m_pid = getpid(m_voltDbRootPath);
        m_prefix = "";

        System.setProperty("VOLT_JUSTATEST", "true");
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        cluster.shutDown();
        deleteOutputFileIfExists();
    }

    private void verifyConfigFilesPresent(File voltDbRoot) throws Exception {

        //  ENG-12684: The deployment and config files are created in another process, so we have to wait for them to be created.

        String configLogDirPath = voltDbRoot.getAbsolutePath() + File.separator + Constants.CONFIG_DIR + File.separator;
        String deploymentPath = configLogDirPath + "deployment.xml";
        String configInfoPath = configLogDirPath + "config.json";

        File deploymentFile = new File(deploymentPath);
        File configInfoFile = new File(configInfoPath);

        for (int i = 0; i < 6; i++) {
            if (!(deploymentFile.exists() && configInfoFile.exists())) {
                System.err.println("Still looking for files, i=" + i + " m_voltdbRoot=" + voltDbRoot.getParentFile().getAbsolutePath() +
                        " deploymentFile=" + deploymentFile.getAbsolutePath() + " configInfoFile=" + configInfoFile.getAbsolutePath());
                Thread.sleep(2000);
            }
            else {
                break;
            }
        }

        Assert.assertTrue("ERROR: deploymentFile does not exist: " + deploymentFile.getAbsolutePath(), deploymentFile.exists());
        Assert.assertTrue("ERROR: configInfo does not exist: " + configInfoFile.getAbsolutePath(), configInfoFile.exists());
    }

    private ZipFile collect(boolean skipHeapDump, int days, boolean force) throws Exception {
        if(resetCurrentTime) {
            Collector.m_currentTimeMillis = System.currentTimeMillis();
        }

        String pathToOutputFile = "";
        ArrayList<String> cliParams = new ArrayList<>(15);
        cliParams.add("--voltdbroot=" + m_voltDbRootPath);
        if (m_prefix.isEmpty()) {
            cliParams.add("--prefix=\"\"");
        } else {
            cliParams.add("--prefix=" + m_prefix);
            pathToOutputFile = System.getProperty("user.dir") + File.separator + m_prefix
                    + "_" + Collector.PREFIX_DEFAULT_COLLECT_FILE + "_"
                    + CoreUtils.getHostnameOrAddress() + Collector.COLLECT_FILE_EXTENSION;
        }
        cliParams.add("--dryrun=false");        // dryRun
        cliParams.add("--skipheapdump=" + String.valueOf(skipHeapDump));
        cliParams.add("--days=" + String.valueOf(days));
        cliParams.add("--libPathForTest=" + getWorkingDir(m_voltDbRootPath) + "/lib");
        cliParams.add("--force=" + String.valueOf(force));
        if (!m_outputFileName.isEmpty()) {
            cliParams.add("--outputFile=" + m_outputFileName);
            pathToOutputFile = m_outputFileName;
        }

        if (pathToOutputFile.trim().isEmpty()) {
            pathToOutputFile =  Collector.PREFIX_DEFAULT_COLLECT_FILE + "_"
                    + CoreUtils.getHostnameOrAddress() + Collector.COLLECT_FILE_EXTENSION;
        }

        Collector.main(cliParams.toArray(new String[cliParams.size()]));

        m_collectBaseFolder = Collector.getZipCollectFolderBase();
        File collectionFile = new File(pathToOutputFile);
        assertTrue(collectionFile.exists());
        m_outputFileName = pathToOutputFile;

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

    private void createLogFiles(Date now) throws Exception {
        Collector.m_currentTimeMillis = now.getTime();

        try {
            String configInfoPath = m_voltDbRootPath + File.separator + Constants.CONFIG_DIR + File.separator
                    + "config.json";
            JSONObject jsonObject = Collector.parseJSONFile(configInfoPath);
            JSONArray jsonArray = jsonObject.getJSONArray("log4jDst");

            // maintain the file naming format
            String fileText = "This is a dummy log file.";
            String workingDir = getWorkingDir(m_voltDbRootPath);
            File logFolder = new File(workingDir + "/obj/release/testoutput/");
            logFolder.mkdir();

            for (File oldLogFile : logFolder.listFiles()) {
                if (oldLogFile.getName().startsWith(LOG_NAME_PREFIX)) {
                    oldLogFile.delete();
                }
            }

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date[] fileDates = new Date[6];
            Calendar cal;
            cal = Calendar.getInstance();

            // Create one log for today, yesterday and 3 days ago
            cal.setTime(now);
            for (int i = 0; i < 3; i++) {
                cal.add(Calendar.DATE, -i);
                fileDates[i] = cal.getTime();
            }

            // 4 days ago
            cal.add(Calendar.DATE, -1);
            fileDates[3] = cal.getTime();

            // One year ago
            cal.setTime(now);
            cal.add(Calendar.YEAR, -1);
            fileDates[4] = cal.getTime();

            // Tomorrow
            cal.setTime(now);
            cal.add(Calendar.DATE, 1);
            fileDates[5] = cal.getTime();

            for (Date fileDate : fileDates) {
                File file = new File(logFolder, LOG_NAME_PREFIX + formatter.format(fileDate));
                file.createNewFile();

                BufferedWriter writer = new BufferedWriter(new FileWriter(file.getAbsolutePath()));
                writer.write(fileText);
                writer.close();

                file.setLastModified(fileDate.getTime());

                JSONObject object = new JSONObject();
                object.put("path", file.getCanonicalPath());
                object.put("format", "'.'" + fileDate);
                jsonArray.put(object);
            }

            // Create a repeat file for today
            File repeatFileFolder = new File(logFolder, "test");
            repeatFileFolder.mkdir();
            String fileDateFormatted = formatter.format(fileDates[0]);
            File file = new File(repeatFileFolder, LOG_NAME_PREFIX + fileDateFormatted);
            file.createNewFile();

            BufferedWriter writer = new BufferedWriter(new FileWriter(file.getAbsolutePath()));
            writer.write(fileText);
            writer.close();
            file.setLastModified(fileDates[0].getTime());

            JSONObject object = new JSONObject();
            object.put("path", file.getCanonicalPath());
            object.put("format", "'.'" + fileDateFormatted);
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

    private void deleteOutputFileIfExists() {
        File outputFile = new File(m_outputFileName);
        if (outputFile.exists()) {
            assertTrue(outputFile.delete());
        }
    }

    private void verifyBasicTestCollect(ZipFile collectionZip) throws Exception {
        String subFolderPath = m_collectBaseFolder + File.separator;
        ZipEntry heapdumpFile = collectionZip.getEntry(subFolderPath + "heap_dumps" + File.separator + "java_pid" + m_pid + ".hprof");
        assertNotNull(heapdumpFile);

        ZipEntry catalogJar = collectionZip.getEntry(subFolderPath + "voltdb_files" + File.separator + "catalog.jar");
        assertNotNull(catalogJar);

        ZipEntry deploymentXml = collectionZip.getEntry(subFolderPath + "voltdb_files" + File.separator + "deployment.xml");
        assertNotNull(deploymentXml);

        ZipEntry systemCheck = collectionZip.getEntry(subFolderPath + "system_logs" + File.separator + "systemcheck");
        assertNotNull(systemCheck);

        List<String> logPaths = getLogPaths(m_voltDbRootPath);
        for (String path : logPaths) {
            ZipEntry logFile = collectionZip.getEntry(subFolderPath + "voltdb_logs" + File.separator + new File(path).getName());
            assertNotNull(logFile);
        }

        InputStream systemStatsIS;
        if (System.getProperty("os.name").contains("Mac")) {
            systemStatsIS = new FileInputStream(getWorkingDir(m_voltDbRootPath)+"/lib/macstats.properties");
        } else {
            systemStatsIS = new FileInputStream(getWorkingDir(m_voltDbRootPath)+"/lib/linuxstats.properties");
        }
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
        File heapdumpGenerated = new File("/tmp", "java_pid" + m_pid + ".hprof");

        PrintWriter writer = new PrintWriter(heapdumpGenerated.getPath());
        heapdumpGenerated.deleteOnExit();
        writer.println("fake heapdump file");
        writer.close();

        File f = new File(m_voltDbRootPath, "systemcheck");
        f.createNewFile();
        FileOutputStream fStream = new FileOutputStream(f);
        fStream.write("fake text for test".getBytes());
        fStream.close();

        ZipFile collectionZip;
        m_outputFileName = new File(m_voltDbRootPath).getParent() + File.separator + m_pid + "_withCrash.zip";
        deleteOutputFileIfExists();
        collectionZip = collect(false, 50, false);
        verifyBasicTestCollect(collectionZip);
        collectionZip.close();
        deleteOutputFileIfExists();


        // negative test - prefix and output set at same time
        m_prefix = "foo_" + m_pid;
        boolean caughtExcp = false;
        try {
            collect(true, 3, false);
        } catch (SimulatedExitException excp) {
            System.out.println(excp.getMessage());
            caughtExcp = true;
        }
        assertTrue(caughtExcp);


        m_outputFileName = "";
        m_prefix = "prefix" + m_pid;
        collectionZip = collect(false, 3, true);
        verifyBasicTestCollect(collectionZip);
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

        m_outputFileName = new File(m_voltDbRootPath).getParent() + File.separator + m_pid + "_withJvmCrash.zip";
        deleteOutputFileIfExists();
        ZipFile collectionZip = collect(true, 50, false);

        String workingDir = getWorkingDir(m_voltDbRootPath);
        File jvmCrashGenerated = new File(workingDir, "hs_err_pid" + m_pid + ".log");
        jvmCrashGenerated.deleteOnExit();
        ZipEntry logFile = collectionZip.getEntry(m_collectBaseFolder + File.separator + "system_logs" + File.separator + "hs_err_pid" + m_pid + ".log");
        assertNotNull(logFile);
        collectionZip.close();
    }

    @Test
    public void testDaysToCollectOption() throws Exception {
        testCollectLogFiles(new Date());
    }

    @Test
    public void testCollectFilesonYearBoundary() throws Exception {
        //set reference date to be 1st January of the current year
        Calendar cal = Calendar.getInstance();
        cal.set(cal.get(Calendar.YEAR), 0, 01);

        testCollectLogFiles(cal.getTime());
    }

    @Test
    public void testRepeatFileName() throws Exception {

        createLogFiles(new Date());
        m_outputFileName = new File(m_voltDbRootPath).getParent() + File.separator + m_pid + "_withRepeatedFileName.zip";
        deleteOutputFileIfExists();

        try (ZipFile collectionZip = collect(true, 3, false)) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            ZipEntry repeatFile = collectionZip.getEntry(m_collectBaseFolder + File.separator + "voltdb_logs"
                    + File.separator + "volt-junit-fulllog.txt." + formatter.format(new Date()) + "(1)");
            assertNotNull(repeatFile);
        }
    }

    private void testCollectLogFiles(Date date) throws Exception {
        createLogFiles(date);

        resetCurrentTime = false;
        m_outputFileName = new File(m_voltDbRootPath).getParent() + File.separator + m_pid + '_' + name.getMethodName()
                + ".zip";
        try (ZipFile collectionZip = collect(true, 4, false)) {
            assertEquals(4, countLogEntries(collectionZip));
        } finally {
            resetCurrentTime = true;
        }
    }

    private int countLogEntries(ZipFile zipFile) {
        String basePath = m_collectBaseFolder + File.separator + "voltdb_logs" + File.separator + LOG_NAME_PREFIX;
        int logCount = 0;
        Enumeration<? extends ZipEntry> e = zipFile.entries();
        while (e.hasMoreElements()) {
            if (e.nextElement().getName().startsWith(basePath)) {
                logCount++;
            }
        }
        return logCount;
    }
}
