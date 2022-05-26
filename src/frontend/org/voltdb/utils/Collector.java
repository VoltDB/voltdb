/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CLIConfig;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.settings.NodeSettings;
import org.voltdb.types.TimestampType;

import com.google_voltpatches.common.base.Throwables;

public class Collector {
    private final static String ZIP_ENTRY_FOLDER_BASE_SUFFIX =  "_volt_collect_logs_";
    final static String PREFIX_DEFAULT_COLLECT_FILE = "voltdb_collect";
    final static String COLLECT_FILE_EXTENSION = ".zip";

    private static String m_configInfoPath = null;
    private static String m_catalogJarPath = null;
    private static String m_deploymentPath = null;
    private static String m_systemCheckPath = null;
    private static String m_pathPropertiesPath = null;
    private static String m_clusterPropertiesPath = null;
    private static CollectConfig m_config;

    public static long m_currentTimeMillis = System.currentTimeMillis();

    private static String m_workingDir = null;
    private static Set<String> m_logPaths = new HashSet<String>();
    private static Properties m_systemStats = new Properties();
    private static File m_voltdbRoot = null;

    private static String m_zipFolderBase = "";
    static String getZipCollectFolderBase() { return m_zipFolderBase; }

    public static String[] cmdFilenames = {"sardata", "dmesgdata", "syscheckdata"};


    static class CollectConfig extends CLIConfig {
        @Option(desc = "file name prefix for uniquely identifying collection")
        String prefix = "";

        @Option(desc = "file name prefix for uniquely identifying collection")
        String outputFile = "";

        @Option(desc = "list the log files without collecting them")
        boolean dryrun = false;

        @Option(desc = "exclude heap dump file from collection")
        boolean skipheapdump = true;

        @Option(desc = "number of days of files to collect (files included are log, crash files), Current day value is 1")
        int days = 7;

        @Option(desc = "the voltdbroot path")
        String voltdbroot = "";

        @Option(desc = "overwrite output file if it exists")
        boolean force= false;

        @Option
        String libPathForTest = "";

        @Override
        public void validate() {
            if (days < 0) exitWithMessageAndUsage("days must be >= 0");
            if (voltdbroot.trim().isEmpty()) exitWithMessageAndUsage("Invalid database directory");
        }
    }

    private static void populateVoltDBCollectionPaths() {
        m_voltdbRoot = new File(m_config.voltdbroot);
        if (!m_voltdbRoot.exists()) {
            System.err.println(m_voltdbRoot.getParentFile().getAbsolutePath() + " does not contain a valid database "
                    + "directory. Specify valid path to the database directory.");
            VoltDB.exit(-1);
        }
        if (!m_voltdbRoot.isDirectory()) {
            System.err.println(m_voltdbRoot.getParentFile().getAbsolutePath() + " is a not directory. Specify valid "
                    + " database directory in --dir option.");
            VoltDB.exit(-1);
        }
        if (!m_voltdbRoot.canRead() || !m_voltdbRoot.canExecute()) {
            System.err.println(m_voltdbRoot.getParentFile().getAbsolutePath() + " does not have read/exceute permission.");
            VoltDB.exit(-1);
        }
        m_config.voltdbroot = m_voltdbRoot.getAbsolutePath();

        // files to collect from config dir
        String configLogDirPath = m_voltdbRoot.getAbsolutePath() + File.separator + Constants.CONFIG_DIR + File.separator;
        m_configInfoPath = configLogDirPath + "config.json";
        m_catalogJarPath = configLogDirPath + "catalog.jar";
        m_deploymentPath = configLogDirPath + "deployment.xml";
        m_pathPropertiesPath = configLogDirPath + "path.properties";
        m_clusterPropertiesPath = configLogDirPath + "cluster.properties";
        m_systemCheckPath =  m_voltdbRoot.getAbsolutePath() + File.separator + "systemcheck";

        // Validate voltdbroot path is valid or not - check if deployment and config info json exists
        File deploymentFile = new File(m_deploymentPath);
        File configInfoFile = new File(m_configInfoPath);

        if (!deploymentFile.exists() || !configInfoFile.exists()) {
            System.err.println("ERROR: Invalid database directory " + m_voltdbRoot.getParentFile().getAbsolutePath()
                               + ". Specify valid database directory using --dir option.");
            VoltDB.exit(-1);
        }

        if (!m_config.prefix.isEmpty()) {
            m_config.outputFile = m_config.prefix + "_" +  PREFIX_DEFAULT_COLLECT_FILE + "_"
                    + CoreUtils.getHostnameOrAddress() + COLLECT_FILE_EXTENSION;
        }
        if (m_config.outputFile.isEmpty()) {
            m_config.outputFile =  PREFIX_DEFAULT_COLLECT_FILE + "_" + CoreUtils.getHostnameOrAddress()
                + COLLECT_FILE_EXTENSION;;
        }

        File outputFile = new File(m_config.outputFile);
        if (outputFile.exists() && !m_config.force) {
            System.err.println("ERROR: Output file " + outputFile.getAbsolutePath() + " already exists."
                    + " Use --force to overwrite an existing file.");
            VoltDB.exit(-1);
        }
    }

    public static void main(String[] args) {
        // get rid of log4j "no appenders could be found for logger" warning when called from VEM
        Logger.getRootLogger().addAppender(new NullAppender());

        m_config = new CollectConfig();
        m_config.parse(Collector.class.getName(), args);
        if (!m_config.outputFile.trim().isEmpty() && !m_config.prefix.trim().isEmpty()) {
            System.err.println("For outputfile, specify either --output or --prefix. Can't specify "
                    + "both of them.");
            m_config.printUsage();
            VoltDB.exit(-1);
        }

        populateVoltDBCollectionPaths();

        JSONObject jsonObject = parseJSONFile(m_configInfoPath);
        parseJSONObject(jsonObject);

        String systemStatsPathBase;
        if (m_config.libPathForTest.isEmpty())
            systemStatsPathBase = System.getenv("VOLTDB_LIB");
        else
            systemStatsPathBase = m_config.libPathForTest;
        String systemStatsPath;
        if (System.getProperty("os.name").contains("Mac"))
            systemStatsPath = systemStatsPathBase + File.separator + "macstats.properties";
        else
            systemStatsPath = systemStatsPathBase + File.separator + "linuxstats.properties";
        try {
            InputStream systemStatsIS = new FileInputStream(systemStatsPath);
            m_systemStats.load(systemStatsIS);
        } catch (IOException e) {
            Throwables.propagate(e);
        }


        Set<String> collectionFilesList = setCollection(m_config.skipheapdump);

        if (m_config.dryrun) {
            System.out.println("List of the files to be collected:");
            for (String path: collectionFilesList) {
                System.out.println("  " + path);
            }
            System.out.println("[dry-run] A tgz file containing above files would be generated in current dir");
            System.out.println("          Use --upload option to enable uploading via SFTP");
        }
        else {
            generateCollection(collectionFilesList);
        }
    }

    public static JSONObject parseJSONFile(String configInfoPath) {
        JSONObject jsonObject = null;

        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(configInfoPath)));

            StringBuilder builder = new StringBuilder();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                builder.append(line);
            }
            bufferedReader.close();

            jsonObject = new JSONObject(builder.toString());
        } catch (FileNotFoundException e) {
            System.err.println("config log file '" + configInfoPath + "' could not be found.");
            VoltDB.exit(-1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            VoltDB.exit(-1);
        } catch (JSONException e) {
            System.err.println("Error with config file: " + configInfoPath);
            System.err.println(e.getLocalizedMessage());
            VoltDB.exit(-1);
        }

        return jsonObject;
    }

    private static void parseJSONObject(JSONObject jsonObject) {
        try {
            m_workingDir = jsonObject.getString("workingDir");

            m_logPaths.clear();
            JSONArray jsonArray = jsonObject.getJSONArray("log4jDst");
            for (int i = 0; i < jsonArray.length(); i++) {
                String path = jsonArray.getJSONObject(i).getString("path");
                m_logPaths.add(path);
            }
        } catch (JSONException e) {
            System.err.println(e.getMessage());
        }
    }

    private static String getLinuxOSInfo() {
        // Supported Linux OS for voltdb are CentOS, Redhat and Ubuntu
        String versionInfo = "";

        BufferedReader br = null;
        // files containing the distribution info
        // Ubuntu - "/etc/lsb-release"
        // Redhat, CentOS - "/etc/redhat-release"
        final List<String> distInfoFilePaths = Arrays.asList("/etc/lsb-release",
                                                            "/etc/redhat-release");
        for (String filePath : distInfoFilePaths) {
            if (Files.exists(Paths.get(filePath))) {
                try {
                    br = new BufferedReader(new FileReader(filePath));
                }
                catch (FileNotFoundException excp) {
                    System.err.println(excp.getMessage());
                }
                break;
            }
        }

        if (br != null) {
            StringBuffer buffer = new StringBuffer();
            try {
                while ((versionInfo = br.readLine()) != null) {
                    buffer.append(versionInfo);
                }
                versionInfo = buffer.toString();
            }
            catch (IOException io) {
                System.err.println(io.getMessage());
                versionInfo = "";
            }
        }
        return versionInfo;
    }

    private static Set<String> setCollection(boolean skipHeapDump) {
        Set<String> collectionFilesList = new HashSet<String>();

        try {
            if (new File(m_deploymentPath).exists()) {
                collectionFilesList.add(m_deploymentPath);
            }
            if (new File(m_catalogJarPath).exists()) {
                collectionFilesList.add(m_catalogJarPath);
            }
            if (new File(m_systemCheckPath).exists()) {
                collectionFilesList.add(m_systemCheckPath);
            }
            if (new File(m_configInfoPath).exists()) {
                collectionFilesList.add(m_configInfoPath);
            }
            if (new File(m_pathPropertiesPath).exists()) {
                collectionFilesList.add(m_pathPropertiesPath);
            }
            if (new File(m_clusterPropertiesPath).exists()) {
                collectionFilesList.add(m_clusterPropertiesPath);
            }

            for (String path: m_logPaths) {
                for (File file: new File(path).getParentFile().listFiles()) {
                    if (file.getName().startsWith(new File(path).getName())
                            && isFileModifiedInCollectionPeriod(file)) {
                       collectionFilesList.add(file.getCanonicalPath());
                    }
                }
            }

            for (File file: new File(m_config.voltdbroot).listFiles()) {
                if (file.getName().startsWith("voltdb_crash") && file.getName().endsWith(".txt")
                        && isFileModifiedInCollectionPeriod(file)) {
                    collectionFilesList.add(file.getCanonicalPath());
                }
                if (file.getName().startsWith("hs_err_pid") && file.getName().endsWith(".log")
                        && isFileModifiedInCollectionPeriod(file)) {
                    collectionFilesList.add(file.getCanonicalPath());
                }
            }

            for (File file: new File(m_workingDir).listFiles()) {
                if (file.getName().startsWith("voltdb_crash") && file.getName().endsWith(".txt")
                        && isFileModifiedInCollectionPeriod(file)) {
                    collectionFilesList.add(file.getCanonicalPath());
                }
                if (file.getName().startsWith("hs_err_pid") && file.getName().endsWith(".log")
                        && isFileModifiedInCollectionPeriod(file)) {
                    collectionFilesList.add(file.getCanonicalPath());
                }
            }

            String systemLogBase;
            final String systemLogBaseDirPath = "/var/log/";
            if (System.getProperty("os.name").startsWith("Mac")) {
                systemLogBase = "system.log";
            } else {
                String versionInfo = getLinuxOSInfo();
                if (versionInfo.contains("Ubuntu")) {
                    systemLogBase = "syslog";
                }
                else {
                    systemLogBase = "messages";
                    if (versionInfo.isEmpty()) {
                        System.err.println("Couldn't find distribution info for supported systems. Perform"
                                + " lookup for system logs in files named: " + systemLogBase);
                    }
                }
            }
            for (File file: new File(systemLogBaseDirPath).listFiles()) {
                if (file.getName().startsWith(systemLogBase)) {
                    collectionFilesList.add(file.getCanonicalPath());
                }
            }

            if (!skipHeapDump) {
                for (File file: new File("/tmp").listFiles()) {
                    if (file.getName().startsWith("java_pid") && file.getName().endsWith(".hprof")
                            && isFileModifiedInCollectionPeriod(file)) {
                        collectionFilesList.add(file.getCanonicalPath());
                    }
                }
            }

            collectionFilesList.add("duvoltdbrootdata (result of executing \"du -h <voltdbroot>\")");
            collectionFilesList.add("dudroverflowdata (result of executing \"du -h <droverflow>\")");
            collectionFilesList.add("duexportoverflowdata (result of executing \"du -h <exportoverflow>\")");
            collectionFilesList.add("ducommandlog (result of executing \"du -h <command_log>\")");
            collectionFilesList.add("ducommandlogsnapshot (result of executing \"du -h <command_log_snapshot>\")");

            for (String fileName : m_systemStats.stringPropertyNames()) {
                collectionFilesList.add(fileName + " (result of executing \"" + m_systemStats.getProperty(fileName) + "\")");
            }

            File varlogDir = new File("/var/log");
            if (varlogDir.canRead()) {
                for (File file: varlogDir.listFiles()) {
                    if ((file.getName().startsWith("syslog") || file.getName().equals("dmesg"))
                            && isFileModifiedInCollectionPeriod(file)) {
                        if (file.canRead()) {
                            collectionFilesList.add(file.getCanonicalPath());
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        return collectionFilesList;
    }

    //checks whether the file is in the duration(days) specified by the user
    //value of diff = 0 indicates current day
    private static boolean isFileModifiedInCollectionPeriod(File file){
        long diff = m_currentTimeMillis - file.lastModified();
        if(diff >= 0) {
            return TimeUnit.MILLISECONDS.toDays(diff)+1 <= m_config.days;
        }
        return false;
    }

    public static void resetCurrentDay() {
        m_currentTimeMillis = System.currentTimeMillis();
    }

    private static void generateCollection(Set<String> paths) {
        try {
            TimestampType ts = new TimestampType(new java.util.Date());
            String timestamp = ts.toString().replace(' ', '-').replace(':', '-');
            // get rid of microsecond part
            timestamp = timestamp.substring(0, "YYYY-mm-DD-HH-MM-ss".length());
            m_zipFolderBase = (m_config.prefix.isEmpty() ? "" : m_config.prefix + "_") +
                    CoreUtils.getHostnameOrAddress() + ZIP_ENTRY_FOLDER_BASE_SUFFIX + timestamp;
            String folderPath = m_zipFolderBase + File.separator;
            String collectionFilePath = m_config.outputFile;

            FileOutputStream collectionStream = new FileOutputStream(collectionFilePath);
            ZipOutputStream zipStream = new ZipOutputStream(collectionStream);

            Map<String, Integer> pathCounter = new HashMap<String, Integer>();
            // Collect files with paths indicated in the list
            for (String path: paths) {
                // Skip particular items corresponding to temporary files that are only generated during collecting
                if (Arrays.asList(cmdFilenames).contains(path.split(" ")[0])) {
                    continue;
                }

                File file = new File(path);
                String filename = file.getName();

                String entryPath = file.getName();
                for (String logPath: m_logPaths) {
                    if (filename.startsWith(new File(logPath).getName())) {
                        entryPath = "voltdb_logs" + File.separator + file.getName();
                        break;
                    }
                }
                if (filename.startsWith("voltdb_crash")) {
                    entryPath = "voltdb_crashfiles" + File.separator + file.getName();
                }
                if (filename.startsWith("syslog") || filename.equals("dmesg") || filename.equals("systemcheck") ||
                        filename.startsWith("hs_err_pid") || path.startsWith("/var/log/")) {
                    entryPath = "system_logs" + File.separator + file.getName();
                }
                if (filename.equals("deployment.xml")
                        || filename.equals("catalog.jar")
                        || filename.equals("config.json")
                        || filename.equals("path.properties")
                        || filename.equals("cluster.properties")) {
                    entryPath = "voltdb_files" + File.separator + file.getName();
                }
                if (filename.endsWith(".hprof")) {
                    entryPath = "heap_dumps" + File.separator + file.getName();
                }

                if (file.isFile() && file.canRead() && file.length() > 0) {
                    String zipPath = folderPath + entryPath;
                    if (pathCounter.containsKey(zipPath)) {
                        Integer pathCount = pathCounter.get(zipPath);
                        pathCounter.put(zipPath, pathCount + 1);
                        zipPath = zipPath.concat("(" + pathCount.toString() + ")");
                    } else {
                        pathCounter.put(zipPath, 1);
                    }

                    ZipEntry zEntry= new ZipEntry(zipPath);
                    zipStream.putNextEntry(zEntry);
                    FileInputStream in = new FileInputStream(path);

                    int len;
                    byte[] buffer = new byte[1024];
                    while ((len = in.read(buffer)) > 0) {
                        zipStream.write(buffer, 0, len);
                    }

                    in.close();
                    zipStream.closeEntry();
                }
            }

            String duCommand = m_systemStats.getProperty("dudata");
            if (duCommand != null) {
                String[] duVoltdbrootCmd = {"bash", "-c", duCommand + " " + m_config.voltdbroot};
                cmd(zipStream, duVoltdbrootCmd, folderPath + "system_logs" + File.separator, "duvoltdbrootdata");

                String drOverflowPath = m_config.voltdbroot + File.separator + "dr_overflow";
                String exportOverflowPath = m_config.voltdbroot + File.separator + "export_overflow";
                String commandLogPath = m_config.voltdbroot + File.separator + "command_log";
                String commandLogSnapshotPath = m_config.voltdbroot + File.separator + "command_log_snapshot";

                if (m_pathPropertiesPath != null && !m_pathPropertiesPath.trim().isEmpty()
                        && (new File(m_pathPropertiesPath)).exists()) {
                    Properties prop = new Properties();
                    InputStream input = null;

                    try {
                        input = new FileInputStream(m_pathPropertiesPath);
                        prop.load(input);
                    } catch (IOException excp) {
                        System.err.println(excp.getMessage());
                    }
                    if (!prop.isEmpty()) {
                        String cmdLogPropPath = prop.getProperty(NodeSettings.CL_PATH_KEY, null);
                        if (cmdLogPropPath != null && !cmdLogPropPath.trim().isEmpty()) {
                            commandLogPath = cmdLogPropPath;
                        }
                        String cmdLogSnapshotPropPath = prop.getProperty(NodeSettings.CL_SNAPSHOT_PATH_KEY, null);
                        if (cmdLogSnapshotPropPath != null && !cmdLogSnapshotPropPath.trim().isEmpty()) {
                            commandLogSnapshotPath = cmdLogSnapshotPropPath;
                        }
                        String drOverflowPropPath = prop.getProperty(NodeSettings.DR_OVERFLOW_PATH_KEY, null);
                        if (drOverflowPropPath != null && !drOverflowPropPath.trim().isEmpty()) {
                            drOverflowPath = drOverflowPropPath;
                        }
                        String exportOverflowPropPath = prop.getProperty(NodeSettings.EXPORT_OVERFLOW_PATH_KEY, null);
                        if (exportOverflowPropPath != null && !exportOverflowPropPath.trim().isEmpty()) {
                            exportOverflowPath = exportOverflowPropPath;
                        }
                    }
                }

                String[] duDrOverflowCmd = {"bash", "-c", duCommand + " " + drOverflowPath};
                cmd(zipStream, duDrOverflowCmd, folderPath + "system_logs" + File.separator, "dudroverflowdata");

                String[] duExportOverflowCmd = {"bash", "-c", duCommand + " " + exportOverflowPath};
                cmd(zipStream, duExportOverflowCmd, folderPath + "system_logs" + File.separator, "duexportoverflowdata");

                String[] duCommadLodCmd = {"bash", "-c", duCommand + " " + commandLogPath};
                cmd(zipStream, duCommadLodCmd, folderPath + "system_logs" + File.separator, "ducommandlog");

                String[] commandLogSnapshotCmd = {"bash", "-c", duCommand + " " + commandLogSnapshotPath};
                cmd(zipStream, commandLogSnapshotCmd, folderPath + "system_logs" + File.separator, "ducommandlogsnapshot");
            }

            for (String fileName : m_systemStats.stringPropertyNames()) {
                String[] statsCmd = {"bash", "-c", m_systemStats.getProperty(fileName)};
                cmd(zipStream, statsCmd, folderPath + "system_logs" + File.separator, fileName);
            }

            zipStream.close();

            File zipFile = new File(collectionFilePath);
            long sizeInByte = zipFile.length();
            String sizeStringInKB = String.format("%5.2f", (double)sizeInByte / 1000);
            if (m_config.outputFile.equals("-")) {
                InputStream input = new BufferedInputStream(new FileInputStream(collectionFilePath));
                byte[] buffer = new byte[8192];
                try {
                    for (int length = 0; (length = input.read(buffer)) != -1;) {
                        System.out.write(buffer, 0, length);
                    }
                } finally {
                    input.close();
                }
                // Delete the collection file in 'stdout' mode.
                zipFile.delete();
            } else {
                System.out.println("\nCollection file created in " + collectionFilePath + "; file size: " + sizeStringInKB + " KB");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    private static void cmd(ZipOutputStream zipStream, String[] command, String folderPath, String resFilename)
            throws IOException, ZipException {
        File tempFile = File.createTempFile(resFilename, null);
        tempFile.deleteOnExit();

        Process p = Runtime.getRuntime().exec(command);
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader ereader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

        String line = null;
        while ((line = reader.readLine()) != null) {
            writer.write(line);
            writer.newLine();
        }
        // If we dont have anything in stdout look in stderr.
        if (tempFile.length() <= 0) {
            if (ereader != null) {
                while ((line = ereader.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                }
            }
        }
        writer.close();

        if (tempFile.length() > 0) {
            ZipEntry zEntry= new ZipEntry(folderPath + resFilename);
            zipStream.putNextEntry(zEntry);
            FileInputStream in = new FileInputStream(tempFile);

            int len;
            byte[] buffer = new byte[1024];
            while ((len = in.read(buffer)) > 0) {
                zipStream.write(buffer, 0, len);
            }

            in.close();
            zipStream.closeEntry();
        }
    }

}
