/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CLIConfig;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.processtools.SFTPSession;
import org.voltdb.processtools.SFTPSession.SFTPException;
import org.voltdb.processtools.SSHTools;
import org.voltdb.types.TimestampType;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.net.HostAndPort;

public class Collector {
    private static String m_configInfoPath = null;
    private static String m_catalogJarPath = null;
    private static String m_deploymentPath = null;
    private static String m_systemCheckPath = null;
    private static CollectConfig m_config;

    public static long m_currentTimeMillis = System.currentTimeMillis();

    private static String m_workingDir = null;
    private static Set<String> m_logPaths = new HashSet<String>();

    public static String[] cmdFilenames = {"sardata", "dmesgdata", "syscheckdata"};

    static class CollectConfig extends CLIConfig {
        @Option(desc = "file name prefix for uniquely identifying collection")
        String prefix = "";

        @Option(desc = "upload resulting collection to HOST via SFTP")
        String host = "";

        @Option(desc = "user name for SFTP upload.")
        String username = "";

        @Option(desc = "password for SFTP upload")
        String password = "";

        @Option(desc = "automatically upload collection (without user prompt)")
        boolean noprompt = false;

        @Option(desc = "list the log files without collecting them")
        boolean dryrun = false;

        @Option(desc = "exclude heap dump file from collection")
        boolean skipheapdump = true;

        @Option(desc = "number of days of files to collect (files included are log, crash files), Current day value is 1")
        int days = 7;

        @Option(desc = "the voltdbroot path")
        String voltdbroot = "";

        @Option
        boolean calledFromVEM = false;

        // generate resulting file in voltdbroot instead of current working dir and do not append timestamp in filename
        // so the resulting file is easier to be located and copied to VEM
        @Option
        boolean copyToVEM=false;

        // generate a list of information (server name, size, and path) of files rather than actually collect files
        // used by files display panel in VEM UI
        @Option(desc = "generate a list of information (server name, size, and path) of files rather than actually collect files")
        boolean fileInfoOnly=false;

        @Override
        public void validate() {
            if (days < 0) exitWithMessageAndUsage("days must be >= 0");
            if (voltdbroot == "") exitWithMessageAndUsage("voltdbroot cannot be null");
        }
    }

    public static void main(String[] args) {
        // get rid of log4j "no appenders could be found for logger" warning when called from VEM
        Logger.getRootLogger().addAppender(new NullAppender());

        m_config = new CollectConfig();
        m_config.parse(Collector.class.getName(), args);

        File voltDbRoot = new File(m_config.voltdbroot);
        if (!voltDbRoot.exists()) {
            System.err.println("voltdbroot path '" + m_config.voltdbroot + "' does not exist.");
            System.exit(-1);
        }

        locatePaths(m_config.voltdbroot);

        JSONObject jsonObject = parseJSONFile(m_configInfoPath);
        parseJSONObject(jsonObject);

        Set<String> collectionFilesList = setCollection(m_config.skipheapdump);

        if (m_config.dryrun) {
            System.out.println("List of the files to be collected:");
            for (String path: collectionFilesList) {
                System.out.println("  " + path);
            }
            System.out.println("[dry-run] A tgz file containing above files would be generated in current dir");
            System.out.println("          Use --upload option to enable uploading via SFTP");
        }
        else if (m_config.fileInfoOnly) {
            String collectionFilesListPath = m_config.voltdbroot + File.separator + m_config.prefix;

            byte jsonBytes[] = null;
            try {
                JSONStringer stringer = new JSONStringer();

                stringer.object();
                stringer.key("server").value(m_config.prefix);
                stringer.key("files").array();
                for (String path: collectionFilesList) {
                    stringer.object();
                    stringer.key("filename").value(path);
                    if (Arrays.asList(cmdFilenames).contains(path.split(" ")[0])) {
                        stringer.key("size").value(0);
                    }
                    else {
                        stringer.key("size").value(new File(path).length());
                    }
                    stringer.endObject();
                }
                stringer.endArray();
                stringer.endObject();

                JSONObject jsObj = new JSONObject(stringer.toString());
                jsonBytes = jsObj.toString(4).getBytes(Charsets.UTF_8);
            } catch (JSONException e) {
                Throwables.propagate(e);
            }

            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(collectionFilesListPath);
                fos.write(jsonBytes);
                fos.getFD().sync();
            } catch (IOException e) {
                Throwables.propagate(e);
            } finally {
                try {
                    fos.close();
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
            }
        }
        else {
            generateCollection(collectionFilesList, m_config.copyToVEM);
        }
    }

    private static void locatePaths(String voltDbRootPath) {
        String configLogDirPath = voltDbRootPath + File.separator + "config_log" + File.separator;

        m_configInfoPath = configLogDirPath + "config.json";
        m_catalogJarPath = configLogDirPath + "catalog.jar";
        m_deploymentPath = configLogDirPath + "deployment.xml";
        m_systemCheckPath = voltDbRootPath + File.separator + "systemcheck";
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
            System.exit(-1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        } catch (JSONException e) {
            System.err.println("Error with config file: " + configInfoPath);
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
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

            if (!skipHeapDump) {
                for (File file: new File("/tmp").listFiles()) {
                    if (file.getName().startsWith("java_pid") && file.getName().endsWith(".hprof")
                            && isFileModifiedInCollectionPeriod(file)) {
                        collectionFilesList.add(file.getCanonicalPath());
                    }
                }
            }

            collectionFilesList.add("sardata (result of executing \"LC_ALL=C; sar -A\" if sar enabled)");
            collectionFilesList.add("dmesgdata (result of executing \"/bin/dmesg\")");
            collectionFilesList.add("iostatdata (result of executing \"iostat -xtz 1 6\")");
            collectionFilesList.add("netstatsdata (result of executing \"netstat -s\")");
            collectionFilesList.add("netstatrndata (result of executing \"netstat -rn\")");
            collectionFilesList.add("psdata (result of executing \"ps -ef\")");
            collectionFilesList.add("lsofdata (result of executing \"lsof\")");
            collectionFilesList.add("datedata (result of executing \"date -u & date\")");
            collectionFilesList.add("dudata (result of executing \"du -h\")");
            collectionFilesList.add("duvoltdbrootdata (result of executing \"du -h <voltdbroot>\")");
            collectionFilesList.add("dudroverflowdata (result of executing \"du -h <droverflow>\")");
            collectionFilesList.add("duexportoverflowdata (result of executing \"du -h <exportoverflow>\")");
            collectionFilesList.add("envdata (result of executing \"env\")");
            collectionFilesList.add("unamedata (result of executing \"uname -a\")");
            collectionFilesList.add("sysctldata (result of executing \"sysctl -a\")");
            collectionFilesList.add("ulimitdata (result of executing \"ulimit -a\")");
            collectionFilesList.add("virtwhatdata (result of executing \"sudo virt-what\")");
            if (System.getProperty("os.name").contains("Mac")) {
                collectionFilesList.add("topdata (result of executing \"top -l 1 -n 20\")");
            } else {
                collectionFilesList.add("vmstatdata (result of executing \"vmstat 1 5\")");
                collectionFilesList.add("freedata (result of executing \"free -m\")");
                collectionFilesList.add("topdata (result of executing \"top -b -n 1 | head -30\")");
                collectionFilesList.add("netstatantplodata (result of executing \"netstat -antplo\")");
                collectionFilesList.add("iptablesdata (result of executing \"sudo iptables -L\")");
                collectionFilesList.add("lsbreleasedata (result of executing \"lsb_release -a\")");
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

    private static void generateCollection(Set<String> paths, boolean copyToVEM) {
        try {
            String timestamp = "";
            String rootpath = "";

            if (copyToVEM) {
                rootpath = m_config.voltdbroot;
            }
            else {
                TimestampType ts = new TimestampType(new java.util.Date());
                timestamp = ts.toString().replace(' ', '-').replace(':', '-');

                // get rid of microsecond part
                timestamp = timestamp.substring(0, "YYYY-mm-DD-HH-MM-ss".length());

                rootpath = System.getProperty("user.dir");
            }

            String folderBase = (m_config.prefix.isEmpty() ? "" : m_config.prefix + "_") +
                                CoreUtils.getHostnameOrAddress() + "_voltlogs_" + timestamp;
            String folderPath = folderBase + File.separator;
            String collectionFilePath = rootpath + File.separator + folderBase + ".zip";

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
                        filename.startsWith("hs_err_pid")) {
                    entryPath = "system_logs" + File.separator + file.getName();
                }
                if (filename.equals("deployment.xml") || filename.equals("catalog.jar") || filename.equals("config.json")) {
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

            String[] sarCmd = {"bash", "-c", "LC_ALL=C; sar -A"};
            cmd(zipStream, sarCmd, folderPath + "system_logs" + File.separator, "sardata");

            String[] dmesgCmd = {"bash", "-c", "/bin/dmesg"};
            cmd(zipStream, dmesgCmd, folderPath + "system_logs" + File.separator, "dmesgdata");

            String[] iostatCmd = {"bash", "-c", "iostat -xtz 1 6"};
            cmd(zipStream, iostatCmd, folderPath + "system_logs" + File.separator, "iostatdata");

            String[] netstatSCmd = {"bash", "-c", "netstat -s"};
            cmd(zipStream, netstatSCmd, folderPath + "system_logs" + File.separator, "netstatsdata");

            String[] netstatRnCmd = {"bash", "-c", "netstat -rn"};
            cmd(zipStream, netstatRnCmd, folderPath + "system_logs" + File.separator, "netstatrndata");

            String[] psCmd = {"bash", "-c", "ps -ef"};
            cmd(zipStream, psCmd, folderPath + "system_logs" + File.separator, "psdata");

            String[] lsofCmd = {"bash", "-c", "lsof"};
            cmd(zipStream, lsofCmd, folderPath + "system_logs" + File.separator, "lsofdata");

            String[] dateCmd = {"bash", "-c", "date -u & date"};
            cmd(zipStream, dateCmd, folderPath + "system_logs" + File.separator, "datedata");

            String[] duCmd = {"bash", "-c", "du -h"};
            cmd(zipStream, duCmd, folderPath + "system_logs" + File.separator, "dudata");

            String[] duVoltdbrootCmd = {"bash", "-c", "du -h " + m_config.voltdbroot};
            cmd(zipStream, duVoltdbrootCmd, folderPath + "system_logs" + File.separator, "duvoltdbrootdata");

            String drOverflowPath = m_config.voltdbroot + File.separator + "dr_overflow";
            String exportOverflowPath = m_config.voltdbroot + File.separator + "export_overflow";
            DeploymentType deployment = CatalogPasswordScrambler.getDeployment(new File(m_deploymentPath));
            PathsType deploymentPaths = deployment.getPaths();
            if (deploymentPaths != null) {
                PathsType.Droverflow drPath = deploymentPaths.getDroverflow();
                if (drPath != null)
                    drOverflowPath = drPath.getPath();
                PathsType.Exportoverflow exportPath = deploymentPaths.getExportoverflow();
                if (exportPath != null)
                    exportOverflowPath = exportPath.getPath();
            }
            String[] duDrOverflowCmd = {"bash", "-c", "du -h " + drOverflowPath};
            cmd(zipStream, duDrOverflowCmd, folderPath + "system_logs" + File.separator, "dudroverflowdata");

            String[] duExportOverflowCmd = {"bash", "-c", "du -h " + exportOverflowPath};
            cmd(zipStream, duExportOverflowCmd, folderPath + "system_logs" + File.separator, "duexportoverflowdata");

            String[] envCmd = {"bash", "-c", "env"};
            cmd(zipStream, envCmd, folderPath + "system_logs" + File.separator, "envdata");

            String[] unameCmd = {"bash", "-c", "uname -a"};
            cmd(zipStream, unameCmd, folderPath + "system_logs" + File.separator, "unamedata");

            String[] sysctlCmd = {"bash", "-c", "sysctl -a"};
            cmd(zipStream, sysctlCmd, folderPath + "system_logs" + File.separator, "sysctldata");

            String[] ulimitCmd = {"bash", "-c", "ulimit -a"};
            cmd(zipStream, ulimitCmd, folderPath + "system_logs" + File.separator, "ulimitdata");

            String[] virtwhatCmd = {"bash", "-c", "sudo virt-what"};
            cmd(zipStream, virtwhatCmd, folderPath + "system_logs" + File.separator, "virtwhatdata");

            if (System.getProperty("os.name").contains("Mac")) {
                String[] topCmd = {"bash", "-c", "top -l 1 -n 20"};
                cmd(zipStream, topCmd, folderPath + "system_logs" + File.separator, "topdata");
            } else {
                String[] vmstatCmd = {"bash", "-c", "vmstat 1 5"};
                cmd(zipStream, vmstatCmd, folderPath + "system_logs" + File.separator, "vmstatdata");

                String[] freeCmd = {"bash", "-c", "free -m"};
                cmd(zipStream, freeCmd, folderPath + "system_logs" + File.separator, "freedata");

                String[] topCmd = {"bash", "-c", "top -b -n 1 | head -30"};
                cmd(zipStream, topCmd, folderPath + "system_logs" + File.separator, "topdata");

                String[] netstatAntploCmd = {"bash", "-c", "netstat -antplo"};
                cmd(zipStream, netstatAntploCmd, folderPath + "system_logs" + File.separator, "netstatantplodata");

                String[] iptablesCmd = {"bash", "-c", "sudo iptables -L"};
                cmd(zipStream, iptablesCmd, folderPath + "system_logs" + File.separator, "iptablesdata");

                String[] lsbreleaseCmd = {"bash", "-c", "lsb_release -a"};
                cmd(zipStream, lsbreleaseCmd, folderPath + "system_logs" + File.separator, "lsbreleasedata");
            }
            zipStream.close();

            long sizeInByte = new File(collectionFilePath).length();
            String sizeStringInKB = String.format("%5.2f", (double)sizeInByte / 1000);
            if (!m_config.calledFromVEM) {
                System.out.println("Collection file created at " + collectionFilePath + " size: " + sizeStringInKB + " KB");
            }

            boolean upload = false;
            if (!m_config.host.isEmpty()) {
                if (m_config.noprompt) {
                    upload = true;
                }
                else {
                    upload = getUserResponse("Upload via SFTP");
                }
            }

            if (upload) {
                if (org.voltdb.utils.MiscUtils.isPro()) {
                    if (m_config.username.isEmpty() && !m_config.noprompt) {
                        System.out.print("username: ");
                        m_config.username = System.console().readLine();
                    }
                    if (m_config.password.isEmpty() && !m_config.noprompt) {
                        System.out.print("password: ");
                        m_config.password = new String(System.console().readPassword());
                    }

                    try {
                        uploadToServer(collectionFilePath, m_config.host, m_config.username, m_config.password);

                        System.out.println("Uploaded " + new File(collectionFilePath).getName() + " via SFTP");

                        boolean delLocalCopy = false;
                        if (m_config.noprompt) {
                            delLocalCopy = true;
                        }
                        else {
                            delLocalCopy = getUserResponse("Delete local copy " + collectionFilePath);
                        }

                        if (delLocalCopy) {
                            try {
                                new File(collectionFilePath).delete();
                                if (!m_config.calledFromVEM) {
                                    System.out.println("Local copy "  + collectionFilePath + " deleted");
                                }
                            } catch (SecurityException e) {
                                System.err.println("Failed to delete local copy " + collectionFilePath + ". " + e.getMessage());
                            }
                        }
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                }
                else {
                    System.out.println("Uploading is only available in the Enterprise Edition");
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    private static boolean getUserResponse(String prompt) {
        while (true) {
            System.out.print(prompt + " [y/n]? ");
            String response = System.console().readLine();

            if (response.isEmpty()) {
                continue;
            }

            switch (response.charAt(0)) {
            case 'Y':
            case 'y':
                return true;
            case 'N':
            case 'n':
                return false;
            default:
                break;
            }
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

    public static boolean uploadToServer(String collectionFilePath, String host, String username, String password) throws Exception {
        attemptConnect(host, username, password);

        SSHTools ssh = new SSHTools(username, null);
        SFTPSession sftp = null;

        HostAndPort hostAndPort = HostAndPort.fromString(host);
        if (hostAndPort.hasPort()) {
            sftp = ssh.getSftpSession(username, password, null, hostAndPort.getHostText(), hostAndPort.getPort(), null);
        }
        else {
            sftp = ssh.getSftpSession(username, password, null, host, null);
        }

        String rootpath = sftp.exec("pwd").trim();

        HashMap<File, File> files = new HashMap<File, File>();
        File src = new File(collectionFilePath);
        File dest = new File(rootpath + File.separator + new File(collectionFilePath).getName());
        files.put(src, dest);

        try {
            sftp.copyOverFiles(files);
        } finally {
            if (sftp != null) {
                sftp.terminate();
            }
        }

        return true;
    }

    public static void attemptConnect(String host, String username, String password) throws Exception {
        SSHTools ssh = new SSHTools(username, null);
        SFTPSession sftp = null;

        try {
            HostAndPort hostAndPort = HostAndPort.fromString(host);
            if (hostAndPort.hasPort()) {
                sftp = ssh.getSftpSession(username, password, null, hostAndPort.getHostText(), hostAndPort.getPort(), null);
            }
            else {
                sftp = ssh.getSftpSession(username, password, null, host, null);
            }
        } catch (SFTPException e) {
            String errorMsg = e.getCause().getMessage();

            /*
             * e.getCause() is JSchException and the java exception class name only appears in message
             * hide java class name and extract error message
             */
            Pattern pattern = Pattern.compile("(java.*Exception: )(.*)");
            Matcher matcher = pattern.matcher(errorMsg);

            if (matcher.matches()) {
                if (errorMsg.startsWith("java.net.UnknownHostException")) {
                    throw new Exception("Unknown host: " + matcher.group(2));
                }
                else {
                    throw new Exception(matcher.group(2));
                }
            }
            else {
                if (errorMsg.equals("Auth cancel") || errorMsg.equals("Auth fail")) {
                    // "Auth cancel" appears when username doesn't exist or password is wrong
                    throw new Exception("Authorization rejected");
                }
                else {
                    throw new Exception(errorMsg.substring(0, 1).toUpperCase() + errorMsg.substring(1));
                }
            }
        } catch (Exception e) {
            String errorMsg = e.getMessage();

            throw new Exception(errorMsg.substring(0, 1).toUpperCase() + errorMsg.substring(1));
        } finally {
            if (sftp != null) {
                sftp.terminate();
            }
        }
    }
}
