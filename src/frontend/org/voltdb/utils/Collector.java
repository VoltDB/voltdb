/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;
import org.hsqldb_voltpatches.lib.tar.TarGenerator;
import org.hsqldb_voltpatches.lib.tar.TarMalformatException;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.CLIConfig;
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
    private static CollectConfig m_config;

    public static long m_currentTimeMillis = System.currentTimeMillis();

    private static String m_workingDir = null;
    private static List<String> m_logPaths = new ArrayList<String>();

    public static String[] cmdFilenames = {"sardata", "dmesgdata"};

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
        boolean skipheapdump = false;

        @Option(desc = "number of days of files to collect (files included are log, crash files), Current day value is 1")
        int days = 14;

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

        List<String> collectionFilesList = listCollection(m_config.skipheapdump);

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
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (JSONException e) {
            System.err.println(e.getMessage());
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

    private static List<String> listCollection(boolean skipHeapDump) {
        List<String> collectionFilesList = new ArrayList<String>();

        try {
            if (new File(m_deploymentPath).exists()) {
                collectionFilesList.add(m_deploymentPath);
            }
            if (new File(m_catalogJarPath).exists()) {
                collectionFilesList.add(m_catalogJarPath);
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

            collectionFilesList.add("sardata (result of executing \"sar -A\" if sar enabled)");
            collectionFilesList.add("dmesgdata (result of executing \"/bin/dmesg\")");

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

    private static void generateCollection(List<String> paths, boolean copyToVEM) {
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

            String collectionFilePath = rootpath + File.separator + m_config.prefix + timestamp + ".tgz";
            File collectionFile = new File(collectionFilePath);
            TarGenerator tarGenerator = new TarGenerator(collectionFile, true, null);
            String folderPath= m_config.prefix + timestamp + File.separator;

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
                        entryPath = "log" + File.separator + file.getName();
                        break;
                    }
                }
                if (filename.startsWith("voltdb_crash")) {
                    entryPath = "voltdb_crash" + File.separator + file.getName();
                }
                if (filename.startsWith("syslog") || filename.equals("dmesg")) {
                    entryPath = "syslog" + File.separator + file.getName();
                }

                if (file.isFile() && file.canRead() && file.length() > 0) {
                    tarGenerator.queueEntry(folderPath + entryPath, file);
                }
            }

            String[] sarCmd = {"bash", "-c", "sar -A"};
            cmd(tarGenerator, sarCmd, folderPath , "sardata");

            String[] dmesgCmd = {"bash", "-c", "/bin/dmesg"};
            cmd(tarGenerator, dmesgCmd, folderPath, "dmesgdata");

            tarGenerator.write(m_config.calledFromVEM ? null : System.out);

            long sizeInByte = collectionFile.length();
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
                                collectionFile.delete();
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

    private static void cmd(TarGenerator tarGenerator, String[] command, String folderPathInTar, String resFilename)
            throws IOException, TarMalformatException {
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
            tarGenerator.queueEntry(folderPathInTar + resFilename, tempFile);
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
