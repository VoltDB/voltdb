/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import org.hsqldb_voltpatches.lib.tar.TarFileOutputStream;
import org.hsqldb_voltpatches.lib.tar.TarGenerator;
import org.hsqldb_voltpatches.lib.tar.TarMalformatException;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.types.TimestampType;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class Collector {
    private static String m_voltDbRootPath = null;
    private static String m_configInfoPath = null;
    private static String m_catalogJarPath = null;
    private static String m_outputTgzPath = null;

    private static String m_nonce = "";
    private static boolean m_skipheapdump = false;
    private static boolean m_upload = false;

    private static int m_pid = 0;
    private static String m_workingDir = null;
    private static String m_deployment = null;
    private static ArrayList<String> m_logPaths = new ArrayList<String>();

    private static final VoltLogger m_log = new VoltLogger("CONSOLE");

    public static void main(String[] args) {
        m_voltDbRootPath = args[0];
        m_nonce = args[1];
        m_skipheapdump = Boolean.parseBoolean(args[2]);
        m_upload = Boolean.parseBoolean(args[3]);

        File voltDbRoot = new File(m_voltDbRootPath);
        if (!voltDbRoot.exists()) {
            m_log.error("voltdbroot path '" + m_voltDbRootPath + "' does not exist.");
            System.exit(-1);
        }

        TimestampType ts = new TimestampType(new java.util.Date());
        m_configInfoPath = m_voltDbRootPath + File.separator + "config_log" + File.separator + "config.json";
        m_catalogJarPath = m_voltDbRootPath + File.separator + "config_log" + File.separator + "catalog.jar";
        m_outputTgzPath = m_voltDbRootPath + File.separator + m_nonce + ts.toString().replace(' ', '-') + ".tgz";

        JSONObject jsonObject = parseJSONFile(m_configInfoPath);

        parseJSONObject(jsonObject);

        generateTgzFile(m_skipheapdump, m_upload);
    }

    private static JSONObject parseJSONFile(String configInfoPath) {
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
            m_log.error("config log file '" + configInfoPath + "' could not be found.");
            System.exit(-1);
        } catch (IOException e) {
            m_log.error(e.getMessage());
        } catch (JSONException e) {
            m_log.error(e.getMessage());
        }

        return jsonObject;
    }

    private static void parseJSONObject(JSONObject jsonObject) {
        try {
            m_pid = jsonObject.getInt("pid");
            m_workingDir = jsonObject.getString("workingDir");
            m_deployment = jsonObject.getString("deployment");
            JSONArray jsonArray = jsonObject.getJSONArray("log4jDst");
            for (int i = 0; i < jsonArray.length(); i++) {
                String path = jsonArray.getJSONObject(i).getString("path");
                m_logPaths.add(path);
            }
        } catch (JSONException e) {
            m_log.error(e.getMessage());
        }
    }

    private static void generateTgzFile(boolean skipheapdump, boolean upload) {
        try {
            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            GZIPOutputStream outputStream = new GZIPOutputStream(byteOutputStream,
                    TarFileOutputStream.Compression.DEFAULT_BLOCKS_PER_RECORD * 512);
            TarGenerator tarGenerator = new TarGenerator(outputStream);

            File deploymentXmlFile = new File(m_deployment);
            tarGenerator.queueEntry(deploymentXmlFile.getName(), deploymentXmlFile);

            File catalogJar = new File(m_catalogJarPath);
            tarGenerator.queueEntry(catalogJar.getName(), catalogJar);

            for (String path: m_logPaths) {
                for (File file: new File(path).getParentFile().listFiles()) {
                    if (file.getName().startsWith(new File(path).getName())) {
                        tarGenerator.queueEntry("log" + File.separator + file.getName(), file);
                    }
                }
            }

            for (File file: new File(m_voltDbRootPath).listFiles()) {
                if (file.getName().startsWith("voltdb_crash") && file.getName().endsWith(".txt")) {
                    tarGenerator.queueEntry("voltdb_crash" + File.separator + file.getName(), file);
                }
            }

            for (File file: new File(m_workingDir).listFiles()) {
                if (file.getName().startsWith("voltdb_crash") && file.getName().endsWith(".txt")) {
                    tarGenerator.queueEntry("voltdb_crash" + File.separator + file.getName(), file);
                }
                if (file.getName().equals("hs_err_pid" + m_pid + ".log")) {
                    tarGenerator.queueEntry(file.getName(), file);
                }
            }

            if (!skipheapdump) {
                for (File file: new File("/tmp").listFiles()) {
                    if (file.getName().equals("java_pid" + m_pid + ".hprof")) {
                        tarGenerator.queueEntry(file.getName(), file);
                    }
                }
            }

            File tempSarFile = File.createTempFile("sar", null);
            tempSarFile.deleteOnExit();
            String[] cmd = {"bash", "-c", "sar -A"};
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempSarFile));
            String line = null;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
            }
            writer.flush();
            writer.close();
            tarGenerator.queueEntry("sardata", tempSarFile);

            tarGenerator.write(true);

            if (!upload) {
                boolean loop = true;
                while (loop) {
                    System.out.print("Upload collection to VoltDB server [y/n]? ");
                    switch (System.console().readLine().charAt(0)) {
                    case 'Y':
                    case 'y':
                        upload = true;
                        loop = false;
                        break;
                    case 'N':
                    case 'n':
                        loop = false;
                        break;
                    default:
                        break;
                    }
                }
            }

            if (upload) {
                if (org.voltdb.utils.MiscUtils.isPro()) {
                    InputStream inputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
                    uploadToServer(inputStream);
                    m_log.info("Uploaded " + new File(m_outputTgzPath).getName() + " to VoltDB server");
                }
                else {
                    m_log.info("Uploading is only available in the Enterprise Edition");
                    upload = false;
                }
            }
            if (!upload){
                FileOutputStream fileOutputStream = new FileOutputStream(m_outputTgzPath);
                fileOutputStream.write(byteOutputStream.toByteArray());
                fileOutputStream.close();
                m_log.info("Created collection file at " + m_outputTgzPath);
            }
        } catch (IOException e) {
            m_log.error(e.getMessage());
        } catch (TarMalformatException e) {
            m_log.error(e.getMessage());
        }
    }

    private static void uploadToServer(InputStream inputStream) {
        System.out.print("username: ");
        String username = System.console().readLine();
        System.out.print("host: ");
        String host = System.console().readLine();
        System.out.print("password: ");
        String password = new String(System.console().readPassword());

        JSch jsch = null;
        Session session = null;
        Channel channel = null;
        ChannelSftp channelSftp = null;
        try {
            jsch = new JSch();
            session = jsch.getSession(username, host, 22);
            session.setPassword(password);
            JSch.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            channel = session.openChannel("sftp");
            channel.connect();
            channelSftp = (ChannelSftp) channel;
        } catch (Exception e) {
            m_log.error(e.getMessage());
        }

        try {
            channelSftp.put(inputStream, new File(m_outputTgzPath).getName());
        } catch (Exception e) {
            m_log.error(e.getMessage());
        }

        channelSftp.disconnect();
        session.disconnect();
    }
}
