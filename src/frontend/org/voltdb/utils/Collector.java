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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.hsqldb_voltpatches.lib.tar.TarGenerator;
import org.hsqldb_voltpatches.lib.tar.TarMalformatException;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.types.TimestampType;

public class Collector {
    private static String m_voltDbRootPath = null;
    private static String m_configInfoPath = null;
    private static String m_catalogJarPath = null;
    private static String m_outputTgzPath = null;
    private static String m_nonce = "";

    private static int m_pid = 0;
    private static String m_workingDir = null;
    private static String m_deployment = null;
    private static ArrayList<String> m_logPaths = new ArrayList<String>();

    private static final VoltLogger m_log = new VoltLogger("CONSOLE");

    static final String usage = "collector <voltdbroot-path> [<nonce>]";

    public static void main(String[] args) {
        if (args.length == 1) {
            m_voltDbRootPath = args[0];
        } else if (args.length == 2) {
            m_voltDbRootPath = args[0];
            m_nonce = args[1];
        } else {
            System.err.printf("Usage: %s\n", usage);
            System.exit(-1);
        }

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

        generateTgzFile();
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

    private static void generateTgzFile() {
        try {
            TarGenerator tarGenerator = new TarGenerator(new File(m_outputTgzPath), true, null);

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

            for (File file: new File("/tmp").listFiles()) {
                if (file.getName().equals("java_pid" + m_pid + ".hprof")) {
                    tarGenerator.queueEntry(file.getName(), file);
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

            tarGenerator.write();
        } catch (IOException e) {
            m_log.error(e.getMessage());
        } catch (TarMalformatException e) {
            m_log.error(e.getMessage());
        }
    }
}