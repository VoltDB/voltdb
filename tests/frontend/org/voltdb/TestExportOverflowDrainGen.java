/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
package org.voltdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import static junit.framework.TestCase.assertTrue;

import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportTestExpectedData;
import org.voltdb.export.PushSpecificGeneration;
import org.voltdb.export.TestExportBaseSocketExport;
import static org.voltdb.export.TestExportBaseSocketExport.GROUPS;
import static org.voltdb.export.TestExportBaseSocketExport.PROCEDURES;
import static org.voltdb.export.TestExportBaseSocketExport.USERS;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;

/**
 * End to end Export tests using the injected custom export.
 */
public class TestExportOverflowDrainGen extends TestExportBaseSocketExport {

    private static final int k_factor = 0;

    static String exportSection = "    <export>\n"
            + "        <configuration target=\"NO_NULLS\" enabled=\"true\" type=\"custom\" exportconnectorclass=\"org.voltdb.exportclient.SocketExporter\">\n"
            + "            <property name=\"socket.dest\">localhost:5001</property>\n"
            + "            <property name=\"replicated\">false</property>\n"
            + "            <property name=\"timezone\">GMT</property>\n"
            + "            <property name=\"skipinternals\">false</property>\n"
            + "        </configuration>\n"
            + "    </export>\n";

    private static final String DEPLOYMENT = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
            + "<deployment>\n"
            + "    <cluster hostcount=\"1\" sitesperhost=\"8\" kfactor=\"1\" id=\"0\" />\n"
            + exportSection
            + "</deployment>";

    public TestExportOverflowDrainGen(String name) {
        super(name);
    }

    private File findExportOverflowDir(File root) {
        if (!root.isDirectory()) {
            return null;
        }

        File foundFile = null;
        for (File f : root.listFiles()) {
            if (f.getName().contains("export_overflow")) {
                foundFile = f;
            } else {
                foundFile = findExportOverflowDir(f);
            }
            if (foundFile != null) {
                break;
            }
        }

        return foundFile;
    }

    public void testExportOverflowDrainGen() throws Exception {

        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, k_factor + 1);

        System.out.println("testExportOverflowDrainGen");
        Client client = getClient();
        for (int i = 0; i < 100; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        client.drain();

        File overflowDir;
        boolean newCli = ((LocalCluster) m_config).isNewCli();
        if (newCli) {
            overflowDir = new File(((LocalCluster) m_config).getServerSpecificRoot("0") + "/export_overflow");
        } else {
            ArrayList<File> subroots = ((LocalCluster) m_config).getSubRoots();
            overflowDir = findExportOverflowDir(subroots.get(0));
        }
        quiesce(client);
        Thread.sleep(500);
        m_config.shutDown();

        startListener();

        File f = File.createTempFile("deployment", "xml");
        f.deleteOnExit();
        try (OutputStream os = new FileOutputStream(f)) {
            os.write(DEPLOYMENT.getBytes());
            os.flush();
            os.close();
        } catch (Exception ex) {
            fail(ex.toString());
        }
        System.out.println("Deployment file for draingen is: " + f.getCanonicalPath());
        String path = f.getCanonicalFile().getAbsolutePath();
        String args[] = {path, overflowDir.getAbsolutePath()};
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        //Now use draingen pointing to SocketExport
        PushSpecificGeneration.main(args);
        m_verifier.verifyRows();
        System.out.println("Passed!");

    }

    static public junit.framework.Test suite() throws Exception {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.RejectingExportClient");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster config;
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.RejectingExportClient");

        final MultiConfigSuiteBuilder builder
                = new MultiConfigSuiteBuilder(TestExportOverflowDrainGen.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl-with-target.sql"));

        wireupExportTableToRejectingExport("NO_NULLS");
        wireupExportTableToRejectingExport("NO_NULLS_GRP");

        project.addProcedures(PROCEDURES);
        config = new LocalCluster("export-ddl-cluster-rep.jar", 1, 1, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
}
