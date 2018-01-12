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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportTestClient;
import org.voltdb.export.ExportTestVerifier;
import org.voltdb.export.TestExportBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.regressionsuites.VoltServerConfig;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportGroups extends TestExportBase {

    private void exportVerify(final Client client)
            throws Exception
    {
        quiesce(client);
        assertTrue(ExportTestClient.allRowsVerified());
    }

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();

    }

    @Override
    public void tearDown() throws Exception {
        ExportTestVerifier.m_closed = true;
        super.tearDown();
        ExportTestClient.clear();
    }

    //  Test Export of a group update.  Export some data to once source.
    //  Then update the catalog to a group that will export to a different source.
    //
    public void testUpdateExportGroup() throws Exception {
        System.out.println("testUpdateExportGroup");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        for (int i = 0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            ExportTestClient.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(client);

        String newCatalogURL = Configuration.getPathToCatalogForTest("export-ddl-diff-grp.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("export-ddl-diff-grp.xml");
        ClientResponse callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                                                                       new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        for (int i = 0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            ExportTestClient.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(client);
    }

    //  Test Export of a group update.  Export some data to once source.
    //  Then update the catalog to a group that will export to a different source.
    //
    public void testUpdateFromNonexistantGroup() throws Exception {
        System.out.println("testUpdateFromNonexistantGroup");
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        String newCatalogURL = Configuration.getPathToCatalogForTest("export-ddl-nonexist-grp.jar");
        String deploymentURL = Configuration.getPathToCatalogForTest("export-ddl-nonexist-grp.xml");
        ClientResponse callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                                                                       new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        exportVerify(client);

        newCatalogURL = Configuration.getPathToCatalogForTest("export-ddl-diff-grp.jar");
        deploymentURL = Configuration.getPathToCatalogForTest("export-ddl-diff-grp.xml");
        callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                                                        new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        for (int i = 0; i < 100; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            ExportTestClient.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(client);

        callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                                                        new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        for (int i = 100; i < 200; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            ExportTestClient.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(client);

        callProcedure = client.updateApplicationCatalog(new File(newCatalogURL),
                                                        new File(deploymentURL));
        assertTrue(callProcedure.getStatus() == ClientResponse.SUCCESS);
        for (int i = 200; i < 300; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            ExportTestClient.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(client);

    }

    public TestExportGroups(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        VoltServerConfig config;
        Map<String, String> additionalEnv = new HashMap<>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportGroups.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl-with-target.sql"));

        Properties props = new Properties();
        props.putAll(ImmutableMap.<String, String>of(
                "type", "csv",
                "batched", "false",
                "with-schema", "true",
                "nonce", "zorag1",
                "outdir", "/tmp/" + System.getProperty("user.name")));
        project.addExport(true /* enabled */, "custom", props, "NO_NULLS");

        // Foo tables for testing export groups
        Properties propsGrp = new Properties();
        propsGrp.putAll(ImmutableMap.<String, String>of(
                "type", "csv",
                "batched", "false",
                "with-schema", "true",
                "nonce", "grpnonce",
                "outdir", "/tmp/" + System.getProperty("user.name")));
        project.addExport(true /* enabled */, "file", propsGrp, "grp");
        project.addProcedures(PROCEDURES);

        // JNI, single server
        // Use the cluster only config. Multiple topologies with the extra catalog for the
        // Add drop tests is harder. Restrict to the single (complex) topology.
        //
        //        config = new LocalSingleProcessServer("export-ddl.jar", 2,
        //                                              BackendTarget.NATIVE_EE_JNI);
        //        config.compile(project);
        //        builder.addServerConfig(config);


        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("export-ddl.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setMaxHeap(1024);
        //ExportToFile needs diff paths which VoltFile magic provides so need to run in old mode.
        ((LocalCluster )config).setNewCli(false);
        boolean compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl.xml"));
        assertTrue(compile);
        builder.addServerConfig(config, false);


        /*
         * compile a catalog with the export table in a different target
         */
        config = new LocalCluster("export-ddl-diff-grp.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        //ExportToFile needs diff paths which VoltFile magic provides so need to run in old mode.
        ((LocalCluster )config).setNewCli(false);
        config.setMaxHeap(1024);
        project = new VoltProjectBuilder();
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl-with-target.sql"));
        project.addExport(true /* enabled */, "custom", props, "NO_NULLS");
        project.addExport(true /* enabled */, "file", propsGrp, "grp");
        project.addProcedures(PROCEDURES);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-diff-grp.xml"));
        assertTrue(compile);

        /*
         * compile a catalog with the export table in a different group
         */
        config = new LocalCluster("export-ddl-nonexist-grp.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        //ExportToFile needs diff paths which VoltFile magic provides so need to run in old mode.
        ((LocalCluster )config).setNewCli(false);
        config.setMaxHeap(1024);
        project = new VoltProjectBuilder();
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl-with-target.sql"));
        project.addExport(true /* enabled */, "custom", props, "NO_NULLS");
        project.addExport(true /* enabled */, "file", propsGrp, "grp");
        project.addProcedures(PROCEDURES);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-nonexist-grp.xml"));
        assertTrue(compile);

        return builder;
    }
}
