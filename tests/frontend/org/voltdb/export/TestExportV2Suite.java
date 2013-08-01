/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
package org.voltdb.export;

import java.io.File;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;

import org.voltdb.BackendTarget;
import org.voltdb.LegacyHashinator;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

public class TestExportV2Suite extends TestExportBase {

    ExportToFileVerifier m_verifier;


    private void quiesceAndVerify(final Client client, ExportToFileVerifier tester)
            throws Exception
            {
                quiesce(client);
                Thread.sleep(2000);
                tester.verifyRows();
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

        m_verifier = new ExportToFileVerifier(
                getServerConfig().getPathInSubroots(
                        new File("/tmp/" + System.getProperty("user.name"))),
                "zorag");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Multi-table test
     */
    public void testExportMultiTable() throws Exception
    {
        System.out.println("testExportMultiTable");
        final Client client = getClient();

        for (int i=0; i < 100; i++) {
            // add data to a first (persistent) table
            Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(
                    "ALLOW_NULLS", i, convertValsToRow(i, 'I', rowdata));
            Object[] params = convertValsToParams("ALLOW_NULLS", i, rowdata);
            client.callProcedure("Insert", params);

            // add data to a second (streaming) table.
            rowdata = TestSQLTypesSuite.m_defaultValues;
            m_verifier.addRow(
                    "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            params = convertValsToParams("NO_NULLS", i, rowdata);
            client.callProcedure("Insert", params);
        }
        quiesceAndVerify(client, m_verifier);
    }

    public TestExportV2Suite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        TheHashinator.initialize(LegacyHashinator.class, LegacyHashinator.getConfigureBytes(3));
        LocalCluster config;

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportV2Suite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setSecurityEnabled(true);
        project.addGroups(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl.sql"));

        Properties props = new Properties();
        props.putAll(ImmutableMap.<String, String>of(
                "type","csv",
                "batched","false",
                "with-schema","true",
                "nonce","zorag",
                "outdir","/tmp/" + System.getProperty("user.name")
                ));
        project.addOnServerExport(true, java.util.Arrays.asList(new String[]{"export"}), "file", props);
        // "WITH_DEFAULTS" is a non-exported persistent table
        project.setTableAsExportOnly("ALLOW_NULLS");
        project.setTableAsExportOnly("NO_NULLS");
        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
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
        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, true, null);
        boolean compile = config.compile(project);
        assertTrue(compile);
        config.setHasLocalServer(false);
        config.setMaxHeap(256);
        builder.addServerConfig(config);


        /*
         * compile a catalog without the NO_NULLS table for add/drop tests
         */
        config = new LocalCluster("export-ddl-sans-nonulls.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI,  LocalCluster.FailureState.ALL_RUNNING, true);
        config.setMaxHeap(256);
        project = new VoltProjectBuilder();
        project.addGroups(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl.sql"));
        project.addOnServerExport(true, java.util.Arrays.asList(new String[]{"export"}), "file", props);
        // "WITH_DEFAULTS" is a non-exported persistent table
        project.setTableAsExportOnly("ALLOW_NULLS");

        // and then project builder as normal
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addProcedures(PROCEDURES2);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml"));
        assertTrue(compile);

        /*
         * compile a catalog with an added table for add/drop tests
         */
        config = new LocalCluster("export-ddl-addedtable.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI,  LocalCluster.FailureState.ALL_RUNNING, true);
        config.setMaxHeap(256);
        project = new VoltProjectBuilder();
        project.addGroups(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-addedtable-export-ddl.sql"));
        project.addOnServerExport(true, java.util.Arrays.asList(new String[]{"export"}), "file", props);
        // "WITH_DEFAULTS" is a non-exported persistent table
        project.setTableAsExportOnly("ALLOW_NULLS");   // persistent table
        project.setTableAsExportOnly("ADDED_TABLE");   // persistent table
        project.setTableAsExportOnly("NO_NULLS");      // streamed table

        // and then project builder as normal
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("ADDED_TABLE", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addProcedures(PROCEDURES);
        project.addProcedures(PROCEDURES3);
        compile = config.compile(project);
        MiscUtils.copyFile(project.getPathToDeployment(),
                Configuration.getPathToCatalogForTest("export-ddl-addedtable.xml"));
        assertTrue(compile);


        return builder;
    }
}
