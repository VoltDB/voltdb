/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportTestExpectedData;
import org.voltdb.export.TestExportBaseSocketExport;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.utils.VoltFile;

/**
 * End to end Export tests using the injected custom export.
 *
 *  Note, this test reuses the TestSQLTypesSuite schema and procedures.
 *  Each table in that schema, to the extent the DDL is supported by the
 *  DB, really needs an Export round trip test.
 */

public class TestExportRejoin extends TestExportBaseSocketExport {
    private static final int k_factor = 1;

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();

        startListener();
        m_verifier = new ExportTestExpectedData(m_serverSockets, m_isExportReplicated, true, k_factor+1);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        closeClientAndServer();
    }

    List<File> collectFiles(File dirPath, String extensionWithDot) {
        File files[] = dirPath.listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(extensionWithDot);
            }
        });
        return Arrays.asList(files);
    }

    Map<File, Long> getFilesModifiedTimesInDir(List<File> files) {
        Map<File, Long> modifiedTimes = new HashMap<>(files.size());
        for (File f : files) {
            modifiedTimes.put(f,  new Long(f.lastModified()));
        }
        return modifiedTimes;
    }

    Map<File, Long> getFilesModifiedTimesInDir(File dirPath, String extensionWithDot) {
        List<File> files = collectFiles(dirPath, extensionWithDot);
        return getFilesModifiedTimesInDir(files);
    }

    public void testExportAndThenRejoinUpdatesExportFlow() throws Exception {
        System.out.println("testExportAndThenRejoinClearsExportOverflow");
        Client client = getClient();
        for (int i = 0; i < 10; i++) {
            final Object[] rowdata = TestSQLTypesSuite.m_midValues;
            m_verifier.addRow(client, "NO_NULLS", i, convertValsToRow(i, 'I', rowdata));
            m_verifier.addRow(client, "NO_NULLS_GRP", i, convertValsToRow(i, 'I', rowdata));
            final Object[] params = convertValsToParams("NO_NULLS", i, rowdata);
            final Object[] paramsGrp = convertValsToParams("NO_NULLS_GRP", i, rowdata);
            client.callProcedure("Insert", params);
            client.callProcedure("Insert", paramsGrp);
        }
        client.drain();

        ((LocalCluster) m_config).killSingleHost(1);
        Thread.sleep(500);
        File exportOverflowDir;
        if (((LocalCluster )m_config).isNewCli()) {
            exportOverflowDir = new File(((LocalCluster )m_config).getServerSpecificRoot("1") + "/export_overflow");
        } else {
              exportOverflowDir  = new File(((LocalCluster) m_config).getSubRoots().get(1),
                        "/tmp/" + System.getProperty("user.name") + "/export_overflow");
        }

        Map<File, Long> adFilesBeforeRecover= getFilesModifiedTimesInDir(exportOverflowDir, ".ad");
        Map<File, Long> pbdFilesBeforeRecover= getFilesModifiedTimesInDir(exportOverflowDir, ".pbd");


        ((LocalCluster) m_config).recoverOne(1, null, "");
        Thread.sleep(500);

        Map<File, Long> adFilesAfterRecover= getFilesModifiedTimesInDir(exportOverflowDir, ".ad");
        Map<File, Long> pbdFilesAfterRecover= getFilesModifiedTimesInDir(exportOverflowDir, ".pbd");

        // ad files
        for (File f: adFilesAfterRecover.keySet()) {
            assert(adFilesBeforeRecover.get(f) != null);
            assert(adFilesBeforeRecover.get(f).equals(adFilesAfterRecover.get(f)));
        }

        for (File f: pbdFilesAfterRecover.keySet()) {
            assert(pbdFilesBeforeRecover.get(f) != null);
            assert(!pbdFilesBeforeRecover.get(f).equals(pbdFilesAfterRecover.get(f)));
        }

        client = getClient();
        // must still be able to verify the export data.
        quiesceAndVerify(client, m_verifier);
    }

    public TestExportRejoin(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        LocalCluster  config;
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportRejoin.class);

        project = new VoltProjectBuilder();
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-export-ddl-with-target.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-export-ddl-with-target.sql"));

        wireupExportTableToSocketExport("ALLOW_NULLS");
        wireupExportTableToSocketExport("NO_NULLS");
        wireupExportTableToSocketExport("ALLOW_NULLS_GRP");
        wireupExportTableToSocketExport("NO_NULLS_GRP");

        project.addProcedures(PROCEDURES);
        config = new LocalCluster("export-ddl-cluster-rep.jar", 8, 3, k_factor,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        config.setMaxHeap(1024);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }
}
