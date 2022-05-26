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

package org.voltdb.regressionsuites;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.voltdb.BackendTarget;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportTestClient;
import org.voltdb.export.ExportTestVerifier;
import org.voltdb.export.TestExportBaseSocketExport;

import com.google_voltpatches.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;


public class TestExportWithMisconfiguredExportClient extends RegressionSuite {
    private static LocalCluster m_config;

    public static final RoleInfo GROUPS[] = new RoleInfo[] {
        new RoleInfo("export", false, false, false, false, false, false),
        new RoleInfo("proc", true, false, true, true, false, false),
        new RoleInfo("admin", true, false, true, true, false, false)
    };

    public static final UserInfo[] USERS = new UserInfo[] {
        new UserInfo("export", "export", new String[]{"export"}),
        new UserInfo("default", "password", new String[]{"proc"}),
        new UserInfo("admin", "admin", new String[]{"proc", "admin"})
    };

    /*
     * Test suite boilerplate
     */

    public TestExportWithMisconfiguredExportClient(final String s) {
        super(s);
    }

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        FileUtils.deleteDirectory(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        ExportTestVerifier.m_paused = false;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ExportTestClient.clear();
    }

    //
    // Only notify the verifier of the first set of rows. Expect that the rows after will be truncated
    // when the snapshot is restored
    // @throws Exception
    //
    public void testFailureOnMissingNonce() throws Exception {
        if (isValgrind()) {
            return;
        }
        System.out.println("testFailureOnMissingNonce");

        m_config.startUp();
        PipeToFile pf = m_config.m_pipes.get(0);
        Thread.currentThread().sleep(10000);

        BufferedReader bi = new BufferedReader(new FileReader(new File(pf.m_filename)));
        String line;
        boolean failed = true;
        final CharSequence cs = "doing what I am being told";
        while ((line = bi.readLine()) != null) {
            if (line.contains(cs)) {
                failed = false;
                break;
            }
        }
        assertFalse(failed);
    }

   static public junit.framework.Test suite() throws Exception
    {
       final MultiConfigSuiteBuilder builder =
               new MultiConfigSuiteBuilder(TestExportWithMisconfiguredExportClient.class);

        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");
        String dexportClientClassName = System.getProperty("exportclass", "");
        System.out.println("Test System override export class is: " + dexportClientClassName);
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.export.ExportTestClient");

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.setSecurityEnabled(true, true);
        project.addRoles(GROUPS);
        project.addUsers(USERS);
        project.addSchema(TestExportBaseSocketExport.class.getResource("export-nonulls-ddl-with-target.sql"));

        Properties props = new Properties();
        // omit nonce
        props.putAll(ImmutableMap.<String, String>of(
                "type", "csv",
                "batched", "false",
                "with-schema", "true",
                "complain", "true",
                "outdir", "/tmp/" + System.getProperty("user.name")));
        project.addExport(true /* enabled */, ServerExportEnum.CUSTOM, props);
        project.addPartitionInfo("S_NO_NULLS", "PKEY");

        project.addProcedures(TestExportBaseSocketExport.NONULLS_PROCEDURES);

        /*
         * compile the catalog all tests start with
         */
        m_config = new LocalCluster("export-ddl-cluster-rep.jar", 8, 1, 0,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        m_config.setExpectedToCrash(true);
        m_config.setHasLocalServer(false);
        boolean compile = m_config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(m_config);

        return builder;
    }
}
