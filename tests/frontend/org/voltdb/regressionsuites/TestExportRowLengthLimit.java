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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.utils.CatalogUtil;

public class TestExportRowLengthLimit extends RegressionSuite {

    public TestExportRowLengthLimit(String name) {
        super(name);
    }

    public void testExceedRowLengthLimit() throws Exception {
        Client client = this.getClient();
        // within row length limit
        ClientResponse response = client.callProcedure("@AdHoc", "CREATE STREAM EV2 (i BIGINT)");
        assertEquals(response.getStatus(), ClientResponse.SUCCESS);

        // exceeding row length limit
        try {
            client.callProcedure("@AdHoc", "CREATE STREAM EV1 (i BIGINT, j BIGINT)");
            fail();
        } catch (ProcCallException e) {
            assert(e.getMessage().contains("exceeding configurated limitation"));
        }
    }

    public void testUpdateRowLengthLimit() throws Exception {
        // update the row length in the deployment
        LocalCluster config = null;
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.RejectingExportClient");

        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        Properties props = new Properties();
        props.put("skipinternals", "true");
        props.put(CatalogUtil.ROW_LENGTH_LIMIT, "16");
        project.addExport(true, ServerExportEnum.CUSTOM, props);

        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setExpectedToCrash(true);
        boolean compile = config.compile(project);
        assertTrue(compile);

        Client client = this.getClient();

        SyncCallback cb2 = new SyncCallback();
        UpdateApplicationCatalog.update(client, cb2, null, new File(project.getPathToDeployment()));
        cb2.waitForResponse();
        assertEquals(ClientResponse.SUCCESS, cb2.getResponse().getStatus());

        // now it should within row length limit
        client = this.getClient();
        ClientResponse response = client.callProcedure("@AdHoc", "CREATE STREAM EV1 (i BIGINT, j BIGINT)");
        assertEquals(response.getStatus(), ClientResponse.SUCCESS);
    }

    static public junit.framework.Test suite() {
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestExportRowLengthLimit.class);

        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.RejectingExportClient");
        LocalCluster config = null;
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.RejectingExportClient");

        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        Properties props = new Properties();
        props.put("skipinternals", "true");
        props.put(CatalogUtil.ROW_LENGTH_LIMIT, "8");
        project.addExport(true, ServerExportEnum.CUSTOM, props);

        config = new LocalCluster("export-ddl-cluster-rep.jar", 2, 3, 1,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        config.setExpectedToCrash(true);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);

        return builder;
    }
}
