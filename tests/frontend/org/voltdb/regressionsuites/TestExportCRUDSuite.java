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

import java.io.IOException;
import java.util.Properties;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;

public class TestExportCRUDSuite extends RegressionSuite {

    static final Class<?>[] PROCEDURES = {};

    public TestExportCRUDSuite(String name) {
        super(name);
    }

    public void testExportTable() throws Exception {
        final Client client = this.getClient();
        for (int i=0; i < 10; i++) {
            ClientResponse resp = client.callProcedure("E1.insert", i);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }
        VoltTable[] results = client.callProcedure("@AdHoc", "SELECT * FROM EV1").getResults();
        assertEquals(10, results[0].getRowCount());

        try {
            client.callProcedure("E1.delete", 0);
            fail();
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("was not found"));
        }

        try {
            client.callProcedure("E1.update", 1, 11);
            fail();
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("was not found"));
        }
    }


    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestExportCRUDSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        final ServerExportEnum exportType;

        try {
            // These settings are the VoltProjectBuilder.addExport() defaults, except no outdir is specified so
            // it will default outdir to a voltdbroot subfolder, preventing interference between nodes.
            Properties p = new Properties();
            p.put("type","tsv");
            p.put("batched","true");
            p.put("with-schema","true");
            p.put("nonce","zorag");
            project.addExport(true, ServerExportEnum.FILE, p);

            project.addLiteralSchema(
                    "CREATE STREAM e1 partition on column x (x INTEGER NOT NULL); " +
                    "CREATE VIEW ev1 (x, c) AS SELECT x, COUNT(*) FROM e1 GROUP BY x;"
            );

        } catch (IOException error) {
            fail(error.getMessage());
        }

        // JNI
        config = new LocalCluster("crud-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        // CLUSTER
        config = new LocalCluster("crud-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);

        return builder;
    }
}
