/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package org.voltdb.groovy;

import org.voltdb.BackendTarget;
import org.voltdb.LegacyHashinator;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;

public class TestGroovyDeployment extends RegressionSuite {

    static final String SCHEMA = "CREATE TABLE MAMMA_MIA (\n" +
            "  MAMMA INTEGER NOT NULL,\n" +
            "  MIA VARCHAR(32) NOT NULL,\n" +
            "  CONSTRAINT PK_MAMMA_MIA PRIMARY KEY(MAMMA)\n" +
            ")\n" +
            ";\n" +
            "PARTITION TABLE MAMMA_MIA ON COLUMN MAMMA\n" +
            ";\n" +
            "\n" +
            "CREATE PROCEDURE voltdb.groovy.example.AddMamma\n" +
            "PARTITION ON TABLE MAMMA_MIA COLUMN MAMMA\n" +
            "AS ###\n" +
            "  addMamma = new SQLStmt('INSERT INTO MAMMA_MIA (MAMMA,MIA) VALUES (?,?);')\n" +
            "  transactOn = { int mammaId, String miaName ->\n" +
            "    voltQueueSQL(addMamma, mammaId, miaName)\n" +
            "    voltExecuteSQL(true)\n" +
            "  }\n" +
            "### LANGUAGE GROOVY;\n" +
            "\n" +
            "CREATE PROCEDURE voltdb.groovy.example.GetMamma\n" +
            "PARTITION ON TABLE MAMMA_MIA COLUMN MAMMA\n" +
            "AS ###\n" +
            "  getMamma = new SQLStmt('SELECT MAMMA,MIA FROM MAMMA_MIA WHERE MAMMA = ?')\n" +
            "  transactOn = { int mammaId ->\n" +
            "    voltQueueSQL(getMamma,mammaId)\n" +
            "    voltExecuteSQL(true)\n" +
            "  }\n" +
            "### LANGUAGE GROOVY;";

    public TestGroovyDeployment(String name) {
        super(name);
    }

    public void testGroovyProcedureInvocation() throws Exception {
        Client client = getClient();
        ClientResponse cr = null;

        cr = client.callProcedure("AddMamma", 1, "Una");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
        cr = client.callProcedure("AddMamma", 2, "Due");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

        cr = client.callProcedure("GetMamma", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Due",result.getString(1));
    }

    static public junit.framework.Test suite() throws Exception
    {
        TheHashinator.initialize(LegacyHashinator.class, LegacyHashinator.getConfigureBytes(2));
        LocalCluster config;

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestGroovyDeployment.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(SCHEMA);

        /*
         * compile the catalog all tests start with
         */
        config = new LocalCluster("groovy-ddl-cluster-rep.jar", 2, 1, 0,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, null);
        boolean compile = config.compile(project);
        assertTrue(compile);
        config.setHasLocalServer(false);
        config.setMaxHeap(512);
        builder.addServerConfig(config);

        return builder;
    }

}
