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

package org.voltdb.parser;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

public class TestSQLLexer extends AdhocDDLTestBase {

    // Test multi-line sql statements with errors
    @Test
    public void testErrorPositionForMultiCreateTable() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        LocalCluster cluster = null;

        try {
            cluster = new LocalCluster("testErrorPositionForMultiCreateTable.jar", 4, 1, 0, BackendTarget.NATIVE_EE_JNI);
            cluster.overrideAnyRequestForValgrind();
            VoltProjectBuilder builder = new VoltProjectBuilder();
            boolean success = cluster.compile(builder);
            assertTrue(success);
            cluster.startUp();
            m_client = ClientFactory.createClient();
            m_client.createConnection("", cluster.port(0));

            // Check basic drop of partitioned table that should work.
            ClientResponse resp = m_client.callProcedure("@SystemCatalog", "TABLES");
            System.out.println(resp.getResults()[0]);
            try {
                m_client.callProcedure("@AdHoc",
                        "create table f1 (\n" +
                        "ID int not null\n" +
                        ");\n" +
                        "create table f2 (\n" +
                        "ID int not null\n" +
                        ");\n" +
                        "create table t1 (\n" +
                        "ID int not null\n" +
                        ")\n" +
                        "create table t2 (\n" +
                        "ID int not null\n" +
                        ");"
                        );
            }
            catch (ProcCallException pce) {
                cluster.verifyLogMessage("line: 10, column: 1");
            }
            assertFalse(findTableInSystemCatalogResults("t1"));
        }
        finally {
            if (cluster != null) {
                cluster.shutDown();
            }
            stopClient();
        }
    }
}
