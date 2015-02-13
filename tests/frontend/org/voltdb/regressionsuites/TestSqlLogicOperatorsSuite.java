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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;


public class TestSqlLogicOperatorsSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    private void fillTables(Client client) throws NoConnectionsException, IOException, ProcCallException
    {
        for (int i = 0; i < 20; i++)
        {
            client.callProcedure("Insert", "P1", i, "p1", i & 0x1, (double)(i%4));
            client.callProcedure("Insert", "P2", i, "p2", i & 0x1, (double)(i%4));
        }
    }

    private void clearTables(Client client) throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("@AdHoc", "delete from p1");
        client.callProcedure("@AdHoc", "delete from p2");
    }

    // select count(*)
    //     from p1 where id > 9 and i == 1    // should be 5
    //     from p1 where id > 9 or i == 1     // should be 15
    //     from p1 where (id > 9 or i == 1) and ??  // should be 15
    //     from p1 where id > 9 or i == 1 and ?? // should be 10
    //     from p1, p2 where (p1.id > 9 or p1.num = 0) and (p2.id or ??)
    //     from p1, p2 where ?? or ?? and ?? or ??

    public void testSelectPrecedenceAndAssociation() throws IOException, ProcCallException
    {
        Client client = getClient();
        fillTables(client);
        VoltTable[] results =
            client.callProcedure("@AdHoc", "select count(*) from p1 where id > 9 and num = 0").getResults();
        assertEquals(5, results[0].asScalarLong());
        results =
            client.callProcedure("@AdHoc", "select count(*) from p1 where id > 9 or num = 0").getResults();
        assertEquals(15, results[0].asScalarLong());
        results =
            client.callProcedure("@AdHoc", "select count(*) from p1 where (id > 9 or num = 0) and ratio = 0.0").getResults();
        assertEquals(5, results[0].asScalarLong());
        results =
            client.callProcedure("@AdHoc", "select count(*) from p1 where id > 9 or num = 0 and ratio = 0.0").getResults();
        assertEquals(13, results[0].asScalarLong());
        results =
            client.callProcedure("@AdHoc", "select count(*) from p1 where ratio = 0.0 and (id > 9 or num = 0)").getResults();
        assertEquals(5, results[0].asScalarLong());
        results =
            client.callProcedure("@AdHoc", "select count(*) from p1 where ratio = 0.0 and id > 9 or num = 0").getResults();
        assertEquals(10, results[0].asScalarLong());
    }

    public void testDeletePrecedenceAndAssociation() throws IOException, ProcCallException
    {
        Client client = getClient();
        fillTables(client);
        VoltTable[] results =
            client.callProcedure("@AdHoc", "delete from p1 where id > 9 and num = 0").getResults();
        assertEquals(5, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "delete from p1 where id > 9 or num = 0").getResults();
        assertEquals(15, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "delete from p1 where (id > 9 or num = 0) and ratio = 0.0").getResults();
        assertEquals(5, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "delete from p1 where id > 9 or num = 0 and ratio = 0.0").getResults();
        assertEquals(13, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "delete from p1 where ratio = 0.0 and (id > 9 or num = 0)").getResults();
        assertEquals(5, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "delete from p1 where ratio = 0.0 and id > 9 or num = 0").getResults();
        assertEquals(10, results[0].asScalarLong());
    }

    public void testUpdatePrecedenceAndAssociation() throws IOException, ProcCallException
    {
        Client client = getClient();
        fillTables(client);
        VoltTable[] results =
            client.callProcedure("@AdHoc", "update p1 set desc = 'changed' where id > 9 and num = 0").getResults();
        assertEquals(5, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "select count(*) from p1 where desc = 'changed'").getResults();
        assertEquals(5, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "update p1 set desc = 'changed' where id > 9 or num = 0").getResults();
        assertEquals(15, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "select count(*) from p1 where desc = 'changed'").getResults();
        assertEquals(15, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "update p1 set desc = 'changed' where (id > 9 or num = 0) and ratio = 0.0").getResults();
        assertEquals(5, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "select count(*) from p1 where desc = 'changed'").getResults();
        assertEquals(5, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "update p1 set desc = 'changed' where id > 9 or num = 0 and ratio = 0.0").getResults();
        assertEquals(13, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "select count(*) from p1 where desc = 'changed'").getResults();
        assertEquals(13, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "update p1 set desc = 'changed' where ratio = 0.0 and (id > 9 or num = 0)").getResults();
        assertEquals(5, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "select count(*) from p1 where desc = 'changed'").getResults();
        assertEquals(5, results[0].asScalarLong());
        clearTables(client);
        fillTables(client);
        results =
            client.callProcedure("@AdHoc", "update p1 set desc = 'changed' where ratio = 0.0 and id > 9 or num = 0").getResults();
        assertEquals(10, results[0].asScalarLong());
        results = client.callProcedure("@AdHoc", "select count(*) from p1 where desc = 'changed'").getResults();
        assertEquals(10, results[0].asScalarLong());
    }

    public void testIndexUseWithOr() throws IOException, ProcCallException
    {
        Client client = getClient();
        fillTables(client);
        VoltTable[] results =
            client.callProcedure("@AdHoc", "select count(*) from p2 where id > 9 and id < 16").getResults();
        assertEquals(6, results[0].asScalarLong());
        results =
            client.callProcedure("@AdHoc", "select count(*) from p2 where id < 10 or id > 15").getResults();
        assertEquals(14, results[0].asScalarLong());
        results =
            client.callProcedure("@AdHoc", "select count(*) from p2 where (id > 15 and num = 0) or (id > 10 and num = 1)").getResults();
        assertEquals(7, results[0].asScalarLong());
        results =
            client.callProcedure("@AdHoc", "select count(*) from p2 where (id > 10 and num = 1) or (id > 15 and num = 0)").getResults();
        assertEquals(7, results[0].asScalarLong());
        results =
            client.callProcedure("@AdHoc", "select count(*) from p2 where (id > 9 and num = 1) or (id < 10 and num = 0)").getResults();
        assertEquals(10, results[0].asScalarLong());
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlLogicOperatorsSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSqlLogicOperatorsSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(Insert.class.getResource("sql-update-ddl.sql"));
        project.addProcedures(PROCEDURES);

        config = new LocalCluster("sqllogic-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqllogic-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqllogic-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // No cluster tests for logic stuff

        return builder;
    }

}
