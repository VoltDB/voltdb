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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSystemCatalogSuite extends RegressionSuite {

    public TestSystemCatalogSuite(String name) {
        super(name);
    }

    public void testInvalidSelector() throws IOException
    {
        Client client = getClient();
        try
        {
            client.callProcedure("@SystemCatalog", "NONSENSE");
        }
        catch (ProcCallException pce)
        {
            assertTrue(pce.getMessage().contains("Invalid @SystemCatalog selector"));
            return;
        }
        fail("Invalid selector should have resulted in a ProcCallException but didn't");
    }

    public void testTablesSelector() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SystemCatalog", "TABLES").getResults();
        assertEquals(10, results[0].getColumnCount());
        System.out.println(results[0]);
        results[0].advanceRow();
        assertEquals("{\"partitionColumn\":\"A1\"}", results[0].get("REMARKS", VoltType.STRING));
        results[0].advanceRow();
        assertEquals("{\"partitionColumn\":\"A1\",\"sourceTable\":\"T\"}", results[0].get("REMARKS", VoltType.STRING));
    }

    public void testColumnsSelector() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SystemCatalog", "COLUMNS").getResults();
        assertEquals(23, results[0].getColumnCount());
        System.out.println(results[0]);
    }

    public void testIndexInfoSelector() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SystemCatalog", "INDEXINFO").getResults();
        assertEquals(13, results[0].getColumnCount());
        System.out.println(results[0]);
    }

    public void testPrimaryKeysSelector() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SystemCatalog", "PRIMARYKEYS").getResults();
        assertEquals(6, results[0].getColumnCount());
        System.out.println(results[0]);
    }

    public void testProceduresSelector() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SystemCatalog", "PROCEDURES").getResults();
        assertEquals(9, results[0].getColumnCount());
        System.out.println(results[0]);
    }

    public void testProcedureColumnsSelector() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SystemCatalog", "PROCEDURECOLUMNS").getResults();
        assertEquals(20, results[0].getColumnCount());
        System.out.println(results[0]);
    }

    public void testTypeInfoSelector() throws IOException, ProcCallException
    {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@SystemCatalog", "TYPEINFO").getResults();
        assertEquals(9, results[0].getRowCount()); // Will break if we add a type, hopefully gets
                                                   // type-adder to double-check they've got things right
        assertEquals(18, results[0].getColumnCount());
        System.out.println(results[0]);
    }

    public void testJdbcAccess() throws IOException, ProcCallException, ClassNotFoundException, SQLException
    {
        String url = String.format("jdbc:voltdb://localhost:%d", port(0));
        String driver = "org.voltdb.jdbc.Driver";
        Class.forName(driver);
        Connection huh = DriverManager.getConnection(url, "", "");
        // Not really a correctness test, just want to make sure we can
        // get real result sets back through the JDBC driver.
        // Just get a bunch of easy stupid stuff to verify for now
        ResultSet blah = huh.getMetaData().getColumns(null, null, "T", null);
        while (blah.next())
        {
            assertEquals("T", blah.getString("TABLE_NAME"));
        }
        blah.close();
        blah = huh.getMetaData().getTables(null, null, "T", null);
        while (blah.next())
        {
            assertEquals("T", blah.getString("TABLE_NAME"));
            assertEquals("TABLE", blah.getString("TABLE_TYPE"));
        }
        blah.close();
        blah = huh.getMetaData().getIndexInfo(null, null, "T", false, false);
        while (blah.next())
        {
            assertEquals("T", blah.getString("TABLE_NAME"));
        }
        blah.close();
        blah = huh.getMetaData().getPrimaryKeys(null, null, "T");
        while (blah.next())
        {
            assertEquals("T", blah.getString("TABLE_NAME"));
        }
        blah.close();
        blah = huh.getMetaData().getProcedures(null, null, null);
        while (blah.next())
        {
            System.out.println(blah.getString(3));
            System.out.println(blah.getString(4));
        }
        blah.close();
        blah = huh.getMetaData().getProcedureColumns(null, null, "InsertA", null);
        while (blah.next())
        {
            System.out.println(blah.getString(3));
            System.out.println(blah.getString(4));
        }
        blah.close();
        blah = huh.getMetaData().getTypeInfo();
        while (blah.next()) {
            assertTrue(blah.getString("TYPE_NAME") != null);
        }
        blah.close();
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException
    {
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSystemCatalogSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("CREATE TABLE T(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));" +
                                 "CREATE VIEW V(A1, S) AS SELECT A1, COUNT(*) FROM T GROUP BY A1;");
        project.addPartitionInfo("T", "A1");
        project.addStmtProcedure("InsertA", "INSERT INTO T VALUES(?,?);", "T.A1: 0");

        LocalCluster lcconfig = new LocalCluster("getclusterinfo-cluster.jar", 2, 2, 1,
                                               BackendTarget.NATIVE_EE_JNI);
        lcconfig.compile(project);
        builder.addServerConfig(lcconfig);

        return builder;
    }
}


