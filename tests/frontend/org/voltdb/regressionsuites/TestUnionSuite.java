/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

public class TestUnionSuite extends RegressionSuite {
    public TestUnionSuite(String name) {
        super(name);
    }

    /**
     * Three table Union - A.PKEY, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnion() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 1); // In the final result set - 0
        client.callProcedure("InsertB", 1, 1); // In the final result set - 1
        client.callProcedure("InsertB", 2, 1); // Eliminated (duplicate)
        client.callProcedure("InsertC", 1, 2); // In the final result set - 2
        client.callProcedure("InsertC", 2, 3); // In the final result set - 3
        client.callProcedure("InsertC", 3, 3); // Eliminated (duplicate)
        VoltTable result = client.callProcedure("@AdHoc", "SELECT PKEY FROM A UNION SELECT I FROM B UNION SELECT I FROM C;")
                                 .getResults()[0];
        assertEquals(4, result.getRowCount());
    }

    /**
     * Three table Union ALL - A.PKEY, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnionAll() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 1); //In the final result set
        client.callProcedure("InsertB", 1, 1); //In the final result set
        client.callProcedure("InsertB", 2, 1); //In the final result set
        client.callProcedure("InsertC", 1, 2); //In the final result set
        client.callProcedure("InsertC", 2, 3); //In the final result set
        VoltTable result = client.callProcedure("@AdHoc", "SELECT PKEY FROM A UNION ALL SELECT I FROM B UNION ALL SELECT I FROM C;")
                                 .getResults()[0];
        assertEquals(5, result.getRowCount());
    }

    /**
     * Two table Union - A.PKEY, A.I and B.PKET, B.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnionMultiColumns() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 1); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertB", 1, 1); //Eliminated (duplicate)
        client.callProcedure("InsertB", 2, 1); //In the final result set
        VoltTable result = client.callProcedure("@AdHoc", "SELECT PKEY, I FROM A UNION SELECT PKEY, I FROM B;")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());
    }

    /**
     * Two table Union ALL - A.PKEY, A.I and B.PKET, B.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnionAllMultiColumns() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 1); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertB", 1, 1); //In the final result set
        client.callProcedure("InsertB", 2, 1); //In the final result set
        VoltTable result = client.callProcedure("@AdHoc", "SELECT PKEY, I FROM A UNION ALL SELECT PKEY, I FROM B;")
                                 .getResults()[0];
        assertEquals(4, result.getRowCount());
    }

    /**
     * Two table Union - A.* and B.*
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testUnionStar() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 1); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertB", 1, 1); //Eliminated (duplicate)
        client.callProcedure("InsertB", 2, 1); //In the final result set
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM A UNION SELECT * FROM B;")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());
    }

    /**
     * Three table Except - A.I, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testExcept() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 1); //Eliminated (by C.PKEY=1)
        client.callProcedure("InsertA", 1, 1); //Eliminated (duplicate)
        client.callProcedure("InsertA", 2, 1); //Eliminated (duplicate)
        client.callProcedure("InsertA", 3, 4); //In the final result set
        client.callProcedure("InsertB", 1, 2); //Eliminated (not in A)
        client.callProcedure("InsertC", 1, 1); //Eliminated (by A.PKEY=0)
        client.callProcedure("InsertC", 2, 2); //Eliminated (not in A)
        VoltTable result = client.callProcedure("@AdHoc", "SELECT I FROM A EXCEPT SELECT I FROM B EXCEPT SELECT I FROM C;")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
    }

    /**
     * Three table Except ALL - A.I, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
   public void testExceptAll() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 0); //In the final result set
        client.callProcedure("InsertA", 1, 0); //In the final result set
        client.callProcedure("InsertA", 2, 1); //Eliminated (by B.PKEY=1)
        client.callProcedure("InsertA", 3, 2); //Eliminated (by B.PKEY=2)
        client.callProcedure("InsertA", 4, 2); //Eliminated (by B.PKEY=3)
        client.callProcedure("InsertA", 5, 5); //Eliminated (by B.PKEY=5)
        client.callProcedure("InsertA", 6, 5); //Eliminated (by C.PKEY=1)
        client.callProcedure("InsertA", 7, 5); //In the final result set
        client.callProcedure("InsertB", 1, 1); //Eliminated (by A.PKEY=2)
        client.callProcedure("InsertB", 2, 2); //Eliminated (by A.PKEY=3)
        client.callProcedure("InsertB", 3, 2); //Eliminated (by A.PKEY=4)
        client.callProcedure("InsertB", 4, 3); //Eliminated (not in A)
        client.callProcedure("InsertB", 5, 5); //Eliminated (by A.PKEY=5)
        client.callProcedure("InsertC", 0, 2); //Eliminated (not in (A-B))
        client.callProcedure("InsertC", 1, 5); //Eliminated (by A.PKEY=6)
        VoltTable result = client.callProcedure("@AdHoc", "SELECT I FROM A EXCEPT ALL SELECT I FROM B EXCEPT ALL SELECT I FROM C;")
                                 .getResults()[0];
        int r = result.getRowCount();
        System.out.println(result.toString());
        assertEquals(3, result.getRowCount());
    }

   /**
    * Three table Intersect - A.I, B.I and C.I
    * @throws NoConnectionsException
    * @throws IOException
    * @throws ProcCallException
    */
    public void testIntersect() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 0); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertA", 2, 1); //Eliminated (duplicate)
        client.callProcedure("InsertB", 1, 0); //Eliminated (duplicate)
        client.callProcedure("InsertB", 2, 1); //Eliminated (duplicate)
        client.callProcedure("InsertB", 3, 2); //Eliminated (not in A)
        client.callProcedure("InsertC", 1, 1); //Eliminated (duplicate)
        client.callProcedure("InsertC", 2, 2); //Eliminated (not in A)
        client.callProcedure("InsertC", 3, 0); //Eliminated (duplicate)
        VoltTable result = client.callProcedure("@AdHoc", "SELECT I FROM A INTERSECT SELECT I FROM B INTERSECT SELECT I FROM C;")
                                 .getResults()[0];
        assertEquals(2, result.getRowCount());
    }

    /**
     * Three table Intersect ALL- A.I, B.I and C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testIntersectAll() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 0); //In the final result set
        client.callProcedure("InsertA", 1, 1); //In the final result set
        client.callProcedure("InsertA", 2, 1); //In the final result set
        client.callProcedure("InsertA", 3, 3); //Eliminated (not in B & C)
        client.callProcedure("InsertA", 4, 3); //Eliminated (not in B & C)
        client.callProcedure("InsertA", 5, 2); //Eliminated (not in B)
        client.callProcedure("InsertB", 0, 1); //Eliminated (same as A.PKEY=1)
        client.callProcedure("InsertB", 1, 1); //Eliminated (same as A.PKEY=2)
        client.callProcedure("InsertB", 2, 1); //Eliminated (not in A & C)
        client.callProcedure("InsertB", 3, 0); //Eliminated (same as A.PKEY=0)
        client.callProcedure("InsertC", 0, 1); //Eliminated (same as A.PKEY=1)
        client.callProcedure("InsertC", 1, 1); //Eliminated (same as A.PKEY=2)
        client.callProcedure("InsertC", 2, 2); //Eliminated (not in B)
        client.callProcedure("InsertC", 3, 0); //Eliminated (same as A.PKEY=0)
        client.callProcedure("InsertC", 4, 0); //Eliminated (A & B have only one 0)
        VoltTable result = client.callProcedure("@AdHoc", "SELECT I FROM A INTERSECT ALL SELECT I FROM B INTERSECT ALL SELECT I FROM C;")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());
    }

    /**
     * Three table - (A.I union B.I) except C.I
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testMultipleSetOperations() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertA", 0, 0); // in A,B union. Eliminated by C.PKEY=3
        client.callProcedure("InsertA", 1, 1); // in A,B union. Eliminated by C.PKEY=1
        client.callProcedure("InsertA", 2, 1); // Eliminated (duplicate in A,B union)
        client.callProcedure("InsertA", 3, 2); // in A,B union. Not in C. In final result set
        client.callProcedure("InsertB", 1, 0); // Eliminated (duplicate A.PKEY=0)
        client.callProcedure("InsertB", 2, 1); // Eliminated (duplicate A.PKEY=1)
        client.callProcedure("InsertB", 3, 2); // Eliminated (duplicate A.PKEY=3)
        client.callProcedure("InsertC", 1, 1); // Eliminated ( in A,B union)
        client.callProcedure("InsertC", 3, 0); // Eliminated ( in A,B union)
        client.callProcedure("InsertC", 4, 3); // Eliminated ( not in A or B)
        VoltTable result = client.callProcedure("@AdHoc", "SELECT I FROM A UNION SELECT I FROM B EXCEPT SELECT I FROM C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
    }


    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestUnionSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestUnionSuite.class.getResource("testunion-ddl.sql"));
        project.addPartitionInfo("A", "PKEY");
        project.addStmtProcedure("InsertA", "INSERT INTO A VALUES(?, ?);");
        project.addStmtProcedure("InsertB", "INSERT INTO B VALUES(?, ?);");
        project.addStmtProcedure("InsertC", "INSERT INTO C VALUES(?, ?);");

        // local
        config = new LocalCluster("testunion-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testunion-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // HSQLDB
        config = new LocalCluster("testunion-cluster.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }

}
