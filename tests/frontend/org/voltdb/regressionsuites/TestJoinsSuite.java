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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestJoinsSuite extends RegressionSuite {
    public TestJoinsSuite(String name) {
        super(name);
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testTwoTableSeqInnerJoin() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertR1", 1, 1, 1); // 1,1,1,3
        client.callProcedure("InsertR1", 1, 1, 1); // 1,1,1,3
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated
        client.callProcedure("InsertR1", 3, 3, 3); // 3,3,3,4
        client.callProcedure("InsertR2", 1, 3); // 1,1,1,3
        client.callProcedure("InsertR2", 3, 4); // 3,3,3,4
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON R1.A = R2.A;")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 USING(A);")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());
        client.callProcedure("InsertP1", 1, 1); // 1,1,1,3
        client.callProcedure("InsertP1", 1, 1); // 1,1,1,3
        client.callProcedure("InsertP1", 2, 2); // Eliminated
        client.callProcedure("InsertP1", 3, 3); // 3,3,3,4
        result = client.callProcedure("@AdHoc", "SELECT * FROM P1 JOIN R2 ON P1.A = R2.A;")
                .getResults()[0];
        assertEquals(3, result.getRowCount());
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testTwoTableSeqInnerWhereJoin() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertR1", 1, 5, 1); // 1,5,1,1,3
        client.callProcedure("InsertR1", 1, 1, 1); // eliminated by WHERE
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertR1", 3, 3, 3); // eliminated by WHERE
        client.callProcedure("InsertR2", 1, 3); // 1,5,1,1,3
        client.callProcedure("InsertR2", 3, 4); // eliminated by WHERE
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON R1.A = R2.A WHERE R1.C > R2.C;")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON R1.A = R2.A WHERE R1.C > R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 INNER JOIN R2 ON R1.A = R2.A WHERE R1.C > R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1, R2 WHERE R1.A = R2.A AND R1.C > R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());

        client.callProcedure("InsertP1", 1, 5); // 1,5,1,1,3
        client.callProcedure("InsertP1", 1, 1); // eliminated by WHERE
        client.callProcedure("InsertP1", 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertP1", 3, 3); // eliminated by WHERE
        result = client.callProcedure("@AdHoc", "SELECT * FROM P1 JOIN R2 ON P1.A = R2.A WHERE P1.C > R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testTwoTableSeqInnerFunctionJoin() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertR1", -1, 5, 1); //  -1,5,1,1,3
        client.callProcedure("InsertR1", 1, 1, 1); // 1,1,1,1,3
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertR1", 3, 3, 3); // 3,3,3,3,4
        client.callProcedure("InsertR2", 1, 3); // 1,1,1,1,3
        client.callProcedure("InsertR2", 3, 4); // 3,3,3,3,4
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON ABS(R1.A) = R2.A;")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());
    }

    /**
     * Two table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testTwoTableSeqInnerMultiJoin() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertR1", 1, 1, 1); // 1,1,1,1,1
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertR1", 3, 3, 3); // Eliminated by JOIN
        client.callProcedure("InsertR2", 1, 1); // 1,1,1,1,1
        client.callProcedure("InsertR2", 1, 3); // Eliminated by JOIN
        client.callProcedure("InsertR2", 3, 4); // Eliminated by JOIN
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 ON R1.A = R2.A AND R1.C = R2.C;")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1, R2 WHERE R1.A = R2.A AND R1.C = R2.C;")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
        result = client.callProcedure("@AdHoc", "SELECT * FROM R1 JOIN R2 USING (A,C);")
                .getResults()[0];
        assertEquals(1, result.getRowCount());

        client.callProcedure("InsertP1", 1, 1); // 1,1,1,1,1
        client.callProcedure("InsertP1", 2, 2); // Eliminated by JOIN
        client.callProcedure("InsertP1", 3, 3); // Eliminated by JOIN
        result = client.callProcedure("@AdHoc", "SELECT * FROM P1 JOIN R2 USING (A,C);")
                .getResults()[0];
        assertEquals(1, result.getRowCount());
   }

    /**
     * Three table NLJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testThreeTableSeqInnerMultiJoin() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertR1", 1, 1, 1); // 1,3,1,1,1,1,3
        client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by P1 R1 JOIN
        client.callProcedure("InsertR1", -1, 3, 3); // -1,0,-1,3,3,4,0 Eliminated by WHERE
        client.callProcedure("InsertR2", 1, 1); // Eliminated by P1 R2 JOIN
        client.callProcedure("InsertR2", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("InsertR2", 3, 4); // Eliminated by P1 R2 JOIN
        client.callProcedure("InsertR2", 4, 0); // Eliminated by WHERE
        client.callProcedure("InsertP1", 1, 3); // 1,3,1,1,1,1,3
        client.callProcedure("InsertP1", -1, 0); // Eliminated by WHERE
        client.callProcedure("InsertP1", 8, 4); // Eliminated by P1 R1 JOIN
        VoltTable result = client.callProcedure(
                "@AdHoc", "select * FROM P1 JOIN R1 ON P1.A = R1.A JOIN R2 ON P1.C = R2.C WHERE P1.A > 0")
                                 .getResults()[0];
        assertEquals(1, result.getRowCount());
    }

    /**
     * Two table NLIJ
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testTwoTableIndexInnerJoin() throws NoConnectionsException, IOException, ProcCallException {
        Client client = this.getClient();
        client.callProcedure("InsertR3", 1, 1); // 1,1,1,3
        client.callProcedure("InsertR3", 1, 1); // 1,1,1,3
        client.callProcedure("InsertR3", 2, 2); // Eliminated
        client.callProcedure("InsertR3", 3, 3); // 3,3,3,4
        client.callProcedure("InsertR2", 1, 3); // 1,1,1,3
        client.callProcedure("InsertR2", 3, 4); // 3,3,3,4
        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R3 JOIN R2 ON R3.A = R2.A;")
                                 .getResults()[0];
        assertEquals(3, result.getRowCount());

        client.callProcedure("InsertP3", 1, 1); // 1,1,1,3
        client.callProcedure("InsertP3", 2, 2); // Eliminated
        client.callProcedure("InsertP3", 3, 3); // 3,3,3,4
        result = client.callProcedure("@AdHoc", "SELECT * FROM P3 JOIN R2 ON P3.A = R2.A;")
                .getResults()[0];
        assertEquals(2, result.getRowCount());
    }

  /**
  * Two table NLIJ
  * @throws NoConnectionsException
  * @throws IOException
  * @throws ProcCallException
  */
 public void testTwoTableIndexInnerWhereJoin() throws NoConnectionsException, IOException, ProcCallException {
     Client client = this.getClient();
     client.callProcedure("InsertR3", 1, 5); // eliminated by WHERE
     client.callProcedure("InsertR3", 1, 1); // eliminated by WHERE
     client.callProcedure("InsertR3", 2, 2); // Eliminated by JOIN
     client.callProcedure("InsertR3", 3, 3); // eliminated by WHERE
     client.callProcedure("InsertR3", 4, 5); // 4,5,4,2
     client.callProcedure("InsertR2", 1, 3); // 1,5,1,1,3
     client.callProcedure("InsertR2", 3, 4); // eliminated by WHERE
     client.callProcedure("InsertR2", 4, 2); // 4,5,4,2
     VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM R3 JOIN R2 ON R3.A = R2.A WHERE R3.A > R2.C;")
                              .getResults()[0];
     assertEquals(1, result.getRowCount());

     client.callProcedure("InsertP3", 1, 5); // eliminated by WHERE
     client.callProcedure("InsertP3", 2, 2); // Eliminated by JOIN
     client.callProcedure("InsertP3", 3, 3); // eliminated by WHERE
     client.callProcedure("InsertP3", 4, 3); // 4,3,4,2
     result = client.callProcedure("@AdHoc", "SELECT * FROM P3 JOIN R2 ON P3.A = R2.A WHERE P3.A > R2.C;")
             .getResults()[0];
     assertEquals(1, result.getRowCount());
 }

/**
* Three table NLIJ
* @throws NoConnectionsException
* @throws IOException
* @throws ProcCallException
*/
public void testThreeTableIndexInnerMultiJoin() throws NoConnectionsException, IOException, ProcCallException {
  Client client = this.getClient();
  client.callProcedure("InsertR1", 1, 1, 1); // 1,3,1,1,1,1,3
  client.callProcedure("InsertR1", 2, 2, 2); // Eliminated by P1 R1 JOIN
  client.callProcedure("InsertR1", -1, 3, 3); // -1,0,-1,3,3,4,0 Eliminated by WHERE
  client.callProcedure("InsertR2", 1, 1); // Eliminated by P1 R2 JOIN
  client.callProcedure("InsertR2", 1, 3); // 1,3,1,1,1,1,3
  client.callProcedure("InsertR2", 3, 4); // Eliminated by P1 R2 JOIN
  client.callProcedure("InsertR2", 4, 0); // Eliminated by WHERE
  client.callProcedure("InsertP3", 1, 3); // 1,3,1,1,1,1,3
  client.callProcedure("InsertP3", -1, 0); // Eliminated by WHERE
  client.callProcedure("InsertP3", 8, 4); // Eliminated by P1 R1 JOIN
  VoltTable result = client.callProcedure(
          "@AdHoc", "select * FROM P3 JOIN R1 ON P3.A = R1.A JOIN R2 ON P3.F = R2.C WHERE P3.A > 0")
                           .getResults()[0];
  assertEquals(1, result.getRowCount());
}

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestJoinsSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestJoinsSuite.class.getResource("testjoins-ddl.sql"));
        project.addStmtProcedure("InsertR1", "INSERT INTO R1 VALUES(?, ?, ?);");
        project.addStmtProcedure("InsertR2", "INSERT INTO R2 VALUES(?, ?);");
        project.addStmtProcedure("InsertR3", "INSERT INTO R3 VALUES(?, ?);");
        project.addStmtProcedure("InsertP1", "INSERT INTO P1 VALUES(?, ?);");
        project.addStmtProcedure("InsertP3", "INSERT INTO P3 VALUES(?, ?);");

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

