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

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestExplainCommandSuite extends RegressionSuite {

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestExplainCommandSuite(String name) {
        super(name);
    }

    public void testExplain() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        String[] strs = {"SELECT COUNT(*) FROM T1 order by A_INT", "SELECT COUNT(*) FROM T1 order by A_INT"};

        vt = client.callProcedure("@Explain", (Object[]) strs ).getResults()[0];
        while( vt.advanceRow() ) {
            System.out.println(vt);
            String plan = (String) vt.get("EXEcution_PlaN", VoltType.STRING);
            assertTrue( plan.contains( "RETURN RESULTS TO STORED PROCEDURE" ));
            // Validate bypass of no-op sort on single-row result.
            assertFalse( plan.contains( "ORDER BY (SORT)"));
            assertTrue( plan.contains( "TABLE COUNT" ));
        }

        //test the index count node
        vt = client.callProcedure("@Explain", "SELECT COUNT(*) FROM t3 where I3 < 100" ).getResults()[0];
        while( vt.advanceRow() ) {
            System.out.println(vt);
            String plan = (String) vt.get(0, VoltType.STRING );
            assertTrue( plan.contains("INDEX COUNT") );
        }

        //test expression index usage
        vt = client.callProcedure("@Explain", "SELECT * FROM t3 where I3 + I4 < 100" ).getResults()[0];
        while( vt.advanceRow() ) {
            System.out.println(vt);
            String plan = (String) vt.get(0, VoltType.STRING );
            assertTrue( plan.contains("INDEX SCAN") );
        }
}

    public void testExplainProc() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        vt = client.callProcedure("@ExplainProc", "T1.insert" ).getResults()[0];
        while( vt.advanceRow() ) {
            System.out.println(vt);
            String sql = (String) vt.get(0, VoltType.STRING );
            String plan = (String) vt.get(1, VoltType.STRING );
            assertTrue( sql.contains( "INSERT INTO T1 VALUES (?, ?, ?)" ));
            assertTrue( plan.contains( "INSERT into \"T1\"" ));
            assertTrue( plan.contains( "MATERIALIZE TUPLE from parameters and/or literals" ));
        }
    }

    /**
     * Build a list of the tests that will be run when TestTPCCSuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        VoltServerConfig config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestExplainCommandSuite.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestExplainCommandSuite.class.getResource("testExplainCommand-ddl.sql"));
        project.addPartitionInfo("t1", "PKEY");
        project.addPartitionInfo("t2", "PKEY");
        project.addPartitionInfo("t3", "PKEY");

        boolean success;

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("testExplainCommand-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        success = config.compile(project);
        assert(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
