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

        vt = client.callProcedure("@Explain", "SELECT COUNT(*) FROM T1" ).getResults()[0];
        while( vt.advanceRow() ) {
            String resultStr = (String) vt.get(0, VoltType.STRING );
            assertTrue( resultStr.contains( "RETURN RESULTS TO STORED PROCEDURE" ));
            assertTrue( resultStr.contains( "AGGREGATION ops" ));
            assertTrue( resultStr.contains( "RECEIVE FROM ALL PARTITIONS" ));
            assertTrue( resultStr.contains( "SEND PARTITION RESULTS TO COORDINATOR" ));
            assertTrue( resultStr.contains( "AGGREGATION ops" ));
            assertTrue( resultStr.contains( "SEQUENTIAL SCAN of \"T1\"" ));
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
            assertTrue( sql.contains( "INSERT INTO T1 VALUES (?, ?, ?);" ));
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
