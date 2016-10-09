package org.voltdb.regressionsuites;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestMaterializedViewNonemptyTables extends RegressionSuite {
    public TestMaterializedViewNonemptyTables(String name) {
		super(name);
	}
    
    public void testUnsafeOperators() throws Exception {
    	Client client = getClient();
    	
    	ClientResponse cr;
    	cr = client.callProcedure("ALPHA.insert", 0, 1);
    	assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    	cr = client.callProcedure("BETA.insert", 0, 1);
    	assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    	cr = client.callProcedure("@AdHoc", "create view vv as select a from alpha");
    	assertEquals(ClientResponse.GRACEFUL_FAILURE, cr.getStatus());
    	String msg = cr.getAppStatusString();
    	System.out.printf("Status: %s\n", msg);
    }

	static public junit.framework.Test suite() {
		VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMaterializedViewNonemptyTables.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestMaterializedViewNonemptyTables.class.getResource("testmvnonemptytables-ddl.sql"));


        // JNI
        config = new LocalCluster("testMaterializedViewNonemptyTables-onesite.jar", 1, 1, 0,
                BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        // CLUSTER
        // config = new LocalCluster("testMateralizedViewNonemptyTables-cluster.jar", 2, 3, 1,
        //        BackendTarget.NATIVE_EE_JNI);
        // boolean t2 = config.compile(project);
        // assertTrue(t2);
        // builder.addServerConfig(config);

        return builder;
    }
}
