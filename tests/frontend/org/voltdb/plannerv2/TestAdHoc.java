package org.voltdb.plannerv2;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;

public class TestAdHoc extends RegressionSuite {

    public TestAdHoc(String name) {
        super(name);
    }

    public void testCompileAdHoc() throws IOException, ProcCallException {
        Client client = getClient();
//        client.callProcedure("@AdHoc", "select a from t where a > 0;");
        client.callProcedure("@AdHoc", "select a from t where a > ? limit 2 offset ?;", 100, 2);
    }

    static public junit.framework.Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAdHoc.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("create table t(a int);");

        LocalCluster config = new LocalCluster("calcite-adhoc.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);
        return builder;
    }
}
