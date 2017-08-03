package org.voltdb.regressionsuites;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestAdHocLargeSuite extends RegressionSuite {

    public void testBasic() throws Exception {
        Client client = getClient();

        client.callProcedure("@AdHoc", "create table t (i integer not null);");

        ClientResponse cr = client.callProcedure("@AdHocLarge", "select count(*) from t");
        assertEquals(0, cr.getResults()[0].asScalarLong());
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAdHocLargeSuite.class);

        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        config = new LocalCluster("adhoclarge-voltdbBackend.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }

    public TestAdHocLargeSuite(String name) {
        super(name);
    }
}
