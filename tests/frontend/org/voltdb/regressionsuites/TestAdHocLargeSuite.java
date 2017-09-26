package org.voltdb.regressionsuites;

import java.util.Collections;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestAdHocLargeSuite extends RegressionSuite {

    public void testBasic() throws Exception {
        Client client = getClient();

        ClientResponse cr;
        cr = client.callProcedure("@AdHocLarge", "select count(*) from (select * from t as t1, t  as t2) as dtbl");
        assertEquals(0, cr.getResults()[0].asScalarLong());

        // Now add some data: 2 rows
        int rowCnt = 0;
        for (; rowCnt < 2; ++rowCnt) {
            String val = String.join("", Collections.nCopies(500000, "a"));
            cr = client.callProcedure("t.Insert", rowCnt, val);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

        // Query should still execute okay
        cr = client.callProcedure("@AdHocLarge", "select count(*) from (select * from t as t1, t  as t2) as dtbl");
        assertEquals(4, cr.getResults()[0].asScalarLong());

        // Add 8 more mores for a total of 10.
        for (; rowCnt < 10; ++rowCnt) {
            String val = String.join("", Collections.nCopies(500000, "a"));
            cr = client.callProcedure("t.Insert", rowCnt, val);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

        // Topend routines to store large temp table blocks just return false (failure to store)
        // So this error is expected.
        verifyProcFails(client, "Could not store a large temp table block to make space in cache",
                "@AdHocLarge",
                "select count(*) from (select * from t as t1, t  as t2) as dtbl");

        // Query succeeds if executed normally.
        cr = client.callProcedure("@AdHoc", "select count(*) from (select * from t as t1, t  as t2) as dtbl");
        assertEquals(100, cr.getResults()[0].asScalarLong());
    }

    static public junit.framework.Test suite() throws Exception {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAdHocLargeSuite.class);

        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("create table t (i integer not null, val varchar(500000));");

        config = new LocalCluster("adhoclarge-voltdbBackend.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        System.setProperty("TEMP_TABLE_MAX_SIZE", "25"); // in MB
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }

    public TestAdHocLargeSuite(String name) {
        super(name);
    }
}
