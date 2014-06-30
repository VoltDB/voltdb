package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSqlInsertSuite extends RegressionSuite {

    final long NULL_VALUE = -1;

    private void verifyResult(ClientResponse resp, long... args) {

        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable vt = resp.getResults()[0];

        assertTrue(vt.advanceRow());
        for (int i = 0; i < args.length; ++i) {
            if (args[i] == NULL_VALUE) {
                vt.getLong(i);
                assertTrue(vt.wasNull());
            } else {
                assertEquals(args[i], vt.getLong(i));
            }
        }
    }

    private void validateInsertStmt(String insertStmt, long... expectedValues) throws IOException, ProcCallException {
        Client client = getClient();
        ClientResponse cr;

        cr = client.callProcedure("@AdHoc", insertStmt);
        verifyResult(cr, 1);
        cr = client.callProcedure("selectAll");
        verifyResult(cr, expectedValues);
        cr = client.callProcedure("deleteAll");
        verifyResult(cr, 1);
    }

    private void verifyFails(String stmt, String expectedMsg) throws IOException {
        Client client = getClient();

        String msg = "no exception thrown";
        try {
            client.callProcedure("@AdHoc", stmt);
        }
        catch (ProcCallException pce) {
            msg = pce.getMessage();
        }

        assertTrue(msg.contains(expectedMsg));
    }

    public void testInsert() throws IOException, ProcCallException
    {

        // test with no fields provided (all column values must be provided)
        validateInsertStmt("insert into p1 values (1, 2, 3, 4, 5, 6);",
                1, 2, 3, 4, 5, 6);

        // not enough values
        verifyFails("insert into p1 values (1, 2, 3);", "row column count mismatch");

        // test with all fields specified (in order)
        validateInsertStmt("insert into p1 (ccc, bbb, aaa, zzz, yyy, xxx) values (1, 2, 3, 4, 5, 6);",
                1, 2, 3, 4, 5, 6);

        // test with all fields specified with permuted order
        validateInsertStmt("insert into p1 (xxx, zzz, bbb, ccc, yyy, aaa) values (1, 2, 3, 4, 5, 6);",
                4, 3, 6, 2, 5, 1);

        // test with some fields specified (in order)
        validateInsertStmt("insert into p1 (bbb, aaa, zzz) values (1024, 2048, 4096);",
                10, 1024, 2048, 4096, 14, NULL_VALUE);

        // test with some fields specified with permuted order
        validateInsertStmt("insert into p1 (zzz, bbb, xxx) values (555, 666, 777);",
                10, 666, 12, 555, 14, 777);

        // test with no values provided for NOT NULL columns
        // explicitly set not null field to null.
        verifyFails("insert into p1 (ccc, zzz) values (null, 7);", "CONSTRAINT VIOLATION");

        // try to insert into not null column with no default value
        verifyFails("insert into p1 (ccc) values (32)", "Column ZZZ has no default and is not nullable");
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlInsertSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestSqlInsertSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE P1 ( " +
                "ccc bigint default 10 not null, " +
                "bbb bigint default 11, " +
                "aaa bigint default 12, " +
                "zzz bigint not null, " +
                "yyy bigint default 14, " +
                "xxx bigint " + // default null
                ");" +
                "PARTITION TABLE P1 ON COLUMN ccc;" +

                "create procedure selectAll as select * from p1;" +
                "create procedure deleteAll as delete from p1;" +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        boolean success;
        config = new LocalCluster("sqlinsert-onesite.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("sqlinsert-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
