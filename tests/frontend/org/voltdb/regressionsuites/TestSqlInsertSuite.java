/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.math.BigDecimal;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSqlInsertSuite extends RegressionSuite {

    private void validateInsertStmt(String insertStmt, long... expectedValues) throws Exception {
        Client client = getClient();

        validateTableOfLongs(client, insertStmt, new long[][] {{1}});
        validateTableOfLongs(client, "select * from p1", new long[][] {expectedValues});
        validateTableOfLongs(client, "delete from p1;", new long[][] {{1}});
    }

    private void validateInsertStmt(String insertStmt, BigDecimal... expectedValues) throws Exception {
        Client client = getClient();

        validateTableOfLongs(client, insertStmt, new long[][]{{1}});
        validateTableOfDecimal(client, "select * from decimaltable;", new BigDecimal[][] {expectedValues});
        validateTableOfLongs(client, "delete from decimaltable;", new long[][] {{1}});
    }

    public void testInsert() throws Exception
    {

        // test with no fields provided (all column values must be provided)
        validateInsertStmt("insert into p1 values (1, 2, 3, 4, 5, 6);",
                1, 2, 3, 4, 5, 6);

        // not enough values
        verifyStmtFails(getClient(), "insert into p1 values (1, 2, 3);", "row column count mismatch");

        // test with all fields specified (in order)
        validateInsertStmt("insert into p1 (ccc, bbb, aaa, zzz, yyy, xxx) values (1, 2, 3, 4, 5, 6);",
                1, 2, 3, 4, 5, 6);

        // test with all fields specified with permuted order
        validateInsertStmt("insert into p1 (xxx, zzz, bbb, ccc, yyy, aaa) values (1, 2, 3, 4, 5, 6);",
                4, 3, 6, 2, 5, 1);

        // test with some fields specified (in order)
        validateInsertStmt("insert into p1 (bbb, aaa, zzz) values (1024, 2048, 4096);",
                10, 1024, 2048, 4096, 14, Long.MIN_VALUE);

        // test with some fields specified with permuted order
        validateInsertStmt("insert into p1 (zzz, bbb, xxx) values (555, 666, 777);",
                10, 666, 12, 555, 14, 777);

        // test with no values provided for NOT NULL columns
        // explicitly set not null field to null.
        verifyStmtFails(getClient(), "insert into p1 (ccc, zzz) values (null, 7);", "CONSTRAINT VIOLATION");

        // try to insert into not null column with no default value
        verifyStmtFails(getClient(), "insert into p1 (ccc) values (32)", "Column ZZZ has no default and is not nullable");
    }


    public void testDecimalScaleInsertion() throws Exception {
        // Sanity check.  See if we can insert a vanilla value.
        validateInsertStmt("insert into decimaltable values 0.9;",
                           new BigDecimal("0.900000000000"));
        // See if we can insert a value bigger then the fixed point
        // scale, and that we round up.
        validateInsertStmt("insert into decimaltable values 0.999999999999999;",
                           new BigDecimal("1.000000000000"));
        // Do the same as the last time, but make the last digit equal to 5.
        // This should round up.
        validateInsertStmt("insert into decimaltable values 0.999999999999500;",
                           new BigDecimal("1.000000000000"));
        // Do the same as the last time, but make the last digit equal to 4.
        // This should round down.
        validateInsertStmt("insert into decimaltable values 0.9999999999994000;",
                           new BigDecimal("0.999999999999"));
        // Rounding gives the an extra digit of precision.  Make sure
        // that we don't take it from the scale.
        validateInsertStmt("insert into decimaltable values 9.9999999999999999;",
                           new BigDecimal("10.000000000000"));
        // Rounding here does *not* give an extra digit of precision.  Make sure
        // that we still get the expected scale.
        validateInsertStmt("insert into decimaltable values 9.4999999999999999;",
                           new BigDecimal("9.500000000000"));
        //
        // Test negative numbers.
        //
        // Rounding gives the an extra digit of precision.  Make sure
        // that we don't take it from the scale.
        validateInsertStmt("insert into decimaltable values -9.9999999999999999;",
                           new BigDecimal("-10.000000000000"));
        // Rounding here does *not* give an extra digit of precision.  Make sure
        // that we still get the expected scale.
        validateInsertStmt("insert into decimaltable values -9.4999999999999999;",
                           new BigDecimal("-9.500000000000"));
        validateInsertStmt("insert into decimaltable values null;", (BigDecimal)null);

        //
        // For these tests we give both a stored procedure and the
        // equivalent ad-hoc sql for an insertion and a query.
        // We execute the stored procedure insertion and query statements
        // and then the ad hoc procedure and query statements back
        // to back.  After each we execute the clean procedure.  That
        // is, we execute:
        //    callStoredInsertProcedure
        //    callStoredQueryProcerue
        //    test that the queried value is what we expect
        //    cleanup the table
        //    callAdHocInsertProcedure
        //    callAdHocQueryProcedure
        //    test that the queried value is what we expect
        //    cleanup the table.

        // Insert overscale decimal.  Round up.
        validateDecimalInsertStmt("INSERT_DECIMAL", "insert into decimaltable values ?",
                                  new BigDecimal("9.9999999999999999"),
                                  "FETCH_DECIMAL",  "select dec from decimaltable;",
                                  new BigDecimal("10.000000000000"),
                                  "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal with 5 in the 13th digit.  Round up.
        validateDecimalInsertStmt("INSERT_DECIMAL", "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999995"),
                                    "FETCH_DECIMAL", "select dec from decimaltable;",
                                    new BigDecimal("10.000000000000"),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal with 4 in the 13th digit.  Round down.
        validateDecimalInsertStmt("INSERT_DECIMAL", "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999994"),
                                    "FETCH_DECIMAL", "select dec from decimaltable;",
                                    new BigDecimal("9.999999999999"),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal with 3 in the 13th digit.  Round down.
        validateDecimalInsertStmt("INSERT_DECIMAL", "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999993"),
                                    "FETCH_DECIMAL", "select dec from decimaltable;",
                                    new BigDecimal("9.999999999999"),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, then search for less then the rounded down value.
        // Expect to find nothing.
        validateDecimalInsertStmtAdHoc("insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999994"),
                                    "select dec from decimaltable where dec < 9.999999999999;",
                                    null,
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, then search for equal to the rounded down value.
        // Expect to find the rounded down row.
        validateDecimalInsertStmtAdHoc("insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999994"),
                                    "select dec from decimaltable where dec = 9.999999999999;",
                                    new BigDecimal("9.999999999999"),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, then search for equal to the inserted value.
        // Expect to find the rounded down row, because the 9.9...2 value in the
        // predicate is rounded as well.
        validateDecimalInsertStmtAdHoc("insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999992"),
                                    "select dec from decimaltable where dec = 9.9999999999992;",
                                    new BigDecimal("9.999999999999"),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, then search for less than the inserted value.
        // Expect to find nothing, because we inserted the rounded down value
        // and the predicate's right hand constant is rounded to the same value.
        validateDecimalInsertStmtAdHoc("insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999992"),
                                    "select dec from decimaltable where dec < 9.9999999999992;",
                                    null,
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, search for the rounded down quantity exactly.
        // Expect to find the rounded down row.
        validateDecimalInsertStmtAdHoc("insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999993"),
                                    "select dec from decimaltable where dec = 9.999999999999;",
                                    new BigDecimal("9.999999999999"),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert some constants.  Check that the rounded down inserted values
        // are truncated in the same way that the constants are.
        validateDecimalQuery("insert into decimaltable values 9.9999999999999999;",
                             "select dec from decimaltable where dec < 9.9999999999999999;",
                             "truncate table decimaltable;"
                             /* No answers expected */
                             );
        // Insert some constants.  Check that the rounded down inserted values
        // are truncated in the same way that the constants are.
        validateDecimalQuery("insert into decimaltable values 9.9999999999999999;",
                             "select dec from decimaltable where dec = 9.9999999999999999;",
                             "truncate table decimaltable;",
                             new BigDecimal("10.000000000000")
                             );
        //
        // Make sure adding the smallest possible value to the
        // largest possible value causes an underflow.
        //
        Client client = getClient();
        ClientResponse cr = client.callProcedure("@AdHoc", "insert into decimaltable values 99999999999999999999999999.999999999999;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        verifyStmtFails(client,
                        "select dec+0.000000000001 from decimaltable;",
                        "Attempted to add 99999999999999999999999999.999999999999 with 0.000000000001 causing overflow/underflow");
        cr = client.callProcedure("@AdHoc", "truncate table decimaltable;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        //
        // Try it again with negative numbers.
        //
        cr = client.callProcedure("@AdHoc", "insert into decimaltable values -99999999999999999999999999.999999999999;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        verifyStmtFails(client,
                        "select dec-0.000000000001 from decimaltable;",
                        "Attempted to subtract 0.000000000001 from -99999999999999999999999999.999999999999 causing overflow/underflow");
        cr = client.callProcedure("@AdHoc", "truncate table decimaltable;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void validateDecimalInsertStmt(String storedInsProcName,
                                           String adHocInsSQL,
                                           BigDecimal parameter,
                                           String storedProcQueryName,
                                           String adHocQuerySQL,
                                           BigDecimal expected,
                                           String cleanup) throws Exception {
        validateDecimalInsertStmtProcedure(storedInsProcName, parameter, storedProcQueryName, expected, cleanup);
        validateDecimalInsertStmtAdHoc(adHocInsSQL, parameter, adHocQuerySQL, expected, cleanup);
    }

    private void validateDecimalInsertStmtAdHoc(String insertStmt,
                                           BigDecimal insertValue,
                                           String fetchStmt,
                                           BigDecimal expected,
                                           String cleanupStmt) throws Exception {
        Client client = getClient();
        ClientResponse cr = client.callProcedure("@AdHoc", insertStmt, insertValue);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc", fetchStmt);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable[] tbls = cr.getResults();
        assertEquals(1, tbls.length);
        int idx = 0;
        VoltTable tbl = tbls[0];
        while (tbl.advanceRow()) {
            BigDecimal actual = tbl.getDecimalAsBigDecimal(0);
            assertNotSame(null, expected);
            assertEquals(expected, actual);
        }
        // A Null expected implies no results are expected.
        if (expected == null) {
            assertEquals(0, idx);
        }
        cr = client.callProcedure("@AdHoc", cleanupStmt);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }


    private void validateDecimalInsertStmtProcedure(String insertProcName,
                                             BigDecimal insertValue,
                                             String fetchProcName,
                                             BigDecimal expected,
                                             String cleanupProcedure) throws Exception {
        Client client = getClient();
        ClientResponse cr = client.callProcedure(insertProcName, insertValue);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(fetchProcName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable[] tbls = cr.getResults();
        assertEquals(1, tbls.length);
        VoltTable tbl = tbls[0];
        int idx = 0;
        while (tbl.advanceRow()) {
            BigDecimal actual = tbl.getDecimalAsBigDecimal(idx);
            assertNotSame(null, expected);
            assertEquals(expected, actual);
        }
        if (expected == null) {
            assertEquals(0, idx);
        }
        cr = client.callProcedure("@AdHoc", cleanupProcedure);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void validateDecimalQuery(String insertStmt,
                                      String fetchStmt,
                                      String cleanupStmt,
                                      BigDecimal... expected) throws Exception {
        Client client = getClient();
        ClientResponse cr = client.callProcedure("@AdHoc", insertStmt);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc", fetchStmt);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable[] resultTable = cr.getResults();
        int idx = 0;
        VoltTable tbl = resultTable[0];
        while (tbl.advanceRow()) {
            BigDecimal actual = tbl.getDecimalAsBigDecimal(0);
            assertTrue(idx < expected.length);
            assertEquals(expected[idx], actual);
            idx += 1;
        }
        assertEquals(idx, expected.length);
        cr = client.callProcedure("@AdHoc", cleanupStmt);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlInsertSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSqlInsertSuite.class);
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
                "CREATE TABLE DECIMALTABLE ( " +
                "dec decimal" +
                ");" +
                "CREATE PROCEDURE INSERT_DECIMAL AS " +
                "INSERT INTO DECIMALTABLE VALUES ?;" +
                "CREATE PROCEDURE FETCH_DECIMAL AS " +
                "SELECT DEC FROM DECIMALTABLE;" +
                "CREATE PROCEDURE TRUNCATE_DECIMAL AS " +
                "TRUNCATE TABLE DECIMALTABLE;" +
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
