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
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.VoltDecimalHelper;

public class TestDecimalRoundingSuite extends RegressionSuite {

    private static int m_defaultScale = 12;
    private static String m_roundingEnabledProperty = "BIGDECIMAL_ROUND";
    private static String m_roundingModeProperty = "BIGDECIMAL_ROUND_POLICY";
    private static String m_defaultRoundingEnablement = "true";
    private static String m_defaultRoundingMode = "HALF_UP";

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestDecimalRoundingSuite(String name) {
        super(name);
    }


    private static String getRoundingString(String label) {
        return String.format("%sRounding %senabled, mode is %s",
                             label == null ? (label + ": ") : "",
                             VoltDecimalHelper.isRoundingEnabled() ? "is " : "is *NOT* ",
                             VoltDecimalHelper.getRoundingMode().toString());
    }
    private void validateInsertStmt(boolean expectSuccess, String insertStmt, BigDecimal... expectedValues) throws Exception {
        Client client = getClient();

        boolean success;
        try {
            validateTableOfLongs(client, insertStmt, new long[][]{{1}});
            success = true;
        } catch (Exception ex) {
            success = false;
        }
        assertEquals(getRoundingString("Insert Statement Failure"), success, expectSuccess);
        if (!success) {
            return;
        }
        validateTableOfDecimal(client, "select * from decimaltable;", new BigDecimal[][] {expectedValues});
        if (success) {
            validateTableOfLongs(client, "delete from decimaltable;", new long[][] {{1}});
        }
    }

    public void testDecimalScaleInsertion() throws Exception {
        Boolean roundIsEnabled = Boolean.valueOf(m_defaultRoundingEnablement);
        RoundingMode roundMode = RoundingMode.valueOf(m_defaultRoundingMode);

        assert(m_config instanceof LocalCluster);
        LocalCluster localCluster = (LocalCluster)m_config;
        Map<String, String> props = localCluster.getAdditionalProcessEnv();
        if (props != null) {
            roundIsEnabled = Boolean.valueOf(props.containsKey(m_roundingEnabledProperty) ? props.get(m_roundingEnabledProperty) : "true");
            roundMode = RoundingMode.valueOf(props.containsKey(m_roundingModeProperty) ? props.get(m_roundingModeProperty) : "HALF_UP");
            System.out.printf("Rounding is %senabled, mode is %s\n",
                              roundIsEnabled ? "" : "not ",
                              roundMode.toString());
        } else {
            System.out.printf("Default rounding (%s), Default Rounding mode (%s).\n",
                              roundIsEnabled.toString(), roundMode.toString());
        }
        doTestDecimalScaleInsertion(roundIsEnabled, roundMode);
    }

    /*
     * This little helper function converts a string to
     * a decimal, and, maybe, rounds it to the Volt default scale
     * using the given mode.  If roundingEnabled is false, no
     * rounding is done.
     */
    private static final BigDecimal roundDecimalValue(String  decimalValueString,
                                                      boolean roundingEnabled,
                                                      RoundingMode mode) {
        BigDecimal bd = new BigDecimal(decimalValueString);
        if (!roundingEnabled) {
            return bd;
        }
        int precision = bd.precision();
        int scale = bd.scale();
        int lostScale = scale - m_defaultScale ;
        if (lostScale <= 0) {
            return bd;
        }
        int newPrecision = precision - lostScale;
        MathContext mc = new MathContext(newPrecision, mode);
        BigDecimal nbd = bd.round(mc);
        assertTrue(nbd.scale() <= m_defaultScale);
        if (nbd.scale() != m_defaultScale) {
            nbd = nbd.setScale(m_defaultScale);
        }
        assertEquals(getRoundingString("Decimal Scale setting failure"), m_defaultScale, nbd.scale());
        return nbd;
    }

    private void doTestDecimalScaleInsertion(boolean roundingEnabled,
                                             RoundingMode mode) throws Exception {
        ClientConfig.setRoundingConfig(roundingEnabled, mode);
        // Sanity check.  See if we can insert a vanilla value.
        validateInsertStmt(true,
                           "insert into decimaltable values 0.9;",
                           roundDecimalValue("0.900000000000",
                                             roundingEnabled,
                                             mode));
        // See if we can insert an overscale number, and that we
        // round appropriately.
        validateInsertStmt(roundingEnabled,
                           "insert into decimaltable values 0.999999999999999;",
                           roundDecimalValue("0.999999999999999",
                                             roundingEnabled,
                                             mode));
        // Do the same as the last time, but make the last digit equal to 5.
        // This should round up.
        validateInsertStmt(roundingEnabled,
                           "insert into decimaltable values 0.999999999999500;",
                           roundDecimalValue("0.999999999999500", roundingEnabled, mode));
        // Do the same as the last time, but make the last digit equal to 4.
        // This should round down.
        validateInsertStmt(roundingEnabled,
                           "insert into decimaltable values 0.9999999999994000;",
                           roundDecimalValue("0.9999999999994000",
                                             roundingEnabled,
                                             mode));
        // Rounding gives the an extra digit of precision.  Make sure
        // that we don't take it from the scale.
        validateInsertStmt(roundingEnabled,
                           "insert into decimaltable values 9.9999999999999999;",
                           roundDecimalValue("9.9999999999999999",
                                             roundingEnabled,
                                             mode));
        // Rounding here does *not* give an extra digit of precision.  Make sure
        // that we still get the expected scale.
        validateInsertStmt(roundingEnabled,
                           "insert into decimaltable values 9.4999999999999999;",
                           roundDecimalValue("9.4999999999999999",
                                             roundingEnabled,
                                             mode));
        //
        // Test negative numbers.
        //
        // Rounding gives the an extra digit of precision.  Make sure
        // that we don't take it from the scale.
        validateInsertStmt(roundingEnabled,
                           "insert into decimaltable values -9.9999999999999999;",
                           roundDecimalValue("-9.9999999999999999", roundingEnabled, mode));
        // Rounding here does *not* give an extra digit of precision.  Make sure
        // that we still get the expected scale.
        validateInsertStmt(roundingEnabled,
                           "insert into decimaltable values -9.4999999999999999;",
                           roundDecimalValue("-9.4999999999999999", roundingEnabled, mode));
        validateInsertStmt(true,
                           "insert into decimaltable values null;", (BigDecimal)null);

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
        validateDecimalInsertStmt(roundingEnabled,
                                  "INSERT_DECIMAL", "insert into decimaltable values ?",
                                  new BigDecimal("9.9999999999999999"),
                                  "FETCH_DECIMAL",  "select dec from decimaltable;",
                                  roundDecimalValue("9.9999999999999999", roundingEnabled, mode),
                                  "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal with 5 in the 13th digit.  Round up.
        validateDecimalInsertStmt(roundingEnabled,
                                  "INSERT_DECIMAL", "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999995"),
                                    "FETCH_DECIMAL", "select dec from decimaltable;",
                                    roundDecimalValue("9.9999999999995", roundingEnabled, mode),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal with 4 in the 13th digit.  Round down.
        validateDecimalInsertStmt(roundingEnabled,
                                  "INSERT_DECIMAL", "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999994"),
                                    "FETCH_DECIMAL", "select dec from decimaltable;",
                                    roundDecimalValue("9.9999999999994", roundingEnabled, mode),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal with 3 in the 13th digit.  Round down.
        validateDecimalInsertStmt(roundingEnabled,
                                  "INSERT_DECIMAL", "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999993"),
                                    "FETCH_DECIMAL", "select dec from decimaltable;",
                                    roundDecimalValue("9.9999999999993", roundingEnabled, mode),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, then search for less then the rounded down value.
        // Expect to find nothing.
        validateDecimalInsertStmtAdHoc(roundingEnabled,
                                    "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999994"),
                                    "select dec from decimaltable where dec < 9.999999999999;",
                                    null,
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, then search for equal to the rounded down value.
        // Expect to find the rounded down row.
        validateDecimalInsertStmtAdHoc(roundingEnabled,
                                    "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999994"),
                                    "select dec from decimaltable where dec = 9.999999999999;",
                                    roundDecimalValue("9.9999999999994", roundingEnabled, mode),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, then search for equal to the inserted value.
        // Expect to find the rounded down row, because the 9.9...2 value in the
        // predicate is rounded as well.
        validateDecimalInsertStmtAdHoc(roundingEnabled,
                                    "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999992"),
                                    "select dec from decimaltable where dec = 9.9999999999992;",
                                    roundDecimalValue("9.9999999999992", roundingEnabled, mode),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, then search for less than the inserted value.
        // Expect to find nothing, because we inserted the rounded down value
        // and the predicate's right hand constant is rounded to the same value.
        validateDecimalInsertStmtAdHoc(roundingEnabled,
                                    "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999992"),
                                    "select dec from decimaltable where dec < 9.9999999999992;",
                                    null,
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert overscale decimal, search for the rounded down quantity exactly.
        // Expect to find the rounded down row.
        validateDecimalInsertStmtAdHoc(roundingEnabled,
                                    "insert into decimaltable values ?",
                                    new BigDecimal("9.9999999999993"),
                                    "select dec from decimaltable where dec = 9.999999999999;",
                                    roundDecimalValue("9.9999999999993", roundingEnabled, mode),
                                    "TRUNCATE TABLE DECIMALTABLE;");
        // Insert some constants.  Check that the rounded down inserted values
        // are truncated in the same way that the constants are.
        validateDecimalQuery(roundingEnabled,
                             "insert into decimaltable values 9.9999999999999999;",
                             "select dec from decimaltable where dec < 9.9999999999999999;",
                             "truncate table decimaltable;"
                             /* No answers expected */
                             );
        // Insert some constants.  Check that the rounded down inserted values
        // are truncated in the same way that the constants are.
        validateDecimalQuery(roundingEnabled,
                             "insert into decimaltable values 9.9999999999999999;",
                             "select dec from decimaltable where dec = 9.9999999999999999;",
                             "truncate table decimaltable;",
                             roundDecimalValue("9.9999999999999999", roundingEnabled, mode)
                             );
        if (roundingEnabled) {
            //
            // Make sure adding the smallest possible value to the
            // largest possible value causes an underflow.
            //
            Client client = getClient();
            ClientResponse cr = client.callProcedure("@AdHoc", "insert into decimaltable values 99999999999999999999999999.999999999999;");
            assertEquals(getRoundingString("Insert statement failure"), ClientResponse.SUCCESS, cr.getStatus());
            verifyStmtFails(client,
                            "select dec+0.000000000001 from decimaltable;",
                            "Attempted to add 99999999999999999999999999.999999999999 with 0.000000000001 causing overflow/underflow");
            cr = client.callProcedure("@AdHoc", "truncate table decimaltable;");
            assertEquals(getRoundingString("Table Cleanup failure"), ClientResponse.SUCCESS, cr.getStatus());

            //
            // Try it again with negative numbers.
            //
            cr = client.callProcedure("@AdHoc", "insert into decimaltable values -99999999999999999999999999.999999999999;");
            assertEquals(getRoundingString("insert statement failure"), ClientResponse.SUCCESS, cr.getStatus());
            verifyStmtFails(client,
                            "select dec-0.000000000001 from decimaltable;",
                            "Attempted to subtract 0.000000000001 from -99999999999999999999999999.999999999999 causing overflow/underflow");
            cr = client.callProcedure("@AdHoc", "truncate table decimaltable;");
            assertEquals(getRoundingString("Table Cleanup failure"), ClientResponse.SUCCESS, cr.getStatus());
        }
    }

    private void validateDecimalInsertStmt(boolean expectSuccess,
                                           String storedInsProcName,
                                           String adHocInsSQL,
                                           BigDecimal parameter,
                                           String storedProcQueryName,
                                           String adHocQuerySQL,
                                           BigDecimal expected,
                                           String cleanup) throws Exception {
        validateDecimalInsertStmtProcedure(expectSuccess, storedInsProcName, parameter, storedProcQueryName, expected, cleanup);
        validateDecimalInsertStmtAdHoc(expectSuccess, adHocInsSQL, parameter, adHocQuerySQL, expected, cleanup);
    }

    private void validateDecimalInsertStmtAdHoc(boolean expectSuccess,
                                                String insertStmt,
                                                BigDecimal insertValue,
                                                String fetchStmt,
                                                BigDecimal expected,
                                                String cleanupStmt) throws Exception {
        Client client = getClient();
        ClientResponse cr = null;
        boolean success;
        try {
            cr = client.callProcedure("@AdHoc", insertStmt, insertValue);
            success = true;
        } catch (Exception ex) {
            success = false;
        }
        assertEquals(getRoundingString("Insert statement Compilation Failure"), expectSuccess, success);
        if (!success) {
            return;
        }
        assertEquals(getRoundingString("Insert Statement Failure."), ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc", fetchStmt);
        assertEquals(getRoundingString("Fetch Data Failure"), ClientResponse.SUCCESS, cr.getStatus());
        VoltTable[] tbls = cr.getResults();
        assertEquals(getRoundingString("Volt Table Size Failure"), 1, tbls.length);
        int idx = 0;
        VoltTable tbl = tbls[0];
        while (tbl.advanceRow()) {
            BigDecimal actual = tbl.getDecimalAsBigDecimal(0);
            assertNotSame(getRoundingString("Unexpected null table."), null, expected);
            assertEquals(getRoundingString("Decimal Scale Failure"), expected, actual);
        }
        // A Null expected implies no results are expected.
        if (expected == null) {
            assertEquals(getRoundingString("Null expected:"), 0, idx);
        }
        cr = client.callProcedure("@AdHoc", cleanupStmt);
        assertEquals(getRoundingString("Cleanup Statement Failure"), ClientResponse.SUCCESS, cr.getStatus());
    }


    private void validateDecimalInsertStmtProcedure(boolean expectSuccess,
                                             String insertProcName,
                                             BigDecimal insertValue,
                                             String fetchProcName,
                                             BigDecimal expected,
                                             String cleanupProcedure) throws Exception {
        Client client = getClient();
        boolean success;
        ClientResponse cr = null;
        try {
            cr = client.callProcedure(insertProcName, insertValue);
            success = true;
        } catch (Exception ex) {
            success = false;
        }
        assertEquals(getRoundingString("Insert Statement Compilation Failure"), expectSuccess, success);
        if (!success) {
            return;
        }

        assertEquals(getRoundingString("Insert Statement Execution Failure"), ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(fetchProcName);
        assertEquals(getRoundingString("Fetch Data Failure"), ClientResponse.SUCCESS, cr.getStatus());
        VoltTable[] tbls = cr.getResults();
        assertEquals(getRoundingString("Number of results incorrect."), 1, tbls.length);
        VoltTable tbl = tbls[0];
        int idx = 0;
        while (tbl.advanceRow()) {
            BigDecimal actual = tbl.getDecimalAsBigDecimal(idx);
            assertNotSame(getRoundingString("Null Table Failure"), null, expected);
            assertEquals(getRoundingString("Decimal scale failure"), expected, actual);
        }
        if (expected == null) {
            assertEquals(getRoundingString("Empty Results Expected."), 0, idx);
        }
        cr = client.callProcedure("@AdHoc", cleanupProcedure);
        assertEquals(getRoundingString(null), ClientResponse.SUCCESS, cr.getStatus());
    }

    private void validateDecimalQuery(boolean expectSuccess,
                                      String insertStmt,
                                      String fetchStmt,
                                      String cleanupStmt,
                                      BigDecimal... expected) throws Exception {
        Client client = getClient();
        boolean success;
        ClientResponse cr = null;
        try {
            cr = client.callProcedure("@AdHoc", insertStmt);
            success = true;
        } catch (Exception ex) {
            success = false;
        }
        assertEquals(getRoundingString(null), expectSuccess, success);
        if (!success) {
            return;
        }
        assertEquals(getRoundingString(null), ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc", fetchStmt);
        assertEquals(getRoundingString(null), ClientResponse.SUCCESS, cr.getStatus());
        VoltTable[] resultTable = cr.getResults();
        int idx = 0;
        VoltTable tbl = resultTable[0];
        while (tbl.advanceRow()) {
            BigDecimal actual = tbl.getDecimalAsBigDecimal(0);
            assertTrue(idx < expected.length);
            assertEquals(getRoundingString(null), expected[idx], actual);
            idx += 1;
        }
        assertEquals(getRoundingString(null), idx, expected.length);
        cr = client.callProcedure("@AdHoc", cleanupStmt);
        assertEquals(getRoundingString("Cleanup statement failure"), ClientResponse.SUCCESS, cr.getStatus());
    }

    private final static Map<String, String> makePropertiesMap(String... entries) {
        assert(entries.length % 2 == 0);
        Map<String, String> answer = new HashMap<String, String>();
        for (int idx = 0; idx < entries.length; idx += 2) {
            answer.put(entries[idx], entries[idx+1]);
        }
        return answer;
    }

    private static void addConfig(int idx, MultiConfigSuiteBuilder builder, VoltProjectBuilder project, Map<String, String> properties) {
        LocalCluster config = null;
        config = new LocalCluster("sqlinsert-onesite.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI, properties);
        config.setPrefix(Integer.toString(idx));
        config.setHasLocalServer(false);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
    }

    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestDecimalRoundingSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE P1 ( id integer );" +
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
        int idx = 0;
        addConfig(idx++, builder, project, null);
        addConfig(idx++, builder, project, makePropertiesMap());
        addConfig(idx++, builder, project, makePropertiesMap(m_roundingEnabledProperty, "true"));
        addConfig(idx++, builder, project, makePropertiesMap(m_roundingEnabledProperty, "true", m_roundingModeProperty, "HALF_UP"));
        addConfig(idx++, builder, project, makePropertiesMap(m_roundingEnabledProperty, "true", m_roundingModeProperty, "HALF_DOWN"));
        addConfig(idx++, builder, project, makePropertiesMap(m_roundingEnabledProperty, "true", m_roundingModeProperty, "CEILING"));
        addConfig(idx++, builder, project, makePropertiesMap(m_roundingEnabledProperty, "true", m_roundingModeProperty, "FLOOR"));
        addConfig(idx++, builder, project, makePropertiesMap(m_roundingEnabledProperty, "true", m_roundingModeProperty, "UP"));
        addConfig(idx++, builder, project, makePropertiesMap(m_roundingEnabledProperty, "true", m_roundingModeProperty, "DOWN"));
        return builder;
    }
}
