/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestDecimalRoundingSuite extends RegressionSuite {

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestDecimalRoundingSuite(String name) {
        super(name);
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

            //
            // Try it with unrepresentable numbers.
            //
            String positiveTest = "insert into decimaltable values 99999999999999999999999999.9999999999995;";
            String positiveTestMsg = "SQL error while compiling query: " +
                                     "Decimal 100000000000000000000000000.000000000000 has more than 38 digits of precision.";
            String negativeTest = "insert into decimaltable values -99999999999999999999999999.9999999999995;";
            String negativeTestMsg = "SQL error while compiling query: " +
                                     "Decimal -100000000000000000000000000.000000000000 has more than 38 digits of precision.";
            switch (mode) {
            case HALF_DOWN:
            case DOWN:
                // Rounding DOWN always rounds towards zero.  So, we always get a
                // representable number.
                cr = client.callProcedure("@AdHoc", positiveTest);
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                cr = client.callProcedure("@AdHoc", negativeTest);
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                break;
            case FLOOR:
                // Floor rounds to the next lowest fixed point number.  So,
                // we never get positive overflow, but we may get negative overflow.
                cr = client.callProcedure("@AdHoc", positiveTest);
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                verifyStmtFails(client, negativeTest, negativeTestMsg);
                break;
            case CEILING:
                // Ceiling rounds to the next highest representable number.  So
                // we never get negative overflow, but we may get positive
                // overflow.
                cr = client.callProcedure("@AdHoc", negativeTest);
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                verifyStmtFails(client, positiveTest, positiveTestMsg);
                break;
            case HALF_UP:
            case UP:
                verifyStmtFails(client, positiveTest, positiveTestMsg);
                verifyStmtFails(client, negativeTest, negativeTestMsg);
                break;
            default:
                // Missed Case.  We only test six of the eight possible
                // rounding modes.  This is here in case the test is expanded.
                fail("Missed Rounding Case");
            }
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
        assertEquals(getRoundingString("Insert statement Invocation Failure"), expectSuccess, success);
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

    public void testEEDecimalScale() throws Exception {
        Boolean roundIsEnabled = Boolean.valueOf(m_defaultRoundingEnablement);
        RoundingMode roundMode = RoundingMode.valueOf(m_defaultRoundingMode);

        assert(m_config instanceof LocalCluster);
        LocalCluster localCluster = (LocalCluster)m_config;
        Map<String, String> props = localCluster.getAdditionalProcessEnv();
        if (props != null) {
            roundIsEnabled = Boolean.valueOf(props.containsKey(m_roundingEnabledProperty) ? props.get(m_roundingEnabledProperty) : "true");
            roundMode = RoundingMode.valueOf(props.containsKey(m_roundingModeProperty) ? props.get(m_roundingModeProperty) : "HALF_UP");
        }
        doTestEEDecimalScale(roundIsEnabled, roundMode);
    }

    private void doTestEEDecimalScale(boolean roundEnabled, RoundingMode roundMode) throws Exception {
        // We currently only support one rounding mode in the EE, and
        // we always round.
        if (roundEnabled && roundMode == RoundingMode.HALF_UP) {
            Client client = getClient();
            ClientResponse cr;
            String[] values = new String[] {
                // Don't round.
                "0.8999999999994",
                // Do round.
                "0.8999999999995",
                // Do round to the left of the dot.
                "0.9999999999995",
                // Do round to the left of the dot with non-zero integer part.
                "1.9999999999995",
                // Do round to the left of the dot with more digits.
                "99.9999999999995",
                // Do round to the left of the dot with no more digits.
                "98.9999999999995",
                // Don't round, but the result is the largest
                // representable decimal value.
                "99999999999999999999999999.9999999999994999999",
                // The following cases replicate the cases above,
                // but with a sign.
                "-0.8999999999994",
                "-0.8999999999995",
                "-0.9999999999995",
                "-1.9999999999995",
                "-99.9999999999995",
                "-98.9999999999995",
                "-99999999999999999999999999.9999999999994999999"
            };
            String[] badValues = new String[] {
                // Too many integer digits.
                "999999999999999999999999999999.0",
                // Round to an unrepresentable value.
                "99999999999999999999999999.9999999999995999999",
                // Round to an unrepresentable value.
                "-99999999999999999999999999.9999999999995999999"
            };
            // Insert some data.
            for (String val : values) {
                cr = client.callProcedure("EEDecimal.Insert", val);
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            }
            // Insert some bad data.
            // We will find this is bad later on.
            for (int idx = 0; idx < badValues.length; idx += 1) {
                String val = badValues[idx];
                cr = client.callProcedure("EEBadDecimal.Insert", idx, val);
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            }
            // Query the data.  Just get them all for now.
            cr = client.callProcedure("EEDecimalFetch");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            VoltTable tbl = cr.getResults()[0];
            while (tbl.advanceRow()) {
                String expectedStr = tbl.getString(0);
                BigDecimal rounded = tbl.getDecimalAsBigDecimal(1);
                BigDecimal expected = roundDecimalValue(expectedStr, roundEnabled, roundMode);
                assertEquals(expected, rounded);
            }
            // Query the data for bad values.  Get them one at a time,
            // because they are all bad in their own way.
            for (int idx = 0; idx < badValues.length; idx += 1) {
                try {
                    cr = client.callProcedure("EEBadDecimalFetch", idx);
                    fail(String.format("Unexpected success, case %d, decimal string \"%s\"",
                         idx, badValues[idx]));
                } catch (ProcCallException ex) {
                    assertTrue(true);
                }
            }
        } else {
            assertTrue(true);
        }
    }

    /**
     * Test rounding on columns with decimal default.  HSQLDB does not give us the
     * right value for decimal strings, so until ENG-8557 is
     * @param roundEnabled
     * @param roundMode
     * @throws Exception
     */
    public final void notestDecimalDefault(boolean roundEnabled, RoundingMode roundMode) throws Exception {
                    validateDecimalDefault("pRoundDecimalDownNC", false, roundEnabled, roundMode,  "0.8999999999994");
            validateDecimalDefault("pRoundDecimalUpNC",   false, roundEnabled, roundMode,  "0.8999999999995");
            validateDecimalDefault("pRoundDecimalDownC",  false, roundEnabled, roundMode,  "0.9999999999994");
            validateDecimalDefault("pRoundDecimalUpC",    false, roundEnabled, roundMode,  "0.9999999999995");
            validateDecimalDefault("pRoundDecimalDownC2", false, roundEnabled, roundMode, "99.9999999999994");
            validateDecimalDefault("pRoundDecimalUpC2",   false, roundEnabled, roundMode, "99.9999999999995");
            validateDecimalDefault("pRoundDecimalMax",    false, roundEnabled, roundMode, "99999999999999999999999999.9999999999994");
            validateDecimalDefault("pRoundDecimalNotRep", true,  roundEnabled, roundMode, "99999999999999999999999999.9999999999995");
            validateDecimalDefault("nRoundDecimalDownNC", false, roundEnabled, roundMode,  "-0.8999999999994");
            validateDecimalDefault("nRoundDecimalUpNC",   false, roundEnabled, roundMode,  "-0.8999999999995");
            validateDecimalDefault("nRoundDecimalDownC",  false, roundEnabled, roundMode,  "-0.9999999999994");
            validateDecimalDefault("nRoundDecimalUpC",    false, roundEnabled, roundMode,  "-0.9999999999995");
            validateDecimalDefault("nRoundDecimalDownC2", false, roundEnabled, roundMode, "-99.9999999999994");
            validateDecimalDefault("nRoundDecimalUpC2",   false, roundEnabled, roundMode, "-99.9999999999995");
            validateDecimalDefault("nRoundDecimalMax",    false, roundEnabled, roundMode, "-99999999999999999999999999.9999999999994");
            validateDecimalDefault("nRoundDecimalNotRep", true,  roundEnabled, roundMode, "-99999999999999999999999999.9999999999995");

    }
    private final void validateDecimalDefault(String tableName,
                                              boolean shouldFail,
                                              boolean roundEnabled,
                                              RoundingMode roundMode,
                                              String value) throws Exception {
        Client client = getClient();
        ClientResponse cr;
        boolean sawFail = false;
        VoltTable tbl = null;
        BigDecimal found = null;
        try {
            cr = client.callProcedure("@AdHoc", String.format("insert into %s (id) values ?;", tableName), 100);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            sawFail = false;
            cr = client.callProcedure("@AdHoc", String.format("select (dec) from %s;", tableName));
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            tbl = cr.getResults()[0];
            assertTrue(tbl.advanceRow());
            found = tbl.getDecimalAsBigDecimal(0);
        } catch (ProcCallException ex) {
            sawFail = true;
        }
        assertEquals(shouldFail ? "Expected a failure here" : "Unexpected failure here",
                    shouldFail, sawFail);
        if (shouldFail) {
            return;
        }
        BigDecimal expected = roundDecimalValue(value, roundEnabled, roundMode);
        assertEquals(String.format("Default decimal value failed: rounding is %s, mode is %s",
                                  roundEnabled ? "enabled" : "not enabled",
                                  roundMode),
                    expected,
                    found);

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
        String renabled = (properties != null) ? properties.get(m_roundingEnabledProperty) : "defEnabled";
        String rmode = (properties != null) ? properties.get(m_roundingModeProperty) : "defEnabled";
        if (renabled == null) {
            renabled = "defEnabled";
        }
        if (rmode == null) {
            rmode = "defMode";
        }
        config.setPrefix(renabled + "-" + rmode);
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
                    "dec decimal"     +
                ");" +
                "CREATE PROCEDURE INSERT_DECIMAL AS " +
                "INSERT INTO DECIMALTABLE VALUES ?;" +
                "CREATE PROCEDURE FETCH_DECIMAL AS " +
                "SELECT DEC FROM DECIMALTABLE;" +
                "CREATE PROCEDURE TRUNCATE_DECIMAL AS " +
                "TRUNCATE TABLE DECIMALTABLE;" +
                "create table EEDecimal (" +
                "  valueIn    varChar(128)" +
                ");" +
                "create procedure EEDecimalFetch as " +
                "select valueIn, cast(valueIn as decimal) from EEDecimal;" +
                "create table EEBadDecimal (" +
                "  id integer primary key not null," +
                "  valueIn    varChar(128)" +
                ");" +
                "create procedure EEBadDecimalFetch as " +
                "select valueIn, cast(valueIn as decimal) from EEBadDecimal where id = ?;" +
                "create table pRoundDecimalDownNC ( id integer, dec decimal default  '0.8999999999994' );" +
                "create table pRoundDecimalUpNC   ( id integer, dec decimal default  '0.8999999999995' );" +
                "create table pRoundDecimalDownC  ( id integer, dec decimal default  '0.9999999999994' );" +
                "create table pRoundDecimalUpC    ( id integer, dec decimal default  '0.9999999999995' );" +
                "create table pRoundDecimalDownC2 ( id integer, dec decimal default '99.9999999999994' );" +
                "create table pRoundDecimalUpC2   ( id integer, dec decimal default '99.9999999999995' );" +
                "create table pRoundDecimalMax    ( id integer, dec decimal default '99999999999999999999999999.9999999999994' );" +
                "create table pRoundDecimalNotRep ( id integer, dec decimal default '99999999999999999999999999.9999999999995' );" +
                "create table nRoundDecimalDownNC ( id integer, dec decimal default  '-0.8999999999994' );" +
                "create table nRoundDecimalUpNC   ( id integer, dec decimal default  '-0.8999999999995' );" +
                "create table nRoundDecimalDownC  ( id integer, dec decimal default  '-0.9999999999994' );" +
                "create table nRoundDecimalUpC    ( id integer, dec decimal default  '-0.9999999999995' );" +
                "create table nRoundDecimalDownC2 ( id integer, dec decimal default '-99.9999999999994' );" +
                "create table nRoundDecimalUpC2   ( id integer, dec decimal default '-99.9999999999995' );" +
                "create table nRoundDecimalMax    ( id integer, dec decimal default '-99999999999999999999999999.9999999999994' );" +
                "create table nRoundDecimalNotRep ( id integer, dec decimal default '-99999999999999999999999999.9999999999995' );" +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        int idx = 0;
        addConfig(idx++, builder, project, null);
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
