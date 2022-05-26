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
import java.sql.Timestamp;
import java.util.*;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestFunctionsSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    public TestFunctionsSuite(String name) {
        super(name);
    }

    // NOTE: Avoid trouble by keeping values in order, half-negative, then zero, then half-positive.
    private static final double orderedIds[] = { -125, -25, 0, 25, 125 };
    private static final int ROWCOUNT = orderedIds.length;

    private static final double orderedByStringIds[] = { -125, -25, 0, 125, 25 };
    private static final double orderedByFloatStringIds[] = { -125, -25, 0, 125, 25 };
    private static final double orderedByDecimalStringIds[] = { -25, -125, 0, 25, 125  };

    // NOTE: Be careful about how this array may be indexed by hard-coded column numbers sprinkled through the code
    // -- and especially keep the non-integer columns last.
    // These correspond to the column types of table NUMBER_TYPES, in the order that they are defined and typically selected.
    private static String numTypeNames[] = { "INTEGER", "TINYINT", "SMALLINT", "BIGINT", "FLOAT", "DECIMAL" };
    private static String numFormatNames[] = { "LONG",  "LONG", "LONG", "LONG", "DOUBLE", "DECIMAL" };
    private static final int COLUMNCOUNT = numTypeNames.length;
    private static final int FLOATCOLINDEX = 4;
    private static final int DECIMALCOLINDEX = 5;

    private static final int VALUECOUNT = ROWCOUNT * COLUMNCOUNT;
    private static final double values[] = new double[VALUECOUNT];
    private static final int NONNEGCOUNT = ((ROWCOUNT + 1) / 2) * COLUMNCOUNT;
    private static final double nonnegs[] = new double[NONNEGCOUNT];
    private static final int POSCOUNT = ((ROWCOUNT - 1) / 2) * COLUMNCOUNT;
    private static final double nonnegnonzeros[] = new double[POSCOUNT];
    static {
        assert(numTypeNames[FLOATCOLINDEX].equals("FLOAT"));
        assert(numTypeNames[DECIMALCOLINDEX].equals("DECIMAL"));
        int kk = 0;
        int nn = 0;
        int pp = 0;
        for (int jj = 0; jj < ROWCOUNT; ++jj) {
            for (int ii = 0; ii < COLUMNCOUNT; ++ii) {
                double rawValue = orderedIds[jj] * ((ii < FLOATCOLINDEX) ? 1.0 : 0.01);
                values[kk++] = rawValue;
                if (rawValue >= 0.0) {
                    nonnegs[nn++] = rawValue;
                    if (rawValue > 0.0) {
                        nonnegnonzeros[pp++] = rawValue;
                    }
                }
            }
        }
        assert(NONNEGCOUNT == nn);
        assert(POSCOUNT == pp);
    }

    private static final String literalSchema =
            "CREATE TABLE P1 ( " +
                    "ID INTEGER DEFAULT 0 NOT NULL, " +
                    "DESC VARCHAR(300), " +
                    "NUM INTEGER, " +
                    "RATIO FLOAT, " +
                    "PAST TIMESTAMP DEFAULT NULL, " +
                    "PRIMARY KEY (ID) ); " +

                    "PARTITION TABLE P1 ON COLUMN ID;" +

                    // Test generalized indexes on a string function and various combos.
                    "CREATE INDEX P1_SUBSTRING_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2) ); " +
                    "CREATE INDEX P1_SUBSTRING_WITH_COL_DESC ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), DESC ); " +
                    "CREATE INDEX P1_NUM_EXPR_WITH_STRING_COL ON P1 ( ABS(ID), DESC ); " +
                    "CREATE INDEX P1_MIXED_TYPE_EXPRS1 ON P1 ( ABS(ID+2), SUBSTRING(DESC FROM 1 FOR 2) ); " +
                    "CREATE INDEX P1_MIXED_TYPE_EXPRS2 ON P1 ( SUBSTRING(DESC FROM 1 FOR 2), ABS(ID+2) ); " +
                    //Another table that has all numeric types, for testing numeric column functions.
                    "CREATE TABLE NUMBER_TYPES ( " +
                    "INTEGERNUM INTEGER DEFAULT 0 NOT NULL, " +
                    "TINYNUM TINYINT, " +
                    "SMALLNUM SMALLINT, " +
                    "BIGNUM BIGINT, " +
                    "FLOATNUM FLOAT, " +
                    "DECIMALNUM DECIMAL, " +
                    "PRIMARY KEY (INTEGERNUM) );" +

                    "CREATE INDEX NUMBER_TYPES_BITAND_IDX ON NUMBER_TYPES ( bitand(bignum, 3) ); " +

                    "CREATE TABLE C_NULL ( " +
                    "ID INTEGER DEFAULT 0 NOT NULL, " +
                    "S1 SMALLINT DEFAULT NULL, " +
                    "I1 INTEGER DEFAULT NULL, " +
                    "F1 FLOAT DEFAULT NULL, " +
                    "D1 DECIMAL DEFAULT NULL, " +
                    "V1 VARCHAR(10) DEFAULT NULL, " +
                    "T1 TIMESTAMP DEFAULT NULL, " +
                    "I2 INTEGER DEFAULT NULL, " +
                    "F2 FLOAT DEFAULT NULL, " +
                    "D2 DECIMAL DEFAULT NULL, " +
                    "V2 VARCHAR(10) DEFAULT NULL, " +
                    "T2 TIMESTAMP DEFAULT NULL, " +
                    "I3 INTEGER DEFAULT NULL, " +
                    "F3 FLOAT DEFAULT NULL, " +
                    "D3 DECIMAL DEFAULT NULL, " +
                    "V3 VARCHAR(10) DEFAULT NULL, " +
                    "T3 TIMESTAMP DEFAULT NULL, " +
                    "PRIMARY KEY (ID) ); " +
                    "PARTITION TABLE C_NULL ON COLUMN ID;" +

                    "CREATE TABLE INLINED_VC_VB_TABLE (" +
                    "ID INTEGER DEFAULT 0 NOT NULL," +
                    "VC1 VARCHAR(6)," +     // inlined
                    "VC2 VARCHAR(16)," +    // not inlined
                    "VB1 VARBINARY(6)," +   // inlined
                    "VB2 VARBINARY(64));" + // not inlined
                    "";

    private static void insertNumbers(Client client, double[] rawData, int nRaws) throws Exception {
        client.callProcedure("@AdHoc", "delete from NUMBER_TYPES;");
        // Insert non-negative or all input values with and without whole number and fractional parts.
        for (int kk = 0; kk < nRaws; kk += COLUMNCOUNT) {
            client.callProcedure("NUMBER_TYPES.insert", (int)rawData[kk], (int)rawData[kk+1],
                    (int)rawData[kk+2], (int)rawData[kk+3],
                    rawData[kk+FLOATCOLINDEX], String.valueOf(rawData[kk+DECIMALCOLINDEX]));
        }
    }

    private static double normalizeZero(double result) { return (result == -0.0) ? 0.0 : result; }

    private static class FunctionTestCase {
        final String m_case;
        final double m_result;
        final double m_filter;

        FunctionTestCase(String proc, double result) {
            m_case = proc;
            m_filter = 0.0;
            // Believe it or not, "negative 0" results were causing problems.
            m_result = normalizeZero(result);
        }
        FunctionTestCase(String proc, double filter, long result) {
            m_case = proc;
            m_filter = normalizeZero(filter);
            m_result = result;
        }
    };

    /**
     * @param expectedFormat
     * @param result
     * @param jj
     * @return
     */
    private double getColumnValue(String expectedFormat, VoltTable result, int jj) {
        double value;
        if (expectedFormat == null) {
            if (jj < FLOATCOLINDEX) {
                value = result.getLong(jj);
            } else if (jj == FLOATCOLINDEX) {
                value = result.getDouble(jj);
            } else {
                value = result.getDecimalAsBigDecimal(jj).doubleValue();
            }
        } else if (expectedFormat.equals("LONG")) {
            value = result.getLong(jj);
        } else if (expectedFormat.equals("DOUBLE")) {
            value = result.getDouble(jj);
        } else {
            value = result.getDecimalAsBigDecimal(jj).doubleValue();
        }
        return value;
    }

    /**
     * @param expectedFormat
     * @param client
     * @param proc
     * @param jj
     * @param filter
     * @return
     * @throws IOException
     * @throws ProcCallException
     */
    private static ClientResponse callWithParameter(String expectedFormat, Client client, String proc, int jj, double filter)
            throws IOException, ProcCallException {
        ClientResponse cr;
        if (expectedFormat == null) {
            if (jj < FLOATCOLINDEX) {
                cr = client.callProcedure(proc, (int)filter);
            } else if (jj == FLOATCOLINDEX) {
                cr = client.callProcedure(proc, filter);
            } else {
                cr = client.callProcedure(proc, BigDecimal.valueOf(filter));
            }
        } else if (expectedFormat.equals("LONG")) {
            cr = client.callProcedure(proc, (int)filter);
        } else if (expectedFormat.equals("DOUBLE")) {
            cr = client.callProcedure(proc, filter * 0.01);
        } else if (expectedFormat.equals("DECIMAL") || expectedFormat.equals("FLOAT")) {
            // NOTE: This has to be made consistent with the static initialization block!
            cr = client.callProcedure(proc, BigDecimal.valueOf(filter * 0.01));
        } else {
            cr = client.callProcedure(proc, BigDecimal.valueOf(filter));
        }
        return cr;
    }

    private FunctionTestCase[] displayFunctionRun(
            Client client, String fname, int rowCount, String expectedFormat) throws Exception {
        ClientResponse cr;
        VoltTable result;

        FunctionTestCase[] resultSet = new FunctionTestCase[numTypeNames.length * rowCount];
        int ii = 0;

        String proc = "DISPLAY_" + fname;
        cr = client.callProcedure(proc);
        result = cr.getResults()[0];
        assertEquals(rowCount, result.getRowCount());
        while (result.advanceRow()) {
            int jj = 0;
            for (String numTypeName : numTypeNames) {
                double value = getColumnValue(expectedFormat, result, jj);
                resultSet[ii++] = new FunctionTestCase(proc + " " + numTypeName, value);
                ++jj;
            }
        }
        return resultSet;
    }

    private FunctionTestCase[] orderFunctionRun(Client client, String fname, int rowCount) throws Exception {
        ClientResponse cr;
        VoltTable result;

        FunctionTestCase[] resultSet = new FunctionTestCase[numTypeNames.length * rowCount];
        int ii = 0;

        for (String numTypeName : numTypeNames) {
            String proc = "ORDER_" + fname + "_" + numTypeName;
            cr = client.callProcedure(proc);
            result = cr.getResults()[0];
            assertEquals(rowCount, result.getRowCount());
            int jj = 0;
            while (result.advanceRow()) {
                try {
                    resultSet[ii] = new FunctionTestCase(proc + " ROW " + jj, result.getLong(0));
                    ii++;
                } catch (IllegalArgumentException iae) {
                    // HSQL has been known to claim that the INTEGERNUM column is being returned as a float -- WTF!
                    resultSet[ii] = new FunctionTestCase(proc + " ROW " + jj, result.getDouble(0));
                    ii++;
                }
                // Extraneous columns beyond the first are provided for debug purposes only
                for (int kk = 1; kk < result.getColumnCount(); ++kk) {
                    if (result.getColumnType(kk) == VoltType.FLOAT) {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getDouble(kk));
                    } else if (result.getColumnType(kk) == VoltType.DECIMAL) {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getDecimalAsBigDecimal(kk));
                    } else if (result.getColumnType(kk) == VoltType.STRING) {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getString(kk));
                    } else {
                        System.out.println("DEBUG " + proc + " Extra column #" + kk + " = " + result.getLong(kk));
                    }
                }
                ++jj;
            }
        }
        return resultSet;
    }

    private FunctionTestCase[] whereFunctionRun(
            Client client, String fname, Set<Double> filters, String expectedFormat) throws Exception {
        ClientResponse cr;
        VoltTable result;

        FunctionTestCase[] resultSet = new FunctionTestCase[numTypeNames.length * filters.size()];
        int kk = 0;
        int jj = 0;
        for (String numTypeName : numTypeNames) {
            for (double filter : filters) {
                String proc = "WHERE_" + fname + "_" + numTypeName;
                cr = callWithParameter(expectedFormat, client, proc, jj, filter);
                result = cr.getResults()[0];
                int rowCount = result.getRowCount();
                assertEquals(rowCount, 1);
                resultSet[kk++] = new FunctionTestCase(proc, filter, result.asScalarLong());
            }
            ++jj;
        }
        return resultSet;
    }

    private static int complaintCount = 1;

    private static void complain(String complaint) {
        System.out.println("Complaint #" + complaintCount + ": " + complaint);
        ++complaintCount; // NICE PLACE FOR A BREAKPOINT.
    }

    static String formatForFuzziness = "%14e";

    private static class FunctionVarCharTestCase {
        private final String m_case;
        private final String m_filter;
        private final long m_result;

        private FunctionVarCharTestCase(String proc, String filter) {
            m_case = proc;
            m_filter = filter;
            m_result = 0;
        }
        private FunctionVarCharTestCase(String proc, String filter, long l) {
            m_case = proc;
            m_filter = filter;
            m_result = l;
        }
    };

    private FunctionVarCharTestCase[] displayVarCharCastRun(Client client, int rowCount) throws Exception {
        ClientResponse cr;
        VoltTable result;

        FunctionVarCharTestCase[] resultSet = new FunctionVarCharTestCase[numTypeNames.length * rowCount];
        int ii = 0;

        String proc = "DISPLAY_VARCHAR";
        cr = client.callProcedure(proc);
        result = cr.getResults()[0];
        assertEquals(rowCount, result.getRowCount());
        while (result.advanceRow()) {
            int jj = 0;
            for (String numTypeName : numTypeNames) {
                String tooSimple = result.getString(jj);
                String value = Double.valueOf(tooSimple).toString();
                resultSet[ii++] = new FunctionVarCharTestCase(proc + " " + numTypeName, value);
                ++jj;
            }
        }
        return resultSet;
    }

    private FunctionVarCharTestCase[] whereVarCharCastRun(Client client, Set<String> filters) throws Exception {
        ClientResponse cr;
        VoltTable result;

        FunctionVarCharTestCase[] resultSet = new FunctionVarCharTestCase[numTypeNames.length * filters.size()];
        int kk = 0;
        int jj = 0;
        for (String numTypeName : numTypeNames) {
            for (String filter : filters) {
                String proc = "WHERE_VARCHAR_CAST_" + numTypeName;
                String param = filter;
                String[] decimalParts = filter.split("\\.");
                if (jj < FLOATCOLINDEX) {
                    // Truncate an integer decimal before the decimal point to match an integer column.
                    if (decimalParts.length < 2 || decimalParts[1].equals("0")) {
                        param = decimalParts[0];
                    }
                    // Else fall through to pass a fractional decimal as it is
                    // to purposely force a mismatch with an integer column.
                } else if (jj > FLOATCOLINDEX) {
                    // Pad the decimal string.
                    if (decimalParts.length < 2 || decimalParts[1].equals("0")) {
                        param = decimalParts[0] + ".000000000000";
                    } else {
                        param = decimalParts[0] + "." + (decimalParts[1] + "000000000000").substring(0,12);
                    }
                } else {
                    // Handle float-to-string cast formatting
                    // TODO: this code may not be right for multiples of 10 or decimals of magnitude < .1
                    // which we don't happen to be using currently to drive this numeric test framework.
                    if (decimalParts.length < 2 || decimalParts[1].equals("0")) {
                        if (decimalParts[0].equals("0")) {
                            param = "0E0";
                        } else {
                            int signedDigitWidth = (decimalParts[0].charAt(0) == '-') ? 2 : 1;
                            param = decimalParts[0].substring(0, signedDigitWidth) +
                                    "." + decimalParts[0].substring(signedDigitWidth) +
                                    "E" + (decimalParts[0].length() - signedDigitWidth);
                        }
                    } else if (decimalParts[0].equals("0")) {
                        param = decimalParts[1].substring(0, 1) +
                                "." + decimalParts[1].substring(1) + "E-1";
                    } else if (decimalParts[0].equals("-0") ) {
                        param = "-" + decimalParts[1].substring(0, 1) +
                                "." + decimalParts[1].substring(1) + "E-1";
                    } else {
                        int signedDigitWidth = (decimalParts[0].charAt(0) == '-') ? 2 : 1;
                        param = decimalParts[0].substring(0, signedDigitWidth) +
                                "." + decimalParts[0].substring(signedDigitWidth) + decimalParts[1] +
                                "E" + (decimalParts[0].length() - signedDigitWidth);
                    }
                }
                cr = client.callProcedure(proc, param);
                result = cr.getResults()[0];
                int rowCount = result.getRowCount();
                assertEquals(rowCount, 1);
                long tupleCount = result.asScalarLong();
                resultSet[kk++] = new FunctionVarCharTestCase(proc, filter, tupleCount);
            }
            ++jj;
        }
        return resultSet;
    }

    @Test
    public void testNumericCasts() throws Exception {
        System.out.println("STARTING test of numeric CAST");
        final double[] rawData = values;
        final double[] resultIntValues = new double[values.length];
        final Set<Double> intFilters = new HashSet<>();
        final Set<Double> rawFilters = new HashSet<>();
        for (int kk = 0; kk < resultIntValues.length; ++kk) {
            resultIntValues[kk] = (int)values[kk];
            intFilters.add(resultIntValues[kk]);
            rawFilters.add(values[kk]);
        }

        Client client = getClient();
        insertNumbers(client, rawData, rawData.length);

        FunctionTestCase[] results;
        double[] resultValues;
        Set<Double> filters;

        for (int jj = 0; jj < numTypeNames.length ; ++jj) {
            if (numFormatNames[jj].equals("DECIMAL") || numFormatNames[jj].equals("DOUBLE")) {
                results = displayFunctionRun(client, numTypeNames[jj], values.length / COLUMNCOUNT, numFormatNames[jj]);
                resultValues = rawData;
                filters = rawFilters;
            } else {
                results = displayFunctionRun(client, numTypeNames[jj], values.length / COLUMNCOUNT, numFormatNames[jj]);
                resultValues = resultIntValues;
                filters = intFilters;
            }
            assertEquals(results.length, values.length);

            Map<String, Integer> valueBag = new HashMap<>();
            int kk = 0;
            for (FunctionTestCase result : results) {
                double expected = resultValues[kk++];
                if (expected != result.m_result) {
                    // Compromise: accuracy errors get complaints but not asserts.
                    complain("Failed " + result.m_case + " expected " + expected + " got " + result.m_result);
                }
                // Use precision-limited string formatting to forgive accuracy errors between the C++ and java floating point function implementations.
                String asExpected = String.format(formatForFuzziness, expected);
                String asResulted = String.format(formatForFuzziness, result.m_result);
                assertEquals(asExpected, asResulted);
                // count occurrences of expected values in anticipation of the WHERE_ tests.
                Integer count = valueBag.get(asExpected);
                if (count == null) {
                    count = 0;
                }
                valueBag.put(asExpected, count + 1);
            }

            // Validate that sorting on the function value does not alter the ordering of its input values.
            results = orderFunctionRun(client, numTypeNames[jj]  + "_CAST", resultValues.length/COLUMNCOUNT);

            // The total number of ordered INTEGERNUM values returned should be the same as the number of stored values.
            assertEquals(results.length, resultValues.length);
            kk = 0;
            for (FunctionTestCase result : results) {
                int idIndex = kk++;
                if (kk == ROWCOUNT) {
                    kk = 0;
                }
                double expected = orderedIds[idIndex];
                assertEquals("Failed " + result.m_case + " expected " + expected + " got " + result.m_result,
                        expected, result.m_result);
            }

            results = whereFunctionRun(client, numTypeNames[jj] + "_CAST", filters, numFormatNames[jj]);

            assertEquals(results.length, COLUMNCOUNT*filters.size());
            // If filters represents all the values in resultValues,
            // the filtered counts should total to resultValues.length.
            int coveringCount = resultValues.length;
            for (FunctionTestCase result : results) {
                if (result.m_result == 0.0) {
                    // complain("NONMATCHING filter " + result.m_case + " " + result.m_filter);
                    continue;
                }
                Integer count = valueBag.get(String.format(formatForFuzziness, result.m_filter));
                assertNotNull("CAST got unexpected result " + result.m_filter + ".", count);
                assertTrue(result.m_case + " value " + result.m_filter + " not expected or previously deleted from " + valueBag + ".",
                        count >= result.m_result);
                valueBag.put(String.format(formatForFuzziness, result.m_filter), count - (int)result.m_result);
                coveringCount -= (int)result.m_result;
            }
        }
    }

    private static void insertNumbersViaVarChar(Client client, double[] rawData, int nRaws) throws Exception {
        client.callProcedure("@AdHoc", "delete from NUMBER_TYPES;");
        // Insert inputs via string values to test casts from VARCHAR
        for (int kk = 0; kk < nRaws; kk += COLUMNCOUNT) {
            client.callProcedure("@AdHoc",
                    "INSERT INTO NUMBER_TYPES VALUES (" +
                            "CAST('" + (int)rawData[kk+0] + "' AS " + numTypeNames[0] + "),  " +
                            "CAST('" + (int)rawData[kk+1] + "' AS " + numTypeNames[1] + "),  " +
                            "CAST('" + (int)rawData[kk+2] + "' AS " + numTypeNames[2] + "),  " +
                            "CAST('" + (int)rawData[kk+3] + "' AS " + numTypeNames[3] + "),  " +
                            "CAST('" + rawData[kk+FLOATCOLINDEX]   + "' AS FLOAT         ),  " +
                            "CAST('" + rawData[kk+DECIMALCOLINDEX] + "' AS DECIMAL       ) );");
        }
        dumpQueryResults(client, "SELECT * FROM NUMBER_TYPES;");
    }

    @Test
    public void testFromVarCharCasts() throws Exception {
        System.out.println("STARTING test of FROM VARCHAR CAST");
        Client client = getClient();
        insertNumbersViaVarChar(client, values, values.length);
        System.out.println("VALIDATING result of 'FROM VARCHAR' CAST via results of 'TO VARCHAR' CASTS");
        subtestVarCharCasts(client);
        System.out.println("ENDING test of FROM VARCHAR CAST");
    }

    @Test
    public void testToVarCharCasts() throws Exception {
        System.out.println("STARTING test of TO VARCHAR CAST");
        Client client = getClient();
        insertNumbers(client, values, values.length);
        subtestVarCharCasts(client);
        subtestInlineVarCharCast(client);
        System.out.println("ENDING test of TO VARCHAR CAST");
    }

    private void subtestInlineVarCharCast(Client client) throws Exception {
        // This is regression test coverage for ENG-6666.
        String sql = "INSERT INTO INLINED_VC_VB_TABLE (ID, VC1, VC2, VB1, VB2) " +
            "VALUES (22, 'FOO', 'BAR', 'DEADBEEF', 'CDCDCDCD');";
        client.callProcedure("@AdHoc", sql);
        sql = "SELECT CAST(VC1 AS VARCHAR) FROM INLINED_VC_VB_TABLE WHERE ID = 22;";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        vt.advanceRow();
        assertEquals("FOO", vt.getString(0));

        sql = "SELECT CAST(VB1 AS VARBINARY) FROM INLINED_VC_VB_TABLE WHERE ID = 22;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        vt.advanceRow();
        assertTrue(VoltTable.varbinaryToPrintableString(vt.getVarbinary(0)).contains("DEADBEEF"));
    }

    private void subtestVarCharCasts(Client client) throws Exception {
        final String[] resultValues = new String[values.length];
        final Set<String> filters = new HashSet<>();
        for (int kk = 0; kk < resultValues.length; ++kk) {
            resultValues[kk] = "" + values[kk];
            filters.add(resultValues[kk]);
        }

        FunctionVarCharTestCase[] results;

        results = displayVarCharCastRun(client, values.length / COLUMNCOUNT);
        assertEquals(results.length, values.length);

        Map<String, Integer> valueBag = new HashMap<>();
        int kk = 0;
        for (FunctionVarCharTestCase result : results) {
            String expected = resultValues[kk++];
            assertEquals("Failed " + result.m_case + " expected " + expected + " got " + result.m_filter,
                    expected, result.m_filter);
            // count occurrences of expected values in anticipation of the WHERE_ tests.
            Integer count = valueBag.get(expected);
            if (count == null) {
                count = 0;
            }
            valueBag.put(expected, count + 1);
            //*VERBOSIFY TO DEBUG:*/ System.out.println("UPDATING " + result.m_case + " found count of " + expected + " to " + (count+1) );
        }

        results = whereVarCharCastRun(client, filters);

        assertEquals(results.length, COLUMNCOUNT*filters.size());
        // If filters represents all the values in resultValues,
        // the filtered counts should total to resultValues.length.
        int coveringCount = resultValues.length;
        for (FunctionVarCharTestCase result : results) {
            Integer count = valueBag.get(result.m_filter);
            assertNotNull("CAST got unexpected result " + result.m_filter + ".", count);
            //project.addStmtProcedure("WHERE_VARCHAR_CAST_DECIMAL",  "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS VARCHAR) = ?");
            if (! result.m_case.equals("WHERE_VARCHAR_CAST_DECIMAL")) {
                assertTrue(result.m_case + " value " + result.m_filter + " not expected or previously deleted from " + valueBag + ".",
                        count >= result.m_result);
            }
            valueBag.put(result.m_filter, count-(int)result.m_result);
            coveringCount -= (int)result.m_result;
        }
        //assertTrue(0 == coveringCount /*I WISH*/ || 5 == coveringCount /* former near miss */ );
        // Validate how sorting on the string value alters the ordering of its input values.
        FunctionTestCase[] orderedResults = orderFunctionRun(client, "VARCHAR_CAST", values.length/COLUMNCOUNT);

        // The total number of ordered INTEGERNUM values returned should be the same as the number of stored values.
        assertEquals(values.length, orderedResults.length);
        kk = 0;
        int jj = 0;
        for (FunctionTestCase result : orderedResults) {
            int idIndex = kk++;
            if (kk == ROWCOUNT) {
                kk = 0;
            }
            double[] expecteds = (jj/ROWCOUNT < FLOATCOLINDEX) ? orderedByStringIds :
                (jj/ROWCOUNT > FLOATCOLINDEX) ? orderedByDecimalStringIds : orderedByFloatStringIds;
            assertEquals("Failed " + result.m_case + " expected " + expecteds[idIndex] + " got " + result.m_result,
                    expecteds[idIndex], result.m_result);
            ++jj;
        }
    }

    // Unicode character to UTF8 string character
    @Test
    public void testChar() throws IOException, ProcCallException {
        System.out.println("STARTING test CHAR");

        // Hsql has wrong answers.
        if (isHSQL()) {
            return;
        }

        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "Xin@VoltDB", 1, 1.0, new Timestamp(1000000000000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        result = client.callProcedure("CHAR", 36158, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("贾", result.getString(1));

        result = client.callProcedure("CHAR", 37995, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals("鑫", result.getString(1));

        String voltDB = "VoltDB";

        for (int i = 0; i < voltDB.length(); i++) {
            char ch = voltDB.charAt(i);
            result = client.callProcedure("CHAR", (int)ch, 1).getResults()[0];
            assertTrue(result.advanceRow());
            assertEquals(String.valueOf(ch), result.getString(1));
        }

        result = client.callProcedure("CHAR", null, 1).getResults()[0];
        assertTrue(result.advanceRow());
        assertNull(result.getString(1));
    }

    // concat params with a sql query string, and test the return value
    private void doTestCoalesceWithoutConst(Client cl, String[] params, String expect, String id) throws Exception {
        String allPara = String.join(",", params);
        String sql;
        if (expect.equals("NULL")) {
            sql = "SELECT CASE WHEN(COALESCE(" + allPara + ") IS NULL)" +
                  " THEN 0 ELSE 1 END FROM C_NULL WHERE ID=" + id;
        } else {
            sql = "SELECT CASE COALESCE(" + allPara + ") " +
                   "WHEN " + expect + " THEN 0 ELSE 1 END FROM C_NULL WHERE ID=" + id;
        }
        validateTableOfLongs(cl, sql, new long[][] {{0}});
    }

    private void doTestCoalesceWithConst(Client cl, String[] params, String cst ,String expect, String id) throws Exception {
        String allPara = String.join(",", params);
        allPara += ","+cst;
        String sql;
        if (expect.equals("NULL")) {
            sql = "SELECT CASE WHEN(COALESCE(" + allPara + ") IS NULL)" +
                  " THEN 0 ELSE 1 END FROM C_NULL WHERE ID=" + id;
        } else {
            sql = "SELECT CASE COALESCE(" + allPara + ") " +
                   "WHEN " + expect + " THEN 0 ELSE 1 END FROM C_NULL WHERE ID=" + id;
        }
        validateTableOfLongs(cl, sql, new long[][] {{0}});
    }

    // col1 is not null while col2 is null
    private void doTestCoalescePairOneNull(Client cl, String col1, String col2) throws Exception {
        // coalesce(col1, col2) == coalesce(col2, col1) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2}, col1, "1");
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1}, col1, "1");
    }

    private void doTestCoalescePairBothNull(Client cl, String col1, String col2) throws Exception{
        // coalesce(col1, col2) == coalesce(col2, col1) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2}, "NULL", "0");
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1}, "NULL", "0");
    }

    // Both the columns are not null
    private void doTestCoalescePairNotNull(Client cl, String col1, String col2) throws Exception {
        // coalesce(col1, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2}, col1, "2");
        // coalesce(col2, col1) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1}, col2, "2");
    }

    // All the columns are not null
    private void doTestCoalesceTriNotNull(Client cl, String col1, String col2, String col3, String cst) throws Exception {
        // coalesce(col1, col2, col3) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2, col3}, col1, "3");
        // coalesce(col1, col3, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col3, col2}, col1, "3");
        // coalesce(col2, col1, col3) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1, col3}, col2, "3");
        // coalesce(col2, col3, col1) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col3, col1}, col2, "3");
        // coalesce(col3, col1, col2) == col3
        doTestCoalesceWithoutConst(cl, new String[]{col3, col1, col2}, col3, "3");
        // coalesce(col3, col2, col1) == col3
        doTestCoalesceWithoutConst(cl, new String[]{col3, col2, col1}, col3, "3");
        // coalesce(col1, col2, col3, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col2, col3}, cst, col1, "3");
        // coalesce(col1, col3, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col3, col2}, cst, col1, "3");
        // coalesce(col2, col1, col3, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col2, col1, col3}, cst, col2, "3");
        // coalesce(col2, col3, col1, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col2, col3, col1}, cst, col2, "3");
        // coalesce(col3, col1, col2, cst) == col3
        doTestCoalesceWithConst(cl, new String[]{col3, col1, col2}, cst, col3, "3");
        // coalesce(col3, col2, col1, cst) == col3
        doTestCoalesceWithConst(cl, new String[]{col3, col2, col1}, cst, col3, "3");
    }

    // col3 is null
    private void doTestCoalesceTriOneNull(Client cl, String col1, String col2, String col3, String cst) throws Exception {
        // coalesce(col1, col2, col3) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2, col3}, col1, "2");
        // coalesce(col1, col3, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col3, col2}, col1, "2");
        // coalesce(col2, col1, col3) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1, col3}, col2, "2");
        // coalesce(col2, col3, col1) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col2, col3, col1}, col2, "2");
        // coalesce(col3, col1, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col3, col1, col2}, col1, "2");
        // coalesce(col3, col2, col2) == col2
        doTestCoalesceWithoutConst(cl, new String[]{col3, col2, col1}, col2, "2");
        // coalesce(col1, col2, col3, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col2, col3}, cst, col1, "2");
        // coalesce(col1, col3, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col3, col2}, cst, col1, "2");
        // coalesce(col2, col1, col3, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col2, col1, col3}, cst, col2, "2");
        // coalesce(col2, col3, col1, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col2, col3, col1}, cst, col2, "2");
        // coalesce(col3, col1, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col3, col1, col2}, cst, col1, "2");
        // coalesce(col3, col1, col2, cst) == col2
        doTestCoalesceWithConst(cl, new String[]{col3, col2, col1}, cst, col2, "2");
    }

    // col2 and col3 are null
    private void doTestCoalesceTriTwoNull(Client cl, String col1, String col2, String col3, String cst) throws Exception {
        // coalesce(col1, col2, col3) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2, col3}, col1, "1");
        // coalesce(col1, col3, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col1, col3, col2}, col1, "1");
        // coalesce(col2, col1, col3) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1, col3}, col1, "1");
        // coalesce(col2, col3, col1) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col2, col3, col1}, col1, "1");
        // coalesce(col3, col1, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col3, col1, col2}, col1, "1");
        // coalesce(col3, col2, col2) == col1
        doTestCoalesceWithoutConst(cl, new String[]{col3, col2, col1}, col1, "1");
        // coalesce(col1, col2, col3, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col2, col3}, cst, col1, "1");
        // coalesce(col1, col3, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col1, col3, col2}, cst, col1, "1");
        // coalesce(col2, col1, col3, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col2, col1, col3}, cst, col1, "1");
        // coalesce(col2, col3, col1, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col2, col3, col1}, cst, col1, "1");
        // coalesce(col3, col1, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col3, col1, col2}, cst, col1, "1");
        // coalesce(col3, col1, col2, cst) == col1
        doTestCoalesceWithConst(cl, new String[]{col3, col2, col1}, cst, col1, "1");
    }

    // all columns are null
    private void doTestCoalesceTriAllNull(Client cl, String col1, String col2, String col3, String cst) throws Exception {
        // coalesce(col1, col2, col3) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col1, col2, col3}, "NULL", "0");
        // coalesce(col1, col3, col2) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col1, col3, col2}, "NULL", "0");
        // coalesce(col2, col1, col3) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col2, col1, col3}, "NULL", "0");
        // coalesce(col2, col3, col1) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col2, col3, col1}, "NULL", "0");
        // coalesce(col3, col1, col2) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col3, col1, col2}, "NULL", "0");
        // coalesce(col3, col2, col2) == NULL
        doTestCoalesceWithoutConst(cl, new String[]{col3, col2, col1}, "NULL", "0");
        // coalesce(col1, col2, col3, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col1, col2, col3}, cst, cst, "0");
        // coalesce(col1, col3, col2, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col1, col3, col2}, cst, cst, "0");
        // coalesce(col2, col1, col3, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col2, col1, col3}, cst, cst, "0");
        // coalesce(col2, col3, col1, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col2, col3, col1}, cst, cst, "0");
        // coalesce(col3, col1, col2, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col3, col1, col2}, cst, cst, "0");
        // coalesce(col3, col1, col2, cst) == cst
        doTestCoalesceWithConst(cl, new String[]{col3, col2, col1}, cst, cst, "0");
    }

    private void doTestTwoColCoalesce(Client cl, String col1, String col2) throws Exception {
        doTestCoalescePairBothNull(cl, col1, col2);
        doTestCoalescePairOneNull(cl, col1, col2);
        doTestCoalescePairNotNull(cl, col1, col2);
    }

    private void doTestThreeColCoalesce(Client cl, String col1, String col2, String col3, String cst) throws Exception {
        doTestCoalesceTriAllNull(cl, col1, col2, col3, cst);
        doTestCoalesceTriTwoNull(cl, col1, col2, col3, cst);
        doTestCoalesceTriOneNull(cl, col1, col2, col3, cst);
        doTestCoalesceTriNotNull(cl, col1, col2, col3, cst);
    }

    @Test
    public void testCoalesce() throws Exception {
        System.out.println("STARTING test COALESCE function...");
        Client cl = getClient();

        // one row with three sets of nulls
        cl.callProcedure("@AdHoc", "insert into C_NULL(ID) values (0);");
        // one row with one set of non-null columns and two sets of nulls
        cl.callProcedure("@AdHoc", "insert into C_NULL(ID,S1,I1,F1,D1,V1,T1) values (1,1,1,1,1,'1',100000)");
        // TODO: below is wrong, because the null timestamp will be regarded as an invalid input by hsql
        //cl.callProcedure("C_NULL.insert", 1,1,1,1,1,"1",new Timestamp(1000000000000L), null, null, null, null, null);
        // two sets of non-null columns and one set of null column
        cl.callProcedure("@AdHoc", "insert into C_NULL(ID,S1,I1,F1,D1,V1,T1,I2,F2,D2,V2,T2)"
                                + " values (2,1,1,1,1,'1',100000,2,2,2,'2',200000)");
        // three set non-nulls
        cl.callProcedure("C_NULL.insert", 3,1,1,1,1,"1",new Timestamp(1000000000000L),
                2,2,2,"2",new Timestamp(2000000000000L),
                3,3,3,"3",new Timestamp(3000000000000L));

        doTestTwoColCoalesce(cl, "I1", "I2");
        doTestTwoColCoalesce(cl, "F1", "F2");
        doTestTwoColCoalesce(cl, "D1", "D2");
        doTestTwoColCoalesce(cl, "V1", "V2");
        doTestTwoColCoalesce(cl, "T1", "T2");

        doTestThreeColCoalesce(cl, "I1", "I2", "I3", "100");
        doTestThreeColCoalesce(cl, "F1", "F2", "F3", "100.0");
        doTestThreeColCoalesce(cl, "D1", "D2", "D3", "100.0");
        doTestThreeColCoalesce(cl, "V1", "V2", "V3", "'hahaha'");
        doTestThreeColCoalesce(cl, "T1", "T2", "T3", "CAST ('2014-07-09 00:00:00.000000' as TIMESTAMP)");

        // test compatiable types
        doTestThreeColCoalesce(cl, "S1", "I2", "I3", "100");
        doTestThreeColCoalesce(cl, "S1", "F2", "D3", "100.0");
        doTestThreeColCoalesce(cl, "I1", "F2", "D3", "100.0");

        // test incompatiable types
        // TODO: Is the exception thrown by coalesce? Or by decode?
        try {
            doTestThreeColCoalesce(cl, "S1", "I2", "V3", "100");
            fail();
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains(
                    USING_CALCITE ? "Illegal mixing of types in CASE or COALESCE statement" :
                            "incompatible data types in combination"));
        }
        try {
            doTestThreeColCoalesce(cl, "S1", "I2", "T3", "100");
            fail();
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("incompatible data types"));
        }
    }

    @Test
    public void testManyExtractTimeFieldFunction() throws Exception {
        System.out.println("STARTING test functions extracting fields in timestamp ...");
        Client cl = getClient();
        VoltTable result;
        String sql;

        ClientResponse cr = cl.callProcedure("P1.insert", 0, null, null, null,
                Timestamp.valueOf("2014-07-15 01:02:03.456"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = cl.callProcedure("P1.insert", 1, null, null, null, Timestamp.valueOf("2012-02-29 12:20:30.123"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = cl.callProcedure("P1.insert", 2, null, null, null, Timestamp.valueOf("2012-12-31 12:59:30"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        validateTableOfLongs(cl, "select id, YEAR(past) from p1 order by id;",
                new long[][]{{0, 2014}, {1, 2012}, {2, 2012}});
        validateTableOfLongs(cl, "select id, MONTH(past) from p1 order by id;",
                new long[][]{{0, 7}, {1, 2}, {2, 12}});
        validateTableOfLongs(cl, "select id, DAY(past) from p1 order by id;",
                new long[][]{{0, 15}, {1, 29}, {2, 31}});
        validateTableOfLongs(cl, "select id, HOUR(past) from p1 order by id;",
                new long[][]{{0, 1}, {1, 12}, {2, 12}});
        validateTableOfLongs(cl, "select id, MINUTE(past) from p1 order by id;",
                new long[][]{{0, 2}, {1, 20}, {2, 59}});
        cr = cl.callProcedure("@AdHoc",
                "select id, cast(SECOND(past) as VARCHAR) from p1 order by id;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        if (isHSQL()) {
            validateTableColumnOfScalarVarchar(result, 1, new String[]{"3.456000", "30.123000", "30.000000"});
        } else {
            validateTableColumnOfScalarVarchar(result, 1, new String[]{"3.456000000000", "30.123000000000",
                    "30.000000000000"});
        }

        validateTableOfLongs(cl, "select id, QUARTER(past) from p1 order by id;",
                new long[][]{{0, 3}, {1, 1}, {2, 4}});
        validateTableOfLongs(cl, "select DAYOFWEEK(past) from p1 order by id;",
                new long[][]{{3}, {4}, {2}});

        sql = "select WEEKDAY(past) from p1 order by id;";
        if (isHSQL()) {
            // we modify the hsql parser, and so it maps to extract week_of_day
            validateTableOfLongs(cl, sql,new long[][]{{3}, {4}, {2}});
        } else {
            // call our ee function, and so return different value
            validateTableOfLongs(cl, sql,new long[][]{{1}, {2}, {0}});
        }

        validateTableOfLongs(cl, "select DAYOFMONTH(past) from p1 order by id;", new long[][]{{15}, {29}, {31}});
        validateTableOfLongs(cl, "select DAYOFYEAR(past) from p1 order by id;", new long[][]{{196}, {60}, {366}});

        // WEEK 1 is often the correct answer for the last day of the year.
        // See https://en.wikipedia.org/wiki/ISO_week_year#Last_week
        validateTableOfLongs(cl, "select WEEK(past) from p1 order by id;", new long[][]{{29}, {9}, {1}});
    }

    // ENG-3283
    public void testAliasesOfSomeStringFunctions() throws IOException, ProcCallException {
        String sql;
        VoltTable result;
        Client cl = getClient();
        ClientResponse cr = cl.callProcedure("P1.insert", 0, "abc123ABC", null, null,
                Timestamp.valueOf("2014-07-15 01:02:03.456"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // LTRIM and RTRIM has been implemented and tested

        // SUBSTR
        cr = cl.callProcedure("@AdHoc", "select SUBSTR(DESC, 1, 2) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ab"});

        cr = cl.callProcedure("@AdHoc", "select SUBSTR(DESC, 4, 3) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"123"});

        cr = cl.callProcedure("@AdHoc", "select SUBSTR(DESC, 3) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"c123ABC"});

        // Test spelled out SUBSTRING with comma delimiters vs. old-school FROM and FOR keywords.
        cr = cl.callProcedure("@AdHoc", "select SUBSTRING(DESC, 1, 2) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ab"});

        cr = cl.callProcedure("@AdHoc", "select SUBSTRING(DESC, 4, 3) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"123"});

        cr = cl.callProcedure("@AdHoc", "select SUBSTRING(DESC, 3) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"c123ABC"});

        // Some weird cases -- the SQL-2003 standard says that even START < 1
        // moves the end point (in this case, to the left) which is based on (LENGTH + START).
        cr = cl.callProcedure("@AdHoc", "select SUBSTR(DESC, 0, 2) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"a"}); // not "ab" !

        cr = cl.callProcedure("@AdHoc", "select SUBSTR(DESC, -1, 2) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not "ab" !

        cr = cl.callProcedure("@AdHoc", "select SUBSTR(DESC, -1, 1) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not "a" !


        cr = cl.callProcedure("@AdHoc", "select SUBSTR(DESC, -3, 1) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not an error !

        cr = cl.callProcedure("@AdHoc", "select SUBSTRING(DESC, 0, 2) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"a"}); // not "ab" !

        cr = cl.callProcedure("@AdHoc", "select SUBSTRING(DESC, -1, 2) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not "ab" !

        cr = cl.callProcedure("@AdHoc", "select SUBSTRING(DESC, -1, 1) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not "a" !

        cr = cl.callProcedure("@AdHoc", "select SUBSTRING(DESC, -3, 1) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{""}); // not an error !

        // LCASE and UCASE
        cr = cl.callProcedure("@AdHoc", "select LCASE(DESC) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"abc123abc"});

        cr = cl.callProcedure("@AdHoc", "select UCASE(DESC) from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABC123ABC"});

        // INSERT
        cr = cl.callProcedure("@AdHoc", "select INSERT(DESC, 1, 3,'ABC') from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABC123ABC"});

        cr = cl.callProcedure("@AdHoc", "select INSERT(DESC, 1, 1,'ABC') from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABCbc123ABC"});

        cr = cl.callProcedure("@AdHoc", "select INSERT(DESC, 1, 4,'ABC') from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABC23ABC"});

        cr = cl.callProcedure("@AdHoc", "select INSERT(DESC, 1, 0,'ABC') from p1 where id = 0;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{"ABCabc123ABC"});
    }

    static public junit.framework.Test suite() throws IOException {

        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestFunctionsSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(literalSchema);
        project.addStmtProcedure("DISPLAY_INTEGER",
                "select CAST(INTEGERNUM AS INTEGER), CAST(TINYNUM AS INTEGER), " +
                        "CAST(SMALLNUM AS INTEGER), CAST(BIGNUM AS INTEGER), CAST(FLOATNUM AS INTEGER), CAST(DECIMALNUM AS INTEGER) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_INTEGER_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS INTEGER)");
        project.addStmtProcedure("ORDER_INTEGER_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS INTEGER)");
        project.addStmtProcedure("ORDER_INTEGER_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS INTEGER)");
        project.addStmtProcedure("ORDER_INTEGER_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS INTEGER)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_INTEGER_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS INTEGER), INTEGERNUM");
        project.addStmtProcedure("ORDER_INTEGER_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS INTEGER), INTEGERNUM");

        project.addStmtProcedure("WHERE_INTEGER_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS INTEGER) = ?");
        project.addStmtProcedure("WHERE_INTEGER_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS INTEGER) = ?");

        project.addStmtProcedure("DISPLAY_TINYINT",
                "select CAST(INTEGERNUM AS TINYINT), CAST(TINYNUM AS TINYINT), " +
                        "CAST(SMALLNUM AS TINYINT), CAST(BIGNUM AS TINYINT), CAST(FLOATNUM AS TINYINT), CAST(DECIMALNUM AS TINYINT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_TINYINT_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS TINYINT)");
        project.addStmtProcedure("ORDER_TINYINT_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS TINYINT)");
        project.addStmtProcedure("ORDER_TINYINT_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS TINYINT)");
        project.addStmtProcedure("ORDER_TINYINT_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS TINYINT)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_TINYINT_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS TINYINT), INTEGERNUM");
        project.addStmtProcedure("ORDER_TINYINT_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS TINYINT), INTEGERNUM");

        project.addStmtProcedure("WHERE_TINYINT_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS TINYINT) = ?");
        project.addStmtProcedure("WHERE_TINYINT_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS TINYINT) = ?");

        project.addStmtProcedure("DISPLAY_SMALLINT",
                "select CAST(INTEGERNUM AS SMALLINT), CAST(TINYNUM AS SMALLINT), " +
                        "CAST(SMALLNUM AS SMALLINT), CAST(BIGNUM AS SMALLINT), CAST(FLOATNUM AS SMALLINT), CAST(DECIMALNUM AS SMALLINT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_SMALLINT_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS SMALLINT)");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS SMALLINT)");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS SMALLINT)");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS SMALLINT)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_SMALLINT_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS SMALLINT), INTEGERNUM");
        project.addStmtProcedure("ORDER_SMALLINT_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS SMALLINT), INTEGERNUM");

        project.addStmtProcedure("WHERE_SMALLINT_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS SMALLINT) = ?");
        project.addStmtProcedure("WHERE_SMALLINT_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS SMALLINT) = ?");

        project.addStmtProcedure("DISPLAY_BIGINT",
                "select CAST(INTEGERNUM AS BIGINT), CAST(TINYNUM AS BIGINT), " +
                        "CAST(SMALLNUM AS BIGINT), CAST(BIGNUM AS BIGINT), CAST(FLOATNUM AS BIGINT), CAST(DECIMALNUM AS BIGINT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_BIGINT_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS BIGINT)");
        project.addStmtProcedure("ORDER_BIGINT_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS BIGINT)");
        project.addStmtProcedure("ORDER_BIGINT_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS BIGINT)");
        project.addStmtProcedure("ORDER_BIGINT_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS BIGINT)");
        // Provide a tie-breaker sort column for lossy casts to ensure a deterministic result.
        project.addStmtProcedure("ORDER_BIGINT_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS BIGINT), INTEGERNUM");
        project.addStmtProcedure("ORDER_BIGINT_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS BIGINT), INTEGERNUM");

        project.addStmtProcedure("WHERE_BIGINT_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS BIGINT) = ?");
        project.addStmtProcedure("WHERE_BIGINT_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS BIGINT) = ?");

        project.addStmtProcedure("DISPLAY_FLOAT",
                "select CAST(INTEGERNUM AS FLOAT), CAST(TINYNUM AS FLOAT), " +
                        "CAST(SMALLNUM AS FLOAT), CAST(BIGNUM AS FLOAT), CAST(FLOATNUM AS FLOAT), CAST(DECIMALNUM AS FLOAT) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_FLOAT_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS FLOAT)");
        project.addStmtProcedure("ORDER_FLOAT_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS FLOAT)");

        project.addStmtProcedure("WHERE_FLOAT_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS FLOAT) = ?");
        project.addStmtProcedure("WHERE_FLOAT_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS FLOAT) = ?");

        project.addStmtProcedure("DISPLAY_DECIMAL",
                "select CAST(INTEGERNUM AS DECIMAL), CAST(TINYNUM AS DECIMAL), " +
                        "CAST(SMALLNUM AS DECIMAL), CAST(BIGNUM AS DECIMAL), CAST(FLOATNUM AS DECIMAL), CAST(DECIMALNUM AS DECIMAL) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_DECIMAL_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS DECIMAL)");
        project.addStmtProcedure("ORDER_DECIMAL_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS DECIMAL)");

        project.addStmtProcedure("WHERE_DECIMAL_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS DECIMAL) = ?");
        project.addStmtProcedure("WHERE_DECIMAL_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS DECIMAL) = ?");

        project.addStmtProcedure("DISPLAY_VARCHAR",
                "select CAST(INTEGERNUM AS VARCHAR), CAST(TINYNUM AS VARCHAR), " +
                        "CAST(SMALLNUM AS VARCHAR), CAST(BIGNUM AS VARCHAR), CAST(FLOATNUM AS VARCHAR), CAST(DECIMALNUM AS VARCHAR) from NUMBER_TYPES order by INTEGERNUM");

        project.addStmtProcedure("ORDER_VARCHAR_CAST_INTEGER",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(INTEGERNUM AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_TINYINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(TINYNUM    AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_SMALLINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(SMALLNUM   AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_BIGINT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(BIGNUM     AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_FLOAT",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(FLOATNUM   AS VARCHAR)");
        project.addStmtProcedure("ORDER_VARCHAR_CAST_DECIMAL",
                "select INTEGERNUM from NUMBER_TYPES order by CAST(DECIMALNUM AS VARCHAR)");

        project.addStmtProcedure("WHERE_VARCHAR_CAST_INTEGER",
                "select count(*) from NUMBER_TYPES where CAST(INTEGERNUM AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_TINYINT",
                "select count(*) from NUMBER_TYPES where CAST(TINYNUM    AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_SMALLINT",
                "select count(*) from NUMBER_TYPES where CAST(SMALLNUM   AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_BIGINT",
                "select count(*) from NUMBER_TYPES where CAST(BIGNUM     AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_FLOAT",
                "select count(*) from NUMBER_TYPES where CAST(FLOATNUM   AS VARCHAR) = ?");
        project.addStmtProcedure("WHERE_VARCHAR_CAST_DECIMAL",
                "select count(*) from NUMBER_TYPES where CAST(DECIMALNUM AS VARCHAR) = ?");

        project.addStmtProcedure("CHAR", "select id, CHAR(?) from P1 where id = ?");

        // CONFIG #1: Local Site/Partitions running on JNI backend
        VoltServerConfig config = new LocalCluster("fixedsql-onesite.jar",
                1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // alternative to enable for debugging */ config = new LocalCluster("IPC-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        // CONFIG #2: HSQL
        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        // no clustering tests for functions

        return builder;
    }
}
