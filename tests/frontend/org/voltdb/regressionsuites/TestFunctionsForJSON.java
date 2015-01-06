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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * Tests for SQL that was recently (early 2012) unsupported, related to JSON
 * functions (which are supported by VoltDB but not HSQL or other databases).
 */

public class TestFunctionsForJSON extends RegressionSuite {

    private static final long[] UPDATED_1ROW = new long[]{1};
    private static final long[][] EMPTY_TABLE = new long[][]{};
    private static final long[][] TABLE_ROW1 = new long[][]{{1}};
    private static final long[][] TABLE_ROW2 = new long[][]{{2}};
    private static final long[][] TABLE_ROW3 = new long[][]{{3}};
    private static final long[][] TABLE_ROW4 = new long[][]{{4}};
    private static final long[][] TABLE_ROW5 = new long[][]{{5}};
    private static final long[][] TABLE_ROW7 = new long[][]{{7}};
    private static final long[][] TABLE_ROW8 = new long[][]{{8}};
    private static final long[][] TABLE_ROW9 = new long[][]{{9}};
    private static final long[][] TABLE_ROW10 = new long[][]{{10}};
    private static final long[][] TABLE_ROWS12  = new long[][]{{1},{2}};
    private static final long[][] TABLE_ROWS13  = new long[][]{{1},{3}};
    private static final long[][] TABLE_ROWS23  = new long[][]{{2},{3}};
    private static final long[][] TABLE_ROWS47  = new long[][]{{4},{7}};
    private static final long[][] TABLE_ROWS59  = new long[][]{{5},{9}};
    private static final long[][] TABLE_ROWS123 = new long[][]{{1},{2},{3}};
    private static final long[][] TABLE_ROWS234 = new long[][]{{2},{3},{4}};
    private static final long[][] FULL_TABLE = new long[][]{{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15}};
    private static final int TOTAL_NUM_ROWS = FULL_TABLE.length;
    private static final boolean DEBUG = false;

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    /**
     * @return
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    private void loadJS1(Client client) throws IOException, NoConnectionsException, ProcCallException
    {

        final String jstemplate = "{\n" +
                "    \"id\": %d,\n" +
                "    \"bool\": true,\n" +
                "    \"numeric\": 1.2,\n" +
                "    \"inner\": {\n" +
                "        \"veggies\": \"good for you\",\n" +
                "        \"贾鑫Vo\": \"wakarimasen\",\n" +
                "        \"second\": {\n" +
                "            \"fruits\": 1,\n" +
                "            \"third\": {\n" +
                "                \"meats\": \"yum\",\n" +
                "                \"dairy\": \"%d\",\n" +
                "                \"numeric\": 2.3\n" +
                "            }\n" +
                "        },\n" +
                "        \"arr\": [\n" +
                "            0,\n" +
                "            %d,\n" +
                "            3.4\n" +
                "        ]\n" +
                "    },\n" +
                "    \"arr\": [\n" +
                "        0,\n" +
                "        %d,\n" +
                "        100\n" +
                "    ],\n" +
                "    \"arr3d\": [\n" +
                "        0,\n" +
                "        [\n" +
                "            \"One\",\n" +
                "            [\n" +
                "                2,\n" +
                "                %d,\n" +
                "                4.5\n" +
                "            ]\n" +
                "        ],\n" +
                "        {\n" +
                "            \"veggies\": \"good for you\",\n" +
                "            \"dairy\": \"%d\",\n" +
                "            \"numeric\": 5.6\n" +
                "        }\n" +
                "    ],\n" +
                "    \"dot.char\": \"foo.bar\",\n" +
                "    \"bracket][[] [ ] chars\": \"[foo]\",\n" +
                "    \"tag\": \"%s\",\n" +
                "    \"last\": \"\\\"foobar\\\"\"\n" +
                "}";

        ClientResponse cr;
        cr = client.callProcedure("JS1.insert", 1, String.format(jstemplate, 1, 1, 1, 1, 1, 1, "One"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 2, String.format(jstemplate, 2, 2, 2, 2, 2, 2, "Two"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 3, String.format(jstemplate, 3, 3, 3, 3, 3, 3, "Three"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 4, "{\"id\":4,\"bool\": false}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 5, "{}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 6, "[]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 7, "{\"id\":7,\"funky\": null}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 8, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 9, "{\"id\":9, \"贾鑫Vo\":\"分かりません\"}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 10, "[1,2,3]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 11, "{\"null\": \"foo\", \"\\\"null\\\"\": \"bar\"}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 12, "{\"foo\": \"null\", \"\\\"foo\\\"\": \"bar\"}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 13, "\"foobar\"");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 14, "true");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert", 15, 42);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private String getDocFromId(Client client, int id) throws Exception {
        ClientResponse cr = client.callProcedure("GetDocFromId", id);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable result = cr.getResults()[0];
        assertTrue(result.advanceRow());
        return result.getString(0);
    }

    private void debugPrintJsonDoc(boolean print, String description, Client client, int... rowIds) throws Exception {
        if (print) {
            System.out.println();
            for (int id : rowIds) {
                System.out.println("JSON document (DOC column), for id=" + id + " ("
                                   + description + "):\n" + getDocFromId(client, id));
            }
        }
    }

    private void debugPrintJsonDoc(String description, Client client, int... rowIds) throws Exception {
        debugPrintJsonDoc(DEBUG, description, client, rowIds);
    }

    private void testProcWithValidJSON(long[] expectedResult, Client client,
                                       String procName, Object... parameters) throws Exception {
        ClientResponse cr = client.callProcedure(procName, parameters);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable result = cr.getResults()[0];
        validateRowOfLongs(result, expectedResult);
    }

    private void testProcWithValidJSON(long[][] expectedResult, Client client,
                                       String procName, Object... parameters) throws Exception {
        ClientResponse cr = client.callProcedure(procName, parameters);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable result = cr.getResults()[0];
        validateTableOfLongs(result, expectedResult);
    }

    private void testProcWithValidJSON(String expectedResult, Client client,
                                       String procName, Object... parameters) throws Exception {
        ClientResponse cr = client.callProcedure(procName, parameters);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable result = cr.getResults()[0];
        assertTrue(result.advanceRow());
        assertEquals(expectedResult, result.get(0, result.getColumnType(0)));
    }

    private void testProcWithInvalidJSON(String expectedErrorMessage, Client client, int rowId,
                                         String procName, Object... parameters) throws Exception {
        String procDescription = "'" + procName + "', with parameters:";
        for (int i=0; i < parameters.length; i++) {
            procDescription += "\n" + (parameters[i] == null ? "null" : parameters[i].toString());
        }
        try {
            testProcWithValidJSON("FAILURE", client, procName, parameters);
        } catch (ProcCallException pcex) {
            String actualMessage = pcex.getMessage();
            assertTrue("For " + procDescription + "\nExpected error message containing:\n'"
                       + expectedErrorMessage + "'\nbut got:\n'" + actualMessage + "'",
                       actualMessage.contains(expectedErrorMessage));
        } catch (AssertionError aex) {
            debugPrintJsonDoc(true, "after call to " + procDescription, client, rowId);
            fail("Failed document validity check for " + procDescription + "\nExpected error message containing:\n'"
                    + expectedErrorMessage + "'\nbut got:\n'" + aex.getMessage() + "'");
        }
    }

    private void testProcWithInvalidJSON(String expectedErrorMessage, Client client,
                                         String procName, Object... parameters) throws Exception {
        testProcWithInvalidJSON(expectedErrorMessage, client, 1, procName, parameters);
    }

    /** Used to test cases involving minimal JSON documents. */
    public void testMinimalJSONdocuments() throws Exception {
        Client client = getClient();
        loadJS1(client);

        testProcWithValidJSON(new long[][]{{13}}, client, "DocEqualsProc", "\"foobar\"");
        testProcWithValidJSON(new long[][]{{14}}, client, "DocEqualsProc", "true");
        testProcWithValidJSON(new long[][]{{15}}, client, "DocEqualsProc", 42);
        testProcWithValidJSON(new long[][]{{5}}, client, "DocEqualsProc", "{}");
        testProcWithValidJSON(new long[][]{{6}}, client, "DocEqualsProc", "[]");

        testProcWithValidJSON(new long[][]{{6}}, client, "ArrayLengthDocProc", "0");
        testProcWithValidJSON(TABLE_ROW10, client, "ArrayLengthDocProc", "3");
        testProcWithValidJSON(EMPTY_TABLE, client, "ArrayLengthDocProc", "1");
        testProcWithValidJSON(EMPTY_TABLE, client, "ArrayLengthDocProc", "2");
    }

    public void testFIELDFunction() throws Exception {
        ClientResponse cr;
        VoltTable result;
        Client client = getClient();
        loadJS1(client);

        // Debug print, echoing the initial JSON documents, to stdout
        if (DEBUG) {
            int[] ids = new int[TOTAL_NUM_ROWS];
            for (int id=1; id <= TOTAL_NUM_ROWS; id++) {
                ids[id-1] = id;
            }
            debugPrintJsonDoc("initial values", client, ids);
        }

        cr = client.callProcedure("IdFieldProc", "id", "1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1L,result.getLong(0));

        try {
            cr = client.callProcedure("IdFieldProc", "id", 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        catch ( ProcCallException pcex) {
            fail("parameter check failed");
        }

        try {
            cr = client.callProcedure("IdFieldProc", 1, "1");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        catch ( ProcCallException pcex) {
            fail("parameter check failed");
        }

        cr = client.callProcedure("IdFieldProc", "tag", "Three");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(3L,result.getLong(0));

        cr = client.callProcedure("IdFieldProc", "bool", "false");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(4L,result.getLong(0));

        cr = client.callProcedure("IdFieldProc", "贾鑫Vo", "分かりません");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(9L,result.getLong(0));

        cr = client.callProcedure("NullFieldProc", "funky");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(TOTAL_NUM_ROWS, result.getRowCount());

        cr = client.callProcedure("NullFieldProc", "id");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        long[] expectedResults = new long[]{5L, 6L, 8L, 10L, 11L, 12L, 13L, 14L, 15L};
        assertEquals(expectedResults.length, result.getRowCount());
        for (long expResult : expectedResults) {
            assertTrue(result.advanceRow());
            assertEquals(expResult,result.getLong(0));
        }

        cr = client.callProcedure("InnerFieldProc", "贾鑫Vo", "wakarimasen");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(3, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(2L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(3L,result.getLong(0));

        cr = client.callProcedure("IdFieldProc", "arr", "[0,2,100]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(2L,result.getLong(0));

        cr = client.callProcedure("@AdHoc", // test scalar not an object
                                  "SELECT FIELD(FIELD(DOC, 'id'), 'value') FROM JS1 WHERE ID = 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        result.getString(0);
        assertTrue(result.wasNull());

        cr = client.callProcedure("@AdHoc", // test array not an object
                                  "SELECT FIELD(FIELD(DOC, 'arr'), 'value') FROM JS1 WHERE ID = 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        result.getString(0);
        assertTrue(result.wasNull());

        // Test that FIELD function is case-sensitive
        testProcWithValidJSON(EMPTY_TABLE, client, "IdFieldProc", "tag", "one");
        testProcWithValidJSON(EMPTY_TABLE, client, "IdFieldProc", "tag", "ONE");
        testProcWithValidJSON(TABLE_ROW1,  client, "IdFieldProc", "tag", "One");

        // Same case-sensitive tests, using ad-hoc queries
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'tag') = 'one' ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'tag') = 'ONE' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW1,  client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'tag') = 'One' ORDER BY ID");
    }

    /** Used to test ENG-6620, part 1 (dotted path notation) / ENG-6832. */
    public void testFIELDFunctionWithDotNotation() throws Exception {
        Client client = getClient();
        loadJS1(client);

        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "inner.veggies", "good for you");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "inner.second.fruits", 1);
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "inner.second.third.meats", "yum");
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "inner.second.third.dairy", "1");

        // Same dot-notation tests, using ad-hoc queries
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'inner.veggies') = 'good for you' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'inner.second.fruits') = '1' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'inner.second.third.meats') = 'yum' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW1,    client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'inner.second.third.dairy') = '1' ORDER BY ID");

        // Test \ escape for dot in element name, not used for sub-path
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "dot\\.char", "foo.bar");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "dot.char", "foo.bar");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'dot\\.char') = 'foo.bar' ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE,   client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'dot.char') = 'foo.bar' ORDER BY ID");

        // Verify that dot notation returns nothing when used on a primitive
        // (integer, float, boolean, string), or an array
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "id.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "numeric.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "bool.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "tag.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "last.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "arr.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "arr.0");

        // Same primitive/array tests, using ad-hoc queries
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'id.veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'numeric.veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'bool.veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'tag.veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'last.veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr.veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr.0') IS NOT NULL ORDER BY ID");

        // Compare with similar behavior when FIELD is called twice
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullField2Proc", "id",   "veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullField2Proc", "numeric", "veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullField2Proc", "bool", "veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullField2Proc", "last", "veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullField2Proc", "arr",  "veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullField2Proc", "arr",  "0");
        testProcWithInvalidJSON("Syntax error: value, object or array expected", client, "NotNullField2Proc", "tag", "veggies");

        // Same FIELD(FIELD) tests, using ad-hoc queries
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, 'id'), 'veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, 'numeric'), 'veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, 'bool'), 'veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, 'last'), 'veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, 'arr'), 'veggies') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, 'arr'), 'veggies') IS NOT NULL ORDER BY ID");
        testProcWithInvalidJSON("Syntax error: value, object or array expected", client, "@AdHoc",
                                "SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, 'tag'), 'veggies') IS NOT NULL ORDER BY ID");
    }

    /** Used to test ENG-6620, part 2 (array index notation) / ENG-6832. */
    public void testFIELDFunctionWithIndexNotation() throws Exception {
        Client client = getClient();
        loadJS1(client);

        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr[0]", 0);
        testProcWithValidJSON(TABLE_ROW2,    client, "IdFieldProc", "arr[1]", 2);
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr[2]", 100);
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr[-1]", 100);
        testProcWithValidJSON(TABLE_ROWS123, client, "NotNullFieldProc", "arr[-1]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr[3]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr[" + Integer.MAX_VALUE + "]");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[1][0]", "One");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[1][1][0]", 2);
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "arr3d[1][1][1]", 3);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[1][1][3]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[1][1][" + Integer.MAX_VALUE + "]");

        // Same index-notation tests, using ad-hoc queries
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[0]') = '0' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW2,    client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[1]') = '2' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[2]') = '100' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[-1]') = '100' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[-1]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE,   client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[3]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE,   client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[" + Integer.MAX_VALUE + "]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[1][0]') = 'One' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[1][1][0]') = '2' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW3,    client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[1][1][1]') = '3' ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE,   client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[1][1][3]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE,   client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[1][1][" + Integer.MAX_VALUE + "]') IS NOT NULL ORDER BY ID");

        // Test using 2147483646, which is Integer.MAX_VALUE - 1
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr[2147483646]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[1][1][2147483646]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[2147483646]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE,   client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[1][1][2147483646]') IS NOT NULL ORDER BY ID");

        // Test \ escape for brackets in element name, not used for array index
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "bracket]\\[\\[] \\[ ] chars", "[foo]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "bracket]]  ] chars", "[foo]");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'bracket]\\[\\[] \\[ ] chars') = '[foo]' ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE,   client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'bracket]]  ] chars') = '[foo]' ORDER BY ID");

        // Verify that index notation returns nothing when used on a primitive
        // (integer, float, boolean, string), or an object
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "id[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "numeric[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "bool[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "tag[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "last[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "inner[0]");

        // Same primitive/object tests, using ad-hoc queries
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'id[0]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'numeric[0]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'bool[0]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'tag[0]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'last[0]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'inner[0]') IS NOT NULL ORDER BY ID");

        // Compare with similar behavior when FIELD is called with ARRAY_ELEMENT
        testProcWithValidJSON(FULL_TABLE, client, "NullArrayProc", "id", 0);
        testProcWithValidJSON(FULL_TABLE, client, "NullArrayProc", "numeric", 0);
        testProcWithValidJSON(FULL_TABLE, client, "NullArrayProc", "bool", 0);
        testProcWithValidJSON(FULL_TABLE, client, "NullArrayProc", "last", 0);
        testProcWithValidJSON(FULL_TABLE, client, "NullArrayProc", "inner", 0);
        testProcWithInvalidJSON("Syntax error: value, object or array expected", client, "NullArrayProc", "tag", 0);

        // Same ARRAY_ELEMENT tests, using ad-hoc queries
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE ARRAY_ELEMENT(FIELD(DOC, 'id'), 0) IS NULL ORDER BY ID");
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE ARRAY_ELEMENT(FIELD(DOC, 'numeric'), 0) IS NULL ORDER BY ID");
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE ARRAY_ELEMENT(FIELD(DOC, 'bool'), 0) IS NULL ORDER BY ID");
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE ARRAY_ELEMENT(FIELD(DOC, 'last'), 0) IS NULL ORDER BY ID");
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE ARRAY_ELEMENT(FIELD(DOC, 'inner'), 0) IS NULL ORDER BY ID");
        testProcWithInvalidJSON("Syntax error: value, object or array expected", client, "@AdHoc",
                                "SELECT ID FROM JS1 WHERE ARRAY_ELEMENT(FIELD(DOC, 'tag'), 0) IS NULL ORDER BY ID");

        // Test index notation with no name specified (a weird case!)
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[0]");
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[1]");
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[2]");
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[-1]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "[3]");
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[0]", 1);
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[1]", 2);
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[2]", 3);
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[-1]", 3);

        // Same nameless array tests, using ad-hoc queries
        testProcWithValidJSON(TABLE_ROW10, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, '[0]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW10, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, '[1]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW10, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, '[2]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW10, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, '[-1]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(EMPTY_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, '[3]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW10, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, '[0]') = '1' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW10, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, '[1]') = '2' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW10, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, '[2]') = '3' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW10, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, '[-1]') = '3' ORDER BY ID");
    }

    /** Used to test ENG-6620, part 3 (dotted path and array index notation, combined) / ENG-6832. */
    public void testFIELDFunctionWithDotAndIndexNotation() throws Exception {
        Client client = getClient();
        loadJS1(client);

        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "inner.arr[0]", 0);
        testProcWithValidJSON(TABLE_ROW2,    client, "IdFieldProc", "inner.arr[1]", 2);
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[2].veggies", "good for you");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[-1].veggies", "good for you");
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "arr3d[2].dairy", 3);
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "arr3d[-1].dairy", 3);
        testProcWithValidJSON(TABLE_ROWS123, client, "NotNullFieldProc", "arr3d[-1]");

        // Same tests, using ad-hoc queries
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'inner.arr[0]') = '0' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW2,    client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'inner.arr[1]') = '2' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[2].veggies') = 'good for you' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[-1].veggies') = 'good for you' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW3,    client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[2].dairy') = '3' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROW3,    client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[-1].dairy') = '3' ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[-1]') IS NOT NULL ORDER BY ID");
    }

    /** Used to test ENG-6620 / ENG-6832, for numeric, floating-point data, which
     *  needs to be handled slightly differently, due to the tendency to add a
     *  trailing zero; including with dotted path and/or array index notation. */
    public void testFIELDFunctionWithNumericData() throws Exception {
        Client client = getClient();
        loadJS1(client);

        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "numeric", "1.2", "1.20");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "inner.second.third.numeric", "2.3", "2.30");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "arr3d[1][1][2]", "4.5", "4.50");
        testProcWithValidJSON(TABLE_ROWS123, client, "NotNullFieldProc", "arr3d[1][-1][-1]");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "arr3d[1][-1][-1]", "4.5", "4.50");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "inner.arr[2]", "3.4", "3.40");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "arr3d[2].numeric", "5.6", "5.60");

        // Same numeric tests, using ad-hoc queries
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'numeric') IN ('1.2', '1.20') ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'inner.second.third.numeric') IN ('2.3', '2.30') ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[1][1][2]') IN ('4.5', '4.50') ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[1][-1][-1]') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[1][-1][-1]') IN ('4.5', '4.50') ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'inner.arr[2]') IN ('3.4', '3.40') ORDER BY ID");
        testProcWithValidJSON(TABLE_ROWS123, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr3d[2].numeric') IN ('5.6', '5.60') ORDER BY ID");
    }

    /** Used to test ENG-6832, for various null values (in both parameters,
     *  with and without quotes), and related queries. */
    public void testFIELDFunctionWithNullValues() throws Exception {
        Client client = getClient();
        loadJS1(client);

        // Call the FIELD function with null first (JSON doc column) parameter
        // (using both the "NullSetFieldDocProc" Stored Proc and ad-hoc queries)
        testProcWithValidJSON(FULL_TABLE, client, "NullFieldDocProc", null, null);
        testProcWithValidJSON(FULL_TABLE, client, "NullFieldDocProc", null, 0);
        testProcWithValidJSON(FULL_TABLE, client, "NullFieldDocProc", null, "true");
        testProcWithValidJSON(FULL_TABLE, client, "NullFieldDocProc", null, "id");
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(null, null) IS NULL ORDER BY ID");
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(null, '0') IS NULL ORDER BY ID");
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(null, 'true') IS NULL ORDER BY ID");
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(null, 'id') IS NULL ORDER BY ID");
        testProcWithValidJSON((String) null, client, "@AdHoc", "SELECT FIELD(null, null) FROM JS1 WHERE ID = 5");

        // Or, similarly, where the DOC column, or 'id' within it, has null value
        long[][] idNotDefined = new long[][]{{5},{6},{8},{10},{11},{12},{13},{14},{15}};
        testProcWithValidJSON(idNotDefined, client, "NullFieldProc", "id");
        testProcWithValidJSON(idNotDefined, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'id') IS NULL ORDER BY ID");
        testProcWithValidJSON((String) null, client, "@AdHoc", "SELECT FIELD(DOC, 'id') FROM JS1 WHERE ID = 5");
        testProcWithValidJSON((String) null, client, "@AdHoc", "SELECT FIELD(DOC, 'id') FROM JS1 WHERE ID = 8");

        // Call the FIELD function with null second ("path") parameter
        // (using both the "NullSetFieldProc" Stored Proc and an ad-hoc query)
        testProcWithInvalidJSON("Invalid FIELD path argument (SQL null)", client, "NullFieldProc", (Object) null);
        testProcWithInvalidJSON("Invalid FIELD path argument (SQL null)", client, "@AdHoc",
                                "SELECT ID FROM JS1 WHERE FIELD(DOC, null) IS NULL");

        // Call the FIELD function with quoted-null second ("path") parameter
        // (using both the "SelectFieldProc" Stored Proc and an ad-hoc query)
        testProcWithValidJSON((String) null, client, "SelectFieldProc", "null", 5);
        testProcWithValidJSON((String) null, client, "@AdHoc", "SELECT FIELD(DOC, 'null') FROM JS1 WHERE ID = 5");
        testProcWithValidJSON("foo", client, "SelectFieldProc", "null", 11);
        testProcWithValidJSON("foo", client, "@AdHoc", "SELECT FIELD(DOC, 'null') FROM JS1 WHERE ID = 11");
        // Similar single-quotes, but without null:
        testProcWithValidJSON((String) null, client, "SelectFieldProc", "foo", 5);
        testProcWithValidJSON((String) null, client, "@AdHoc", "SELECT FIELD(DOC, 'foo') FROM JS1 WHERE ID = 5");
        testProcWithValidJSON("null", client, "SelectFieldProc", "foo", 12);
        testProcWithValidJSON("null", client, "@AdHoc", "SELECT FIELD(DOC, 'foo') FROM JS1 WHERE ID = 12");
        // Note: quotes for "null" are double-escaped, for Java and JSON; the
        // resulting JSON document is actually: {"\"null\"":-1}
        testProcWithValidJSON((String) null, client, "SelectFieldProc", "\"null\"", 5);
        testProcWithValidJSON((String) null, client, "@AdHoc", "SELECT FIELD(DOC, '\"null\"') FROM JS1 WHERE ID = 5");
        testProcWithValidJSON("bar", client, "SelectFieldProc", "\"null\"", 11);
        testProcWithValidJSON("bar", client, "@AdHoc", "SELECT FIELD(DOC, '\"null\"') FROM JS1 WHERE ID = 11");
        // Similar double-quotes, but without null:
        testProcWithValidJSON((String) null, client, "SelectFieldProc", "\"foo\"", 5);
        testProcWithValidJSON((String) null, client, "@AdHoc", "SELECT FIELD(DOC, '\"foo\"') FROM JS1 WHERE ID = 5");
        testProcWithValidJSON("bar", client, "SelectFieldProc", "\"foo\"", 12);
        testProcWithValidJSON("bar", client, "@AdHoc", "SELECT FIELD(DOC, '\"foo\"') FROM JS1 WHERE ID = 12");

        // More single-quotes tests, with and without null
        testProcWithValidJSON(new long[][]{{11}}, client, "NotNullFieldProc", "null");
        testProcWithValidJSON(new long[][]{{12}}, client, "NotNullFieldProc", "foo");
        testProcWithValidJSON(new long[][]{{11}}, client, "IdFieldProc", "null", "foo");
        testProcWithValidJSON(new long[][]{{12}}, client, "IdFieldProc", "foo", "null");
        testProcWithValidJSON(new long[][]{{11}}, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'null') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(new long[][]{{12}}, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'foo') IS NOT NULL ORDER BY ID");
        testProcWithValidJSON(new long[][]{{11}}, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'null') = 'foo' ORDER BY ID");
        testProcWithValidJSON(new long[][]{{12}}, client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'foo') = 'null' ORDER BY ID");
    }

    /** Used to test ENG-6832 (invalid array index notation, for FIELD). */
    public void testFIELDFunctionWithInvalidIndexNotation() throws Exception {
        Client client = getClient();
        loadJS1(client);
        testProcWithInvalidJSON("Invalid JSON path: Array index less than -1 [position 6]",
                                client, "IdFieldProc", "arr[-2]",  0);
        testProcWithInvalidJSON("Invalid JSON path: Unexpected character in array index [position 4]",
                                client, "IdFieldProc", "arr[]",    0);
        testProcWithInvalidJSON("Invalid JSON path: Unexpected character in array index [position 4]",
                                client, "IdFieldProc", "arr[abc]", 0);
        testProcWithInvalidJSON("Invalid JSON path: Unexpected termination (unterminated array access) [position 3]",
                                client, "IdFieldProc", "arr[",     0);
        testProcWithInvalidJSON("Invalid JSON path: Missing ']' after array index [position 6]",
                                client, "IdFieldProc", "arr[123",  0);
        testProcWithInvalidJSON("Invalid JSON path: Array index less than -1 [position 14]",
                                client, "IdFieldProc", "arr[" + Integer.MIN_VALUE + "]",  0);

        // Same invalid-query tests, using ad-hoc queries
        testProcWithInvalidJSON("Invalid JSON path: Array index less than -1 [position 6]",
                                client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[-2]') = '0' ORDER BY ID");
        testProcWithInvalidJSON("Invalid JSON path: Unexpected character in array index [position 4]",
                                client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[]') = '0' ORDER BY ID");
        testProcWithInvalidJSON("Invalid JSON path: Unexpected character in array index [position 4]",
                                client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[abc]') = '0' ORDER BY ID");
        testProcWithInvalidJSON("Invalid JSON path: Unexpected termination (unterminated array access) [position 3]",
                                client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[') = '0' ORDER BY ID");
        testProcWithInvalidJSON("Invalid JSON path: Missing ']' after array index [position 6]",
                                client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[123') = '0' ORDER BY ID");
        testProcWithInvalidJSON("Invalid JSON path: Array index less than -1 [position 14]",
                                client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[" + Integer.MIN_VALUE + "]') = '0' ORDER BY ID");

        // Test using 2147483648, which is Integer.MAX_VALUE + 1
        testProcWithInvalidJSON("Invalid JSON path: Array index greater than the maximum integer value [position 13]",
                                client, "IdFieldProc", "arr[2147483648]", 0);
        testProcWithInvalidJSON("Invalid JSON path: Array index greater than the maximum integer value [position 13]",
                                client, "@AdHoc", "SELECT ID FROM JS1 WHERE FIELD(DOC, 'arr[2147483648]') = '0' ORDER BY ID");

        // Test the wrong number of parameters
        testProcWithInvalidJSON("PROCEDURE IdFieldProc EXPECTS 2 PARAMS, BUT RECEIVED 0",
                                client, "IdFieldProc");
        testProcWithInvalidJSON("PROCEDURE IdFieldProc EXPECTS 2 PARAMS, BUT RECEIVED 1",
                                client, "IdFieldProc", "arr[0]");
        testProcWithInvalidJSON("PROCEDURE IdFieldProc EXPECTS 2 PARAMS, BUT RECEIVED 3",
                                client, "IdFieldProc", "arr[0]", 0, 0);
    }

    /** Used to test ENG-6879 (invalid array index notation, for SET_FIELD). */
    public void testSET_FIELDFunctionWithInvalidIndexNotation() throws Exception {
        Client client = getClient();
        loadJS1(client);
        testProcWithInvalidJSON("Invalid JSON path: Array index less than -1 [position 6]",
                                client, "UpdateSetFieldProc", "arr[-2]",  "-1", 1);
        testProcWithInvalidJSON("Invalid JSON path: Unexpected character in array index [position 4]",
                                client, "UpdateSetFieldProc", "arr[]",    "-1", 1);
        testProcWithInvalidJSON("Invalid JSON path: Unexpected character in array index [position 4]",
                                client, "UpdateSetFieldProc", "arr[abc]", "-1", 1);
        testProcWithInvalidJSON("Invalid JSON path: Unexpected termination (unterminated array access) [position 3]",
                                client, "UpdateSetFieldProc", "arr[",     "-1", 1);
        testProcWithInvalidJSON("Invalid JSON path: Missing ']' after array index [position 6]",
                                client, "UpdateSetFieldProc", "arr[123",  "-1", 1);
        // 1568 is the minimum array index that will trigger this error
        testProcWithInvalidJSON("exceeds the size of the VARCHAR(8192) column",
                                client, "UpdateSetFieldProc", "arr[1568]", "-1", 1);
        testProcWithInvalidJSON("Invalid JSON path: Array index less than -1 [position 11]",
                                client, "UpdateSetFieldProc", "arr[" + Integer.MIN_VALUE + "]", "-1", 1);

        // Same invalid-query tests, using ad-hoc queries
        testProcWithInvalidJSON("Invalid JSON path: Array index less than -1 [position 6]",
                                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[-2]', '-1') WHERE ID = 1");
        testProcWithInvalidJSON("Invalid JSON path: Unexpected character in array index [position 4]",
                                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[]', '-1') WHERE ID = 1");
        testProcWithInvalidJSON("Invalid JSON path: Unexpected character in array index [position 4]",
                                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[abc]', '-1') WHERE ID = 1");
        testProcWithInvalidJSON("Invalid JSON path: Unexpected termination (unterminated array access) [position 3]",
                                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[', '-1') WHERE ID = 1");
        testProcWithInvalidJSON("Invalid JSON path: Missing ']' after array index [position 6]",
                                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[123', '-1') WHERE ID = 1");
        // 1568 is the minimum array index that will trigger this error
        testProcWithInvalidJSON("exceeds the size of the VARCHAR(8192) column",
                                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[1568]', '-1') WHERE ID = 1");
        testProcWithInvalidJSON("Invalid JSON path: Array index less than -1 [position 11]",
                                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[" + Integer.MIN_VALUE + "]', '-1') WHERE ID = 1");

        // Test using 2147483648, which is Integer.MAX_VALUE + 1
        testProcWithInvalidJSON("Invalid JSON path: Array index greater than the maximum allowed value of 500000 [position 10]",
                                client, "UpdateSetFieldProc", "arr[2147483648]", "-1", 1);
        testProcWithInvalidJSON("Invalid JSON path: Array index greater than the maximum allowed value of 500000 [position 10]",
                                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[2147483648]', '-1') WHERE ID = 1");

        // Test the wrong number of parameters
        testProcWithInvalidJSON("PROCEDURE UpdateSetFieldProc EXPECTS 3 PARAMS, BUT RECEIVED 0",
                                client, "UpdateSetFieldProc");
        testProcWithInvalidJSON("PROCEDURE UpdateSetFieldProc EXPECTS 3 PARAMS, BUT RECEIVED 1",
                                client, "UpdateSetFieldProc", "arr[0]");
        testProcWithInvalidJSON("PROCEDURE UpdateSetFieldProc EXPECTS 3 PARAMS, BUT RECEIVED 2",
                                client, "UpdateSetFieldProc", "arr[0]", "-1");
        testProcWithInvalidJSON("PROCEDURE UpdateSetFieldProc EXPECTS 3 PARAMS, BUT RECEIVED 4",
                                client, "UpdateSetFieldProc", "arr[0]", "-1", 1, 1);
    }

    public void testSET_FIELDFunctionWithMediumLargeIndexValue() throws Exception {
        // Test using 8192/5, which is just over what fits in the declared column size when
        // all of the null array elements are spelled out: "...null,null,null,..."
        // but not large enough to exceed absolute maximum string size.
        int largeIndex = 8192/5;
        Client client = getClient();
        loadJS1(client);
        testProcWithInvalidJSON("exceeds the size of the VARCHAR(8192) column",
                client, "UpdateSetFieldProc", "arr[" + largeIndex + "]", "-1", 1);
        testProcWithInvalidJSON("exceeds the size of the VARCHAR(8192) column",
                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[" + largeIndex + "]', '-1') WHERE ID = 1");
    }

    public void testSET_FIELDFunctionWithLargeIndexValue() throws Exception {
        // Test using 500000, which is the max allowed for SET_FIELD.
        int largeIndex = 500000;
        Client client = getClient();
        loadJS1(client);
        testProcWithInvalidJSON("exceeds the size of the VARCHAR(1048576) column",
                client, "UpdateSetFieldProc", "arr[" + largeIndex + "]", "-1", 1);
        testProcWithInvalidJSON("exceeds the size of the VARCHAR(1048576) column",
                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[" + largeIndex + "]', '-1') WHERE ID = 1");
    }

    /** Used to test bug ENG-6979: SET_FIELD(x,arr[largeIndex],z) causes memory error. */
    public void testSET_FIELDFunctionWithVeryLargeIndexValue() throws Exception {
        // Test using 2147483646, which is Integer.MAX_VALUE - 1
        int largeIndex = 2147483646;
        Client client = getClient();
        loadJS1(client);
        testProcWithInvalidJSON("Invalid JSON path: Array index greater than the maximum allowed value of 500000 [position 10]",
                client, "UpdateSetFieldProc", "arr[" + largeIndex + "]", "-1", 1);
        testProcWithInvalidJSON("Invalid JSON path: Array index greater than the maximum allowed value of 500000 [position 10]",
                client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[" + largeIndex + "]', '-1') WHERE ID = 1");
    }

    /** Used to test bug ENG-6978: SET_FIELD(x,arr[Integer.MAX_VALUE],z) sets value arr[]. */
    public void testSET_FIELDFunctionWithIntegerMaxValueIndex() throws Exception {
        Client client = getClient();
        loadJS1(client);
        testProcWithInvalidJSON("Invalid JSON path: Array index greater than the maximum allowed value of 500000 [position 10]",
                                client, 5, "SelectSetFieldProc", "arr[" + Integer.MAX_VALUE + "]", "-1", 5);
        testProcWithInvalidJSON("Invalid JSON path: Array index greater than the maximum allowed value of 500000 [position 10]",
                                client, 5, "@AdHoc", "SELECT SET_FIELD(DOC, 'arr[" + Integer.MAX_VALUE + "]', '-1') FROM JS1 WHERE ID = 5");
    }

    /** Used to test ENG-6621, part 1 (without dotted path or array index notation) / ENG-6879. */
    public void testSET_FIELDFunction() throws Exception {
        Client client = getClient();
        loadJS1(client);

        // Confirm expected results before calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "id", 1);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "id", -1);
        testProcWithValidJSON(TABLE_ROW2,    client, "IdFieldProc", "id", 2);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "id", -2);
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "bool", "true");
        testProcWithValidJSON(TABLE_ROW4,    client, "IdFieldProc", "bool", "false");
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "tag", "Three");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "tag", "Four");
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "tag", "One");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "tag", "Five");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newint");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newbool");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newstr");

        // Call the "UpdateSetFieldProc" Stored Proc, which uses the SET_FIELD function
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "id",   "-1",       1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "bool", "false",    2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "tag",  "\"Four\"", 3);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newint", "7",     4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newbool", "true",  4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newstr", "\"newvalue\"", 4);

        // Call the SET_FIELD function directly, using ad-hoc queries
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'id', '-2') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'bool', 'false') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'tag', '\"Five\"') WHERE ID = 1");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newint', '7') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newbool', 'true') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newstr', '\"newvalue\"') WHERE ID = 7");

        debugPrintJsonDoc("with new primitive values", client, 4, 7);

        // Confirm modified results after calling the SET_FIELD function
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "id", 1);
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "id", -1);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "id", 2);
        testProcWithValidJSON(TABLE_ROW2,    client, "IdFieldProc", "id", -2);
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "bool", "true");
        testProcWithValidJSON(TABLE_ROWS234, client, "IdFieldProc", "bool", "false");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "tag", "Three");
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "tag", "Four");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "tag", "One");
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "tag", "Five");
        testProcWithValidJSON(TABLE_ROWS47,  client, "NotNullFieldProc", "newint");
        testProcWithValidJSON(TABLE_ROWS47,  client, "NotNullFieldProc", "newbool");
        testProcWithValidJSON(TABLE_ROWS47,  client, "NotNullFieldProc", "newstr");
        testProcWithValidJSON(TABLE_ROWS47,  client, "IdFieldProc", "newint", 7);
        testProcWithValidJSON(TABLE_ROWS47,  client, "IdFieldProc", "newbool", "true");
        testProcWithValidJSON(TABLE_ROWS47,  client, "IdFieldProc", "newstr", "newvalue");
    }

    /** Used to test ENG-6621, part 2 (dotted path notation) / ENG-6879. */
    public void testSET_FIELDFunctionWithDotNotation() throws Exception {
        Client client = getClient();
        loadJS1(client);

        // Confirm expected results before calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "inner.veggies", "good for you");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "inner.veggies", "bad for you");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "inner.second.fruits", 1);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "inner.second.fruits", -1);
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "inner.second.third.meats", "yum");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "inner.second.third.meats", "yuck");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "inner.newint");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "inner.newstr");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "inner.newbool");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj.newint");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj.newstr");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj.newbool");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "dot\\.char", "foo.bar");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "dot\\.char", "bar.foo");

        // Call the "UpdateSetFieldProc" Stored Proc, which uses the SET_FIELD function
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.veggies", "\"bad for you\"", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.second.fruits", -1, 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.second.third.meats", "\"yuck\"", 3);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.newint", 7, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.newstr", "\"newvalue\"", 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newobj.newbool", "true", 4);

        // Call the SET_FIELD function directly, using ad-hoc queries
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner.veggies', '\"bad for you\"') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner.second.fruits', '-1') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner.second.third.meats', '\"yuck\"') WHERE ID = 1");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner.newbool', 'true') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newobj.newint', '7') WHERE ID = 4");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newobj.newstr', '\"newvalue\"') WHERE ID = 4");

        // Test \ escape for dot in element name, not used for sub-path
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "dot\\.char", "\"bar.foo\"", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'dot\\.char', '\"bar.foo\"') WHERE ID = 2");

        debugPrintJsonDoc("with new object values", client, 1, 2, 3, 4);

        // Confirm modified results after calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROW3,   client, "IdFieldProc", "inner.veggies", "good for you");
        testProcWithValidJSON(TABLE_ROWS12, client, "IdFieldProc", "inner.veggies", "bad for you");
        testProcWithValidJSON(TABLE_ROW1,   client, "IdFieldProc", "inner.second.fruits", 1);
        testProcWithValidJSON(TABLE_ROWS23, client, "IdFieldProc", "inner.second.fruits", -1);
        testProcWithValidJSON(TABLE_ROW2,   client, "IdFieldProc", "inner.second.third.meats", "yum");
        testProcWithValidJSON(TABLE_ROWS13, client, "IdFieldProc", "inner.second.third.meats", "yuck");
        testProcWithValidJSON(TABLE_ROW1,   client, "IdFieldProc", "inner.newint", 7);
        testProcWithValidJSON(TABLE_ROW2,   client, "IdFieldProc", "inner.newstr", "newvalue");
        testProcWithValidJSON(TABLE_ROW3,   client, "IdFieldProc", "inner.newbool", "true");
        testProcWithValidJSON(TABLE_ROW4,   client, "IdFieldProc", "newobj.newint", 7);
        testProcWithValidJSON(TABLE_ROW4,   client, "IdFieldProc", "newobj.newstr", "newvalue");
        testProcWithValidJSON(TABLE_ROW4,   client, "IdFieldProc", "newobj.newbool", "true");
        testProcWithValidJSON(TABLE_ROW3,   client, "IdFieldProc", "dot\\.char", "foo.bar");
        testProcWithValidJSON(TABLE_ROWS12, client, "IdFieldProc", "dot\\.char", "bar.foo");
    }

    /** Used to test ENG-6620, part 3 (array index notation) / ENG-6879. */
    public void testSET_FIELDFunctionWithIndexNotation() throws Exception {
        Client client = getClient();
        loadJS1(client);

        // Confirm expected results before calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr[0]", 0);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr[0]", -1);
        testProcWithValidJSON(TABLE_ROW2,    client, "IdFieldProc", "arr[1]", 2);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr[1]", -2);
        testProcWithValidJSON(TABLE_ROWS123, client, "NotNullFieldProc", "arr[2]");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr[2]", 100);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr[3]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr[4]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr[5]");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[0]", 0);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[0]", -1);
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[1][0]", "One");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[1][0]", "Four");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[1][1][0]", 2);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[1][1][0]", -2);
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "arr3d[1][1][1]", 1);
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "arr3d[1][1][1]", 3);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[1][1][1]", -3);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[1][1][3]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[1][1][4]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[1][1][5]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[0]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[1]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[2]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[3]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[4]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[5]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr3d[0]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr3d[1]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr3d[2]");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "bracket]\\[\\[] \\[ ] chars", "[foo]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "bracket]\\[\\[] \\[ ] chars", "[bar]");

        // Call the "UpdateSetFieldProc" Stored Proc, which uses the SET_FIELD function
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr[0]", -1, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr[3]", -4, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr[-1]", -4, 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr[4]", -5, 3);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[0]", -1, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[1][0]", "\"Four\"", 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[1][1][0]", -2, 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[1][1][1]", -3, 3);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[1][1][-1]", -4, 3);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[1][1][3]", -5, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newarr[2]", 7, 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newarr[-1]", "true", 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newarr[-1]", "\"newvalue\"", 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newarr3d[1][1][1]", 7, 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newarr3d[1][1][-1]", 8, 4);

        // Call the SET_FIELD function directly, using ad-hoc queries
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[1]', '-2') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[4]', '-5') WHERE ID = 1");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[-1]', '-5') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr[-1]', '-6') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[0]', '-1') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[1][0]', '\"Four\"') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[1][1][0]', '-2') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[1][1][1]', '-3') WHERE ID = 1");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[1][1][-1]', '-6') WHERE ID = 1");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[1][1][3]', '-5') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newarr[3]', 'true') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newarr[-1]', '\"newvalue\"') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newarr[2]', '7') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newarr3d[-1][-1][-1]', '9') WHERE ID = 7");

        // Test \ escape for brackets in element name, not used for array index
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "bracket]\\[\\[] \\[ ] chars", "\"[bar]\"", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'bracket]\\[\\[] \\[ ] chars', '\"[bar]\"') WHERE ID = 2");

        debugPrintJsonDoc("with new array values", client, 1, 2, 3, 4, 7);

        // Confirm modified results after calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROWS23,  client, "IdFieldProc", "arr[0]", 0);
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "arr[0]", -1);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr[1]", 2);
        testProcWithValidJSON(TABLE_ROW2,    client, "IdFieldProc", "arr[1]", -2);
        testProcWithValidJSON(TABLE_ROWS123, client, "NotNullFieldProc", "arr[2]");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr[2]", 100);
        testProcWithValidJSON(TABLE_ROWS12,  client, "NotNullFieldProc", "arr[3]");
        testProcWithValidJSON(TABLE_ROWS12,  client, "IdFieldProc", "arr[3]", -4);
        testProcWithValidJSON(TABLE_ROWS123, client, "NotNullFieldProc", "arr[4]");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr[4]", -5);
        testProcWithValidJSON(TABLE_ROW3,    client, "NotNullFieldProc", "arr[5]");
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "arr[5]", -6);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr[6]");
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "arr3d[0]", 0);
        testProcWithValidJSON(TABLE_ROWS12,  client, "IdFieldProc", "arr3d[0]", -1);
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "arr3d[1][0]", "One");
        testProcWithValidJSON(TABLE_ROWS23,  client, "IdFieldProc", "arr3d[1][0]", "Four");
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "arr3d[1][1][0]", 2);
        testProcWithValidJSON(TABLE_ROWS23,  client, "IdFieldProc", "arr3d[1][1][0]", -2);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[1][1][1]", 1);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[1][1][1]", 3);
        testProcWithValidJSON(TABLE_ROWS13,  client, "IdFieldProc", "arr3d[1][1][1]", -3);
        testProcWithValidJSON(TABLE_ROWS123, client, "NotNullFieldProc", "arr3d[1][1][2]");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "arr3d[1][1][2]", "4.5", "4.50");
        testProcWithValidJSON(TABLE_ROWS123, client, "NotNullFieldProc", "arr3d[1][1][3]");
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "arr3d[1][1][3]", -4);
        testProcWithValidJSON(TABLE_ROWS12,  client, "IdFieldProc", "arr3d[1][1][3]", -5);
        testProcWithValidJSON(TABLE_ROW1,    client, "NotNullFieldProc", "arr3d[1][1][4]");
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "arr3d[1][1][4]", -6);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[1][1][5]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[0]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[1]");
        testProcWithValidJSON(TABLE_ROWS47,  client, "NotNullFieldProc", "newarr[2]");
        testProcWithValidJSON(TABLE_ROWS47,  client, "IdFieldProc", "newarr[2]", 7);
        testProcWithValidJSON(TABLE_ROWS47,  client, "NotNullFieldProc", "newarr[3]");
        testProcWithValidJSON(TABLE_ROWS47,  client, "IdFieldProc", "newarr[3]", "true");
        testProcWithValidJSON(TABLE_ROWS47,  client, "NotNullFieldProc", "newarr[4]");
        testProcWithValidJSON(TABLE_ROWS47,  client, "IdFieldProc", "newarr[4]", "newvalue");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[5]");
        testProcWithValidJSON(TABLE_ROW7,    client, "NotNullFieldProc", "newarr3d[0]");
        testProcWithValidJSON(TABLE_ROW7,    client, "NotNullFieldProc", "newarr3d[0][0]");
        testProcWithValidJSON(TABLE_ROW7,    client, "NotNullFieldProc", "newarr3d[0][0][0]");
        testProcWithValidJSON(TABLE_ROW7,    client, "IdFieldProc", "newarr3d[0][0][0]", 9);
        testProcWithValidJSON(TABLE_ROW4,    client, "NotNullFieldProc", "newarr3d[1]");
        testProcWithValidJSON(TABLE_ROW4,    client, "NotNullFieldProc", "newarr3d[1][1]");
        testProcWithValidJSON(TABLE_ROW4,    client, "NotNullFieldProc", "newarr3d[1][1][1]");
        testProcWithValidJSON(TABLE_ROW4,    client, "IdFieldProc", "newarr3d[1][1][1]", 7);
        testProcWithValidJSON(TABLE_ROW4,    client, "NotNullFieldProc", "newarr3d[1][1][2]");
        testProcWithValidJSON(TABLE_ROW4,    client, "IdFieldProc", "newarr3d[1][1][2]", 8);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr3d[2]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr3d[1][2]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr3d[1][1][3]");
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "bracket]\\[\\[] \\[ ] chars", "[foo]");
        testProcWithValidJSON(TABLE_ROWS12,  client, "IdFieldProc", "bracket]\\[\\[] \\[ ] chars", "[bar]");
    }

    /** Used to test a weird part of ENG-6620 / ENG-6879: index notation, used
     *  with no name specified. */
    public void testSET_FIELDFunctionWithIndexNotationButNoName() throws Exception {
        Client client = getClient();
        loadJS1(client);

        // Confirm expected results before calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[0]", 1);
        testProcWithValidJSON(EMPTY_TABLE, client, "IdFieldProc", "[0]", -1);
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[1]", 2);
        testProcWithValidJSON(EMPTY_TABLE, client, "IdFieldProc", "[1]", -2);
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[2]");
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[2]", 3);
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "[3]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "[4]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "[5]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "[6]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "[7]");

        // Call the "UpdateSetFieldProc" Stored Proc, which uses the SET_FIELD function
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "[0]", -1, 10);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "[3]", -4, 10);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "[-1]", -5, 10);

        // Call the SET_FIELD function directly, using ad-hoc queries
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, '[1]', '-2') WHERE ID = 10");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, '[5]', '-6') WHERE ID = 10");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, '[-1]', '-7') WHERE ID = 10");

        debugPrintJsonDoc("with new nameless array values", client, 10);

        // Confirm modified results after calling the SET_FIELD function
        testProcWithValidJSON(EMPTY_TABLE, client, "IdFieldProc", "[0]", 1);
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[0]", -1);
        testProcWithValidJSON(EMPTY_TABLE, client, "IdFieldProc", "[1]", 2);
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[1]", -2);
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[2]");
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[2]", 3);
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[3]");
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[3]", -4);
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[4]");
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[4]", -5);
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[5]");
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[5]", -6);
        testProcWithValidJSON(TABLE_ROW10, client, "NotNullFieldProc", "[6]");
        testProcWithValidJSON(TABLE_ROW10, client, "IdFieldProc", "[6]", -7);
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "[7]");
    }

    /** Used to test ENG-6620, part 4 (dotted path and array index notation, combined) / ENG-6879. */
    public void testSET_FIELDFunctionWithDotAndIndexNotation() throws Exception {
        Client client = getClient();
        loadJS1(client);

        // Confirm expected results before calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "inner.arr[0]", 0);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "inner.arr[0]", -1);
        testProcWithValidJSON(TABLE_ROW2,    client, "IdFieldProc", "inner.arr[1]", 2);
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "inner.arr[1]", 3);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "inner.arr[1]", -2);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "inner.newarr[0]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "inner.newarr[1]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "inner.newarr[2]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "inner.newarr[3]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj.newarr[0]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj.newarr[1]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj.newarr[2]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj.newarr[3]");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[0]", 0);
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[2].veggies", "good for you");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[2].veggies", "bad for you");
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "arr3d[2].dairy", "1");
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "arr3d[2].dairy", "3");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[2].dairy", "-3");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[0].newint");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[2].newint");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[3].newint");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[4]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[0].newint");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[1].newint");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[2].newint");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[3]");

        // Call the "UpdateSetFieldProc" Stored Proc, which uses the SET_FIELD function
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.arr[0]", "-1", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.arr[1]", "-2", 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.newarr[1]", "5", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newobj.newarr[1]", "7", 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[2].veggies", "\"bad for you\"", 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[2].dairy", "-3", 3);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[2].newint", "5", 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newarr[1].newint", "7", 4);

        // Call the SET_FIELD function directly, using ad-hoc queries
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner.arr[0]', '-1') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner.arr[1]', '-2') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner.newarr[-1]', '6') WHERE ID = 1");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newobj.newarr[-1]', '8') WHERE ID = 4");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[2].veggies', '\"bad for you\"') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[2].dairy', '-3') WHERE ID = 1");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[-1].newint', '6') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newarr[-1].newint', '8') WHERE ID = 4");

        // Confirm that these lines have no effect, since arr3d[0] is not an object:
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[0].newint", "-9", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[0].newint', '-9') WHERE ID = 2");

        debugPrintJsonDoc("with new object/array values", client, 1, 2, 3, 4);

        // Confirm modified results after calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROW3,    client, "IdFieldProc", "inner.arr[0]", 0);
        testProcWithValidJSON(TABLE_ROWS12,  client, "IdFieldProc", "inner.arr[0]", -1);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "inner.arr[1]", 2);
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "inner.arr[1]", 3);
        testProcWithValidJSON(TABLE_ROWS23,  client, "IdFieldProc", "inner.arr[1]", -2);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "inner.newarr[0]");
        testProcWithValidJSON(TABLE_ROW1,    client, "NotNullFieldProc", "inner.newarr[1]");
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "inner.newarr[1]", 5);
        testProcWithValidJSON(TABLE_ROW1,    client, "NotNullFieldProc", "inner.newarr[2]");
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "inner.newarr[2]", 6);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "inner.newarr[3]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj.newarr[0]");
        testProcWithValidJSON(TABLE_ROW4,    client, "NotNullFieldProc", "newobj.newarr[1]");
        testProcWithValidJSON(TABLE_ROW4,    client, "IdFieldProc", "newobj.newarr[1]", 7);
        testProcWithValidJSON(TABLE_ROW4,    client, "NotNullFieldProc", "newobj.newarr[2]");
        testProcWithValidJSON(TABLE_ROW4,    client, "IdFieldProc", "newobj.newarr[2]", 8);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj.newarr[3]");
        testProcWithValidJSON(TABLE_ROWS123, client, "IdFieldProc", "arr3d[0]", 0);
        testProcWithValidJSON(TABLE_ROW1,    client, "IdFieldProc", "arr3d[2].veggies", "good for you");
        testProcWithValidJSON(TABLE_ROWS23,  client, "IdFieldProc", "arr3d[2].veggies", "bad for you");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[2].dairy", "1");
        testProcWithValidJSON(EMPTY_TABLE,   client, "IdFieldProc", "arr3d[2].dairy", "3");
        testProcWithValidJSON(TABLE_ROWS13,  client, "IdFieldProc", "arr3d[2].dairy", "-3");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[0].newint");
        testProcWithValidJSON(TABLE_ROW2,    client, "NotNullFieldProc", "arr3d[2].newint");
        testProcWithValidJSON(TABLE_ROW2,    client, "IdFieldProc", "arr3d[2].newint", 5);
        testProcWithValidJSON(TABLE_ROW2,    client, "NotNullFieldProc", "arr3d[3].newint");
        testProcWithValidJSON(TABLE_ROW2,    client, "IdFieldProc", "arr3d[3].newint", 6);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "arr3d[4]");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[0].newint");
        testProcWithValidJSON(TABLE_ROW4,    client, "NotNullFieldProc", "newarr[1].newint");
        testProcWithValidJSON(TABLE_ROW4,    client, "IdFieldProc", "newarr[1].newint", 7);
        testProcWithValidJSON(TABLE_ROW4,    client, "NotNullFieldProc", "newarr[2].newint");
        testProcWithValidJSON(TABLE_ROW4,    client, "IdFieldProc", "newarr[2].newint", 8);
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr[3]");
    }

    /** Used to test ENG-6879, part #2.b., with dot notation applied to an existing
     *  primitive or array, and index notation applied to an existing primitive or
     *  object; these have no effect (which is contrary to initial expectations,
     *  but not unreasonable). */
    public void testSET_FIELDFunctionWithMisplacedDotOrIndexNotation() throws Exception {
        Client client = getClient();
        loadJS1(client);

        // Confirm expected results before calling the SET_FIELD function
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "id.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "id2.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "numeric.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "bool.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "boo2.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "tag.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "last.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "arr.veggies");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "arr.0");

        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "id[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "id2[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "numeric[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "bool[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "bool2[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "tag[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "last[0]");
        testProcWithValidJSON(EMPTY_TABLE, client, "NotNullFieldProc", "inner[0]");

        // Call the "UpdateSetFieldProc" Stored Proc, which uses the SET_FIELD function:
        // note that none of these have any effect
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "id.veggies", -9, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "numeric.veggies", -9.1, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "bool.veggies", "true", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "tag.veggies", "\"newTagValue\"", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "last.veggies", "\"newLastValue\"", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr.veggies", -9, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr.0", -9, 1);

        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "id[0]", -9, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "numeric[0]", -9.1, 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "bool[0]", "true", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "tag[0]", "\"newTagValue\"", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "last[0]", "\"newLastValue\"", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner[0]", -9, 1);

        // Similar calls on a row without those primitives, arrays or objects defined do have an effect
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "id.veggies", -9, 5);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "numeric.veggies", -9.1, 5);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "bool.veggies", "true", 5);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "tag.veggies", "\"newTagValue\"", 5);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "last.veggies", "\"newLastValue\"", 5);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr.veggies", -9, 5);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr.0", -9, 5);

        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "id2[0]", -9, 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "numeric[0]", -9.1, 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "bool2[0]", "true", 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "tag[0]", "\"newTagValue\"", 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "last[0]", "\"newLastValue\"", 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner[0]", -9, 4);

        // Call the SET_FIELD function directly, using ad-hoc queries:
        // again, none of these have any effect
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'id.veggies', '-9') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'numeric.veggies', '-9.1') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'bool.veggies', 'true') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'tag.veggies', '\"newTagValue\"') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'last.veggies', '\"newLastValue\"') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr.veggies', '-9') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr.0', '-9') WHERE ID = 2");

        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'id[0]', '-9') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'numeric[0]', '-9.1') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'bool[0]', 'true') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'tag[0]', '\"newTagValue\"') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'last[0]', '\"newLastValue\"') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner[0]', '-9') WHERE ID = 2");

        // Similar calls on a row without those primitives, arrays or objects defined do have an effect
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'id2.veggies', '-9') WHERE ID = 9");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'numeric.veggies', '-9.1') WHERE ID = 9");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'bool2.veggies', 'true') WHERE ID = 9");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'tag.veggies', '\"newTagValue\"') WHERE ID = 9");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'last.veggies', '\"newLastValue\"') WHERE ID = 9");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr.veggies', '-9') WHERE ID = 9");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr.0', '-9') WHERE ID = 9");

        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'id2[0]', '-9') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'numeric[0]', '-9.1') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'bool2[0]', 'true') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'tag[0]', '\"newTagValue\"') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'last[0]', '\"newLastValue\"') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner[0]', '-9') WHERE ID = 7");

        debugPrintJsonDoc("with misplaced dot/index values", client, 4, 5, 7, 9);

        // Confirm modified results after calling the SET_FIELD function (confirm
        // that much of the above had no effect, but the 'similar calls' did)
        testProcWithValidJSON(TABLE_ROW5,   client, "NotNullFieldProc", "id.veggies");
        testProcWithValidJSON(TABLE_ROW5,   client, "IdFieldProc", "id.veggies", -9);
        testProcWithValidJSON(TABLE_ROW9,   client, "NotNullFieldProc", "id2.veggies");
        testProcWithValidJSON(TABLE_ROW9,   client, "IdFieldProc", "id2.veggies", -9);
        testProcWithValidJSON(TABLE_ROWS59, client, "NotNullFieldProc", "numeric.veggies");
        testProcWithValidJSON(TABLE_ROWS59, client, "NumericFieldProc", "numeric.veggies", "-9.1", "-9.10");
        testProcWithValidJSON(TABLE_ROW5,   client, "NotNullFieldProc", "bool.veggies");
        testProcWithValidJSON(TABLE_ROW5,   client, "IdFieldProc", "bool.veggies", "true");
        testProcWithValidJSON(TABLE_ROW9,   client, "NotNullFieldProc", "bool2.veggies");
        testProcWithValidJSON(TABLE_ROW9,   client, "IdFieldProc", "bool2.veggies", "true");
        testProcWithValidJSON(TABLE_ROWS59, client, "NotNullFieldProc", "tag.veggies");
        testProcWithValidJSON(TABLE_ROWS59, client, "IdFieldProc", "tag.veggies", "newTagValue");
        testProcWithValidJSON(TABLE_ROWS59, client, "NotNullFieldProc", "last.veggies");
        testProcWithValidJSON(TABLE_ROWS59, client, "IdFieldProc", "last.veggies", "newLastValue");
        testProcWithValidJSON(TABLE_ROWS59, client, "NotNullFieldProc", "arr.veggies");
        testProcWithValidJSON(TABLE_ROWS59, client, "IdFieldProc", "arr.veggies", -9);
        testProcWithValidJSON(TABLE_ROWS59, client, "NotNullFieldProc", "arr.0");
        testProcWithValidJSON(TABLE_ROWS59, client, "IdFieldProc", "arr.0", -9);

        testProcWithValidJSON(EMPTY_TABLE,  client, "NotNullFieldProc", "id[0]");
        testProcWithValidJSON(EMPTY_TABLE,  client, "IdFieldProc", "id[0]", -9);
        testProcWithValidJSON(TABLE_ROWS47, client, "NotNullFieldProc", "id2[0]");
        testProcWithValidJSON(TABLE_ROWS47, client, "IdFieldProc", "id2[0]", -9);
        testProcWithValidJSON(TABLE_ROWS47, client, "NotNullFieldProc", "numeric[0]");
        testProcWithValidJSON(TABLE_ROWS47, client, "NumericFieldProc", "numeric[0]", "-9.1", "-9.10");
        testProcWithValidJSON(EMPTY_TABLE,  client, "NotNullFieldProc", "bool[0]");
        testProcWithValidJSON(EMPTY_TABLE,  client, "IdFieldProc", "bool[0]", "true");
        testProcWithValidJSON(TABLE_ROWS47, client, "NotNullFieldProc", "bool2[0]");
        testProcWithValidJSON(TABLE_ROWS47, client, "IdFieldProc", "bool2[0]", "true");
        testProcWithValidJSON(TABLE_ROWS47, client, "NotNullFieldProc", "tag[0]");
        testProcWithValidJSON(TABLE_ROWS47, client, "IdFieldProc", "tag[0]", "newTagValue");
        testProcWithValidJSON(TABLE_ROWS47, client, "NotNullFieldProc", "last[0]");
        testProcWithValidJSON(TABLE_ROWS47, client, "IdFieldProc", "last[0]", "newLastValue");
        testProcWithValidJSON(TABLE_ROWS47, client, "NotNullFieldProc", "inner[0]");
        testProcWithValidJSON(TABLE_ROWS47, client, "IdFieldProc", "inner[0]", -9);
    }

    /** Used to test ENG-6620 / ENG-6879, for numeric, floating-point data, which
     *  needs to be handled slightly differently, due to the tendency to add a
     *  trailing zero; including with dotted path and/or array index notation. */
    public void testSET_FIELDFunctionWithNumericData() throws Exception {
        Client client = getClient();
        loadJS1(client);

        // Confirm expected results before calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "numeric", "1.2", "1.20");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NumericFieldProc", "numeric", "-1.2", "-1.20");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NumericFieldProc", "numeric", "-2.3", "-2.30");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "inner.second.third.numeric", "2.3", "2.30");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NumericFieldProc", "inner.second.third.numeric", "-2.3", "-2.30");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "arr3d[1][1][2]", "4.5", "4.50");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NumericFieldProc", "arr3d[1][1][2]", "-4.5", "-4.50");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "inner.arr[2]", "3.4", "3.40");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NumericFieldProc", "inner.arr[2]", "-3.4", "-3.40");
        testProcWithValidJSON(TABLE_ROWS123, client, "NumericFieldProc", "arr3d[2].numeric", "5.6", "5.60");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NumericFieldProc", "arr3d[2].numeric", "-5.6", "-5.60");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newnum");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newobj");
        testProcWithValidJSON(EMPTY_TABLE,   client, "NotNullFieldProc", "newarr");

        // Call the "UpdateSetFieldProc" Stored Proc, which uses the SET_FIELD function
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "numeric", "-1.2", 1);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.second.third.numeric", "-2.3", 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[1][1][2]", "-4.5", 3);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "inner.arr[2]", "-3.4", 2);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "arr3d[2].numeric", "-5.6", 3);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newnum", "6.789", 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newobj.newnum", "7.8", 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newarr[1]", "8.9", 4);
        testProcWithValidJSON(UPDATED_1ROW, client, "UpdateSetFieldProc", "newarr[-1]", "9.1", 4);

        // Test the SET_FIELD function directly, using an ad-hoc query
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'numeric', '-2.3') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner.second.third.numeric', '-2.3') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[1][1][2]', '-4.5') WHERE ID = 1");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'inner.arr[2]', '-3.4') WHERE ID = 3");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'arr3d[2].numeric', '-5.6') WHERE ID = 2");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newnum', '6.789') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newobj.newnum', '7.8') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newarr[1]', '8.9') WHERE ID = 7");
        testProcWithValidJSON(UPDATED_1ROW, client, "@AdHoc", "UPDATE JS1 SET DOC = SET_FIELD(DOC, 'newarr[-1]', '9.1') WHERE ID = 7");

        debugPrintJsonDoc("with new numeric values", client, 1, 2, 3, 4, 7);

        // Confirm modified results after calling the SET_FIELD function
        testProcWithValidJSON(TABLE_ROW3,   client, "NumericFieldProc", "numeric", "1.2", "1.20");
        testProcWithValidJSON(TABLE_ROW1,   client, "NumericFieldProc", "numeric", "-1.2", "-1.20");
        testProcWithValidJSON(TABLE_ROW2,   client, "NumericFieldProc", "numeric", "-2.3", "-2.30");
        testProcWithValidJSON(TABLE_ROW1,   client, "NumericFieldProc", "inner.second.third.numeric", "2.3", "2.30");
        testProcWithValidJSON(TABLE_ROWS23, client, "NumericFieldProc", "inner.second.third.numeric", "-2.3", "-2.30");
        testProcWithValidJSON(TABLE_ROW2,   client, "NumericFieldProc", "arr3d[1][1][2]", "4.5", "4.50");
        testProcWithValidJSON(TABLE_ROWS13, client, "NumericFieldProc", "arr3d[1][1][2]", "-4.5", "-4.50");
        testProcWithValidJSON(TABLE_ROW1,   client, "NumericFieldProc", "inner.arr[2]", "3.4", "3.40");
        testProcWithValidJSON(TABLE_ROWS23, client, "NumericFieldProc", "inner.arr[2]", "-3.4", "-3.40");
        testProcWithValidJSON(TABLE_ROW1,   client, "NumericFieldProc", "arr3d[2].numeric", "5.6", "5.60");
        testProcWithValidJSON(TABLE_ROWS23, client, "NumericFieldProc", "arr3d[2].numeric", "-5.6", "-5.60");
        testProcWithValidJSON(TABLE_ROWS47, client, "NotNullFieldProc", "newnum");
        testProcWithValidJSON(TABLE_ROWS47, client, "NumericFieldProc", "newnum", "6.789", "6.7890");
        testProcWithValidJSON(TABLE_ROWS47, client, "NotNullFieldProc", "newobj");
        testProcWithValidJSON(TABLE_ROWS47, client, "NumericFieldProc", "newobj.newnum", "7.8", "7.80");
        testProcWithValidJSON(TABLE_ROWS47, client, "NotNullFieldProc", "newarr");
        testProcWithValidJSON(EMPTY_TABLE,  client, "NotNullFieldProc", "newarr[0]");
        testProcWithValidJSON(TABLE_ROWS47, client, "NumericFieldProc", "newarr[1]", "8.9", "8.90");
        testProcWithValidJSON(TABLE_ROWS47, client, "NumericFieldProc", "newarr[2]", "9.1", "9.10");
    }

    /** Used to test ENG-6879, for various null values (in various parameters,
     *  with and without quotes), and related queries. */
    public void testSET_FIELDFunctionWithNullValues() throws Exception {
        Client client = getClient();
        loadJS1(client);

        // Call the SET_FIELD function with null first (JSON doc column) parameter
        // (using both the "NullSetFieldDocProc" Stored Proc and ad-hoc queries)
        testProcWithValidJSON(FULL_TABLE, client, "NullSetFieldDocProc", null, "id", -1);
        testProcWithValidJSON(FULL_TABLE, client, "NullSetFieldDocProc", null, null, -1);
        testProcWithValidJSON(FULL_TABLE, client, "NullSetFieldDocProc", null, null, null);
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE SET_FIELD(null, 'id', '-1') IS NULL ORDER BY ID");
        testProcWithValidJSON(FULL_TABLE, client, "@AdHoc", "SELECT ID FROM JS1 WHERE SET_FIELD(null, null, '-1') IS NULL ORDER BY ID");
        testProcWithValidJSON((String) null, client, "@AdHoc", "SELECT SET_FIELD(null, null, null) FROM JS1 WHERE ID = 5");
        // Or, similarly, where the DOC column has null value
        testProcWithValidJSON(TABLE_ROW8, client, "NullSetFieldProc", "id", -1);
        testProcWithValidJSON(TABLE_ROW8, client, "@AdHoc", "SELECT ID FROM JS1 WHERE SET_FIELD(DOC, 'id', '-1') IS NULL ORDER BY ID");
        testProcWithValidJSON((String) null, client, "@AdHoc", "SELECT SET_FIELD(DOC, 'id', '-1') FROM JS1 WHERE ID = 8");

        // Call the SET_FIELD function with null second ("path") parameter
        // (using both the "NullSetFieldProc" Stored Proc and an ad-hoc query)
        testProcWithInvalidJSON("Invalid SET_FIELD path argument (SQL null)", client, "NullSetFieldProc", null, -1);
        testProcWithInvalidJSON("Invalid SET_FIELD path argument (SQL null)", client, "@AdHoc",
                                "SELECT ID FROM JS1 WHERE SET_FIELD(DOC, null, '-1') IS NULL");

        // Call the SET_FIELD function with null third ("value") parameter
        // (using both the "NullSetFieldProc" Stored Proc and an ad-hoc query)
        testProcWithInvalidJSON("Invalid SET_FIELD value argument (SQL null)", client, "NullSetFieldProc", "id", null);
        testProcWithInvalidJSON("Invalid SET_FIELD value argument (SQL null)", client, "@AdHoc",
                                "SELECT ID FROM JS1 WHERE SET_FIELD(DOC, 'id', null) IS NULL");

        // Call the SET_FIELD function with quoted-null second ("path") parameter
        // (using both the "SelectSetFieldProc" Stored Proc and an ad-hoc query)
        testProcWithValidJSON("{\"null\":-1}", client, "SelectSetFieldProc", "null", -1, 5);
        testProcWithValidJSON("{\"null\":-1}", client, "@AdHoc", "SELECT SET_FIELD(DOC, 'null', '-1') FROM JS1 WHERE ID = 5");
        // Similar single-quotes, but without null:
        testProcWithValidJSON("{\"foo\":-1}", client, "SelectSetFieldProc", "foo", -1, 5);
        testProcWithValidJSON("{\"foo\":-1}", client, "@AdHoc", "SELECT SET_FIELD(DOC, 'foo', '-1') FROM JS1 WHERE ID = 5");
        // Note: quotes for "null" are double-escaped, for Java and JSON; the
        // resulting JSON document is actually: {"\"null\"":-1}
        testProcWithValidJSON("{\"\\\"null\\\"\":-1}", client, "SelectSetFieldProc", "\"null\"", -1, 5);
        testProcWithValidJSON("{\"\\\"null\\\"\":-1}", client, "@AdHoc", "SELECT SET_FIELD(DOC, '\"null\"', '-1') FROM JS1 WHERE ID = 5");
        // Similar double-quotes, but without null:
        testProcWithValidJSON("{\"\\\"foo\\\"\":-1}", client, "SelectSetFieldProc", "\"foo\"", -1, 5);
        testProcWithValidJSON("{\"\\\"foo\\\"\":-1}", client, "@AdHoc", "SELECT SET_FIELD(DOC, '\"foo\"', '-1') FROM JS1 WHERE ID = 5");

        // Call the SET_FIELD function with quoted-null third ("value") parameter
        // (using both the "SelectSetFieldProc" Stored Proc and an ad-hoc query)
        testProcWithValidJSON("{\"id\":null}", client, "SelectSetFieldProc", "id", "null", 5);
        testProcWithValidJSON("{\"id\":null}", client, "@AdHoc", "SELECT SET_FIELD(DOC, 'id', 'null') FROM JS1 WHERE ID = 5");
        // Similar single-quotes, but with non-null string (which is invalid):
        testProcWithInvalidJSON("Invalid JSON * Line 1, Column 1\n  Syntax error: value, object or array expected",
                                client, "SelectSetFieldProc", "id", "foo", 5);
        testProcWithInvalidJSON("Invalid JSON * Line 1, Column 1\n  Syntax error: value, object or array expected",
                                client, "@AdHoc", "SELECT SET_FIELD(DOC, 'id', 'foo') FROM JS1 WHERE ID = 5");
        // Note: quotes for "id" and "null" are escaped for Java; the resulting
        // JSON document is actually: {"id":"null"}
        testProcWithValidJSON("{\"id\":\"null\"}", client, "SelectSetFieldProc", "id", "\"null\"", 5);
        testProcWithValidJSON("{\"id\":\"null\"}", client, "@AdHoc", "SELECT SET_FIELD(DOC, 'id', '\"null\"') FROM JS1 WHERE ID = 5");
        // Similar double-quotes, but without null:
        testProcWithValidJSON("{\"id\":\"foo\"}", client, "SelectSetFieldProc", "id", "\"foo\"", 5);
        testProcWithValidJSON("{\"id\":\"foo\"}", client, "@AdHoc", "SELECT SET_FIELD(DOC, 'id', '\"foo\"') FROM JS1 WHERE ID = 5");
        // Without quotes, for comparison:
        testProcWithValidJSON("{\"id\":-1}", client, "SelectSetFieldProc", "id", -1, 5);
        testProcWithValidJSON("{\"id\":-1}", client, "@AdHoc", "SELECT SET_FIELD(DOC, 'id', '-1') FROM JS1 WHERE ID = 5");

        // Another edge case (not involving null), when the third ("value")
        // parameter is a JSON object
        testProcWithValidJSON("{\"id\":{\"foo\":-1}}", client, "SelectSetFieldProc", "id", "{\"foo\":-1}", 5);
        testProcWithValidJSON("{\"id\":{\"foo\":-1}}", client, "@AdHoc", "SELECT SET_FIELD(DOC, 'id', '{\"foo\":-1}') FROM JS1 WHERE ID = 5");
    }

    public void testARRAY_ELEMENTFunction() throws Exception {
        ClientResponse cr;
        VoltTable result;
        Client client = getClient();
        loadJS1(client);

        cr = client.callProcedure("IdArrayProc", "arr", 1, "1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1L,result.getLong(0));

        try {
            cr = client.callProcedure("IdArrayProc", "arr", "NotNumeric", "1");
            fail("parameter check failed");
        }
        catch ( ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("TYPE ERROR FOR PARAMETER 1"));
        }

        try {
            cr = client.callProcedure("IdArrayProc", 1, 1, "1");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        catch ( ProcCallException pcex) {
            fail("parameter check failed");
        }

        cr = client.callProcedure("NullArrayProc", "funky", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(TOTAL_NUM_ROWS, result.getRowCount());

        cr = client.callProcedure("IdArrayProc", "id", 1, "1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(0, result.getRowCount());

        cr = client.callProcedure("@AdHoc", // test index out of bounds
                                  "SELECT ARRAY_ELEMENT(FIELD(DOC, 'arr'), 99) FROM JS1 WHERE ID = 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        result.getString(0);
        assertTrue(result.wasNull());

        cr = client.callProcedure("@AdHoc", // test negative index
                                  "SELECT ARRAY_ELEMENT(FIELD(DOC, 'arr'), -1) FROM JS1 WHERE ID = 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        result.getString(0);
        assertTrue(result.wasNull());

        cr = client.callProcedure("@AdHoc", // test scalar not an array
                                  "SELECT ARRAY_ELEMENT(FIELD(DOC, 'id'), 1) FROM JS1 WHERE ID = 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        result.getString(0);
        assertTrue(result.wasNull());

        cr = client.callProcedure("@AdHoc", // test object not an array
                                  "SELECT ARRAY_ELEMENT(FIELD(DOC, 'inner'), 1) FROM JS1 WHERE ID = 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        result.getString(0);
        assertTrue(result.wasNull());

        // Test top-level json array.
        int id = TOTAL_NUM_ROWS + 1;
        cr = client.callProcedure("JS1.insert", id, "[0, 10, 100]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                                  "SELECT ARRAY_ELEMENT(DOC, 1) FROM JS1 WHERE ID = " + id);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("10",result.getString(0));

        // Test empty json array.
        cr = client.callProcedure("JS1.insert", ++id, "[]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                                  "SELECT ARRAY_ELEMENT(DOC, 0) FROM JS1 WHERE ID = " + id);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        result.getString(0);
        assertTrue(result.wasNull());
    }

    public void testARRAY_LENGTHFunction() throws Exception {
        ClientResponse cr;
        VoltTable result;
        Client client = getClient();
        loadJS1(client);

        cr = client.callProcedure("IdArrayLengthProc", "arr", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(3, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(2L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(3L,result.getLong(0));

        try {
            cr = client.callProcedure("IdArrayLengthProc", "arr", "NoNumber");
            fail("parameter check failed");
        }
        catch ( ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("TYPE ERROR FOR PARAMETER 1"));
        }

        try {
            cr = client.callProcedure("IdArrayLengthProc", 1, 3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        catch ( ProcCallException pcex) {
            fail("parameter check failed");
        }

        cr = client.callProcedure("NullFieldProc", "funky");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(TOTAL_NUM_ROWS, result.getRowCount());

        cr = client.callProcedure("NullArrayLengthProc", "arr");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        long[] expectedResults = new long[]{4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L};
        assertEquals(expectedResults.length, result.getRowCount());
        for (long expResult : expectedResults) {
            assertTrue(result.advanceRow());
            assertEquals(expResult,result.getLong(0));
        }

        cr = client.callProcedure("LargeArrayLengthProc", "arr", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(0, result.getRowCount());

        cr = client.callProcedure("LargeArrayLengthProc", "arr", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(3, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(2L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(3L,result.getLong(0));

        cr = client.callProcedure("SmallArrayLengthProc", "arr", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(0, result.getRowCount());

        cr = client.callProcedure("SmallArrayLengthProc", "arr", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(3, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(2L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(3L,result.getLong(0));

        cr = client.callProcedure("@AdHoc", // test scalar not an array
                                  "SELECT ARRAY_LENGTH(FIELD(DOC, 'id')) FROM JS1 WHERE ID = 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        result.getLong(0);
        assertTrue(result.wasNull());

        cr = client.callProcedure("@AdHoc", // test object not an array
                                  "SELECT ARRAY_LENGTH(FIELD(DOC, 'inner')) FROM JS1 WHERE ID = 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        result.getLong(0);
        assertTrue(result.wasNull());

        // Test top-level json array.
        int id = TOTAL_NUM_ROWS + 1;
        cr = client.callProcedure("JS1.insert", id, "[0, 10, 100]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc", // test object not an array
                                  "SELECT ARRAY_LENGTH(DOC) FROM JS1 WHERE ID = " + id);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(3L,result.getLong(0));

        // Test empty json array.
        cr = client.callProcedure("JS1.insert", ++id, "[]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                                  "SELECT ARRAY_LENGTH(DOC) FROM JS1 WHERE ID = " + id);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(0L,result.getLong(0));
    }

    public void testFunctionsWithInvalidJSON() throws Exception {

        Client client = getClient();
        ClientResponse cr;

        cr = client.callProcedure(
                "JSBAD.insert", 1, // OOPS. skipped comma before "bool"
                "{\"id\":1 \"bool\": false}"
                );
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure(
                "JSBAD.insert", 2, // OOPS. semi-colon in place of colon before "bool"
                "{\"id\":2, \"bool\"; false, \"贾鑫Vo\":\"分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません\"}"
                );
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        final String jsTrailingCommaArray = "[ 0, 100, ]"; // OOPS. trailing comma in array
        final String jsWithTrailingCommaArray = "{\"id\":3, \"trailer\":" + jsTrailingCommaArray + "}";

        cr = client.callProcedure("JSBAD.insert", 3, jsWithTrailingCommaArray);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("JSBAD.insert", 4, jsTrailingCommaArray);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());


        String[] jsonProcs = { "BadIdFieldProc", "BadIdArrayProc", "BadIdArrayLengthProc" };

        for (String procname : jsonProcs) {
            try {
                cr = client.callProcedure(procname, 1, "id", "1");
                fail("document validity check failed for " + procname);
            }
            catch(ProcCallException pcex) {
                assertTrue(pcex.getMessage().contains("Invalid JSON * Line 1, Column 9"));
            }

            try {
                cr = client.callProcedure(procname, 2, "id", "2");
                fail("document validity check failed for " + procname);
            }
            catch(ProcCallException pcex) {
                assertTrue(pcex.getMessage().contains("Invalid JSON * Line 1, Column 16"));
            }

            try {
                cr = client.callProcedure(procname, 3, "id", "3");
                fail("document validity check failed for " + procname);
            }
            catch(ProcCallException pcex) {
                assertTrue(pcex.getMessage().contains("Invalid JSON * Line 1, Column 30"));
            }

            try {
                cr = client.callProcedure(procname, 4, "id", "4");
                fail("document validity check failed for " + procname);
            }
            catch(ProcCallException pcex) {
                assertTrue(pcex.getMessage().contains("Invalid JSON * Line 1, Column 11"));
            }
        }
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestFunctionsForJSON(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestFunctionsForJSON.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE JS1 (\n" +
                "  ID INTEGER NOT NULL, \n" +
                "  DOC VARCHAR(8192),\n" +
                "  PRIMARY KEY(ID))\n" +
                ";\n" +

                "CREATE PROCEDURE DocEqualsProc AS\n" +
                "   SELECT ID FROM JS1 WHERE DOC = ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE ArrayLengthDocProc AS\n" +
                "   SELECT ID FROM JS1 WHERE ARRAY_LENGTH(DOC) = ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE IdFieldProc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(DOC, ?) = ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE NumericFieldProc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(DOC, ?) IN (?, ?) ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE InnerFieldProc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, 'inner'), ?) = ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE NullFieldDocProc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(?, ?) IS NULL ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE NullFieldProc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(DOC, ?) IS NULL ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE NotNullFieldProc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(DOC, ?) IS NOT NULL ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE NotNullField2Proc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, ?), ?) IS NOT NULL ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE SelectFieldProc AS\n" +
                "   SELECT FIELD(DOC, ?) FROM JS1 WHERE ID = ?\n" +
                ";\n" +
                "CREATE PROCEDURE IdArrayProc AS\n" +
                "   SELECT ID FROM JS1 WHERE ARRAY_ELEMENT(FIELD(DOC, ?), ?) = ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE NullArrayProc AS\n" +
                "   SELECT ID FROM JS1 WHERE ARRAY_ELEMENT(FIELD(DOC, ?), ?) IS NULL ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE IdArrayLengthProc AS\n" +
                "   SELECT ID FROM JS1 WHERE ARRAY_LENGTH(FIELD(DOC, ?)) = ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE NullArrayLengthProc AS\n" +
                "   SELECT ID FROM JS1 WHERE ARRAY_LENGTH(FIELD(DOC, ?)) IS NULL ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE SmallArrayLengthProc AS\n" +
                "   SELECT ID FROM JS1 WHERE ARRAY_LENGTH(FIELD(DOC, ?)) BETWEEN 0 AND ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE LargeArrayLengthProc AS\n" +
                "   SELECT ID FROM JS1 WHERE ARRAY_LENGTH(FIELD(DOC, ?)) > ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE UpdateSetFieldProc AS\n" +
                "   UPDATE JS1 SET DOC = SET_FIELD(DOC, ?, ?) WHERE ID = ?\n" +
                ";\n" +
                "CREATE PROCEDURE NullSetFieldDocProc AS\n" +
                "   SELECT ID FROM JS1 WHERE SET_FIELD(?, ?, ?) IS NULL ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE NullSetFieldProc AS\n" +
                "   SELECT ID FROM JS1 WHERE SET_FIELD(DOC, ?, ?) IS NULL ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE SelectSetFieldProc AS\n" +
                "   SELECT SET_FIELD(DOC, ?, ?) FROM JS1 WHERE ID = ?\n" +
                ";\n" +

                // Useful for debugging:
                "CREATE PROCEDURE GetDocFromId AS\n" +
                "   SELECT DOC FROM JS1 WHERE ID = ?\n" +
                ";\n" +

                "CREATE TABLE JSBAD (\n" +
                "  ID INTEGER NOT NULL,\n" +
                "  DOC VARCHAR(8192),\n" +
                "  PRIMARY KEY(ID))\n" +
                ";\n" +
                "CREATE PROCEDURE BadIdFieldProc AS\n" +
                "  SELECT ID FROM JSBAD WHERE ID = ? AND FIELD(DOC, ?) = ?\n" +
                ";\n" +
                "CREATE PROCEDURE BadIdArrayProc AS\n" +
                "  SELECT ID FROM JSBAD WHERE ID = ? AND ARRAY_ELEMENT(FIELD(DOC, ?), 1) = ?\n" +
                ";\n" +
                "CREATE PROCEDURE BadIdArrayLengthProc AS\n" +
                "  SELECT ID FROM JSBAD WHERE ID = ? AND ARRAY_LENGTH(FIELD(DOC, ?)) = ?\n" +
                ";\n" +
                "";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        // CONFIG #1: Local Site/Partition running on JNI backend
        config = new LocalCluster("fixedsql-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: Local Site/Partitions running on JNI backend
        config = new LocalCluster("fixedsql-threesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);
/*

        // CONFIG #2: HSQL -- disabled, the functions being tested are not HSQL compatible
        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

*/
        // no clustering tests for functions

        return builder;
    }
}
