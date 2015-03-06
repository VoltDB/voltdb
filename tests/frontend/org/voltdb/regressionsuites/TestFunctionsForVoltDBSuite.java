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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.GotBadParamCountsInJava;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestFunctionsForVoltDBSuite extends RegressionSuite {

    public void testExplicitErrorUDF() throws Exception
    {
        System.out.println("STARTING testExplicitErrorUDF");
        Client client = getClient();
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse)
                    throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException("Failed with response: " + clientResponse.getStatusString());
                }
            }
        };
        /*
        CREATE TABLE P1 (
                ID INTEGER DEFAULT '0' NOT NULL,
                DESC VARCHAR(300),
                NUM INTEGER,
                RATIO FLOAT,
                PRIMARY KEY (ID)
                );
        */
        for(int id=7; id < 15; id++) {
            client.callProcedure(callback, "P1.insert", - id, "X"+String.valueOf(id), 10, 1.1);
            client.drain();
        }
        ClientResponse cr = null;

        // Exercise basic syntax without runtime invocation.
        cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123) from P1 where ID = 0");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select SQL_ERROR('abc') from P1 where ID = 0");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123, 'abc') from P1 where ID = 0");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        boolean caught = false;

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123, 'abc') from P1");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("abc") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123.5) from P1");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("Specific error code") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            cr = client.callProcedure("@AdHoc", "select SQL_ERROR('abc') from P1");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.indexOf("abc") != -1);
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            // This wants to be a statement compile-time error.
            cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123, 123) from P1");
            assertTrue(cr.getStatus() != ClientResponse.SUCCESS);
        } catch (ProcCallException e) {
            String msg = e.getMessage();
            assertTrue(msg.matches(".*SQL ERROR\n.*VARCHAR.*"));
            caught = true;
        }
        assertTrue(caught);

    }

    public void testOctetLength() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING OCTET_LENGTH");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "贾鑫Vo", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "Xin", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("OCTET_LENGTH", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(8, result.getLong(1));

        cr = client.callProcedure("OCTET_LENGTH", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(3, result.getLong(1));

        // null case
        cr = client.callProcedure("OCTET_LENGTH", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(VoltType.NULL_BIGINT,result.getLong(1));
    }

    // this test is put here instead of TestFunctionSuite, because HSQL uses
    // a different null case standard with standard sql
    public void testPosition() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING Position");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "贾鑫Vo", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "Xin@Volt", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("POSITION","Vo", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(3, result.getLong(1));

        cr = client.callProcedure("POSITION","DB", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(0, result.getLong(1));

        cr = client.callProcedure("POSITION","Vo", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(5, result.getLong(1));

        // null case
        cr = client.callProcedure("POSITION","Vo", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(VoltType.NULL_BIGINT,result.getLong(1));
    }

    // this test is put here instead of TestFunctionSuite, because HSQL uses
    // a different null case standard with standard sql
    public void testCharLength() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING Char length");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "贾鑫Vo", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "Xin@Volt", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("CHAR_LENGTH", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(4, result.getLong(1));

        cr = client.callProcedure("CHAR_LENGTH", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(8, result.getLong(1));

        // null case
        cr = client.callProcedure("CHAR_LENGTH", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(VoltType.NULL_BIGINT,result.getLong(1));
    }

    public void testDECODE() throws NoConnectionsException, IOException, ProcCallException {
        subtestDECODE();
        subtestDECODENoDefault();
        subtestDECODEVeryLong();
        subtestDECODEInlineVarcharColumn_ENG5078();
        subtestDECODEAsInput();
        subtestDECODEWithNULL();
    }

    private void subtestDECODE() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DECODE");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("@AdHoc", "Delete from P1;");
        cr = client.callProcedure("P1.insert", 1, "IBM", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "Microsoft", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, "Hewlett Packard", 10, 1.1);
        cr = client.callProcedure("P1.insert", 4, "Gateway", 10, 1.1);
        cr = client.callProcedure("P1.insert", 5, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // match 1st condition
        cr = client.callProcedure("DECODE", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("zheng",result.getString(1));

        // match 2nd condition
        cr = client.callProcedure("DECODE", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("li",result.getString(1));

        // match 3rd condition
        cr = client.callProcedure("DECODE", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("at",result.getString(1));

        // match 4th condition
        cr = client.callProcedure("DECODE", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("VoltDB",result.getString(1));

        // null case
        cr = client.callProcedure("DECODE", 5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("where",result.getString(1));

        // param cases
        // For project.addStmtProcedure("DECODE_PARAM_INFER_STRING", "select desc,  DECODE (desc,?,?,desc) from P1 where id = ?");
        cr = client.callProcedure("DECODE_PARAM_INFER_STRING", "Gateway", "You got it!", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("You got it!",result.getString(1));

        // For project.addStmtProcedure("DECODE_PARAM_INFER_INT", "select desc,  DECODE (id,?,?,id) from P1 where id = ?");
        cr = client.callProcedure("DECODE_PARAM_INFER_INT", 4, -4, 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(-4,result.getLong(1));

        // For project.addStmtProcedure("DECODE_PARAM_INFER_DEFAULT", "select desc,  DECODE (?,?,?,?) from P1 where id = ?");
        cr = client.callProcedure("DECODE_PARAM_INFER_DEFAULT", "Gateway", "Gateway", "You got it!", "You ain't got it!", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("You got it!",result.getString(1));

        // For project.addStmtProcedure("DECODE_PARAM_INFER_CONFLICTING", "select desc,  DECODE (id,1,?,2,99,'99') from P1 where id = ?");
        cr = client.callProcedure("DECODE_PARAM_INFER_CONFLICTING", "贾鑫?贾鑫!", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("贾鑫?贾鑫!",result.getString(1));

        // For project.addStmtProcedure("DECODE_PARAM_INFER_CONFLICTING", "select desc,  DECODE (id,1,?,2,99,'99') from P1 where id = ?");
        try {
            cr = client.callProcedure("DECODE_PARAM_INFER_CONFLICTING", 1000, 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        } catch (ProcCallException pce) {
            fail("Should have thrown unfortunate type error.");
        }
    }

    private void subtestDECODENoDefault() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DECODE No Default");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("@AdHoc", "Delete from P1;");
        cr = client.callProcedure("P1.insert", 1, "zheng", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "li", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // null case
        cr = client.callProcedure("DECODEND", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(null,result.getString(1));
    }

    private void subtestDECODEVeryLong() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DECODE Exceed Limit");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("@AdHoc", "Delete from P1;");
        cr = client.callProcedure("P1.insert", 1, "zheng", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "li", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // null case
        cr = client.callProcedure("DECODEVERYLONG", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("where",result.getString(1));
    }

    private void subtestDECODEInlineVarcharColumn_ENG5078()
    throws NoConnectionsException, IOException, ProcCallException
    {
        System.out.println("STARTING DECODE inline varchar column pass-through");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("@AdHoc", "Delete from P3_INLINE_DESC;");
        cr = client.callProcedure("P3_INLINE_DESC.insert", 1, "zheng", "zheng2", 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // null case
        try {
            cr = client.callProcedure("@AdHoc",
                                      "select DECODE(id, -1, 'INVALID', desc) from P3_INLINE_DESC");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals("zheng",result.getString(0));

            cr = client.callProcedure("@AdHoc",
                                      "select DECODE(id, 1, desc, 'INVALID') from P3_INLINE_DESC where id > 0");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals("zheng",result.getString(0));

            cr = client.callProcedure("@AdHoc",
                    "update P3_INLINE_DESC set desc = DECODE(id, 1, desc2, 'INVALID'), desc2 = DECODE(id, 1, desc, 'INVALID') where id > 0");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals(1, result.getLong(0));

            cr = client.callProcedure("@AdHoc",
                    "select DECODE(id, 1, desc, 'INVALID') from P3_INLINE_DESC where id > 0");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals("zheng2",result.getString(0));

            cr = client.callProcedure("@AdHoc",
                    "select DECODE(id, 1, desc2, 'INVALID') from P3_INLINE_DESC where id > 0");
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals("zheng",result.getString(0));


        } catch (ProcCallException pce) {
            System.out.println(pce);
            fail("Looks like a regression of ENG-5078 inline varchar column pass-through by decode");
        }
    }

    private void subtestDECODEAsInput() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DECODE No Default");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("@AdHoc", "Delete from P1;");
        cr = client.callProcedure("P1.insert", 1, "zheng", 10, 1.1);
        cr = client.callProcedure("P1.insert", 2, "li", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // use DECODE as string input to operator
        cr = client.callProcedure("@AdHoc", "select desc || DECODE(id, 1, ' is the 1', ' is not the 1') from P1 where id = 2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("li is not the 1",result.getString(0));

        // use DECODE as integer input to operator
        cr = client.callProcedure("@AdHoc", "select id + DECODE(desc, 'li', 0, -2*id) from P1 where id = 2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(2,result.getLong(0));

        // use DECODE as integer input to operator, with unused incompatible option
        cr = client.callProcedure("@AdHoc", "select id + DECODE(id, 2, 0, 'incompatible') from P1 where id = 2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(2,result.getLong(0));

        // use DECODE as integer input to operator, with used incompatible option
        try {
            cr = client.callProcedure("@AdHoc", "select id + DECODE(id, 1, 0, 'incompatible') from P1 where id = 2");
            fail("failed to except incompatible option");
        } catch (ProcCallException pce) {
            String message = pce.getMessage();
            // It's about that string argument to the addition operator.
            assertTrue(message.contains("varchar"));
        }
    }

    private void checkDecodeNullResult (ClientResponse cr, Object input) {
        VoltTable result;
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        System.out.println("testDECODEWithNULL:" + result);

        if (input instanceof String[]) {
            String[] expected = (String[]) input;
            for (int i = 0; i < expected.length; i++) {
                if ( (i == 4 || i == 7) && !expected[i].startsWith("null") ) {
                    // Float type, decimal type
                    assertTrue(Math.abs(
                            Double.valueOf(expected[i]) - Double.valueOf(result.getString(i))
                            ) < 0.00001);
                } else {
                    assertEquals(expected[i],result.getString(i));
                }
            }
        } else if  (input instanceof Long[]) {
            Long[] expected = (Long[]) input;
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i],Long.valueOf(result.getLong(i)));
            }
        }
    }

    private void subtestDECODEWithNULL() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING DECODE with NULL");
        Client client = getClient();
        ClientResponse cr;

        cr = client.callProcedure("@AdHoc", "Delete from R3;");
        cr = client.callProcedure("R3.insert", 1, 1, 1, 1, 1, 1.1, "2013-07-18 02:00:00.123457", "IBM", 1);
        cr = client.callProcedure("R3.insert", 2, null, null, null, null, null, null, null, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Stored procedure tests
        cr = client.callProcedure("TestDecodeNull", 1);
        checkDecodeNullResult(cr, new String[]{"1","1","1","1","1.1","tm","IBM","1"});

        cr = client.callProcedure("TestDecodeNull", 2);
        checkDecodeNullResult(cr, new String[]{"null tiny","null small", "null num", "null big",
                "null ratio", "null tm", "null var", "null dec"});

        cr = client.callProcedure("TestDecodeNullParam", null, null, null, null, null, null, null, null, 1);
        checkDecodeNullResult(cr, new String[]{"1","1","1","1","1.1","tm","IBM","1"});

        cr = client.callProcedure("TestDecodeNullParam", null, null, null, null, null, null, null, null, 2);
        checkDecodeNullResult(cr, new String[]{"null tiny","null small", "null num", "null big",
                "null ratio", "null tm", "null var", "null dec"});

        // Test CSV_NULL for params
        cr = client.callProcedure("TestDecodeNullParam", "\\N","\\N","\\N","\\N","\\N","\\N","\\N","\\N", 1);
        checkDecodeNullResult(cr, new String[]{"1","1","1","1","1.1","tm","IBM","1"});

        cr = client.callProcedure("TestDecodeNullParam", "\\N","\\N","\\N","\\N","\\N","\\N","\\N","\\N", 2);
        checkDecodeNullResult(cr, new String[]{"null tiny","null small", "null num", "null big",
                "null ratio", "null tm", "null var", "null dec"});


        // AdHoc queries tests
        cr = client.callProcedure("@AdHoc", "select DECODE(tiny, NULL, 'null tiny', tiny)," +
                "DECODE(small, NULL, 'null small', small), DECODE(num, NULL, 'null num', num),  " +
                "DECODE(big, NULL, 'null big', big), DECODE(ratio, NULL, 'null ratio', ratio),  " +
                "DECODE(tm, NULL, 'null tm', 'tm'), DECODE(var, NULL, 'null var', var), " +
                "DECODE(dec, NULL, 'null dec', dec) from R3 where id = 1");
        checkDecodeNullResult(cr, new String[]{"1","1","1","1","1.1","tm","IBM","1"});

        cr = client.callProcedure("@AdHoc", "select DECODE(tiny, NULL, 'null tiny', tiny)," +
                "DECODE(small, NULL, 'null small', small), DECODE(num, NULL, 'null num', num),  " +
                "DECODE(big, NULL, 'null big', big), DECODE(ratio, NULL, 'null ratio', ratio),  " +
                "DECODE(tm, NULL, 'null tm', 'tm'), DECODE(var, NULL, 'null var', var), " +
                "DECODE(dec, NULL, 'null dec', dec) from R3 where id = 2");
        checkDecodeNullResult(cr, new String[]{"null tiny","null small", "null num", "null big",
                "null ratio", "null tm", "null var", "null dec"});


        cr = client.callProcedure("P2.insert", 1, new Timestamp(1000L));
        cr = client.callProcedure("P2.insert", 2, null);
        // Test timestamp
        cr = client.callProcedure("TestDecodeNullTimestamp", 1);
        checkDecodeNullResult(cr, new String[]{"2013-07-18 02:00:00.123457"});

        // Test NULL as the second search expression.
        cr = client.callProcedure("@AdHoc", "select DECODE(tiny, -1, -1, NULL, 0, tiny)," +
                "DECODE(small, -1, -1, NULL, 0, small), DECODE(num, -1, -1, NULL, 0, num),  " +
                "DECODE(big, -1, -1, NULL, 0, big) from R3 where id = 1");
        checkDecodeNullResult(cr, new Long[]{1L,1L,1L,1L});
        cr = client.callProcedure("@AdHoc", "select DECODE(tiny, -1, -1, NULL, 0, tiny)," +
                "DECODE(small, -1, -1, NULL, 0, small), DECODE(num, -1, -1, NULL, 0, num),  " +
                "DECODE(big, -1, -1, NULL, 0, big) from R3 where id = 2");
        checkDecodeNullResult(cr, new Long[]{0L,0L,0L,0L});

        // Test Null return type
        cr = client.callProcedure("@AdHoc","select DECODE(tiny, 4, 5, NULL, NULL, 10) " +
                " from R3 where id = 2");
        assertTrue(cr.getResults()[0].getRowCount() == 1);
        assertTrue(cr.getResults()[0].advanceRow());
        assertEquals(Integer.MIN_VALUE, cr.getResults()[0].getLong(0));

        verifyStmtFails(client, "select DECODE(tiny, 4, 5, NULL, 'tiny null', tiny)  from R3 where id = 2",
                "Could not convert to number");
    }

    public void testSINCE_EPOCH() throws Exception {
        System.out.println("STARTING SINCE_EPOCH");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P2.insert", 0, new Timestamp(0L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 1, new Timestamp(1L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 2, new Timestamp(1000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 3, new Timestamp(-1000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 4, new Timestamp(1371808830000L));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 5, "2013-07-18 02:00:00.123457");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Test AdHoc
        cr = client.callProcedure("@AdHoc", "select SINCE_EPOCH (SECOND, TM), TM from P2 where id = 4");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1371808830L, result.getLong(0));
        assertEquals(1371808830000000L, result.getTimestampAsLong(1));

        // Test constants timestamp with string
        cr = client.callProcedure("@AdHoc", "select TM, TO_TIMESTAMP(MICROS, SINCE_EPOCH (MICROS, '2013-07-18 02:00:00.123457') ) from P2 where id = 5");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(result.getTimestampAsLong(0), result.getTimestampAsLong(1));

        // Test user error input, Only accept JDBC's timestamp format: YYYY-MM-DD-SS.sss.
        try {
            cr = client.callProcedure("@AdHoc", "select SINCE_EPOCH (MICROS, 'I am a timestamp')  from P2 where id = 5");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("PlanningErrorException"));
            assertTrue(ex.getMessage().contains("incompatible data type in conversion"));
        }

        String[] procedures = {"SINCE_EPOCH_SECOND", "SINCE_EPOCH_MILLIS",
                "SINCE_EPOCH_MILLISECOND", "SINCE_EPOCH_MICROS", "SINCE_EPOCH_MICROSECOND"};

        for (int i=0; i< procedures.length; i++) {
            String proc = procedures[i];

            cr = client.callProcedure(proc, 0);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "SINCE_EPOCH_SECOND") {
                assertEquals(0, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MILLIS" || proc == "SINCE_EPOCH_MILLISECOND") {
                assertEquals(0, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MICROS" || proc == "SINCE_EPOCH_MICROSECOND") {
                assertEquals(0, result.getLong(0));
            }

            cr = client.callProcedure(proc, 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "SINCE_EPOCH_SECOND") {
                assertEquals(0, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MILLIS" || proc == "SINCE_EPOCH_MILLISECOND") {
                assertEquals(1, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MICROS" || proc == "SINCE_EPOCH_MICROSECOND") {
                assertEquals(1000, result.getLong(0));
            } else {
                fail();
            }

            cr = client.callProcedure(proc, 2);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "SINCE_EPOCH_SECOND") {
                assertEquals(1, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MILLIS" || proc == "SINCE_EPOCH_MILLISECOND") {
                assertEquals(1000, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MICROS" || proc == "SINCE_EPOCH_MICROSECOND") {
                assertEquals(1000000, result.getLong(0));
            } else {
                fail();
            }

            cr = client.callProcedure(proc, 3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "SINCE_EPOCH_SECOND") {
                assertEquals(-1, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MILLIS" || proc == "SINCE_EPOCH_MILLISECOND") {
                assertEquals(-1000, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MICROS" || proc == "SINCE_EPOCH_MICROSECOND") {
                assertEquals(-1000000, result.getLong(0));
            } else {
               fail();
            }

            cr = client.callProcedure(proc, 4);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "SINCE_EPOCH_SECOND") {
                assertEquals(1371808830L, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MILLIS" || proc == "SINCE_EPOCH_MILLISECOND") {
                assertEquals(1371808830000L, result.getLong(0));
            } else if (proc == "SINCE_EPOCH_MICROS" || proc == "SINCE_EPOCH_MICROSECOND") {
                assertEquals(1371808830000000L, result.getLong(0));
            } else {
               fail();
            }
        }
    }

    public void testENG6861() throws Exception {
        System.out.println("STARTING testENG6861");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        // Test user-found anomaly around complex filter using functions
        try {
            cr = client.callProcedure("@Explain",
                    "SELECT TOP ? * FROM PAULTEST WHERE NAME IS NOT NULL AND " +
                    "    ( LOCK_TIME IS NULL OR " +
                    "      SINCE_EPOCH(MILLIS,CURRENT_TIMESTAMP)-? < SINCE_EPOCH(MILLIS,LOCK_TIME) );");
            //* enable for debug */ System.out.println(cr.getResults()[0]);
        } catch (Exception ex) {
            fail();
        }

        try {
            cr = client.callProcedure("@AdHoc",
                    "SELECT TOP ? * FROM PAULTEST WHERE NAME IS NOT NULL AND " +
                    "    ( LOCK_TIME IS NULL OR " +
                    "      SINCE_EPOCH(MILLIS,CURRENT_TIMESTAMP)-? < SINCE_EPOCH(MILLIS,LOCK_TIME) );",
                    10, 10000);
            result = cr.getResults()[0];
            assertEquals(0, result.getRowCount());
            //* enable for debug */ System.out.println(result);
        } catch (Exception ex) {
            fail();
        }

        try {
            cr = client.callProcedure("GOT_BAD_PARAM_COUNTS_INLINE", 10, 10000);
            result = cr.getResults()[0];
            assertEquals(0, result.getRowCount());
            //* enable for debug */ System.out.println(result);
        } catch (Exception ex) {
            fail();
        }

        try {
            cr = client.callProcedure("GotBadParamCountsInJava", 10, 10000);
            result = cr.getResults()[0];
            assertEquals(0, result.getRowCount());
            //* enable for debug */ System.out.println(result);
        } catch (Exception ex) {
            fail();
        }

        try {
            // Purposely neglecting to list an select columns or '*'.
            cr = client.callProcedure("@Explain", "SELECT TOP ? FROM PAULTEST WHERE NAME IS NOT NULL AND (LOCK_TIME IS NULL OR SINCE_EPOCH(MILLIS,CURRENT_TIMESTAMP)-? < SINCE_EPOCH(MILLIS,LOCK_TIME));");
            //* enable for debug */ System.out.println(cr.getResults()[0]);
            fail("Expected to detect missing SELECT columns");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("PlanningErrorException"));
            assertTrue(ex.getMessage().contains("unexpected token: FROM"));
            return;
        }
        //* enable for debug */ System.out.println(cr.getResults()[0]);
    }

    public void testTO_TIMESTAMP() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING TO_TIMESTAMP");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P2.insert", 0, new Timestamp(0L));
        cr = client.callProcedure("P2.insert", 1, new Timestamp(1L));
        cr = client.callProcedure("P2.insert", 2, new Timestamp(1000L));
        cr = client.callProcedure("P2.insert", 3, new Timestamp(-1000L));

        // Test AdHoc
        cr = client.callProcedure("@AdHoc", "select to_timestamp(second, 1372640523) from P2 limit 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1372640523 * 1000000L, result.getTimestampAsLong(0));

        // Test string input number, expect error
        try {
            cr = client.callProcedure("@AdHoc", "select to_timestamp(second, '1372640523') from P2 limit 1");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("PlanningErrorException"));
            assertTrue(ex.getMessage().contains("incompatible data type"));
        }

        String[] procedures = {"FROM_UNIXTIME", "TO_TIMESTAMP_SECOND", "TO_TIMESTAMP_MILLIS",
                "TO_TIMESTAMP_MILLISECOND", "TO_TIMESTAMP_MICROS", "TO_TIMESTAMP_MICROSECOND"};

        for (int i=0; i< procedures.length; i++) {
            String proc = procedures[i];

            cr = client.callProcedure(proc, 0L , 0);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "TO_TIMESTAMP_SECOND" || proc == "FROM_UNIXTIME") {
                assertEquals(0L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MILLIS" || proc == "TO_TIMESTAMP_MILLISECOND") {
                assertEquals(0L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MICROS" || proc == "TO_TIMESTAMP_MICROSECOND") {
                assertEquals(0L, result.getTimestampAsLong(0));
            } else {
                fail();
            }

            cr = client.callProcedure(proc, 1L , 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "TO_TIMESTAMP_SECOND" || proc == "FROM_UNIXTIME") {
                assertEquals(1000000L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MILLIS" || proc == "TO_TIMESTAMP_MILLISECOND") {
                assertEquals(1000L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MICROS" || proc == "TO_TIMESTAMP_MICROSECOND") {
                assertEquals(1L, result.getTimestampAsLong(0));
            } else {
                fail();
            }

            cr = client.callProcedure(proc, 1000L , 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "TO_TIMESTAMP_SECOND" || proc == "FROM_UNIXTIME") {
                assertEquals(1000000000L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MILLIS" || proc == "TO_TIMESTAMP_MILLISECOND") {
                assertEquals(1000000L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MICROS" || proc == "TO_TIMESTAMP_MICROSECOND") {
                assertEquals(1000L, result.getTimestampAsLong(0));
            } else {
                fail();
            }

            cr = client.callProcedure(proc, -1000 , 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "TO_TIMESTAMP_SECOND" || proc == "FROM_UNIXTIME") {
                assertEquals(-1000000000L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MILLIS" || proc == "TO_TIMESTAMP_MILLISECOND") {
                assertEquals(-1000000L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MICROS" || proc == "TO_TIMESTAMP_MICROSECOND") {
                assertEquals(-1000L, result.getTimestampAsLong(0));
            } else {
                fail();
            }

            cr = client.callProcedure(proc, 1371808830000L, 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            if (proc == "TO_TIMESTAMP_SECOND" || proc == "FROM_UNIXTIME") {
                assertEquals(1371808830000000000L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MILLIS" || proc == "TO_TIMESTAMP_MILLISECOND") {
                assertEquals(1371808830000000L, result.getTimestampAsLong(0));
            } else if (proc == "TO_TIMESTAMP_MICROS" || proc == "TO_TIMESTAMP_MICROSECOND") {
                assertEquals(1371808830000L, result.getTimestampAsLong(0));
            } else {
                fail();
            }
        }

    }

    public void testTRUNCATE() throws Exception {
        System.out.println("STARTING TRUNCATE with timestamp");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;
        VoltDB.setDefaultTimezone();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        //System.out.println(dateFormat.getTimeZone());
        Date time = null;

        // Test Standard TRUNCATE function for floating numbers
        Exception ex = null;
        try {
            cr = client.callProcedure("@AdHoc", "select TRUNCATE (1.2, 1), TM from P2 where id = 0");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            ex = e;
        } finally {
            assertNotNull(ex);
            assertTrue((ex.getMessage().contains("PlanningErrorException")));
            assertTrue((ex.getMessage().contains("TRUNCATE")));
        }

        // Test date before Gregorian calendar beginning.
        cr = client.callProcedure("P2.insert", 0, Timestamp.valueOf("1582-03-06 13:56:40.123456"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        ex = null;
        try {
            cr = client.callProcedure("TRUNCATE", 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            ex = e;
        } finally {
            assertNotNull(ex);
            assertTrue((ex.getMessage().contains("SQL ERROR")));
        }

        // Test Timestamp Null value
        cr = client.callProcedure("P2.insert", 1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("TRUNCATE", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        for (int i=0; i< 11; i++) {
            assertNull(result.getTimestampAsTimestamp(i));
        }

        // Test normal TRUNCATE functionalities
        cr = client.callProcedure("P2.insert", 2, Timestamp.valueOf("2001-09-09 01:46:40.035123"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("TRUNCATE", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());

        cr = client.callProcedure("TRUNCATE", 2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());

        time = dateFormat.parse("2001-01-01 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(0));

        time = dateFormat.parse("2001-07-01 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(1));

        time = dateFormat.parse("2001-09-01 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(2));

        time = dateFormat.parse("2001-09-09 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(3));

        time = dateFormat.parse("2001-09-09 01:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(4));

        time = dateFormat.parse("2001-09-09 01:46:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(5));

        time = dateFormat.parse("2001-09-09 01:46:40.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(6));

        time = dateFormat.parse("2001-09-09 01:46:40.035");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(7));

        time = dateFormat.parse("2001-09-09 01:46:40.035");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(8));

        assertEquals(1000000000035123L, result.getTimestampAsLong(9));
        assertEquals(1000000000035123L, result.getTimestampAsLong(10));

        // Test time before EPOCH
        cr = client.callProcedure("P2.insert", 3, Timestamp.valueOf("1583-11-24 13:56:40.123456"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("TRUNCATE", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());

        time = dateFormat.parse("1583-01-01 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(0));

        time = dateFormat.parse("1583-10-01 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(1));

        time = dateFormat.parse("1583-11-01 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(2));

        time = dateFormat.parse("1583-11-24 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(3));

        time = dateFormat.parse("1583-11-24 13:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(4));

        time = dateFormat.parse("1583-11-24 13:56:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(5));

        time = dateFormat.parse("1583-11-24 13:56:40.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(6));

        time = dateFormat.parse("1583-11-24 13:56:40.123");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(7));

        time = dateFormat.parse("1583-11-24 13:56:40.123");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(7));

        assertEquals(-12184250599876544L, result.getTimestampAsLong(9));
        assertEquals(-12184250599876544L, result.getTimestampAsLong(10));

        // Test date in far future
        cr = client.callProcedure("P2.insert", 4, Timestamp.valueOf("2608-03-06 13:56:40.123456"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("TRUNCATE", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());

        time = dateFormat.parse("2608-01-01 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(0));

        time = dateFormat.parse("2608-01-01 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(1));

        time = dateFormat.parse("2608-03-01 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(2));

        time = dateFormat.parse("2608-03-06 00:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(3));

        time = dateFormat.parse("2608-03-06 13:00:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(4));

        time = dateFormat.parse("2608-03-06 13:56:00.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(5));

        time = dateFormat.parse("2608-03-06 13:56:40.000");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(6));

        time = dateFormat.parse("2608-03-06 13:56:40.123");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(7));

        time = dateFormat.parse("2608-03-06 13:56:40.123");
        assertEquals(time.getTime() * 1000, result.getTimestampAsLong(7));

        assertEquals(20138939800123456L, result.getTimestampAsLong(9));
        assertEquals(20138939800123456L, result.getTimestampAsLong(10));
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

    public void testFormatCurrency() throws Exception
    {
        System.out.println("STARTING testFormatCurrency");
        Client client = getClient();
        ClientResponse cr = null;
        VoltTable result;
        String str;

        cr = client.callProcedure("@AdHoc", "Delete from D1;");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        cr = client.callProcedure("@AdHoc", "Delete from R3;");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        String[] decimal_strs = {"123456.64565",     // id = 0
                                 "-123456.64565",    // id = 1
                                 "1123456785.555",   // id = 2
                                 "-1123456785.555",  // id = 3
                                 "0.0",              // id = 4
                                 "-0.0",             // id = 5
                                 "0",                // id = 6
                                 "-0",               // id = 7
                                 "99999999999999999999999999.999999999999", // id = 8
                                 "-99999999999999999999999999.99999999999", // id = 9
                                 "1500",             // id = 10
                                 "2500",             // id = 11
                                 "8223372036854775807.123456789125",        // id = 12
                                 "8223372036854775807.123456789175"};       // id = 13
        for(int i = 0; i < decimal_strs.length; i++) {
            BigDecimal bd = new BigDecimal(decimal_strs[i]);
            cr = client.callProcedure("D1.insert", i, bd);
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        }
        cr = client.callProcedure("R3.insert", 1, 1, 1, 1, 1, 1.1, "2013-07-18 02:00:00.123457", "IBM", 1);
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, 1), FORMAT_CURRENCY(DEC, 2),"
                                                 + "FORMAT_CURRENCY(DEC, 3), FORMAT_CURRENCY(DEC, 4),"
                                                 + "FORMAT_CURRENCY(DEC, 0), FORMAT_CURRENCY(DEC, -1),"
                                                 + "FORMAT_CURRENCY(DEC, -2), FORMAT_CURRENCY(DEC, -3) from D1 where id = 0");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getString(0);
        assertEquals(str, "123,456.6");    // rounding down
        str = result.getString(1);
        assertEquals(str, "123,456.65");   // rounding up
        str = result.getString(2);
        assertEquals(str, "123,456.646");
        str = result.getString(3);
        assertEquals(str, "123,456.6456"); // banker's rounding: half to nearest even when previous digit is even
        // rounding to none-positive places, or say the whole part
        str = result.getString(4);
        assertEquals(str, "123,457");      // rounding up
        str = result.getString(5);
        assertEquals(str, "123,460");
        str = result.getString(6);
        assertEquals(str, "123,500");
        str = result.getString(7);
        assertEquals(str, "123,000");      // rounding down


        cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, 1), FORMAT_CURRENCY(DEC, 2),"
                                                 + "FORMAT_CURRENCY(DEC, 3), FORMAT_CURRENCY(DEC, 4),"
                                                 + "FORMAT_CURRENCY(DEC, 0), FORMAT_CURRENCY(DEC, -1),"
                                                 + "FORMAT_CURRENCY(DEC, -2), FORMAT_CURRENCY(DEC, -3) from D1 where id = 1");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getString(0);
        assertEquals(str, "-123,456.6");    // rounding down
        str = result.getString(1);
        assertEquals(str, "-123,456.65");   // rounding up
        str = result.getString(2);
        assertEquals(str, "-123,456.646");
        str = result.getString(3);
        assertEquals(str, "-123,456.6456"); //banker's rounding: half to nearest even when previous digit is even
        // rounding to none-positive places, or say the whole part
        str = result.getString(4);
        assertEquals(str, "-123,457");      // rounding up
        str = result.getString(5);
        assertEquals(str, "-123,460");
        str = result.getString(6);
        assertEquals(str, "-123,500");
        str = result.getString(7);
        assertEquals(str, "-123,000");      // rounding down

        cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, 1), FORMAT_CURRENCY(DEC, 2),"
                                                 + "FORMAT_CURRENCY(DEC, 3), FORMAT_CURRENCY(DEC, 4),"
                                                 + "FORMAT_CURRENCY(DEC, 0), FORMAT_CURRENCY(DEC, -1),"
                                                 + "FORMAT_CURRENCY(DEC, -2), FORMAT_CURRENCY(DEC, -3) from D1 where id = 2");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getString(0);
        assertEquals(str, "1,123,456,785.6");
        str = result.getString(1);
        assertEquals(str, "1,123,456,785.56"); // banker's rounding: half to nearest even when previous digit is odd
        str = result.getString(2);
        assertEquals(str, "1,123,456,785.555");
        str = result.getString(3);
        assertEquals(str, "1,123,456,785.5550"); // add trailing zero if rounding to a larger place
        // rounding to none-positive places, or say the whole part
        str = result.getString(4);
        assertEquals(str, "1,123,456,786");
        str = result.getString(5);
        assertEquals(str, "1,123,456,790");
        str = result.getString(6);
        assertEquals(str, "1,123,456,800");
        str = result.getString(7);
        assertEquals(str, "1,123,457,000");

        cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, 1), FORMAT_CURRENCY(DEC, 2),"
                                                 + "FORMAT_CURRENCY(DEC, 3), FORMAT_CURRENCY(DEC, 4),"
                                                 + "FORMAT_CURRENCY(DEC, 0), FORMAT_CURRENCY(DEC, -1),"
                                                 + "FORMAT_CURRENCY(DEC, -2), FORMAT_CURRENCY(DEC, -3) from D1 where id = 3");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getString(0);
        assertEquals(str, "-1,123,456,785.6");
        str = result.getString(1);
        assertEquals(str, "-1,123,456,785.56"); // banker's rounding: half to nearest even when previous digit is odd
        str = result.getString(2);
        assertEquals(str, "-1,123,456,785.555");
        str = result.getString(3);
        assertEquals(str, "-1,123,456,785.5550"); // add trailing zero if rounding to a larger place
        // rounding to none-positive places, or say the whole part
        str = result.getString(4);
        assertEquals(str, "-1,123,456,786");
        str = result.getString(5);
        assertEquals(str, "-1,123,456,790");
        str = result.getString(6);
        assertEquals(str, "-1,123,456,800");
        str = result.getString(7);
        assertEquals(str, "-1,123,457,000");

        cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, -3) from D1 where id = 10");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        str = result.getString(0);
        // banker's rounding to a negative place: half to nearest even when previous digit is odd
        assertEquals(str, "2,000");

        cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, -3) from D1 where id = 11");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        str = result.getString(0);
        // banker's rounding to a negative place: half to nearest even when previous digit is even
        assertEquals(str, "2,000");

        // zeros with different init input
        for (int i = 4; i < 8; i++) {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, 2) from D1 where id = "+i);
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, "0.00");
        }

        // out of int64_t range
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(dec, 2) from D1 where id = 8");
            fail("range validity check failed for FORMAT_CURRENCY");
        }
        catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("out of range"));
        }
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(dec, 2) from D1 where id = 9");
            fail("range validity check failed for FORMAT_CURRENCY");
        }
        catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("out of range"));
        }

        // check invalid type
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(id, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(tiny, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(small, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(num, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(big, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(ratio, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(tm, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            // TODO: I have no idea why the exception is different
            assertTrue(pcex.getMessage().contains("incompatible data type in operation"));
        }
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(var, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("incompatible data type in operation"));
        }

        String[] s = {"1,000,000.00", "100,000.00", "10,000.00", "1,000.00", "100.00", "10.00", "1.00", "0.10", "0.01", "0.00"};
        for (int i = 0; i < 10; i++){
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(CAST("+ Math.pow(10, 6-i) +" as DECIMAL), 2) from D1 where id = 1");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, s[i]);
        }
        for (int i = 0; i < 10; i++){
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(CAST("+ -Math.pow(10, 6-i) +" as DECIMAL), 2) from D1 where id = 1");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, "-" + s[i]);
        }

        // TODO: The precision depends on the ability of TTInt, and there may exist some number whose rounding is wrong.
        // test places from 11 to -25
        String[] s2 = {"8,223,372,036,854,775,807.12345678912","8,223,372,036,854,775,807.1234567891","8,223,372,036,854,775,807.123456789",
                  "8,223,372,036,854,775,807.12345679","8,223,372,036,854,775,807.1234568","8,223,372,036,854,775,807.123457",
                  "8,223,372,036,854,775,807.12346","8,223,372,036,854,775,807.1235","8,223,372,036,854,775,807.123","8,223,372,036,854,775,807.12",
                  "8,223,372,036,854,775,807.1","8,223,372,036,854,775,807","8,223,372,036,854,775,810","8,223,372,036,854,775,800",
                  "8,223,372,036,854,776,000","8,223,372,036,854,780,000","8,223,372,036,854,800,000","8,223,372,036,855,000,000",
                  "8,223,372,036,850,000,000","8,223,372,036,900,000,000","8,223,372,037,000,000,000","8,223,372,040,000,000,000","8,223,372,000,000,000,000",
                  "8,223,372,000,000,000,000","8,223,370,000,000,000,000","8,223,400,000,000,000,000","8,223,000,000,000,000,000","8,220,000,000,000,000,000",
                  "8,200,000,000,000,000,000","8,000,000,000,000,000,000","not used","0","0","0","0","0","0"};
        for (int i=11; i > -19; i--){
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, "+i+") from D1 where id = 12");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, s2[11-i]);
        }
        // it will go out of the range of int64_t
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, -19) from D1 where id = 12");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("out of range"));
        }
        // now it is zero
        for (int i=-20; i >= -25; i--){
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, "+i+") from D1 where id = 12");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, s2[11-i]);
        }
        String s3[] ={"8,223,372,036,854,775,807.12345678918","8,223,372,036,854,775,807.1234567892","8,223,372,036,854,775,807.123456789",
                "8,223,372,036,854,775,807.12345679","8,223,372,036,854,775,807.1234568","8,223,372,036,854,775,807.123457",
                "8,223,372,036,854,775,807.12346","8,223,372,036,854,775,807.1235","8,223,372,036,854,775,807.123","8,223,372,036,854,775,807.12",
                "8,223,372,036,854,775,807.1","8,223,372,036,854,775,807","8,223,372,036,854,775,810","8,223,372,036,854,775,800",
                "8,223,372,036,854,776,000","8,223,372,036,854,780,000","8,223,372,036,854,800,000","8,223,372,036,855,000,000",
                "8,223,372,036,850,000,000","8,223,372,036,900,000,000","8,223,372,037,000,000,000","8,223,372,040,000,000,000","8,223,372,000,000,000,000",
                "8,223,372,000,000,000,000","8,223,370,000,000,000,000","8,223,400,000,000,000,000","8,223,000,000,000,000,000","8,220,000,000,000,000,000",
                "8,200,000,000,000,000,000","8,000,000,000,000,000,000","not used","0","0","0","0","0","0"};
        for (int i=11; i > -19; i--){
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, "+i+") from D1 where id = 13");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, s3[11-i]);
        }
        // it will go out of the range of int64_t
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, -19) from D1 where id = 13");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("out of range"));
        }
        // now it is zero
        for (int i=-20; i >= -25; i--){
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, "+i+") from D1 where id = 13");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, s3[11-i]);
        }

        // check the validity of the second parameter
        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, 15) from D1 where id = 0");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("the second parameter"));
        }

        try {
            cr = client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, -26) from D1 where id = 0");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("the second parameter"));
        }
    }

    public void testConcat() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING test Concat and its Operator");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "Xin", 1, 1.0);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("CONCAT2", "", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Xin", result.getString(1));

        cr = client.callProcedure("CONCAT2", "@VoltDB", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Xin@VoltDB", result.getString(1));

        cr = client.callProcedure("ConcatOpt", "", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Xin", result.getString(1));

        cr = client.callProcedure("ConcatOpt", "@VoltDB", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("Xin@VoltDB", result.getString(1));
    }

    public void testConcatMoreThan2Param() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING test Concat with more than two parameters");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        cr = client.callProcedure("P1.insert", 1, "Yetian", 1, 1.0);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("CONCAT3", "@Volt", "DB", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, 1, new String[]{"Yetian@VoltDB"});

        cr = client.callProcedure("CONCAT3", "", "@VoltDB", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, 1, new String[]{"Yetian@VoltDB"});

        cr = client.callProcedure("CONCAT4", "@Volt", "", "DB", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, 1, new String[]{"Yetian@VoltDB"});

        cr = client.callProcedure("CONCAT4", "", "@VoltDB", "", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, 1, new String[]{"Yetian@VoltDB"});

        cr = client.callProcedure("CONCAT5", "@Volt", "D", "B", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, 1, new String[]{"Yetian@VoltDB1"});

        cr = client.callProcedure("CONCAT5", "", "@VoltDB", "", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        validateTableColumnOfScalarVarchar(result, 1, new String[]{"Yetian@VoltDB1"});

        try {
            cr = client.callProcedure("@AdHoc", "select CONCAT('a', 'b', id) from p1 where id = 1");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("can't be cast as VARCHAR"));
        }
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestFunctionsForVoltDBSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestFunctionsForVoltDBSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(300), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PRIMARY KEY (ID) ); " +
                "PARTITION TABLE P1 ON COLUMN ID;" +

                "CREATE TABLE P2 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
                "PARTITION TABLE P2 ON COLUMN ID;\n" +

                "CREATE TABLE P3_INLINE_DESC ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "DESC VARCHAR(15), " +
                "DESC2 VARCHAR(15), " +
                "NUM INTEGER, " +
                "RATIO FLOAT, " +
                "PRIMARY KEY (ID) ); " +
                "PARTITION TABLE P3_INLINE_DESC ON COLUMN ID;" +

                "CREATE TABLE R3 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "TINY TINYINT, " +
                "SMALL SMALLINT, " +
                "NUM INTEGER, " +
                "BIG BIGINT, " +
                "RATIO FLOAT, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "VAR VARCHAR(300), " +
                "DEC DECIMAL, " +
                "PRIMARY KEY (ID) ); " +

                "CREATE TABLE JS1 (\n" +
                "  ID INTEGER NOT NULL, \n" +
                "  DOC VARCHAR(8192),\n" +
                "  PRIMARY KEY(ID))\n" +
                ";\n" +

                "CREATE TABLE D1 (\n" +
                "  ID INTEGER NOT NULL, \n" +
                "  DEC DECIMAL, \n" +
                "  PRIMARY KEY(ID))\n" +
                ";\n" +

                "CREATE PROCEDURE IdFieldProc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(DOC, ?) = ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE InnerFieldProc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(FIELD(DOC, 'inner'), ?) = ? ORDER BY ID\n" +
                ";\n" +
                "CREATE PROCEDURE NullFieldProc AS\n" +
                "   SELECT ID FROM JS1 WHERE FIELD(DOC, ?) IS NULL ORDER BY ID\n" +
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

                "CREATE TABLE PAULTEST (ID INTEGER, NAME VARCHAR(12), LOCK_TIME TIMESTAMP, PRIMARY KEY(ID));" +
                "\n" +
                "CREATE PROCEDURE GOT_BAD_PARAM_COUNTS_INLINE AS \n" +
                "    SELECT TOP ? * FROM PAULTEST WHERE NAME IS NOT NULL AND " +
                "                                       (LOCK_TIME IS NULL OR " +
                "                                        SINCE_EPOCH(MILLIS,CURRENT_TIMESTAMP)-? < " +
                "                                        SINCE_EPOCH(MILLIS,LOCK_TIME))\n" +
                ";\n" +

                "";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        // Test DECODE
        project.addStmtProcedure("DECODE", "select desc,  DECODE (desc,'IBM','zheng'," +
                        "'Microsoft','li'," +
                        "'Hewlett Packard','at'," +
                        "'Gateway','VoltDB'," +
                        "'where') from P1 where id = ?");
        project.addStmtProcedure("DECODEND", "select desc,  DECODE (desc,'zheng','a') from P1 where id = ?");
        project.addStmtProcedure("DECODEVERYLONG", "select desc,  DECODE (desc,'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'a','a'," +
                "'where') from P1 where id = ?");
        project.addStmtProcedure("DECODE_PARAM_INFER_STRING", "select desc,  DECODE (desc,?,?,desc) from P1 where id = ?");
        project.addStmtProcedure("DECODE_PARAM_INFER_INT", "select desc,  DECODE (id,?,?,id) from P1 where id = ?");
        project.addStmtProcedure("DECODE_PARAM_INFER_DEFAULT", "select desc,  DECODE (?,?,?,?) from P1 where id = ?");
        project.addStmtProcedure("DECODE_PARAM_INFER_CONFLICTING", "select desc,  DECODE (id,1,?,2,99,'贾鑫') from P1 where id = ?");
        // Test OCTET_LENGTH
        project.addStmtProcedure("OCTET_LENGTH", "select desc,  OCTET_LENGTH (desc) from P1 where id = ?");
        // Test POSITION and CHAR_LENGTH
        project.addStmtProcedure("POSITION", "select desc, POSITION (? IN desc) from P1 where id = ?");
        project.addStmtProcedure("CHAR_LENGTH", "select desc, CHAR_LENGTH (desc) from P1 where id = ?");
        // Test SINCE_EPOCH
        project.addStmtProcedure("SINCE_EPOCH_SECOND", "select SINCE_EPOCH (SECOND, TM) from P2 where id = ?");
        project.addStmtProcedure("SINCE_EPOCH_MILLIS", "select SINCE_EPOCH (MILLIS, TM) from P2 where id = ?");
        project.addStmtProcedure("SINCE_EPOCH_MILLISECOND", "select SINCE_EPOCH (MILLISECOND, TM) from P2 where id = ?");
        project.addStmtProcedure("SINCE_EPOCH_MICROS", "select SINCE_EPOCH (MICROS, TM) from P2 where id = ?");
        project.addStmtProcedure("SINCE_EPOCH_MICROSECOND", "select SINCE_EPOCH (MICROSECOND, TM) from P2 where id = ?");
        // Test TO_TIMESTAMP
        project.addStmtProcedure("TO_TIMESTAMP_SECOND", "select TO_TIMESTAMP (SECOND, ?) from P2 where id = ?");
        project.addStmtProcedure("TO_TIMESTAMP_MILLIS", "select TO_TIMESTAMP (MILLIS, ?) from P2 where id = ?");
        project.addStmtProcedure("TO_TIMESTAMP_MILLISECOND", "select TO_TIMESTAMP (MILLISECOND, ?) from P2 where id = ?");
        project.addStmtProcedure("TO_TIMESTAMP_MICROS", "select TO_TIMESTAMP (MICROS, ?) from P2 where id = ?");
        project.addStmtProcedure("TO_TIMESTAMP_MICROSECOND", "select TO_TIMESTAMP (MICROSECOND, ?) from P2 where id = ?");

        project.addStmtProcedure("TRUNCATE", "select TRUNCATE(YEAR, TM), TRUNCATE(QUARTER, TM), TRUNCATE(MONTH, TM), " +
                "TRUNCATE(DAY, TM), TRUNCATE(HOUR, TM),TRUNCATE(MINUTE, TM),TRUNCATE(SECOND, TM), TRUNCATE(MILLIS, TM), " +
                "TRUNCATE(MILLISECOND, TM), TRUNCATE(MICROS, TM), TRUNCATE(MICROSECOND, TM) from P2 where id = ?");

        project.addStmtProcedure("FROM_UNIXTIME", "select FROM_UNIXTIME (?) from P2 where id = ?");

        project.addStmtProcedure("TestDecodeNull", "select DECODE(tiny, NULL, 'null tiny', tiny)," +
                "DECODE(small, NULL, 'null small', small), DECODE(num, NULL, 'null num', num),  " +
                "DECODE(big, NULL, 'null big', big), DECODE(ratio, NULL, 'null ratio', ratio),  " +
                "DECODE(tm, NULL, 'null tm', 'tm'), DECODE(var, NULL, 'null var', var), " +
                "DECODE(dec, NULL, 'null dec', dec) from R3 where id = ?");
        project.addStmtProcedure("TestDecodeNullParam", "select DECODE(tiny, ?, 'null tiny', tiny)," +
                "DECODE(small, ?, 'null small', small), DECODE(num, ?, 'null num', num),  " +
                "DECODE(big, ?, 'null big', big), DECODE(ratio, ?, 'null ratio', ratio),  " +
                "DECODE(tm, ?, 'null tm', 'tm'), DECODE(var, ?, 'null var', var), " +
                "DECODE(dec, ?, 'null dec', dec) from R3 where id = ?");

        project.addStmtProcedure("TestDecodeNullTimestamp", "select DECODE(tm, NULL, 'null tm', tm) from R3 where id = ?");

        project.addStmtProcedure("CONCAT2", "select id, CONCAT(DESC,?) from P1 where id = ?");
        project.addStmtProcedure("CONCAT3", "select id, CONCAT(DESC,?,?) from P1 where id = ?");
        project.addStmtProcedure("CONCAT4", "select id, CONCAT(DESC,?,?,?) from P1 where id = ?");
        project.addStmtProcedure("CONCAT5", "select id, CONCAT(DESC,?,?,?,cast(ID as VARCHAR)) from P1 where id = ?");
        project.addStmtProcedure("ConcatOpt", "select id, DESC || ? from P1 where id = ?");

        project.addProcedures(GotBadParamCountsInJava.class);
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
