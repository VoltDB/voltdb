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
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestFunctionsForVoltDBSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

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
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select SQL_ERROR('abc') from P1 where ID = 0");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123, 'abc') from P1 where ID = 0");
        assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

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
        cr = client.callProcedure("P3_INLINE_DESC.insert", 1, "zheng", 10, 1.1);
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

        try {
            cr = client.callProcedure("@AdHoc","select DECODE(tiny, 4, 5, NULL, 'tiny null', tiny) " +
                    " from R3 where id = 2");
            fail();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            assertTrue(ex.getMessage().contains("SQL ERROR"));
            assertTrue(ex.getMessage().contains("value: 'tiny null'"));
        }
    }

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
                "    \"inner\": {\n" +
                "        \"veggies\": \"good for you\",\n" +
                "        \"贾鑫Vo\": \"wakarimasen\"\n" +
                "    },\n" +
                "    \"arr\": [\n" +
                "        0,\n" +
                "        %d,\n" +
                "        100\n" +
                "    ],\n" +
                "    \"tag\": \"%s\"\n" +
                "}";

        ClientResponse cr;
        cr = client.callProcedure("JS1.insert",1,String.format(jstemplate, 1, 1, "one"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert",2,String.format(jstemplate, 2, 2, "two"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert",3,String.format(jstemplate, 3, 3, "three"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert",4,"{\"id\":4,\"bool\": false}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert",5,"{}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert",6,"[]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert",7,"{\"id\":7,\"funky\": null}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert",8, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("JS1.insert",9, "{\"id\":9, \"贾鑫Vo\":\"分かりません\"}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    public void testFIELDFunction() throws Exception {
        ClientResponse cr;
        VoltTable result;
        Client client = getClient();
        loadJS1(client);

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

        cr = client.callProcedure("IdFieldProc", "tag", "three");
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
        assertEquals(9, result.getRowCount());

        cr = client.callProcedure("NullFieldProc", "id");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(3, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(5L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(6L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(8L,result.getLong(0));

        cr = client.callProcedure("InnerFieldProc", "贾鑫Vo" ,"wakarimasen");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(3, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(2L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(3L,result.getLong(0));

        cr = client.callProcedure("IdFieldProc", "arr" ,"[0,2,100]");
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
        assertEquals(9, result.getRowCount());

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
        cr = client.callProcedure("JS1.insert", 10, "[0, 10, 100]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                                  "SELECT ARRAY_ELEMENT(DOC, 1) FROM JS1 WHERE ID = 10");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals("10",result.getString(0));

        // Test empty json array.
        cr = client.callProcedure("JS1.insert", 11, "[]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                                  "SELECT ARRAY_ELEMENT(DOC, 0) FROM JS1 WHERE ID = 11");
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
        assertEquals(9, result.getRowCount());

        cr = client.callProcedure("NullArrayLengthProc", "arr");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(6, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(4L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(5L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(6L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(7L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(8L,result.getLong(0));
        assertTrue(result.advanceRow());
        assertEquals(9L,result.getLong(0));

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
        cr = client.callProcedure("JS1.insert", 10, "[0, 10, 100]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc", // test object not an array
                                  "SELECT ARRAY_LENGTH(DOC) FROM JS1 WHERE ID = 10");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(3L,result.getLong(0));

        // Test empty json array.
        cr = client.callProcedure("JS1.insert", 11, "[]");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                                  "SELECT ARRAY_LENGTH(DOC) FROM JS1 WHERE ID = 11");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(0L,result.getLong(0));
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
            assertTrue((ex.getMessage().contains("Error compiling query")));
            assertTrue((ex.getMessage().contains("PlanningErrorException")));
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
                "DESC VARCHAR(30), " +
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
