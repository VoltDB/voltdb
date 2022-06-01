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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.Stream;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;
import org.voltdb_testprocs.regressionsuites.failureprocs.BadParamTypesForTimestamp;
import org.voltdb_testprocs.regressionsuites.fixedsql.GotBadParamCountsInJava;

import com.google_voltpatches.common.base.Strings;


/**
 * Tests for SQL that was recently (early 2012) unsupported.
 */

public class TestFunctionsForVoltDBSuite extends RegressionSuite {
    private static long GREGORIAN_EPOCH = -12212553600000000L;
    private static long NYE9999         = 253402300799999999L;
    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestFunctionsForVoltDBSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestFunctionsForVoltDBSuite.class);
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

                "CREATE TABLE P3DIFF ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "TM1 TIMESTAMP DEFAULT NULL, " +
                "TM2 TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
                "PARTITION TABLE P3DIFF ON COLUMN ID;\n" +

                "CREATE TABLE P3SLICE ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "TM1 TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
                "PARTITION TABLE P3SLICE ON COLUMN ID;\n" +

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

                "CREATE INDEX R3_IDX_HEX ON R3 (hex(big));" +
                "CREATE INDEX R3_IDX_bit_shift_left ON R3 (bit_shift_left(big, 3));" +
                "CREATE INDEX R3_IDX_bit_shift_right ON R3 (bit_shift_right(big, 3));" +

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

                "create table M1 MIGRATE TO TARGET archiver (" +
                "ts timestamp default now() not null," +
                "a int not null, " +
                "b int not null) " +
                "USING TTL 10 minutes ON COLUMN ts;" +

                "create table M2 MIGRATE TO TARGET archiver (" +
                "ts timestamp default now() not null," +
                "a int not null, " +
                "b int not null) " +
                "USING TTL 10 minutes ON COLUMN ts;" +
                "PARTITION TABLE M2 ON COLUMN a;\n" +

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

                "CREATE INDEX ENG7792_UNUSED_INDEX_USES_CONCAT ON P3_INLINE_DESC (CONCAT(DESC, DESC2))" +
                ";\n" +

                "CREATE TABLE BINARYTEST (ID INTEGER, bdata varbinary(256), PRIMARY KEY(ID));" +
                // DDL for testing internet address functions
                //
                // INET4_TEST and INET6_TEST will have a bunch of rows, each
                // with a presentation and binary version.  We will test
                // that the translation from one equals the other and
                // that the other equals the translation to the one.
                "CREATE TABLE INET4_TEST ( " +
                "    PRES         VARCHAR, " +
                "    BIN          BIGINT " +
                ");" +
                "CREATE TABLE INET4_TEST_PPRES ( " +
                "    PRES         VARCHAR NOT NULL, " +
                "    BIN          BIGINT " +
                ");" +
                "PARTITION TABLE INET4_TEST_PPRES ON COLUMN PRES;" +
                "CREATE TABLE INET6_TEST ( " +
                "    PRES         VARCHAR, " +
                "    BIN          VARBINARY " +
                ");" +
                "CREATE TABLE INET6_TEST_PPRES ( " +
                "    PRES         VARCHAR NOT NULL, " +
                "    BIN          VARBINARY " +
                ");" +
                "PARTITION TABLE INET6_TEST_PPRES ON COLUMN PRES;" +
                "";
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            fail();
        }

        // load the procedures for the test
        loadProcedures(project);

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

    static private void loadProcedures(VoltProjectBuilder project) {
        // Test DECODE
        project.addStmtProcedure("DECODE", "select desc,  DECODE (desc,'IBM','zheng'," +
                "'Microsoft', 'li', 'Hewlett Packard', 'at', 'Gateway','VoltDB', 'where') from P1 where id = ?");
        project.addStmtProcedure("DECODEND", "select desc,  DECODE (desc,'zheng','a') from P1 where id = ?");
        project.addStmtProcedure("DECODEVERYLONG", "select desc,  DECODE (desc," +
                Strings.repeat("'a','a',", 12) + "'where') from P1 where id = ?");
        project.addStmtProcedure("DECODE_PARAM_INFER_STRING", "select desc,  DECODE (desc,?,?,desc) from P1 where id = ?");
        project.addStmtProcedure("DECODE_PARAM_INFER_INT", "select desc,  DECODE (id,?,?,id) from P1 where id = ?");
        project.addStmtProcedure("DECODE_PARAM_INFER_DEFAULT", "select desc,  DECODE (?,?,?,?) from P1 where id = ?");
        project.addStmtProcedure("DECODE_PARAM_INFER_CONFLICTING", "select desc,  DECODE (id,1,?,2,99,'贾鑫') from P1 where id = ?");
        // Test OCTET_LENGTH
        project.addStmtProcedure("OCTET_LENGTH", "select desc,  OCTET_LENGTH (desc) from P1 where id = ?");
        // Test POSITION and CHAR_LENGTH
        project.addStmtProcedure("POSITION", "select desc, POSITION (? IN desc) from P1 where id = ?");
        project.addStmtProcedure("CHAR_LENGTH", "select desc, CHAR_LENGTH (desc) from P1 where id = ?");
        project.addStmtProcedure("CHAR_LGTH_PARAM", "select desc, CHAR_LENGTH (?) from P1 where id = ?");
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
        // Test MIN_VALID_TIMESTAMP and MAX_VALID_TIMESTAMP
        project.addStmtProcedure("GET_MAX_VALID_TIMESTAMP", "select MAX_VALID_TIMESTAMP(), MAX_VALID_TIMESTAMP from P2 limit 1");
        project.addStmtProcedure("GET_MIN_VALID_TIMESTAMP", "select MIN_VALID_TIMESTAMP(), MIN_VALID_TIMESTAMP from P2 limit 1");
        // Test IS_VALID_TIMESTAMP
        project.addStmtProcedure("TEST_IS_VALID_TIMESTAMP", "select ID from P2 where ID = ? and IS_VALID_TIMESTAMP(TM)");
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

        project.addStmtProcedure("BITWISE_SHIFT_PARAM_1",
                "select BIT_SHIFT_LEFT(?, BIG), BIT_SHIFT_RIGHT(?, BIG) from R3 where id = ?");
        project.addStmtProcedure("BITWISE_SHIFT_PARAM_2",
                "select BIT_SHIFT_LEFT(BIG, ?), BIT_SHIFT_RIGHT(BIG, ?) from R3 where id = ?");

        project.addStmtProcedure("FORMAT_TIMESTAMP", "select FORMAT_TIMESTAMP (TM, ?) from P2 where id = ?");

        project.addProcedure(GotBadParamCountsInJava.class);
        project.addProcedure(BadParamTypesForTimestamp.class);
    }

    @Test
    public void testExplicitErrorUDF() throws Exception {
        System.out.println("STARTING testExplicitErrorUDF");
        Client client = getClient();
        ProcedureCallback callback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) {
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
        ClientResponse cr;

        // Exercise basic syntax without runtime invocation.
        cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123) from P1 where ID = 0");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select SQL_ERROR('abc') from P1 where ID = 0");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select SQL_ERROR(123, 'abc') from P1 where ID = 0");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        // negative tests
        verifyStmtFails(client, "select SQL_ERROR(123, 'abc') from P1", "abc");
        verifyStmtFails(client, "select SQL_ERROR('abc') from P1", "abc");
        verifyStmtFails(client, "select SQL_ERROR(123, 123) from P1", ".*SQL ERROR.*VARCHAR.*");

        verifyStmtFails(client, "select SQL_ERROR(123.5) from P1", "Type DECIMAL can't be cast as BIGINT");
        verifyStmtFails(client, "select SQL_ERROR(123.5E-2) from P1", "Type FLOAT can't be cast as BIGINT");
    }

    @Test
    public void testOctetLength() throws IOException, ProcCallException {
        System.out.println("STARTING OCTET_LENGTH");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        client.callProcedure("P1.insert", 1, "贾鑫Vo", 10, 1.1);
        client.callProcedure("P1.insert", 2, "Xin", 10, 1.1);
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

        // octet length on varbinary
        cr = client.callProcedure("BINARYTEST.insert", 1, new byte[] {'x', 'i', 'n'});
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                "select bdata, OCTET_LENGTH(bdata) from BINARYTEST where ID=1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(3, result.getLong(1));
    }

    // this test is put here instead of TestFunctionSuite, because HSQL uses
    // a different null case standard with standard sql
    @Test
    public void testPosition() throws IOException, ProcCallException {
        System.out.println("STARTING Position");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        client.callProcedure("P1.insert", 1, "贾鑫Vo", 10, 1.1);
        client.callProcedure("P1.insert", 2, "Xin@Volt", 10, 1.1);
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
    @Test
    public void testCharLength() throws IOException, ProcCallException {
        System.out.println("STARTING Char length");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        client.callProcedure("P1.insert", 1, "贾鑫Vo", 10, 1.1);
        client.callProcedure("P1.insert", 2, "Xin@Volt", 10, 1.1);
        client.callProcedure("P1.insert", 3, "क्षीण", 10, 1.1);
        cr = client.callProcedure("P1.insert", 4, null, 10, 1.1);

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

        cr = client.callProcedure("CHAR_LENGTH", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(5, result.getLong(1));

        cr = client.callProcedure("CHAR_LGTH_PARAM", "क्षीण", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(5, result.getLong(1));

        // null case
        cr = client.callProcedure("CHAR_LENGTH", 4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(VoltType.NULL_BIGINT,result.getLong(1));

     // try char_length on incompatible data type
        try {
            client.callProcedure("@AdHoc",
                    "select bdata, CHAR_LENGTH(bdata) from BINARYTEST where ID = 1");
            fail("char_length on columns which are not string expression is not supported");
        } catch (ProcCallException pce) {
            assertTrue(pce.getMessage().contains(
                    USING_CALCITE ?
                            "Cannot apply 'CHAR_LENGTH' to arguments of type 'CHAR_LENGTH(<VARBINARY(256)>)'. Supported form(s): 'CHAR_LENGTH(<CHARACTER>)'" :
                            "incompatible data type in operation"));
        }
    }

    @Test
    public void subtestDECODE() throws IOException, ProcCallException {
        System.out.println("STARTING DECODE");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        client.callProcedure("@AdHoc", "Delete from P1;");
        client.callProcedure("P1.insert", 1, "IBM", 10, 1.1);
        client.callProcedure("P1.insert", 2, "Microsoft", 10, 1.1);
        client.callProcedure("P1.insert", 3, "Hewlett Packard", 10, 1.1);
        client.callProcedure("P1.insert", 4, "Gateway", 10, 1.1);
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
        cr = client.callProcedure("DECODE_PARAM_INFER_DEFAULT",
                "Gateway", "Gateway", "You got it!", "You ain't got it!", 4);
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

    @Test
    public void testDECODENoDefault() throws IOException, ProcCallException {
        System.out.println("STARTING DECODE No Default");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        client.callProcedure("@AdHoc", "Delete from P1;");
        client.callProcedure("P1.insert", 1, "zheng", 10, 1.1);
        client.callProcedure("P1.insert", 2, "li", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // null case
        cr = client.callProcedure("DECODEND", 3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertNull(result.getString(1));
    }

    @Test
    public void testDECODEVeryLong() throws IOException, ProcCallException {
        System.out.println("STARTING DECODE Exceed Limit");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        client.callProcedure("@AdHoc", "Delete from P1;");
        client.callProcedure("P1.insert", 1, "zheng", 10, 1.1);
        client.callProcedure("P1.insert", 2, "li", 10, 1.1);
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

    @Test
    public void testDECODEInlineVarcharColumn_ENG5078() throws IOException, ProcCallException {
        System.out.println("STARTING DECODE inline varchar column pass-through");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        client.callProcedure("@AdHoc", "Delete from P3_INLINE_DESC;");
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

    @Test
    public void testDECODEAsInput() throws IOException, ProcCallException {
        System.out.println("STARTING DECODE No Default");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        client.callProcedure("@AdHoc", "Delete from P1;");
        client.callProcedure("P1.insert", 1, "zheng", 10, 1.1);
        client.callProcedure("P1.insert", 2, "li", 10, 1.1);
        cr = client.callProcedure("P1.insert", 3, null, 10, 1.1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // use DECODE as string input to operator
        cr = client.callProcedure("@AdHoc",
                "select desc || DECODE(id, 1, ' is the 1', ' is not the 1') from P1 where id = 2");
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
        cr = client.callProcedure("@AdHoc",
                "select id + DECODE(id, 2, 0, 'incompatible') from P1 where id = 2");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(2,result.getLong(0));

        // use DECODE as integer input to operator, with used incompatible option
        try {
            client.callProcedure("@AdHoc",
                    "select id + DECODE(id, 1, 0, 'incompatible') from P1 where id = 2");
            fail("failed to except incompatible option");
        } catch (ProcCallException pce) {
            String message = pce.getMessage();
            // It's about that string argument to the addition operator.
            assertTrue(message, message.contains("VARCHAR"));
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
                            Double.parseDouble(expected[i]) - Double.parseDouble(result.getString(i))) < 0.00001);
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

    @Test
    public void testDECODEWithNULL() throws IOException, ProcCallException {
        System.out.println("STARTING DECODE with NULL");
        Client client = getClient();
        ClientResponse cr;

        client.callProcedure("@AdHoc", "Delete from R3;");
        client.callProcedure("R3.insert", 1, 1, 1, 1, 1, 1.1, "2013-07-18 02:00:00.123457", "IBM", 1);
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


        client.callProcedure("P2.insert", 1, new Timestamp(1000L));
        client.callProcedure("P2.insert", 2, null);
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
        assertEquals(1, cr.getResults()[0].getRowCount());
        assertTrue(cr.getResults()[0].advanceRow());
        assertEquals(Integer.MIN_VALUE, cr.getResults()[0].getLong(0));

        verifyStmtFails(client, "select DECODE(tiny, 4, 5, NULL, 'tiny null', tiny)  from R3 where id = 2",
                "Could not convert to number");
    }

    @Test
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
        cr = client.callProcedure("@AdHoc",
                "select SINCE_EPOCH (SECOND, TM), TM from P2 where id = 4");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1371808830L, result.getLong(0));
        assertEquals(1371808830000000L, result.getTimestampAsLong(1));

        // Test constants timestamp with string
        cr = client.callProcedure("@AdHoc",
                "select TM, TO_TIMESTAMP(MICROS, SINCE_EPOCH (MICROS, '2013-07-18 02:00:00.123457') ) from P2 where id = 5");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(result.getTimestampAsLong(0), result.getTimestampAsLong(1));

        // Test user error input, Only accept JDBC's timestamp format: YYYY-MM-DD-SS.sss.
        try {
            client.callProcedure("@AdHoc",
                    "select SINCE_EPOCH (MICROS, 'I am a timestamp')  from P2 where id = 5");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("SQL error while compiling query"));
            assertTrue(ex.getMessage().contains("incompatible data type in conversion"));
        }

        String[] procedures = {"SINCE_EPOCH_SECOND", "SINCE_EPOCH_MILLIS", "SINCE_EPOCH_MILLISECOND",
                "SINCE_EPOCH_MICROS", "SINCE_EPOCH_MICROSECOND"};

        for (String proc : procedures) {
            cr = client.callProcedure(proc, 0);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            assertEquals(0, result.getLong(0));

            cr = client.callProcedure(proc, 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            switch (proc) {
                case "SINCE_EPOCH_SECOND":
                    assertEquals(0, result.getLong(0));
                    break;
                case "SINCE_EPOCH_MILLIS":
                case "SINCE_EPOCH_MILLISECOND":
                    assertEquals(1, result.getLong(0));
                    break;
                case "SINCE_EPOCH_MICROS":
                case "SINCE_EPOCH_MICROSECOND":
                    assertEquals(1000, result.getLong(0));
                    break;
                default:
                    fail();
                    break;
            }

            cr = client.callProcedure(proc, 2);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            switch (proc) {
                case "SINCE_EPOCH_SECOND":
                    assertEquals(1, result.getLong(0));
                    break;
                case "SINCE_EPOCH_MILLIS":
                case "SINCE_EPOCH_MILLISECOND":
                    assertEquals(1000, result.getLong(0));
                    break;
                case "SINCE_EPOCH_MICROS":
                case "SINCE_EPOCH_MICROSECOND":
                    assertEquals(1000000, result.getLong(0));
                    break;
                default:
                    fail();
                    break;
            }

            cr = client.callProcedure(proc, 3);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            switch (proc) {
                case "SINCE_EPOCH_SECOND":
                    assertEquals(-1, result.getLong(0));
                    break;
                case "SINCE_EPOCH_MILLIS":
                case "SINCE_EPOCH_MILLISECOND":
                    assertEquals(-1000, result.getLong(0));
                    break;
                case "SINCE_EPOCH_MICROS":
                case "SINCE_EPOCH_MICROSECOND":
                    assertEquals(-1000000, result.getLong(0));
                    break;
                default:
                    fail();
                    break;
            }

            cr = client.callProcedure(proc, 4);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            switch (proc) {
                case "SINCE_EPOCH_SECOND":
                    assertEquals(1371808830L, result.getLong(0));
                    break;
                case "SINCE_EPOCH_MILLIS":
                case "SINCE_EPOCH_MILLISECOND":
                    assertEquals(1371808830000L, result.getLong(0));
                    break;
                case "SINCE_EPOCH_MICROS":
                case "SINCE_EPOCH_MICROSECOND":
                    assertEquals(1371808830000000L, result.getLong(0));
                    break;
                default:
                    fail();
                    break;
            }
        }
    }

    @Test
    public void testENG6861() throws Exception {
        System.out.println("STARTING testENG6861");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        // Test user-found anomaly around complex filter using functions
        client.callProcedure("@Explain",
                "SELECT TOP ? * FROM PAULTEST WHERE NAME IS NOT NULL AND " +
                        "  (LOCK_TIME IS NULL OR SINCE_EPOCH(MILLIS,CURRENT_TIMESTAMP)-? < SINCE_EPOCH(MILLIS,LOCK_TIME));",
                10, 5000);

        cr = client.callProcedure("@AdHoc",
                "SELECT TOP ? * FROM PAULTEST WHERE NAME IS NOT NULL AND " +
                        " (LOCK_TIME IS NULL OR SINCE_EPOCH(MILLIS,CURRENT_TIMESTAMP)-? < SINCE_EPOCH(MILLIS,LOCK_TIME));",
                10, 10000);
        result = cr.getResults()[0];
        assertEquals(0, result.getRowCount());

        cr = client.callProcedure("GOT_BAD_PARAM_COUNTS_INLINE", 10, 10000);
        result = cr.getResults()[0];
        assertEquals(0, result.getRowCount());

        cr = client.callProcedure("GotBadParamCountsInJava", 10, 10000);
        result = cr.getResults()[0];
        assertEquals(0, result.getRowCount());

        try {
            // Purposely neglecting to list an select columns or '*'.
            client.callProcedure("@Explain",
                    "SELECT TOP ? FROM PAULTEST WHERE NAME IS NOT NULL AND " +
                            "(LOCK_TIME IS NULL OR SINCE_EPOCH(MILLIS,CURRENT_TIMESTAMP)-? < SINCE_EPOCH(MILLIS,LOCK_TIME));");
            fail("Expected to detect missing SELECT columns");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("SQL error while compiling query"));
            assertTrue(ex.getMessage().contains("unexpected token: FROM"));
        }
    }

    @Test
    public void testTO_TIMESTAMP() throws IOException, ProcCallException {
        System.out.println("STARTING TO_TIMESTAMP");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;

        client.callProcedure("P2.insert", 0, new Timestamp(0L));
        client.callProcedure("P2.insert", 1, new Timestamp(1L));
        client.callProcedure("P2.insert", 2, new Timestamp(1000L));
        client.callProcedure("P2.insert", 3, new Timestamp(-1000L));

        // Test AdHoc
        cr = client.callProcedure("@AdHoc",
                "select to_timestamp(second, 1372640523) from P2 limit 1");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        assertEquals(1372640523 * 1000000L, result.getTimestampAsLong(0));

        // Test string input number, expect error
        try {
            client.callProcedure("@AdHoc",
                    "select to_timestamp(second, '1372640523') from P2 limit 1");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("SQL error while compiling query"));
            assertTrue(ex.getMessage().contains("incompatible data type"));
        }

        String[] procedures = {"FROM_UNIXTIME", "TO_TIMESTAMP_SECOND", "TO_TIMESTAMP_MILLIS",
                "TO_TIMESTAMP_MILLISECOND", "TO_TIMESTAMP_MICROS", "TO_TIMESTAMP_MICROSECOND"};

        for (String proc : procedures) {
            cr = client.callProcedure(proc, 0L , 0);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            switch (proc) {
                case "TO_TIMESTAMP_SECOND":
                case "FROM_UNIXTIME":
                case "TO_TIMESTAMP_MILLIS":
                case "TO_TIMESTAMP_MILLISECOND":
                case "TO_TIMESTAMP_MICROS":
                case "TO_TIMESTAMP_MICROSECOND":
                    assertEquals(0L, result.getTimestampAsLong(0));
                    break;
                default:
                    fail();
            }

            cr = client.callProcedure(proc, 1L , 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            switch (proc) {
                case "TO_TIMESTAMP_SECOND":
                case "FROM_UNIXTIME":
                    assertEquals(1000000L, result.getTimestampAsLong(0));
                    break;
                case "TO_TIMESTAMP_MILLIS":
                case "TO_TIMESTAMP_MILLISECOND":
                    assertEquals(1000L, result.getTimestampAsLong(0));
                    break;
                case "TO_TIMESTAMP_MICROS":
                case "TO_TIMESTAMP_MICROSECOND":
                    assertEquals(1L, result.getTimestampAsLong(0));
                    break;
                default:
                    fail();
                    break;
            }

            cr = client.callProcedure(proc, 1000L , 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            switch (proc) {
                case "TO_TIMESTAMP_SECOND":
                case "FROM_UNIXTIME":
                    assertEquals(1000000000L, result.getTimestampAsLong(0));
                    break;
                case "TO_TIMESTAMP_MILLIS":
                case "TO_TIMESTAMP_MILLISECOND":
                    assertEquals(1000000L, result.getTimestampAsLong(0));
                    break;
                case "TO_TIMESTAMP_MICROS":
                case "TO_TIMESTAMP_MICROSECOND":
                    assertEquals(1000L, result.getTimestampAsLong(0));
                    break;
                default:
                    fail();
                    break;
            }

            cr = client.callProcedure(proc, -1000 , 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            switch (proc) {
                case "TO_TIMESTAMP_SECOND":
                case "FROM_UNIXTIME":
                    assertEquals(-1000000000L, result.getTimestampAsLong(0));
                    break;
                case "TO_TIMESTAMP_MILLIS":
                case "TO_TIMESTAMP_MILLISECOND":
                    assertEquals(-1000000L, result.getTimestampAsLong(0));
                    break;
                case "TO_TIMESTAMP_MICROS":
                case "TO_TIMESTAMP_MICROSECOND":
                    assertEquals(-1000L, result.getTimestampAsLong(0));
                    break;
                default:
                    fail();
                    break;
            }

            final long maxSec = GREGORIAN_EPOCH / 1000000;
            cr = client.callProcedure(proc, maxSec, 1);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            switch (proc) {
                case "TO_TIMESTAMP_SECOND":
                case "FROM_UNIXTIME":
                    assertEquals(maxSec * 1000000, result.getTimestampAsLong(0));
                    break;
                case "TO_TIMESTAMP_MILLIS":
                case "TO_TIMESTAMP_MILLISECOND":
                    assertEquals(maxSec * 1000, result.getTimestampAsLong(0));
                    break;
                case "TO_TIMESTAMP_MICROS":
                case "TO_TIMESTAMP_MICROSECOND":
                    assertEquals(maxSec, result.getTimestampAsLong(0));
                    break;
                default:
                    fail();
                    break;
            }
        }
    }

    @Test
    public void testTRUNCATE() throws Exception {
        System.out.println("STARTING TRUNCATE with timestamp");
        Client client = getClient();
        ClientResponse cr;
        VoltTable result;
        VoltDB.setDefaultTimezone();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date time;

        // Test Standard TRUNCATE function for floating numbers
        Exception ex = null;
        try {
            client.callProcedure("@AdHoc", "select TRUNCATE (1.2, 1), TM from P2 where id = 0");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            ex = e;
        } finally {
            assertNotNull(ex);
            assertTrue((ex.getMessage().contains("SQL error while compiling query")));
            assertTrue((ex.getMessage().contains("TRUNCATE")));
        }

        // Test date before Gregorian calendar beginning.
        cr = client.callProcedure("P2.insert", 0, Timestamp.valueOf("1582-03-06 13:56:40.123456"));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        ex = null;
        try {
            client.callProcedure("TRUNCATE", 0);
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

    @Test
    public void testFunctionsWithInvalidJSON() throws Exception {
        Client client = getClient();
        ClientResponse cr;

        cr = client.callProcedure("JSBAD.insert", 1, // OOPS. skipped comma before "bool"
                "{\"id\":1 \"bool\": false}");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("JSBAD.insert", 2, // OOPS. semi-colon in place of colon before "bool"
                "{\"id\":2, \"bool\"; false, \"贾鑫Vo\":\"分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません分かりません\"}"
                );
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        final String jsTrailingCommaArray = "[ 0, 100, ]"; // OOPS. trailing comma in array
        final String jsWithTrailingCommaArray = "{\"id\":3, \"trailer\":" + jsTrailingCommaArray + "}";

        cr = client.callProcedure("JSBAD.insert", 3, jsWithTrailingCommaArray);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("JSBAD.insert", 4, jsTrailingCommaArray);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());


        String[] jsonProcs = {"BadIdFieldProc", "BadIdArrayProc", "BadIdArrayLengthProc"};

        for (String procname : jsonProcs) {
            try {
                client.callProcedure(procname, 1, "id", "1");
                fail("document validity check failed for " + procname);
            } catch(ProcCallException pcex) {
                assertTrue(pcex.getMessage().contains("Invalid JSON * Line 1, Column 9"));
            }

            try {
                client.callProcedure(procname, 2, "id", "2");
                fail("document validity check failed for " + procname);
            } catch(ProcCallException pcex) {
                assertTrue(pcex.getMessage().contains("Invalid JSON * Line 1, Column 16"));
            }

            try {
                client.callProcedure(procname, 3, "id", "3");
                fail("document validity check failed for " + procname);
            } catch(ProcCallException pcex) {
                assertTrue(pcex.getMessage().contains("Invalid JSON * Line 1, Column 30"));
            }

            try {
                client.callProcedure(procname, 4, "id", "4");
                fail("document validity check failed for " + procname);
            } catch(ProcCallException pcex) {
                assertTrue(pcex.getMessage().contains("Invalid JSON * Line 1, Column 11"));
            }
        }
    }

    @Test
    public void testFormatCurrency() throws Exception {
        System.out.println("STARTING testFormatCurrency");
        Client client = getClient();
        ClientResponse cr;
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

        cr = client.callProcedure("@AdHoc",
                "select FORMAT_CURRENCY(DEC, 1), FORMAT_CURRENCY(DEC, 2), FORMAT_CURRENCY(DEC, 3), " +
                        "FORMAT_CURRENCY(DEC, 4), FORMAT_CURRENCY(DEC, 0), FORMAT_CURRENCY(DEC, -1), " +
                        "FORMAT_CURRENCY(DEC, -2), FORMAT_CURRENCY(DEC, -3) from D1 where id = 1");
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

        cr = client.callProcedure("@AdHoc",
                "select FORMAT_CURRENCY(DEC, 1), FORMAT_CURRENCY(DEC, 2), FORMAT_CURRENCY(DEC, 3), " +
                        "FORMAT_CURRENCY(DEC, 4), FORMAT_CURRENCY(DEC, 0), FORMAT_CURRENCY(DEC, -1), " +
                        "FORMAT_CURRENCY(DEC, -2), FORMAT_CURRENCY(DEC, -3) from D1 where id = 2");
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

        cr = client.callProcedure("@AdHoc",
                "select FORMAT_CURRENCY(DEC, 1), FORMAT_CURRENCY(DEC, 2), FORMAT_CURRENCY(DEC, 3), " +
                        "FORMAT_CURRENCY(DEC, 4), FORMAT_CURRENCY(DEC, 0), FORMAT_CURRENCY(DEC, -1),"
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
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(dec, 2) from D1 where id = 8");
            fail("range validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("out of range"));
        }
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(dec, 2) from D1 where id = 9");
            fail("range validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("out of range"));
        }

        // check invalid type
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(id, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(tiny, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(small, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(num, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(big, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(ratio, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("can't be cast as DECIMAL"));
        }
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(tm, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("incompatible data type in operation"));
        }
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(var, 2) from R3 where id = 1");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex) {
            assertTrue(pcex.getMessage().contains("incompatible data type in operation"));
        }

        String[] s = {"1,000,000.00", "100,000.00", "10,000.00", "1,000.00", "100.00", "10.00", "1.00", "0.10", "0.01", "0.00"};
        for (int i = 0; i < 10; i++){
            cr = client.callProcedure("@AdHoc",
                    "select FORMAT_CURRENCY(CAST("+ Math.pow(10, 6-i) +" as DECIMAL), 2) from D1 where id = 1");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, s[i]);
        }
        for (int i = 0; i < 10; i++){
            cr = client.callProcedure("@AdHoc",
                    "select FORMAT_CURRENCY(CAST("+ -Math.pow(10, 6-i) +" as DECIMAL), 2) from D1 where id = 1");
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
            cr = client.callProcedure("@AdHoc",
                    "select FORMAT_CURRENCY(DEC, "+i+") from D1 where id = 12");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, s2[11-i]);
        }
        // it will go out of the range of int64_t
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, -19) from D1 where id = 12");
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
        String[] s3 ={"8,223,372,036,854,775,807.12345678918","8,223,372,036,854,775,807.1234567892","8,223,372,036,854,775,807.123456789",
                "8,223,372,036,854,775,807.12345679","8,223,372,036,854,775,807.1234568","8,223,372,036,854,775,807.123457",
                "8,223,372,036,854,775,807.12346","8,223,372,036,854,775,807.1235","8,223,372,036,854,775,807.123","8,223,372,036,854,775,807.12",
                "8,223,372,036,854,775,807.1","8,223,372,036,854,775,807","8,223,372,036,854,775,810","8,223,372,036,854,775,800",
                "8,223,372,036,854,776,000","8,223,372,036,854,780,000","8,223,372,036,854,800,000","8,223,372,036,855,000,000",
                "8,223,372,036,850,000,000","8,223,372,036,900,000,000","8,223,372,037,000,000,000","8,223,372,040,000,000,000","8,223,372,000,000,000,000",
                "8,223,372,000,000,000,000","8,223,370,000,000,000,000","8,223,400,000,000,000,000","8,223,000,000,000,000,000","8,220,000,000,000,000,000",
                "8,200,000,000,000,000,000","8,000,000,000,000,000,000","not used","0","0","0","0","0","0"};
        for (int i=11; i > -19; i--){
            cr = client.callProcedure("@AdHoc",
                    "select FORMAT_CURRENCY(DEC, "+i+") from D1 where id = 13");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, s3[11-i]);
        }
        // it will go out of the range of int64_t
        try {
            client.callProcedure("@AdHoc", "select FORMAT_CURRENCY(DEC, -19) from D1 where id = 13");
            fail("type validity check failed for FORMAT_CURRENCY");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("out of range"));
        }
        // now it is zero
        for (int i=-20; i >= -25; i--){
            cr = client.callProcedure("@AdHoc",
                    "select FORMAT_CURRENCY(DEC, "+i+") from D1 where id = 13");
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getString(0);
            assertEquals(str, s3[11-i]);
        }

        // check the validity of the second parameter
        verifyStmtFails(client, "select FORMAT_CURRENCY(DEC, 15) from D1 where id = 0",
                "the second parameter");
        verifyStmtFails(client, "select FORMAT_CURRENCY(DEC, -26) from D1 where id = 0",
                "the second parameter");
    }

    @Test
    public void testFunc_Str() throws Exception {
        System.out.println("STARTING testFunc_Str");
        Client client = getClient();
        ClientResponse cr;
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
        cr = client.callProcedure("R3.insert",
                1, 1, 1, 1, 1, 1.1, "2013-07-18 02:00:00.123457", "IBM", 1);
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);

        cr = client.callProcedure("@AdHoc", "select STR(DEC, 12, 1), STR(DEC, 12, 2), "
                + "STR(DEC, 12, 3), STR(DEC, 12, 4), STR(DEC, 12, 0) from D1 where id = 0");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getString(0);
        assertEquals(str, "123456.6");    // rounding down
        str = result.getString(1);
        assertEquals(str, "123456.65");   // rounding up
        str = result.getString(2);
        assertEquals(str, "123456.646");
        str = result.getString(3);
        assertEquals(str, "123456.6456"); // banker's rounding: half to nearest even when previous digit is even
        // rounding to none-positive places, or say the whole part
        str = result.getString(4);
        assertEquals(str, "123457");      // rounding up

        cr = client.callProcedure("@AdHoc", "select STR(DEC,15, 1), STR(DEC, 10, 2),"
                + "STR(DEC,10), STR(DEC) from D1 where id = 2");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getString(0);
        assertEquals(str, "1123456785.6");
        str = result.getString(1);
        assertEquals(str, "**********");
        str = result.getString(2);
        assertEquals(str, "1123456786");
        str = result.getString(3);
        assertEquals(str, "1123456786");

        // it will go out of the range of int64_t
        verifyStmtFails(client,
                "select STR(DEC, -19) from D1 where id = 12", "the second parameter should be <= 38 and > 0");
    }

    @Test
    public void testRound() throws Exception {
        System.out.println("STARTING testRound");
        Client client = getClient();
        ClientResponse cr = null;
        VoltTable result;
        BigDecimal str;

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

        cr = client.callProcedure("@AdHoc",
                "select ROUND(DEC, 1), ROUND(DEC, 2), ROUND(DEC, 3), ROUND(DEC, 4), ROUND(DEC, 0), " +
                        "ROUND(DEC, -1), ROUND(DEC, -2), ROUND(DEC, -3) from D1 where id = 0");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getDecimalAsBigDecimal(0);
        assertEquals(str.toString(), "123456.600000000000");    // rounding down
        str = result.getDecimalAsBigDecimal(1);
        assertEquals(str.toString(), "123456.650000000000");   // rounding up
        str = result.getDecimalAsBigDecimal(2);
        assertEquals(str.toString(), "123456.646000000000");
        str = result.getDecimalAsBigDecimal(3);
        assertEquals(str.toString(), "123456.645600000000"); // banker's rounding: half to nearest even when previous digit is even
        // rounding to none-positive places, or say the whole part
        str = result.getDecimalAsBigDecimal(4);
        assertEquals(str.toString(), "123457.000000000000");      // rounding up
        str = result.getDecimalAsBigDecimal(5);
        assertEquals(str.toString(), "123460.000000000000");
        str = result.getDecimalAsBigDecimal(6);
        assertEquals(str.toString(), "123500.000000000000");
        str = result.getDecimalAsBigDecimal(7);
        assertEquals(str.toString(), "123000.000000000000");      // rounding down


        cr = client.callProcedure("@AdHoc",
                "select ROUND(DEC, 1), ROUND(DEC, 2), ROUND(DEC, 3), ROUND(DEC, 4), " +
                        "ROUND(DEC, 0), ROUND(DEC, -1), ROUND(DEC, -2), ROUND(DEC, -3) from D1 where id = 1");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getDecimalAsBigDecimal(0);
        assertEquals(str.toString(), "-123456.600000000000");    // rounding down
        str = result.getDecimalAsBigDecimal(1);
        assertEquals(str.toString(), "-123456.650000000000");   // rounding up
        str = result.getDecimalAsBigDecimal(2);
        assertEquals(str.toString(), "-123456.646000000000");
        str = result.getDecimalAsBigDecimal(3);
        assertEquals(str.toString(), "-123456.645600000000"); //banker's rounding: half to nearest even when previous digit is even
        // rounding to none-positive places, or say the whole part
        str = result.getDecimalAsBigDecimal(4);
        assertEquals(str.toString(), "-123457.000000000000");      // rounding up
        str = result.getDecimalAsBigDecimal(5);
        assertEquals(str.toString(), "-123460.000000000000");
        str = result.getDecimalAsBigDecimal(6);
        assertEquals(str.toString(), "-123500.000000000000");
        str = result.getDecimalAsBigDecimal(7);
        assertEquals(str.toString(), "-123000.000000000000");      // rounding down

        cr = client.callProcedure("@AdHoc",
                "select ROUND(DEC, 1), ROUND(DEC, 2), ROUND(DEC, 3), ROUND(DEC, 4), " +
                        "ROUND(DEC, 0), ROUND(DEC, -1), ROUND(DEC, -2), ROUND(DEC, -3) from D1 where id = 2");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getDecimalAsBigDecimal(0);
        assertEquals(str.toString(), "1123456785.600000000000");
        str = result.getDecimalAsBigDecimal(1);
        assertEquals(str.toString(), "1123456785.560000000000"); // banker's rounding: half to nearest even when previous digit is odd
        str = result.getDecimalAsBigDecimal(2);
        assertEquals(str.toString(), "1123456785.555000000000");
        str = result.getDecimalAsBigDecimal(3);
        assertEquals(str.toString(), "1123456785.555000000000"); // add trailing zero if rounding to a larger place
        // rounding to none-positive places, or say the whole part
        str = result.getDecimalAsBigDecimal(4);
        assertEquals(str.toString(), "1123456786.000000000000");
        str = result.getDecimalAsBigDecimal(5);
        assertEquals(str.toString(), "1123456790.000000000000");
        str = result.getDecimalAsBigDecimal(6);
        assertEquals(str.toString(), "1123456800.000000000000");
        str = result.getDecimalAsBigDecimal(7);
        assertEquals(str.toString(), "1123457000.000000000000");

        cr = client.callProcedure("@AdHoc",
                "select ROUND(DEC, 1), ROUND(DEC, 2), ROUND(DEC, 3), ROUND(DEC, 4), " +
                        "ROUND(DEC, 0), ROUND(DEC, -1), ROUND(DEC, -2), ROUND(DEC, -3) from D1 where id = 3");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        // rounding to positive places
        str = result.getDecimalAsBigDecimal(0);
        assertEquals(str.toString(), "-1123456785.600000000000");
        str = result.getDecimalAsBigDecimal(1);
        assertEquals(str.toString(), "-1123456785.560000000000"); // banker's rounding: half to nearest even when previous digit is odd
        str = result.getDecimalAsBigDecimal(2);
        assertEquals(str.toString(), "-1123456785.555000000000");
        str = result.getDecimalAsBigDecimal(3);
        assertEquals(str.toString(), "-1123456785.555000000000"); // add trailing zero if rounding to a larger place
        // rounding to none-positive places, or say the whole part
        str = result.getDecimalAsBigDecimal(4);
        assertEquals(str.toString(), "-1123456786.000000000000");
        str = result.getDecimalAsBigDecimal(5);
        assertEquals(str.toString(), "-1123456790.000000000000");
        str = result.getDecimalAsBigDecimal(6);
        assertEquals(str.toString(), "-1123456800.000000000000");
        str = result.getDecimalAsBigDecimal(7);
        assertEquals(str.toString(), "-1123457000.000000000000");

        cr = client.callProcedure("@AdHoc", "select ROUND(DEC, -3) from D1 where id = 10");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        str = result.getDecimalAsBigDecimal(0);
        // banker's rounding to a negative place: half to nearest even when previous digit is odd
        assertEquals(str.toString(), "2000.000000000000");

        cr = client.callProcedure("@AdHoc", "select ROUND(DEC, -3) from D1 where id = 11");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        result = cr.getResults()[0];
        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        str = result.getDecimalAsBigDecimal(0);
        // banker's rounding to a negative place: half to nearest even when previous digit is even
        assertEquals(str.toString(), "2000.000000000000");

        // zeros with different init input
        for (int i = 4; i < 8; i++) {
            cr = client.callProcedure("@AdHoc", "select ROUND(DEC, 2) from D1 where id = "+i);
            assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
            result = cr.getResults()[0];
            assertEquals(1, result.getRowCount());
            assertTrue(result.advanceRow());
            str = result.getDecimalAsBigDecimal(0);
            assertEquals(str.intValue(), 0);
        }

        // out of int64_t range
        verifyStmtFails(client, "select ROUND(dec, 2) from D1 where id = 8", "out of range");

        // check invalid type
        verifyStmtFails(client, "select ROUND(id, 2) from R3 where id = 1", "can't be cast as DECIMAL");
        verifyStmtFails(client, "select ROUND(tiny, 2) from R3 where id = 1",
                        "can't be cast as DECIMAL");
        verifyStmtFails(client, "select ROUND(small, 2) from R3 where id = 1", "can't be cast as DECIMAL");
        verifyStmtFails(client, "select ROUND(num, 2) from R3 where id = 1", "can't be cast as DECIMAL");
        verifyStmtFails(client, "select ROUND(big, 2) from R3 where id = 1", "can't be cast as DECIMAL");
        verifyStmtFails(client, "select ROUND(tm, 2) from R3 where id = 1",
                USING_CALCITE ? "Cannot apply 'ROUND' to arguments of type" : "incompatible data type in operation" );
        verifyStmtFails(client, "select ROUND(var, 2) from R3 where id = 1",
                USING_CALCITE ? "Cannot apply 'ROUND' to arguments of type" : "incompatible data type in operation" );
        verifyStmtFails(client, "select ROUND(DEC, -19) from D1 where id = 12", "out of range");
        // check the validity of the second parameter
        verifyStmtFails(client, "select ROUND(DEC, 15) from D1 where id = 0", "the second parameter");
        verifyStmtFails(client, "select ROUND(DEC, -26) from D1 where id = 0",
                "the second parameter");
    }

    @Test
    public void testConcat() throws IOException, ProcCallException {
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

    @Test
    public void testConcatMoreThan2Param() throws IOException, ProcCallException {
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
            client.callProcedure("@AdHoc", "select CONCAT('a', 'b', id) from p1 where id = 1");
        } catch (ProcCallException pcex){
            assertTrue(pcex.getMessage().contains("can't be cast as VARCHAR"));
        }
    }

    static private long[] bitnotInterestingValues = new long[] {
            // skipping Long.MIN_VALUE because it's our null value
            // (tested in testBitnotNull)
            Long.MIN_VALUE + 1, Long.MIN_VALUE + 1000, -1, 0, 1, 1000, Long.MAX_VALUE - 1
            // Long.MAX_VALUE produces Long.MIN_VALUE when bitnot'd,
            // which can represent the null value
            // (tested in testBitnotNull)
    };

    @Test
    public void testBitnot() throws Exception {
        System.out.println("STARTING test Bitnot");
        Client client = getClient();
        VoltTable result;

        int i = 0;
        for (long val : bitnotInterestingValues) {
            client.callProcedure("@AdHoc", "insert into R3(id, big) values (?, ?)", i, val);
            ++i;
        }

        for (long val : bitnotInterestingValues) {
            result = client.callProcedure("@AdHoc",
                    "select big, bitnot(big) from R3 where big = " + val).getResults()[0];
            validateRowOfLongs(result, new long[] {val, ~val});
        }

        // 2^63 is out of range
        verifyStmtFails(client,
                "select bitnot(9223372036854775808) from R3", "numeric value out of range");

        // as is -(2^63) - 1
        verifyStmtFails(client,
                "select bitnot(-9223372036854775809) from R3", "numeric value out of range");
    }

    @Test
    public void testBitnotWithParam() throws Exception {
        System.out.println("STARTING test Bitnot with a parameter");
        Client client = getClient();

        client.callProcedure("@AdHoc", "insert into R3(id) values (0)");

        for (long val : bitnotInterestingValues) {
            final VoltTable result = client.callProcedure("@AdHoc",
                    "select bitnot(?) from R3", val).getResults()[0];
            validateRowOfLongs(result, new long[] {~val});
        }
    }

    @Test
    public void testBitnotNull() throws Exception {
        System.out.println("STARTING test Bitnot with null value");
        Client client = getClient();
        VoltTable result;

        client.callProcedure("@AdHoc", "insert into R3(id, big) values (0, ?)",
                Long.MIN_VALUE); // this is really a NULL value
        client.callProcedure("@AdHoc", "insert into R3(id, big) values (1, ?)",
                Long.MAX_VALUE);

        result = client.callProcedure("@AdHoc",
                "select bitnot(big) from r3 where id = 0")
                .getResults()[0];

        result.advanceRow();

        // bitnot(null) produces null
        long val = result.getLong(0);
        assertTrue(result.wasNull());
        assertEquals(Long.MIN_VALUE, val);

        // bitnot(MAX_VALUE) produces MIN_VALUE, which would be
        // a null value, so an exception is thrown.
        verifyStmtFails(client, "select bitnot(big) from r3 where id = 1",
                "Application of bitwise function BITNOT would produce INT64_MIN, "
                + "which is reserved for SQL NULL values.");
    }

    private void bitwiseShiftChecker(long pk, long big, long param) throws IOException, ProcCallException {
        VoltTable vt;
        Client client = getClient();

        client.callProcedure("@AdHoc", String.format("insert into R3(id, big) values (%d, %d)", pk, big));

        if (big >= 0) {
            vt = client.callProcedure("BITWISE_SHIFT_PARAM_1", param, param, pk).getResults()[0];
            System.out.println(vt);
            if (big >= 64) {
                validateRowOfLongs(vt, new long[]{0, 0});
            } else {
                validateRowOfLongs(vt, new long[]{param << big, param >>> big });
            }
        }

        if (param >= 0) {
            vt = client.callProcedure("BITWISE_SHIFT_PARAM_2", param, param, pk).getResults()[0];
            System.out.println(vt);
            if (param >= 64) {
                validateRowOfLongs(vt, new long[]{0, 0});
            } else {
                validateRowOfLongs(vt, new long[]{big << param, big >>> param });
            }
        }
    }

    @Test
    public void testBitwiseShift() throws IOException, ProcCallException {
        System.out.println("STARTING test bitwise shifting tests");

        bitwiseShiftChecker(1, 1, 1); bitwiseShiftChecker(2, -1, 1);

        bitwiseShiftChecker(3, 3, 60); bitwiseShiftChecker(4, -3, 60);

        bitwiseShiftChecker(5, 3, 64); bitwiseShiftChecker(6, -3, 64);

        bitwiseShiftChecker(7, 3, 65); bitwiseShiftChecker(8, -3, 65);

        bitwiseShiftChecker(9, 3, 127); bitwiseShiftChecker(10, -3, 127);

        bitwiseShiftChecker(11, 3, 128); bitwiseShiftChecker(12, -3, 128);

        bitwiseShiftChecker(13, 3, 129); bitwiseShiftChecker(14, -3, 129);

        bitwiseShiftChecker(15, 8, 63); bitwiseShiftChecker(16, -8, 63);

        bitwiseShiftChecker(17, 8, 0); bitwiseShiftChecker(18, -8, 0);

        // Min/MAX
        bitwiseShiftChecker(50, Long.MAX_VALUE, 3);  bitwiseShiftChecker(51, 3, Long.MAX_VALUE);
        bitwiseShiftChecker(52, Long.MAX_VALUE, -3);  bitwiseShiftChecker(53, -3, Long.MAX_VALUE);
        bitwiseShiftChecker(54, Long.MIN_VALUE+1, 6);  bitwiseShiftChecker(55, 6, Long.MIN_VALUE+1);
        bitwiseShiftChecker(56, Long.MIN_VALUE+1, -6);  bitwiseShiftChecker(57, -6, Long.MIN_VALUE+1);

        try {
            bitwiseShiftChecker(19, 3, 63);
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("would produce INT64_MIN, which is reserved for SQL NULL values"));
        }

        try {
            bitwiseShiftChecker(20, -3, 63);
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("would produce INT64_MIN, which is reserved for SQL NULL values"));
        }

        Client client = getClient();
        // out of range tests
        verifyStmtFails(client, "select BIT_SHIFT_LEFT(big, 9223372036854775809) from R3;",
                "numeric value out of range");

        verifyStmtFails(client, "select BIT_SHIFT_LEFT(big, 0.5) from R3;",
                "incompatible data type in conversion");

        verifyStmtFails(client, "select BIT_SHIFT_RIGHT(3.6, 2) from R3;",
                "incompatible data type in conversion");

        // negative shifting tests
        verifyStmtFails(client, "select BIT_SHIFT_LEFT(big, -1) from R3;",
                "unsupported negative value for bit shifting");

        verifyStmtFails(client, "select BIT_SHIFT_RIGHT(big, -1) from R3;",
                "unsupported negative value for bit shifting");

        VoltTable vt;
        // NULL tests: null in null out
        client.callProcedure("@AdHoc", "insert into R3(id, big) values (100, null)");
        vt = client.callProcedure("BITWISE_SHIFT_PARAM_1", 2, 2, 100).getResults()[0];
        validateRowOfLongs(vt, new long[]{Long.MIN_VALUE, Long.MIN_VALUE });

        vt = client.callProcedure("BITWISE_SHIFT_PARAM_2", 2, 2, 100).getResults()[0];
        validateRowOfLongs(vt, new long[]{Long.MIN_VALUE, Long.MIN_VALUE });
    }

    @Test
    public void testHex() throws IOException, ProcCallException {
        System.out.println("STARTING test HEX function tests");

        Client client = getClient();
        VoltTable result = null;

        // test null: Long.MIN_VALUE is our null value
        client.callProcedure("@AdHoc", "insert into R3(id, big) values (?, ?)", 500, Long.MIN_VALUE);
        result = client.callProcedure("@AdHoc", "select hex(big) from R3 where id = 500").getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{null});

        // normal tests
        long[] hexInterestingValues = new long[] {
                Long.MIN_VALUE + 1, Long.MIN_VALUE + 1000, -1, 0, 1, 1000, Long.MAX_VALUE - 1, Long.MAX_VALUE
        };

        int i = 0;
        for (long val : hexInterestingValues) {
            client.callProcedure("@AdHoc", "insert into R3(id, big) values (?, ?)", i, val);
            ++i;
        }

        for (long val : hexInterestingValues) {
            result = client.callProcedure("@AdHoc",
                    "select hex(big) from R3 where big = " + val).getResults()[0];
            String hexString = Long.toHexString(val).toUpperCase();
            validateTableColumnOfScalarVarchar(result, new String[]{hexString});

            result = client.callProcedure("@AdHoc",
                    String.format("select hex(%d) from R3 where big = %d", val, val)).getResults()[0];
            validateTableColumnOfScalarVarchar(result, new String[]{hexString});
        }
    }

    public void testBin() throws IOException, ProcCallException {
        System.out.println("STARTING test BIN function tests");

        Client client = getClient();
        VoltTable result = null;

        // test null: Long.MIN_VALUE is our null value
        client.callProcedure("@AdHoc", "insert into R3(id, big) values (?, ?)", 500, Long.MIN_VALUE);
        result = client.callProcedure("@AdHoc", "select bin(big) from R3 where id = 500").getResults()[0];
        validateTableColumnOfScalarVarchar(result, new String[]{null});

        // normal tests
        long[] binInterestingValues = new long[] {
                Long.MIN_VALUE + 1, Long.MIN_VALUE + 1000, -1, 0, 1, 1000, Long.MAX_VALUE - 1, Long.MAX_VALUE
        };

        int i = 0;
        for (long val : binInterestingValues) {
            client.callProcedure("@AdHoc", "insert into R3(id, big) values (?, ?)", i, val);
            ++i;
        }

        for (long val : binInterestingValues) {
            result = client.callProcedure("@AdHoc",
                    "select bin(big) from R3 where big = " + val).getResults()[0];
            String binString = Long.toBinaryString(val);
            validateTableColumnOfScalarVarchar(result, new String[]{binString});

            result = client.callProcedure("@AdHoc",
                    String.format("select bin(%d) from R3 where big = %d", val, val)).getResults()[0];
            validateTableColumnOfScalarVarchar(result, new String[]{binString});
        }
    }

    private void validateIPv4Addr(Client client, String tableName, String presentation, Long binary)
            throws IOException, ProcCallException {
        ClientResponse cr;
        VoltTable vt;

        cr = client.callProcedure("@AdHoc", "TRUNCATE TABLE " + tableName + ";");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(tableName.toUpperCase() + ".INSERT", presentation, binary);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //
        // First, see if converting the binary matches the presentation.
        //
        cr = client.callProcedure("@AdHoc",
                String.format("select inet_ntoa(cast(bin as bigint)) from %s;", tableName));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(1, vt.getRowCount());
        assertEquals(1, vt.getColumnCount());
        assertTrue(vt.advanceRow());
        if (presentation != null) {
            assertEquals(presentation.toUpperCase(), vt.getString(0).toUpperCase());
        } else {
            assertNull(vt.getString(0));
            assertTrue(vt.wasNull());
        }

        // Now, test if converting the presentation matches the binary.
        cr = client.callProcedure("@AdHoc",
                String.format("select inet_aton(pres) from %s;", tableName));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(1, vt.getRowCount());
        assertEquals(1, vt.getColumnCount());
        assertTrue(vt.advanceRow());
        if (binary != null) {
            assertEquals(binary, Long.valueOf(vt.getLong(0)));
        } else {
            // Fetch the value.  It should be null.
            vt.getLong(0);
            assertTrue(vt.wasNull());
        }

        // Now test that the two are inverses one of the other.
        cr = client.callProcedure("@AdHoc",
                String.format("select inet_aton(inet_ntoa(cast(bin as bigint))) from %s;", tableName));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(1, vt.getRowCount());
        assertEquals(1, vt.getColumnCount());
        assertTrue(vt.advanceRow());
        if (binary != null) {
            assertEquals(binary, Long.valueOf(vt.getLong(0)));
        } else {
            // Fetch the value, but we don't care about it.
            // It should be NULL.
            vt.getLong(0);
            assert(vt.wasNull());
        }

        // Finally, test that the two are inverses the other of the one.
        cr = client.callProcedure("@AdHoc",
                String.format("select inet_ntoa(inet_aton(pres)) from %s;", tableName));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(1, vt.getRowCount());
        assertEquals(1, vt.getColumnCount());
        assertTrue(vt.advanceRow());
        if (presentation != null) {
            assertEquals(presentation, vt.getString(0));
        } else {
            assertNull(vt.getString(0));
            assertTrue(vt.wasNull());
        }
    }

    private void invalidIPAddr(Client client, String tableName, String presentation) throws Exception {
        ClientResponse cr = client.callProcedure("@AdHoc", "TRUNCATE TABLE " + tableName + ";");
        String ipVersion = tableName.startsWith("INET4_") ? "" : "6";
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        if (ipVersion.length() == 0) {
            cr = client.callProcedure(tableName.toUpperCase() + ".INSERT", presentation, 0x0);
        } else {
            byte[] zeros = new byte[16];
            cr = client.callProcedure(tableName.toUpperCase() + ".INSERT", presentation, zeros);
        }
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        Exception ex = null;
        try {
            String sql = String.format("select inet%s_aton(pres) from %s;", ipVersion, tableName);
            cr = client.callProcedure("@AdHoc", sql);
            fail(String.format("Expected inet address %s to fail.", presentation));
        } catch (Exception e) {
            ex = e;
        } finally {
            assertNotNull(ex);
            assertTrue("Expected a SQL ERROR here", ex.getMessage().contains("SQL ERROR"));
        }
    }

    private void assertEqualByteArrays(byte[] bytes, byte[] actual) {
        for (int idx = 0; idx < actual.length; idx += 1) {
            assertEquals(String.format("Byte %d is different: %s != %s", idx, bytes[idx], actual[idx]),
                    bytes[idx], actual[idx]);
        }
    }

    private void validateIPv6Addr(Client client, String tableName, String presentation, short[] addr)
            throws IOException, ProcCallException {
        ClientResponse cr;
        VoltTable vt;
        String actual_str;
        byte[] bytes = null;
        if (addr != null) {
            assertEquals(8, addr.length);
            bytes = new byte[8 * 2];
            // Make bytes be an IPv6 address in network byte order.
            for (int idx = 0; idx < 8; idx += 1) {
                // Network byte order is big endian
                // We are just swapping by shorts.
                bytes[2 * idx] = (byte)((addr[idx] >> 8) & 0xFF);
                bytes[2 * idx + 1] = (byte)(addr[idx] & 0xFF);
            }
        }
        String tableNameUpper = tableName.toUpperCase();
        cr = client.callProcedure("@AdHoc", String.format("TRUNCATE TABLE %s;", tableNameUpper));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(tableNameUpper + ".insert", presentation, bytes);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(tableNameUpper + ".insert", presentation, bytes);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", String.format("select inet6_aton(pres) from %s;", tableNameUpper));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());
        byte[] actual = vt.getVarbinary(0);
        if (bytes != null) {
            assertEquals(bytes.length, actual.length);
            assertEqualByteArrays(bytes, actual);
        } else {
            assertTrue(vt.wasNull());
        }

        cr = client.callProcedure("@AdHoc", String.format("select inet6_ntoa(bin) from %s;", tableNameUpper));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());
        actual_str = vt.getString(0);
        if (presentation != null) {
            assertEquals(presentation, actual_str);
        } else {
            assertTrue(vt.wasNull());
        }

        cr = client.callProcedure("@AdHoc",
                String.format("select inet6_ntoa(inet6_aton(pres)) from %s;", tableNameUpper));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());
        actual_str = vt.getString(0);
        if (presentation != null) {
            assertEquals(presentation, actual_str);
        } else {
            assertTrue(vt.wasNull());
        }

        cr = client.callProcedure("@AdHoc",
                String.format("select inet6_aton(inet6_ntoa(bin)) from %s;", tableNameUpper));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());
        actual = vt.getVarbinary(0);
        if (bytes != null) {
            assertEqualByteArrays(bytes, actual);
        } else {
            assertTrue(vt.wasNull());
        }
    }

    @Test
    public void testInternetAddresses() throws Exception {
        Client client = getClient();

        validateIPv4Addr(client, "INET4_TEST", null, null);
        validateIPv4Addr(client, "INET4_TEST", "127.0.0.1", 0x7f000001L);
        validateIPv4Addr(client, "INET4_TEST_PPRES", "127.0.0.1", 0x7f000001L);
        validateIPv4Addr(client, "INET4_TEST", "192.168.7.40", 0xc0a80728L);
        validateIPv4Addr(client, "INET4_TEST_PPRES", "192.168.7.40", 0xc0a80728L);
        validateIPv4Addr(client, "INET4_TEST", "1.1.1.1", 0x01010101L);
        validateIPv4Addr(client, "INET4_TEST", "1.1.1.1", 0x01010101L);
        validateIPv4Addr(client, "INET4_TEST", "1.2.3.4", 0x01020304L);
        validateIPv4Addr(client, "INET4_TEST_PPRES", "1.2.3.4", 0x01020304L);
        invalidIPAddr(client, "INET4_TEST", "01.02.03.04");
        invalidIPAddr(client, "INET4_TEST", "arglebargle");
        invalidIPAddr(client, "INET4_TEST", "192.168.7.6/24");

        // Only test for nulls on INET6_TEST and not the
        // partitioned table, since we can't insert a null
        // in a partitioned column.
        validateIPv6Addr(client, "INET6_TEST", null, null);
        // The function inet_ntop does not put leading zeros
        // in any of these numbers.  So "::0102:" would
        // fail.
        validateIPv6Addr(client, "INET6_TEST", "ab01:cd12:ef21:1ab:12cd:34ef:a01b:c23d",
                new short[] {
                        (short)0xab01, (short)0xcd12, (short)0xef21, (short)0x01ab,
                        (short)0x12cd, (short)0x34ef, (short)0xa01b, (short)0xc23d
                });
        validateIPv6Addr(client, "INET6_TEST_PPRES", "ab01:cd12:ef21:1ab:12cd:34ef:a01b:c23d",
                new short[] {
                        (short)0xab01, (short)0xcd12, (short)0xef21, (short)0x01ab,
                        (short)0x12cd, (short)0x34ef, (short)0xa01b, (short)0xc23d
                });
        validateIPv6Addr(client, "INET6_TEST", "::",
                new short[] {
                        (short)0x0000, (short)0x0000, (short)0x0000, (short)0x0000,
                        (short)0x0000, (short)0x0000, (short)0x0000, (short)0x0000
                });
        validateIPv6Addr(client, "INET6_TEST_PPRES", "::",
                new short[] {
                        (short)0x0000, (short)0x0000, (short)0x0000, (short)0x0000,
                        (short)0x0000, (short)0x0000, (short)0x0000, (short)0x0000
                });
        validateIPv6Addr(client, "INET6_TEST", "::1",
                new short[] {
                        (short)0x0000, (short)0x0000, (short)0x0000, (short)0x0000,
                        (short)0x0000, (short)0x0000, (short)0x0000, (short)0x0001
                });
        validateIPv6Addr(client, "INET6_TEST_PPRES", "::1",
                new short[] {
                        (short)0x0000, (short)0x0000, (short)0x0000, (short)0x0000,
                        (short)0x0000, (short)0x0000, (short)0x0000, (short)0x0001
                });
        invalidIPAddr(client, "INET6_TEST", ":::");
        invalidIPAddr(client, "INET6_TEST", "arglebargle");
    }

    public void testDateadd() throws IOException, ProcCallException {
        System.out.println("STARTING test DATEADD function tests");

        /*
         *      "CREATE TABLE P2 ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
                "PARTITION TABLE P2 ON COLUMN ID;\n" +
         */
        Client client = getClient();
        ClientResponse cr;
        VoltTable vt;

        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P2 (ID, TM) VALUES (10000, '2000-01-01 01:00:00.000000');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(year, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2001-01-01 01:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(quarter, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-04-01 01:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(month, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-02-01 01:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(day, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(vt.getTimestampAsSqlTimestamp(0), Timestamp.valueOf("2000-01-02 01:00:00.000000"));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(hour, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-01-01 02:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(minute, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-01-01 01:01:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(second, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-01-01 01:00:01.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(millisecond, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-01-01 01:00:00.001000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(millis, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-01-01 01:00:00.001000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(microsecond, 1, TM) FROM P2 WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-01-01 01:00:00.000001"), vt.getTimestampAsSqlTimestamp(0));

        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P2 (ID, TM) VALUES (20000, '2007-01-01 13:10:10.111111');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(millisecond, 1, TM) FROM P2 WHERE ID = 20000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2007-01-01 13:10:10.112111"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(millisecond, 2, TM) FROM P2 WHERE ID = 20000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2007-01-01 13:10:10.113111"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(microsecond, 1, TM) FROM P2 WHERE ID = 20000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2007-01-01 13:10:10.111112"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(microsecond, 2, TM) FROM P2 WHERE ID = 20000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2007-01-01 13:10:10.111113"), vt.getTimestampAsSqlTimestamp(0));

        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P2 (ID, TM) VALUES (20001, '2007-01-01 01:01:01.111111');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(quarter, 4, TM) FROM P2 WHERE ID = 20001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2008-01-01 01:01:01.111111"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(month, 13, TM) FROM P2 WHERE ID = 20001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2008-02-01 01:01:01.111111"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(day, 365, TM) FROM P2 WHERE ID = 20001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2008-01-01 01:01:01.111111"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(hour, 23, TM) FROM P2 WHERE ID = 20001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2007-01-02 00:01:01.111111"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(minute, 59, TM) FROM P2 WHERE ID = 20001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2007-01-01 02:00:01.111111"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(second, 59, TM) FROM P2 WHERE ID = 20001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2007-01-01 01:02:00.111111"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(millisecond, 59, TM) FROM P2 WHERE ID = 20001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2007-01-01 01:01:01.170111"), vt.getTimestampAsSqlTimestamp(0));

        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P2 (ID, TM) VALUES (20002, '2000-01-01 00:00:00.000000');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(year, -1, TM) FROM P2 WHERE ID = 20002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("1999-01-01 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(quarter, -1, TM) FROM P2 WHERE ID = 20002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("1999-10-01 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(month, -1, TM) FROM P2 WHERE ID = 20002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("1999-12-01 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(day, -1, TM) FROM P2 WHERE ID = 20002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("1999-12-31 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(hour, -1, TM) FROM P2 WHERE ID = 20002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("1999-12-31 23:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(minute, -1, TM) FROM P2 WHERE ID = 20002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("1999-12-31 23:59:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(second, -1, TM) FROM P2 WHERE ID = 20002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("1999-12-31 23:59:59.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(millisecond, -1, TM) FROM P2 WHERE ID = 20002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("1999-12-31 23:59:59.999000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(microsecond, -1, TM) FROM P2 WHERE ID = 20002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("1999-12-31 23:59:59.999999"), vt.getTimestampAsSqlTimestamp(0));

        //leap year test case
        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P2 (ID, TM) VALUES (20003, '2000-02-29 00:00:00.000000');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(year, 1, TM) FROM P2 WHERE ID = 20003").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2001-02-28 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P2 (ID, TM) VALUES (20004, '2000-01-31 00:00:00.000000');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(month, 1, TM) FROM P2 WHERE ID = 20004").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-02-29 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(day, 31, TM) FROM P2 WHERE ID = 20004").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-03-02 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P2 (ID, TM) VALUES (20005, '1999-12-31 00:00:00.000000');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(year, 1, TM) FROM P2 WHERE ID = 20005").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-12-31 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(quarter, 2, TM) FROM P2 WHERE ID = 20005").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-06-30 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(month, 2, TM) FROM P2 WHERE ID = 20005").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-02-29 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(day, 60, TM) FROM P2 WHERE ID = 20005").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-02-29 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(hour, 1440, TM) FROM P2 WHERE ID = 20005").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-02-29 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(minute, 86400, TM) FROM P2 WHERE ID = 20005").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-02-29 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(second, 5184000, TM) FROM P2 WHERE ID = 20005").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-02-29 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(millisecond, 5184000000, TM) FROM P2 WHERE ID = 20005").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-02-29 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(microsecond, 5184000000000, TM) FROM P2 WHERE ID = 20005").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(Timestamp.valueOf("2000-02-29 00:00:00.000000"), vt.getTimestampAsSqlTimestamp(0));

        // Test null interval
        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(YEAR, NULL, TM), DATEADD(QUARTER, NULL,TM), DATEADD(MONTH, NULL,TM), "
                        + "DATEADD(DAY, NULL,TM), DATEADD(HOUR, NULL,TM), DATEADD(MINUTE, NULL,TM), "
                        + "DATEADD(SECOND, NULL,TM), DATEADD(MILLISECOND, NULL,TM), "
                        + "DATEADD(MICROSECOND, NULL,TM) FROM P2 WHERE ID = 20005;").getResults()[0];
        assertTrue(vt.advanceRow());
        assertNull(vt.getTimestampAsTimestamp(0));
        assertNull(vt.getTimestampAsTimestamp(1));
        assertNull(vt.getTimestampAsTimestamp(2));
        assertNull(vt.getTimestampAsTimestamp(3));
        assertNull(vt.getTimestampAsTimestamp(4));
        assertNull(vt.getTimestampAsTimestamp(5));
        assertNull(vt.getTimestampAsTimestamp(6));
        assertNull(vt.getTimestampAsTimestamp(7));
        assertNull(vt.getTimestampAsTimestamp(8));

        // Test null timestamp
        cr = client.callProcedure("@AdHoc", "INSERT INTO P2 (ID, TM) VALUES (20006, null)");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEADD(YEAR, 1, TM), DATEADD(QUARTER, 1, TM), DATEADD(MONTH, 1, TM), "
                        + "DATEADD(DAY, 1, TM), DATEADD(HOUR, 1, TM), DATEADD(MINUTE, 1, TM), "
                        + "DATEADD(SECOND, 1, TM), DATEADD(MILLISECOND, 1, TM), "
                        + "DATEADD(MICROSECOND, 1, TM) FROM P2 WHERE ID = 20006;").getResults()[0];
        assertTrue(vt.advanceRow());
        assertNull(vt.getTimestampAsTimestamp(0));
        assertNull(vt.getTimestampAsTimestamp(1));
        assertNull(vt.getTimestampAsTimestamp(2));
        assertNull(vt.getTimestampAsTimestamp(3));
        assertNull(vt.getTimestampAsTimestamp(4));
        assertNull(vt.getTimestampAsTimestamp(5));
        assertNull(vt.getTimestampAsTimestamp(6));
        assertNull(vt.getTimestampAsTimestamp(7));
        assertNull(vt.getTimestampAsTimestamp(8));

        // Test null or illegal datepart
        boolean throwed = false;
        try {
            client.callProcedure("@AdHoc", "SELECT DATEADD(NULL, 1, TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("SQL error while compiling query"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc", "SELECT DATEADD(WEEK, 1, TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("SQL error while compiling query"));
            throwed = true;
        }
        assertTrue(throwed);

        // Test large intervals caused exceptions
        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(YEAR, " + ((long) Integer.MAX_VALUE + 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
       assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(YEAR, " + ((long) Integer.MIN_VALUE - 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(QUARTER, " + ((long) Integer.MAX_VALUE + 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(QUARTER, " + ((long) Integer.MIN_VALUE - 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(MONTH, " + ((long) Integer.MAX_VALUE + 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(MONTH, " + ((long) Integer.MIN_VALUE - 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(DAY, " + Long.MAX_VALUE + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(DAY, " + (Long.MIN_VALUE + 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(HOUR, " + Long.MAX_VALUE + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(HOUR, " + (Long.MIN_VALUE + 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(MINUTE, " + Long.MAX_VALUE + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(MINUTE, " + (Long.MIN_VALUE + 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(SECOND, " + Long.MAX_VALUE + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(SECOND, " + (Long.MIN_VALUE + 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(MILLISECOND, " + Long.MAX_VALUE + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(MILLISECOND, " + (Long.MIN_VALUE + 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(MICROSECOND, " + Long.MAX_VALUE + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);

        throwed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT DATEADD(MICROSECOND, " + (Long.MIN_VALUE + 1) + ", TM) FROM P2 WHERE ID = 20005;");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("interval is too large for DATEADD function"));
            throwed = true;
        }
        assertTrue(throwed);
    }

    public void testDatediff() throws IOException, ProcCallException {
        System.out.println("STARTING test DATEDIFF function tests");

        /*
         *      "CREATE TABLE P3DIFF ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "TM1 TIMESTAMP DEFAULT NULL, " +
                "TM2 TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
                "PARTITION TABLE P3DIFF ON COLUMN ID;\n" +
         */
        Client client = getClient();
        ClientResponse cr;
        VoltTable vt;

        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P3DIFF (ID, TM1, TM2) VALUES (10000, '2000-01-01 01:00:00.00000', '2001-02-02 02:01:01.001001');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P3DIFF (ID, TM1, TM2) VALUES (10001, '2000-01-01 01:00:00.00000', DATEADD(DAY, 1, '2000-01-01 01:00:00.00000'));");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Simple day apart timestamps
        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(year, TM1, TM2), DATEDIFF(month, TM1, TM2), DATEDIFF(quarter, TM1, TM2), DATEDIFF(day, TM1, TM2), " +
                        "DATEDIFF(hour, TM1, TM2), DATEDIFF(minute, TM1, TM2), DATEDIFF(second, TM1, TM2), DATEDIFF(millis, TM1, TM2), " +
                        "DATEDIFF(micros, TM1, TM2), DATEDIFF(week, TM1, TM2) FROM P3DIFF WHERE ID = 10001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(0, vt.getLong(0));
        assertEquals(0, vt.getLong(1));
        assertEquals(0, vt.getLong(2));
        assertEquals(1, vt.getLong(3));
        assertEquals(24, vt.getLong(4));
        assertEquals(1440, vt.getLong(5));
        assertEquals(86400, vt.getLong(6));
        assertEquals(86400000, vt.getLong(7));
        assertEquals(86400000000L, vt.getLong(8));
        assertEquals(0, vt.getLong(9));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(year, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(1, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(quarter, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(4, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(month, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(13, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(day, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(398, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(week, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(56, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(hour, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(9553, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(minute, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(573181, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(second, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(34390861, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(millis, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(34390861001L, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(micros, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(34390861001001L, vt.getLong(0));

        // Test null interval
        try {
            vt = client.callProcedure("@AdHoc",
                    "SELECT DATEDIFF(null, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
            fail("Bad interval should throw exception.");
        } catch (ProcCallException pce) {
            // All good
        }
        // Test null or illegal datepart
        try {
            vt = client.callProcedure("@AdHoc",
                    "SELECT DATEDIFF(SUMMER, TM1, TM2) FROM P3DIFF WHERE ID = 10000").getResults()[0];
            fail("Bad interval should throw exception.");
        } catch (ProcCallException pce) {
            // All good
        }

        // Test null timestamp
        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P3DIFF (ID, TM1, TM2) VALUES (10002, null, null);");
        vt = client.callProcedure("@AdHoc",
                "SELECT DATEDIFF(micros, TM1, TM2) FROM P3DIFF WHERE ID = 10002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(null, vt.get(0));
    }

    // Values expected in this test came from snowflake running the time_window function.
    public void testTimeWindow() throws IOException, ProcCallException {
        System.out.println("STARTING test TIME_WINDOW function tests");

        /*
         *      "CREATE TABLE P3SLICE ( " +
                "ID INTEGER DEFAULT '0' NOT NULL, " +
                "TM1 TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) ); " +
                "PARTITION TABLE P3SLICE ON COLUMN ID;\n" +
         */
        Client client = getClient();
        ClientResponse cr;
        VoltTable vt;

        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P3SLICE (ID, TM1) VALUES (10000, '2001-04-04 14:04:04.00000');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P3SLICE (ID, TM1) VALUES (10001, '1965-04-04 14:04:04.00000');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P3SLICE (ID, TM1) VALUES (10002, NULL);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = client.callProcedure("@AdHoc",
                "SELECT " +
                        "TIME_WINDOW(YEAR, 133, TM1, START), " +
                        "TIME_WINDOW(QUARTER, 133, TM1, START), " +
                        "TIME_WINDOW(MONTH, 133, TM1, START), " +
                        "TIME_WINDOW(WEEK, 133, TM1, START), " +
                        "TIME_WINDOW(DAY, 133, TM1, START), " +
                        "TIME_WINDOW(HOUR, 133, TM1, START), " +
                        "TIME_WINDOW(MINUTE, 133, TM1, START), " +
                        "TIME_WINDOW(SECOND, 133, TM1, START), " +
                        "TIME_WINDOW(MILLIS, 133, TM1, START), " +
                        "TIME_WINDOW(MILLISECOND, 133, TM1, START) " +
                        "FROM P3SLICE WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(new TimestampType("1970-01-01 00:00:000000"), vt.get(0));
        assertEquals(new TimestampType("1970-01-01 00:00:000000"), vt.get(1));
        assertEquals(new TimestampType("1992-03-01 00:00:000000"), vt.get(2));
        assertEquals(new TimestampType("2000-07-31 00:00:000000"), vt.get(3));
        assertEquals(new TimestampType("2000-12-14 00:00:000000"), vt.get(4));
        assertEquals(new TimestampType("2001-04-03 20:00:000000"), vt.get(5));
        assertEquals(new TimestampType("2001-04-04 13:44:000000"), vt.get(6));
        assertEquals(new TimestampType("2001-04-04 14:03:57"), vt.get(7));
        assertEquals(new TimestampType("2001-04-04 14:04:03.916000"), vt.get(8));
        assertEquals(new TimestampType("2001-04-04 14:04:03.916000"), vt.get(9));

        vt = client.callProcedure("@AdHoc",
                "SELECT " +
                        "TIME_WINDOW(YEAR, 133, TM1, END), " +
                        "TIME_WINDOW(QUARTER, 133, TM1, END), " +
                        "TIME_WINDOW(MONTH, 133, TM1, END), " +
                        "TIME_WINDOW(WEEK, 133, TM1, END), " +
                        "TIME_WINDOW(DAY, 133, TM1, END), " +
                        "TIME_WINDOW(HOUR, 133, TM1, END), " +
                        "TIME_WINDOW(MINUTE, 133, TM1, END), " +
                        "TIME_WINDOW(SECOND, 133, TM1, END), " +
                        "TIME_WINDOW(MILLIS, 133, TM1, END), " +
                        "TIME_WINDOW(MILLISECOND, 133, TM1, END) " +
                        "FROM P3SLICE WHERE ID = 10000").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(new TimestampType("2103-01-01 00:00:000000"), vt.get(0));
        assertEquals(new TimestampType("2003-04-01 00:00:000000"), vt.get(1));
        assertEquals(new TimestampType("2003-04-01 00:00:000000"), vt.get(2));
        assertEquals(new TimestampType("2003-02-17 00:00:000000"), vt.get(3));
        assertEquals(new TimestampType("2001-04-26 00:00:000000"), vt.get(4));
        assertEquals(new TimestampType("2001-04-09 09:00:000000"), vt.get(5));
        assertEquals(new TimestampType("2001-04-04 15:57:000000"), vt.get(6));
        assertEquals(new TimestampType("2001-04-04 14:06:10"), vt.get(7));
        assertEquals(new TimestampType("2001-04-04 14:04:04.049000"), vt.get(8));
        assertEquals(new TimestampType("2001-04-04 14:04:04.049000"), vt.get(9));

        // Time less than epoch
        vt = client.callProcedure("@AdHoc",
                "SELECT " +
                        "TIME_WINDOW(YEAR, 133, TM1, END), " +
                        "TIME_WINDOW(QUARTER, 133, TM1, END), " +
                        "TIME_WINDOW(MONTH, 133, TM1, END), " +
                        "TIME_WINDOW(WEEK, 133, TM1, END), " +
                        "TIME_WINDOW(DAY, 133, TM1, END), " +
                        "TIME_WINDOW(HOUR, 133, TM1, END), " +
                        "TIME_WINDOW(MINUTE, 133, TM1, END), " +
                        "TIME_WINDOW(SECOND, 133, TM1, END), " +
                        "TIME_WINDOW(MILLIS, 133, TM1, END), " +
                        "TIME_WINDOW(MILLISECOND, 133, TM1, END) " +
                        "FROM P3SLICE WHERE ID = 10001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(new TimestampType("1970-01-01 00:00:000000"), vt.get(0));
        assertEquals(new TimestampType("1970-01-01 00:00:000000"), vt.get(1));
        assertEquals(new TimestampType("1970-01-01 00:00:000000"), vt.get(2));
        assertEquals(new TimestampType("1967-06-12 00:00:000000"), vt.get(3));
        assertEquals(new TimestampType("1965-04-08 00:00:000000"), vt.get(4));
        assertEquals(new TimestampType("1965-04-08 00:00:000000"), vt.get(5));
        assertEquals(new TimestampType("1965-04-04 16:12:000000"), vt.get(6));
        assertEquals(new TimestampType("1965-04-04 14:05:39"), vt.get(7));
        assertEquals(new TimestampType("1965-04-04 14:04:04.038000"), vt.get(8));
        assertEquals(new TimestampType("1965-04-04 14:04:04.038000"), vt.get(9));

        vt = client.callProcedure("@AdHoc",
                "SELECT " +
                        "TIME_WINDOW(YEAR, 133, TM1), " +
                        "TIME_WINDOW(QUARTER, 133, TM1), " +
                        "TIME_WINDOW(MONTH, 133, TM1), " +
                        "TIME_WINDOW(WEEK, 133, TM1), " +
                        "TIME_WINDOW(DAY, 133, TM1), " +
                        "TIME_WINDOW(HOUR, 133, TM1), " +
                        "TIME_WINDOW(MINUTE, 133, TM1), " +
                        "TIME_WINDOW(SECOND, 133, TM1), " +
                        "TIME_WINDOW(MILLIS, 133, TM1), " +
                        "TIME_WINDOW(MILLISECOND, 133, TM1) " +
                        "FROM P3SLICE WHERE ID = 10001").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(new TimestampType("1837-01-01 00:00:000000"), vt.get(0));
        assertEquals(new TimestampType("1936-10-01 00:00:000000"), vt.get(1));
        assertEquals(new TimestampType("1958-12-01 00:00:000000"), vt.get(2));
        assertEquals(new TimestampType("1964-11-23 00:00:000000"), vt.get(3));
        assertEquals(new TimestampType("1964-11-26 00:00:000000"), vt.get(4));
        assertEquals(new TimestampType("1965-04-02 11:00:000000"), vt.get(5));
        assertEquals(new TimestampType("1965-04-04 13:59:000000"), vt.get(6));
        assertEquals(new TimestampType("1965-04-04 14:03:26"), vt.get(7));
        assertEquals(new TimestampType("1965-04-04 14:04:03.905000"), vt.get(8));
        assertEquals(new TimestampType("1965-04-04 14:04:03.905000"), vt.get(9));

        // Invalid intervals should throw exception
        String maxLimitsUnit[] = { "YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND", "MILLIS"};
        for (int i = 0; i < maxLimitsUnit.length; i++) {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT TIME_WINDOW(").append(maxLimitsUnit[i]).append(", -1, , TM1) FROM P3SLICE WHERE ID = 10001");
                vt = client.callProcedure("@AdHoc", sb.toString()).getResults()[0];
                fail("window bigger than supported must throw exception.");
            } catch (ProcCallException ex) {
            }
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("SELECT TIME_WINDOW(").append(maxLimitsUnit[i]).append(", 0, , TM1) FROM P3SLICE WHERE ID = 10001");
                vt = client.callProcedure("@AdHoc", sb.toString()).getResults()[0];
                fail("window bigger than supported must throw exception.");
            } catch (ProcCallException ex) {
            }
        }

        // Null timestamp test
        vt = client.callProcedure("@AdHoc",
                "SELECT " +
                        "TIME_WINDOW(YEAR, 133, TM1), " +
                        "TIME_WINDOW(QUARTER, 133, TM1), " +
                        "TIME_WINDOW(MONTH, 133, TM1), " +
                        "TIME_WINDOW(WEEK, 133, TM1), " +
                        "TIME_WINDOW(DAY, 133, TM1), " +
                        "TIME_WINDOW(HOUR, 133, TM1), " +
                        "TIME_WINDOW(MINUTE, 133, TM1), " +
                        "TIME_WINDOW(SECOND, 133, TM1), " +
                        "TIME_WINDOW(MILLIS, 133, TM1), " +
                        "TIME_WINDOW(MILLISECOND, 133, TM1) " +
                        "FROM P3SLICE WHERE ID = 10002").getResults()[0];
        assertTrue(vt.advanceRow());
        assertNull(vt.get(0));
        assertNull(vt.get(1));
        assertNull(vt.get(2));
        assertNull(vt.get(3));
        assertNull(vt.get(4));
        assertNull(vt.get(5));
        assertNull(vt.get(6));
        assertNull(vt.get(7));
        assertNull(vt.get(8));
        assertNull(vt.get(9));
    }

    @Test
    public void testBadParamTypeForTimeStampField() throws IOException, ProcCallException {
        Client client = getClient();
        // seed dummy data into table
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P2 (ID, TM) VALUES (10000, '2000-01-01 01:00:00.000000');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        final int numberOfProcsToTest = BadParamTypesForTimestamp.procs.length;
        final int numberOfValuesToTest = BadParamTypesForTimestamp.values.length;
        for (int procEntry = 0; procEntry < numberOfProcsToTest; procEntry++) {
            for (int valueIndexToTestWith = 0; valueIndexToTestWith < numberOfValuesToTest; valueIndexToTestWith++) {
                verifyProcFails(client,
                        "VOLTDB ERROR: SQL ERROR.* can't be cast as TIMESTAMP",
                        "BadParamTypesForTimestamp", procEntry, valueIndexToTestWith);
            }
        }
    }

    @Test
    public void testRegexpPosition() throws Exception {
        System.out.println("STARTING testRegexpPosition");

        Client client = getClient();
        ClientResponse cr = null;
        VoltTable vt = null;
        /*
            "CREATE TABLE P1 ( " +
            "ID INTEGER DEFAULT 0 NOT NULL, " +
            "DESC VARCHAR(300), " +
            "NUM INTEGER, " +
            "RATIO FLOAT, " +
            "PAST TIMESTAMP DEFAULT NULL, " +
            "PRIMARY KEY (ID) ); " +
         */

       cr = client.callProcedure("@AdHoc",
               "INSERT INTO P1 (ID, DESC) VALUES (200, 'TEST reGexp_poSiTion123456Test')");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, 'TEST') FROM P1 WHERE REGEXP_POSITION(DESC, 'TEST') > 0;")
                .getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(1, vt.asScalarLong());

        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, '[a-z](\\d+)[a-z]') FROM P1 WHERE REGEXP_POSITION(DESC, '[a-z](\\d+)[a-z]') > 0")
                .getResults()[0];
        assertFalse(vt.advanceRow());

        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, '[a-z](\\d+)[A-Z]') FROM P1 WHERE REGEXP_POSITION(DESC, '[a-z](\\d+)[A-Z]') > 0")
                .getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(20, vt.asScalarLong());

        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, '[a-z](\\d+)[a-z]', 'i') FROM P1 WHERE REGEXP_POSITION(DESC, '[a-z](\\d+)[a-z]', 'i') > 0")
                .getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(20, vt.asScalarLong());

        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, '[a-z](\\d+)[a-z]', 'ci') FROM P1 WHERE REGEXP_POSITION(DESC, '[a-z](\\d+)[A-Z]') > 0")
                .getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(20, vt.asScalarLong());

        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, '[a-z](\\d+)[a-z]', 'iiccii') FROM P1 WHERE REGEXP_POSITION(DESC, '[a-z](\\d+)[A-Z]') > 0")
                .getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(20, vt.asScalarLong());

        boolean expectedExceptionThrowed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT REGEXP_POSITION(DESC, '[a-z](a]') FROM P1 WHERE REGEXP_POSITION(DESC, '[a-z](a]') > 0");
            fail("Expected exception for illegal regular expression in regexp_position.");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains(
                    "Regular Expression Compilation Error: missing closing parenthesis"));
            expectedExceptionThrowed = true;
        }
        assertTrue(expectedExceptionThrowed);

        expectedExceptionThrowed = false;
        try {
            client.callProcedure("@AdHoc",
                    "SELECT REGEXP_POSITION(DESC, '[a-z](\\d+)[A-Z]', 'k') FROM P1 WHERE REGEXP_POSITION(DESC, '[a-z](\\d+)[A-Z]', 'k') > 0");
            fail("Expected exception for illegal match flag in regexp_position.");
        } catch (ProcCallException e) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, e.getClientResponse().getStatus());
            assertTrue(e.getClientResponse().getStatusString().contains("Illegal Match Flags"));
            expectedExceptionThrowed = true;
        }
        assertTrue(expectedExceptionThrowed);

        // test null strings
        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, NULL) FROM P1 WHERE ID = 200").getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getLong(0);
        assertTrue(vt.wasNull());

        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P1 (ID, DESC) VALUES (201, NULL);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, 'TEST') FROM P1 WHERE ID = 201").getResults()[0];
        assertTrue(vt.advanceRow());
        vt.getLong(0);
        assertTrue(vt.wasNull());

        // test utf-8 strings
        cr = client.callProcedure("@AdHoc",
                "INSERT INTO P1 (ID, DESC) VALUES (202, 'vVoltDBBB贾贾贾');");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, '[A-Z]贾') FROM P1 WHERE ID = 202;").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(9, vt.getLong(0));

        vt = client.callProcedure("@AdHoc",
                "SELECT REGEXP_POSITION(DESC, '[a-z]贾', 'i') FROM P1 WHERE ID = 202;").getResults()[0];
        assertTrue(vt.advanceRow());
        assertEquals(9, vt.getLong(0));


        // clear test data
        cr = client.callProcedure("@AdHoc", "DELETE FROM P1 WHERE ID IN (200, 201, 202);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

    }

    private void doTestTimestampLimit(String procName, long value) throws Exception {
        Client client = getClient();
        ClientResponse cr = client.callProcedure(procName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        vt.advanceRow();
        assertEquals(1,  vt.getRowCount());
        assertEquals(value, vt.getTimestampAsLong(0));
        assertEquals(value, vt.getTimestampAsLong(1));
    }

    private void doTestIsValidTimestamp(long id, boolean expected) throws Exception {
        Client client = getClient();
        ClientResponse cr = client.callProcedure("TEST_IS_VALID_TIMESTAMP", id);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        vt.advanceRow();
        // If we expect the timestamp to be valid, we expect one
        // row of answer.  If we expect it to be invalid, we expect
        // no answers.
        if (expected) {
            assertEquals(1, vt.getRowCount());
            assertEquals(id, vt.getLong(0));
        } else {
            assertEquals(0, vt.getRowCount());
        }
    }

    @Test
    public void testTimestampValidityFunctions() throws Exception {
        // Insert some valid and invalid data.
        Client client = getClient();
        ClientResponse cr;
        cr = client.callProcedure("P2.insert", 100, GREGORIAN_EPOCH - 1000);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 101, NYE9999 + 1000);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 200, GREGORIAN_EPOCH);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 201, 0);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 202, NYE9999);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Test MIN_VALID_TIMESTAMP
        doTestTimestampLimit("GET_MIN_VALID_TIMESTAMP", GREGORIAN_EPOCH);
        doTestTimestampLimit("GET_MAX_VALID_TIMESTAMP", NYE9999);

        // Test IS_VALID_TIMESTAMP too low.
        doTestIsValidTimestamp(100, false);
        doTestIsValidTimestamp(101, false);
        doTestIsValidTimestamp(200, true);
        doTestIsValidTimestamp(201, true);
        doTestIsValidTimestamp(202, true);

        // How these functions might typically be used:
        validateTableOfLongs(client, "update p2 set tm = min_valid_timestamp() where tm < min_valid_timestamp()",
                new long[][] {{1}});
        validateTableOfLongs(client, "update p2 set tm = max_valid_timestamp() where tm > max_valid_timestamp()",
                new long[][] {{1}});

        // No more invalid timestamps
        validateTableOfLongs(client, "select * from p2 where not is_valid_timestamp(tm)",
                new long[][] {});
    }

    @Test
    public void testFORMAT_TIMESTAMP() throws Exception {
        System.out.println("STARTING testFORMAT_TIMESTAMP");

        Client client = getClient();
        ClientResponse cr;

        cr = client.callProcedure("P2.insert", 1, "2013-07-18 02:00:00.123457");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("P2.insert", 2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // null timestamp will return null
        cr = client.callProcedure("FORMAT_TIMESTAMP", "UTC", 2);
        assertContentOfTable(new Object[][]{{null}}, cr.getResults()[0]);

        // null offset means offset == 0
        cr = client.callProcedure("FORMAT_TIMESTAMP", null, 1);
        assertContentOfTable(new Object[][]{{"2013-07-18 02:00:00.123457"}}, cr.getResults()[0]);

        String[] invalid_offsets = {"I_AM_INVALID", "HOME/TRASH", "10:00", "-1:00"};
        String expectedError = "Invalid timezone string\\s*";
        for (String invalid_offset : invalid_offsets) {
            verifyProcFails(client, expectedError, "FORMAT_TIMESTAMP", invalid_offset, 1);
        }

        String[] offsets = {"UTC", "Australia/Adelaide", "America/Atikokan", "America/Belize", "Asia/Shanghai",
                "+02:30", "-10:00", "  +02:30", " -10:00 "};
        String[] results = {"2013-07-18 02:00:00.123457", "2013-07-18 11:30:00.123457", "2013-07-17 21:00:00.123457",
                "2013-07-17 20:00:00.123457", "2013-07-18 10:00:00.123457", "2013-07-18 04:30:00.123457",
                "2013-07-17 16:00:00.123457", "2013-07-18 04:30:00.123457", "2013-07-17 16:00:00.123457"};

        for (int i = 0; i < offsets.length; i++) {
            cr = client.callProcedure("FORMAT_TIMESTAMP", offsets[i], 1);
            assertContentOfTable(new Object[][]{{results[i]}}, cr.getResults()[0]);
        }
    }

    @Test
    public void testMigrating() throws IOException, ProcCallException {
        Client client = getClient();
        // insert some data into the migrating table.
        // m1 is replicated and m2 is partitioned.
        Stream.of("insert into m1(a, b) values(1, 11);",
                "insert into m1(a, b) values(2, 22);",
                "insert into m1(a, b) values(3, 33);",
                "insert into m1(a, b) values(4, 44);",
                "insert into m2(a, b) values(1, 10);",
                "insert into m2(a, b) values(2, 20);",
                "insert into m2(a, b) values(3, 30);",
                "insert into m2(a, b) values(4, 40);",
                "insert into p1 values(1, null, 4, 4.1);")
                .forEachOrdered(stmt -> {
                    try {
                        final ClientResponse cr = client.callProcedure("@AdHoc", stmt);
                        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                    } catch (IOException | ProcCallException e) {
                        fail("Query \"" + stmt + "\" should have worked fine");
                    }
                });
        final Object[][] expected_m1 = {{1, 11}, {2, 22}, {3, 33}, {4, 44}},
                expected_m2 = {{1, 10}, {2, 20}, {3, 30}, {4, 40}}, empty = {};
        ClientResponse cr;
        // select migrating rows
        cr = client.callProcedure("@AdHoc", "select a, b from m1 where not migrating() order by a, b;");
        assertContentOfTable(expected_m1, cr.getResults()[0]);

        cr = client.callProcedure("@AdHoc", "select a, b from m2 where not migrating() order by a, b;");
        assertContentOfTable(expected_m2, cr.getResults()[0]);

        // forbid select !migrating rows
        cr = client.callProcedure("@AdHoc", "select a, b from m1 where migrating() order by a, b;");
        assertContentOfTable(empty, cr.getResults()[0]);
        cr = client.callProcedure("@AdHoc", "select a, b from m2 where migrating() order by a, b;");
        assertContentOfTable(empty, cr.getResults()[0]);

        cr = client.callProcedure("@AdHoc",
                "select a, b from m1 where not migrating() and a >= 3 order by a, b;");
        assertContentOfTable(new Object[][]{{3, 33}, {4, 44}}, cr.getResults()[0]);

        cr = client.callProcedure("@AdHoc",
                "select a, b from m2 where not migrating() and a >= 3 order by a, b;");
        assertContentOfTable(new Object[][]{{3, 30}, {4, 40}}, cr.getResults()[0]);

        // migrating with aggregate functions.
        cr = client.callProcedure("@AdHoc",
                "select count(*) from m1 where not migrating() and a >= 3;");
        assertContentOfTable(new Object[][]{{2}}, cr.getResults()[0]);

        cr = client.callProcedure("@AdHoc",
                "select count(*) from m2 where not migrating() and a >= 3;");
        assertContentOfTable(new Object[][]{{2}}, cr.getResults()[0]);

        // migrate() in subquery select
        cr = client.callProcedure("@AdHoc",
                "select t1.a from (select * from m2 where not migrating() and b < 30) as t1 order by t1.a");
        assertContentOfTable(new Object[][]{{1}, {2}}, cr.getResults()[0]);

        // Can not apply MIGRATING function on non-migrating tables.
        verifyAdHocFails(client, "Can not apply MIGRATING function on non-migrating tables.\\s*",
                "select * from p1 where not migrating();");

        // we do not support migrating() in SELECT clause
        verifyAdHocFails(client, "A SELECT clause does not allow a BOOLEAN expression.\\s*",
                "select not migrating() from m1;");

        // we do not support migrating() with joins
        verifyAdHocFails(client,
                "Join with filters that do not depend on joined tables is not supported in VoltDB\\s*",
                "select * from m2, p1 where not migrating();");

        // we do not support migrating() with subquery joins
        verifyAdHocFails(client,
                "Join with filters that do not depend on joined tables is not supported in VoltDB\\s*",
                "select t1.a from   (select * from m1 where not migrating() and b < 30) as t1 "
                        + "  inner join (select * from m2 where a > 0) as t2 on t1.a = t2.a ");
    }
}
