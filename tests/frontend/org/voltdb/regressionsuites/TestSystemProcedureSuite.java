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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.regressionsuites.malicious.GoSleep;

import junit.framework.Test;

public class TestSystemProcedureSuite extends RegressionSuite {
    private static int SITES = 3;
    private static int HOSTS = MiscUtils.isPro() ? 2 : 1;
    private static int KFACTOR = MiscUtils.isPro() ? 1 : 0;
    private static boolean hasLocalServer = true; //// false;

    // Having 2 different table name prefixes simplifies the process of
    // writing and maintaining positive @SwapTables test cases which require
    // pairs of identical (or nearly so) tables.
    static final String[] SWAPPY_PREFIX_PAIR = { "SWAP_THIS", "SWAP_THAT" };

    // Entries in this array are three element arrays defining customizations
    // to a base table, each having a different name ending: e.g.
    // "_NORMAL" for a base case with no customizations.
    // The first element of each three element array is the root/suffix part
    // of the table name.
    // The second element is a continuation of the common table definition line
    // that starts "ID INTEGER " and can optionally be extended with additional
    // syntax to define column qualifiers for ID, and/or a comma to end the
    // column declaration followed by a constraint definition.
    // The third element is one or more table modifier statements to establish
    // partitioning and/or external indexes for that variant of the table
    // after the table definition.
    private static final String[][] SWAPPY_TABLES = {
            { "_NORMAL", // 0
                "",
                "",
            },
            { "_WITH_PARTITIONING", // 1
                " NOT NULL",
                " PARTITION TABLE %s_WITH_PARTITIONING ON COLUMN ID;",
            },

            { "_WITH_NAMED_TREE_PK", // 2
                ", CONSTRAINT %s_NAMED PRIMARY KEY (ID)\n",
                "",
            },
            { "_WITH_ANONYMOUS_PK", // 3
                ", PRIMARY KEY (ID)\n",
                "",
            },
            { "_WITH_INLINE_PK", // 4
                " PRIMARY KEY NOT NULL\n",
                "",
            },

            { "_WITH_NAMED_UNIQUE", // 5
                ", CONSTRAINT %s_NAMED UNIQUE (ID)\n",
                "",
            },
            { "_WITH_ANONYMOUS_UNIQUE", // 6
                ", UNIQUE (ID)\n",
                "",
            },
            { "_WITH_INLINE_UNIQUE", // 7
                " UNIQUE NOT NULL\n",
                "",
            },
            { "_WITH_EXTERNAL_UNIQUE", // 8
                "",
                " CREATE UNIQUE INDEX %s_WITH_EXTERNAL_UNIQUE_INDEX ON %s_WITH_EXTERNAL_UNIQUE (ID);\n",
            },

            { "_WITH_NAMED_ASSUME_UNIQUE", // 9
                ", CONSTRAINT %s_NAMED ASSUMEUNIQUE (ID)\n",
                " PARTITION TABLE %s_WITH_NAMED_ASSUME_UNIQUE ON COLUMN NAME;\n",
            },
            { "_WITH_ANONYMOUS_ASSUME_UNIQUE", // 10
                ", ASSUMEUNIQUE (ID)\n",
                " PARTITION TABLE %s_WITH_ANONYMOUS_ASSUME_UNIQUE ON COLUMN NAME;\n",
            },
            { "_WITH_INLINE_ASSUME_UNIQUE", // 11
                " ASSUMEUNIQUE NOT NULL\n",
                " PARTITION TABLE %s_WITH_INLINE_ASSUME_UNIQUE ON COLUMN NAME;\n",
            },
            { "_WITH_EXTERNAL_ASSUME_UNIQUE", // 12
                "",
                " PARTITION TABLE %s_WITH_EXTERNAL_ASSUME_UNIQUE ON COLUMN NAME;\n" +
                        " CREATE ASSUMEUNIQUE INDEX " +
                        " %s_WITH_EXTERNAL_ASSUME_UNIQUE_ID " +
                        " ON %s_WITH_EXTERNAL_ASSUME_UNIQUE (ID);\n",
            },
            { "_WITH_EXTERNAL_NONUNIQUE", // 13
                "",
                " CREATE INDEX %s_WITH_EXTERNAL_NONUNIQUE ON %s_WITH_EXTERNAL_NONUNIQUE (ID);\n",
            },
            { "_WITH_ANONYMOUS_ALTPK", // 14
                ", PRIMARY KEY (NONID)\n",
                "",
            },
            { "_WITH_PARTIAL_INDEX", // 15
                "",
                " CREATE INDEX %s_WITH_PARTIAL_INDEX ON %s_WITH_PARTIAL_INDEX (ID) WHERE ID > 0;\n",
            },
            { "_WITH_ALT_PARTITIONING", // 16
                "",
                " PARTITION TABLE %s_WITH_ALT_PARTITIONING ON COLUMN NONID;",
            },
            { "_WITH_MANY_FEATURES", // 17
              ", PRIMARY KEY (ID), UNIQUE (NONID, NAME),",
              " CREATE INDEX %s_WITH_MANY_FEATURES ON %s_WITH_MANY_FEATURES (NONID / 2); "
              + "CREATE INDEX %s_PARTIAL_WITH_MANY_FEATURES ON %s_WITH_MANY_FEATURES (NONID / 3) WHERE NONID > 1000;",
            }
    };

    // These template tables only need one instantiation each and are
    // more complete than the SWAPPY_TABLES templates for more flexibility.
    // Each entry is a two string vector. The first string is a complete table
    // name and the second is a complete body of the definition for the table.
    // The main motivation for breaking apart the table name and its
    // definition rather than just using a big block of literal schema text
    // is to allow iteration over just the table names when generating
    // the fixed set of DML and query statements against each of them.
    //
    //    { "ORIG",
    //        " (\n" +
    //        "  NAME VARCHAR(32 BYTES) NOT NULL," +
    //        "  PRICE FLOAT," +
    //        "  NONID INTEGER NOT NULL," +
    //        "  ID INTEGER " +
    //        ");\n",
    //    },
    private static final String[][] NONSWAPPY_TABLES = {
            { "NONSWAP_REORDERED",
                " (\n" +
                "  PRICE FLOAT," +
                "  NAME VARCHAR(32 BYTES) NOT NULL," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n",
            },
            { "NONSWAP_RENAMED",
                " (\n" +
                "  NAME VARCHAR(32 BYTES) NOT NULL," +
                "  APRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n",
            },
            { "NONSWAP_RETYPED",
                " (\n" +
                "  NAME VARCHAR(32 BYTES) NOT NULL," +
                "  PRICE DECIMAL," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n",
            },
            { "NONSWAP_RETYPED2",
                " (\n" +
                "  NAME VARCHAR(33 BYTES) NOT NULL," +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n",
            },
            { "NONSWAP_RETYPED3",
                " (\n" +
                "  NAME VARCHAR(32 BYTES) NOT NULL," +
                "  PRICE FLOAT," +
                "  NONID BIGINT NOT NULL," +
                "  ID INTEGER " +
                ");\n",
            },
            { "NONSWAP_NOT_BYTES",
                " (\n" +
                "  NAME VARCHAR(32) NOT NULL," +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n",
            },
            { "NONSWAP_NOT_BYTES2",
                " (\n" +
                "  NAME VARCHAR(8) NOT NULL," +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n",
            },
            { "NONSWAP_SHORTENED",
                " (\n" +
                "  NAME VARCHAR(32 BYTES) NOT NULL,\n" +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                ");\n",
            },
            { "NONSWAP_INSERTED",
                " (\n" +
                "  NAME VARCHAR(32 BYTES) NOT NULL,\n" +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  LIST_PRICE FLOAT," +
                "  ID INTEGER " +
                ");\n",
            },
            { "NONSWAP_STRUNG_UP",
                " (\n" +
                "  NAME VARCHAR(128 BYTES) NOT NULL,\n" +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n",
            },
            { "NONSWAP_BIGGER",
                " (\n" +
                "  NAME VARCHAR(32 BYTES) NOT NULL,\n" +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  ID BIGINT " +
                ");\n",
            },
            { "NONSWAP_VIEWED_1",
                " (\n" +
                "  NAME VARCHAR(32 BYTES) NOT NULL,\n" +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n" +
                "CREATE VIEW VIEWING_1 AS SELECT ID, COUNT(*), SUM(PRICE) \n" +
                " FROM NONSWAP_VIEWED_1 GROUP BY ID;\n"
            },
            { "NONSWAP_VIEWED_2",
                " (\n" +
                "  NAME VARCHAR(32 BYTES) NOT NULL,\n" +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n" +
                "CREATE TABLE JOINED_2 (DISCOUNT FLOAT, ID INTEGER);\n" +
                "CREATE VIEW VIEWING_2 AS SELECT A.ID, COUNT(*), SUM(A.PRICE*B.DISCOUNT) \n" +
                "FROM NONSWAP_VIEWED_2 A, JOINED_2 B WHERE A.ID = B.ID GROUP BY A.ID;\n"
            },
            { "NONSWAP_VIEWED_3",
                " (\n" +
                "  NAME VARCHAR(32 BYTES) NOT NULL,\n" +
                "  PRICE FLOAT," +
                "  NONID INTEGER NOT NULL," +
                "  ID INTEGER " +
                ");\n" +
                "CREATE TABLE JOINED_3 (DISCOUNT FLOAT, ID INTEGER);\n" +
                "CREATE VIEW VIEWING_3A AS SELECT ID, COUNT(*), SUM(PRICE) \n" +
                " FROM NONSWAP_VIEWED_3 GROUP BY ID;\n" +
                "CREATE VIEW VIEWING_3B AS SELECT A.ID, COUNT(*), SUM(A.PRICE*B.DISCOUNT) \n" +
                " FROM NONSWAP_VIEWED_3 A, JOINED_3 B WHERE A.ID = B.ID GROUP BY A.ID;\n",
            },
            { "NONSWAP_NO_MATCHING_COLUMNS",
              " (\n" +
              "  FOO FLOAT NOT NULL " +
              ");\n",
            },

    };

    public TestSystemProcedureSuite(String name) {
        super(name);
    }

    private static void addSwappyTables(String prefix, VoltProjectBuilder project)
            throws Exception {
        StringBuilder schema = new StringBuilder();
        for (String[] tableDefPart : SWAPPY_TABLES) {
            String tableName = prefix + tableDefPart[0];
            String internalExtras = String.format(tableDefPart[1], tableName);
            String externalExtras = String.format(tableDefPart[2], prefix, prefix, prefix, prefix, prefix);
            schema.append("CREATE TABLE ").append(tableName)
            .append(" (\n" +
                    "  NAME VARCHAR(32 BYTES) NOT NULL,\n" +
                    "  TS TIMESTAMP DEFAULT NOW() NOT NULL," +
                    "  PRICE FLOAT," +
                    "  NONID INTEGER NOT NULL," +
                    "  ID INTEGER NOT NULL").append(internalExtras)
            .append(") USING TTL 10 SECONDS ON COLUMN TS;\n")
            .append(externalExtras);
        }
        //*enable to debug*/ System.out.println(schema.toString());
        project.addLiteralSchema(schema.toString());
    }

    // Non-swappy tables are all incompatible for swapping with SWAP_THIS_NORMAL
    // due to various column differences or from their defined views.
    private static void addNonSwappyTables(VoltProjectBuilder project)
    throws Exception {
        StringBuilder schema = new StringBuilder();
        for (String[] tableDefPart : NONSWAPPY_TABLES) {
            schema.append("CREATE TABLE ")
            .append(tableDefPart[0])
            .append(tableDefPart[1]);
        }
        //*enable to debug*/ System.out.println(schema.toString());
        project.addLiteralSchema(schema.toString());
    }

    public void testPing() throws IOException, ProcCallException {
        Client client = getClient();
        ClientResponse cr = client.callProcedure("@Ping");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    public void testInvalidProcedureName() throws IOException {
        Client client = getClient();
        try {
            client.callProcedure("@SomeInvalidSysProcName", "1", "2");
        }
        catch (Exception e2) {
            assertEquals("Procedure @SomeInvalidSysProcName was not found", e2.getMessage());
            return;
        }
        fail("Expected exception.");
    }

    private final String m_loggingConfig =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" +
        "<!DOCTYPE log4j:configuration SYSTEM \"log4j.dtd\">" +
        "<log4j:configuration xmlns:log4j=\"http://jakarta.apache.org/log4j/\">" +
            "<appender name=\"Console\" class=\"org.apache.log4j.ConsoleAppender\">" +
                "<param name=\"Target\" value=\"System.out\" />" +
                "<layout class=\"org.apache.log4j.TTCCLayout\">" +
                "</layout>" +
            "</appender>" +
            "<appender name=\"Async\" class=\"org.apache.log4j.AsyncAppender\">" +
                "<param name=\"Blocking\" value=\"true\" />" +
                "<appender-ref ref=\"Console\" /> " +
            "</appender>" +
            "<root>" +
               "<priority value=\"info\" />" +
               "<appender-ref ref=\"Async\" />" +
            "</root>" +
        "</log4j:configuration>";

    public void testUpdateLogging() throws Exception {
        Client client = getClient();
        VoltTable results[] = null;
        results = client.callProcedure("@UpdateLogging", m_loggingConfig).getResults();
        for (VoltTable result : results) {
            assertEquals( 0, result.asScalarLong());
        }
    }

    public void testPromoteMaster() throws Exception {
        Client client = getClient();
        try {
            client.callProcedure("@Promote");
            fail("ORLY @Promote succeeded?");
        }
        catch (ProcCallException pce) {
            assertEquals(ClientResponse.GRACEFUL_FAILURE, pce.getClientResponse().getStatus());
        }
    }

    // Pretty lame test but at least invoke the procedure.
    // "@Quiesce" is used more meaningfully in TestExportSuite.
    public void testQuiesce() throws IOException, ProcCallException {
        Client client = getClient();
        VoltTable results[] = client.callProcedure("@Quiesce").getResults();
        assertEquals(1, results.length);
        results[0].advanceRow();
        assertEquals(results[0].get(0, VoltType.BIGINT), new Long(0));
    }

    public void testLoadMultipartitionTableProceduresInsertPartitionTable() throws Exception{
        // using insert for @Load*Table
        byte upsertMode = (byte) 0;
        Client client = getClient();
        // should not be able to upsert to new_order since it has no primary key
        try {
            client.callProcedure("@LoadMultipartitionTable", "new_order",  upsertMode, null);
            fail("ORLY @LoadMultipartitionTable new_order succeeded w/o a primary key?");
        }
        catch (ProcCallException ex) {
            assertTrue(ex.getMessage().contains("LoadMultipartitionTable no longer supports loading partitioned tables"));
        }
    }

    public void testLoadMultipartitionTableProceduresUpsertWithNoPrimaryKey() throws Exception{
        // using upsert for @Load*Table
        byte upsertMode = (byte) 1;
        Client client = getClient();
        // should not be able to upsert to PAUSE_TEST_TBL since it has no primary key
        try {
            client.callProcedure("@LoadMultipartitionTable", "PAUSE_TEST_TBL",  upsertMode, null);
            fail("ORLY @LoadMultipartitionTable PAUSE_TEST_TBL succeeded w/o a primary key?");
        }
        catch (ProcCallException ex) {
            assertTrue(ex.getMessage().contains("the table PAUSE_TEST_TBL does not have a primary key"));
        }
    }

    public void testLoadMultipartitionTableAndIndexStatsAndValidatePartitioning() throws Exception {
        // using insert for @Load*Table
        byte upsertMode = (byte) 0;
        Client client = getClient();

        // Load a little partitioned data for the mispartitioned row check
        Random r = new Random(0);
        for (int ii = 0; ii < 50; ii++) {
            client.callProcedure(new NullCallback(), "@AdHoc",
                    "INSERT INTO new_order values (" + (short)(r.nextDouble() * Short.MAX_VALUE) + ");");
        }

        // try the failure case first
        try {
            client.callProcedure("@LoadMultipartitionTable", "DOES_NOT_EXIST", upsertMode, null);
            fail("ORLY - @LoadMultipartitionTable DOES_NOT_EXIST succeeds");
        }
        catch (ProcCallException ex) {
            assertTrue(ex.getMessage().contains("Table not present in catalog"));
        }

        // make a TPCC warehouse table
        VoltTable partitioned_table = new VoltTable(
                new VoltTable.ColumnInfo("W_ID", org.voltdb.VoltType.SMALLINT),
                new VoltTable.ColumnInfo("W_NAME", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STREET_1", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STREET_2", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_CITY", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_STATE", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_ZIP", org.voltdb.VoltType.get((byte)9)),
                new VoltTable.ColumnInfo("W_TAX",org.voltdb.VoltType.get((byte)8)),
                new VoltTable.ColumnInfo("W_YTD", org.voltdb.VoltType.get((byte)8))
        );

        for (int i = 1; i < 21; i++) {
            Object[] row = new Object[] {new Short((short) i),
                                         "name_" + i,
                                         "street1_" + i,
                                         "street2_" + i,
                                         "city_" + i,
                                         "ma",
                                         "zip_"  + i,
                                         new Double(i),
                                         new Double(i)};
            partitioned_table.addRow(row);
        }

        // make a TPCC item table
        VoltTable replicated_table =
            new VoltTable(new VoltTable.ColumnInfo("I_ID", VoltType.INTEGER),
                          new VoltTable.ColumnInfo("I_IM_ID", VoltType.INTEGER),
                          new VoltTable.ColumnInfo("I_NAME", VoltType.STRING),
                          new VoltTable.ColumnInfo("I_PRICE", VoltType.FLOAT),
                          new VoltTable.ColumnInfo("I_DATA", VoltType.STRING));

        for (int i = 1; i < 21; i++) {
            Object[] row = new Object[] {i,
                                         i,
                                         "name_" + i,
                                         new Double(i),
                                         "data_"  + i};

            replicated_table.addRow(row);
        }

        try {
            try {
                client.callProcedure("@LoadMultipartitionTable", "WAREHOUSE",
                            upsertMode, partitioned_table);
                fail("ORLY - @LoadMultipartitionTable succeeds on partitioned WAREHOUSE table?");
            }
            catch (ProcCallException ex) {
                assertTrue(ex.getMessage().contains(
                        "LoadMultipartitionTable no longer supports loading partitioned tables"));
            }
            client.callProcedure("@LoadMultipartitionTable", "ITEM",
                            upsertMode, replicated_table);

            // 20 rows per site for the replicated table.  Wait for it...
            int rowcount = 0;
            VoltTable results[] = client.callProcedure("@Statistics", "table", 0).getResults();
            while (rowcount != (20 * SITES * HOSTS)) {
                rowcount = 0;
                results = client.callProcedure("@Statistics", "table", 0).getResults();
                // Check that tables loaded correctly
                while(results[0].advanceRow()) {
                    if (results[0].getString("TABLE_NAME").equals("ITEM")) {
                        rowcount += results[0].getLong("TUPLE_COUNT");
                    }
                }
            }

            //*enable to debug*/ System.out.println(results[0]);

            // Check that tables loaded correctly
            int foundItem = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while(results[0].advanceRow()) {
                if (results[0].getString("TABLE_NAME").equals("ITEM")) {
                    ++foundItem;
                    //Different values depending on local cluster vs. single process hence ||
                    assertEquals(20, results[0].getLong("TUPLE_COUNT"));
                }
            }
            assertEquals(MiscUtils.isPro() ? 6 : 3, foundItem);

            // Table finally loaded fully should mean that index is okay on first read.
            VoltTable indexStats =
                    client.callProcedure("@Statistics", "INDEX", 0).getResults()[0];
            //*enable to debug*/ System.out.println(indexStats);
            long memorySum = 0;
            while (indexStats.advanceRow()) {
                memorySum += indexStats.getLong("MEMORY_ESTIMATE");
            }

            //
            // It takes about a minute to spin through this 1000 times.
            // Should definitely give a 1 second tick time to fire
            //
            long indexMemorySum = 0;
            for (int ii = 0; ii < 1000; ii++) {
                indexMemorySum = 0;
                indexStats = client.callProcedure("@Statistics", "MEMORY", 0).getResults()[0];
                //*enable to debug*/ System.out.println(indexStats);
                while (indexStats.advanceRow()) {
                    indexMemorySum += indexStats.getLong("INDEXMEMORY");
                }
                boolean success = indexMemorySum != 120;//That is a row count, not memory usage
                if (success) {
                    success = memorySum == indexMemorySum;
                    if (success) {
                        break;
                    }
                }
                Thread.sleep(1);
            }
            assertTrue(indexMemorySum != 120);//That is a row count, not memory usage
            // only have one copy of index of replicated table Per Host
            assertEquals(memorySum / SITES, indexMemorySum);

            //
            // Test once using the current correct hash function,
            // expect no mispartitioned rows
            //
            ClientResponse cr = client.callProcedure("@ValidatePartitioning", (Object)null);

            VoltTable hashinatorMatches = cr.getResults()[1];
            hashinatorMatches.advanceRow();

            while (hashinatorMatches.advanceRow()) {
                assertEquals(1L, hashinatorMatches.getLong("HASHINATOR_MATCHES"));
            }

            VoltTable validateResult = cr.getResults()[0];
            //*enable to debug*/ System.out.println(validateResult);
            while (validateResult.advanceRow()) {
                assertEquals(0L, validateResult.getLong("MISPARTITIONED_ROWS"));
            }

            //
            // Test again with a bad hash function, expect mispartitioned rows
            //
            cr = client.callProcedure("@ValidatePartitioning", new byte[] { 0, 0, 0, 9 });

            hashinatorMatches = cr.getResults()[1];
            while (hashinatorMatches.advanceRow()) {
                assertEquals(0L, hashinatorMatches.getLong("HASHINATOR_MATCHES"));
            }

            validateResult = cr.getResults()[0];
            //*enable to debug*/ System.out.println(validateResult);
            long mispart = 0;
            while (validateResult.advanceRow()) {
                if (validateResult.getString("TABLE").equals("NEW_ORDER")) {
                    long part  = validateResult.getLong("MISPARTITIONED_ROWS");
                    if (part > 0) {
                        mispart = part;
                    }
                }
            }
            assertTrue(mispart > 0);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
    }

    // verify that these commands don't blow up
    public void testProfCtl() throws Exception {
        Client client = getClient();

        //
        // SAMPLER_START
        //
        ClientResponse resp = client.callProcedure("@ProfCtl", "SAMPLER_START");
        VoltTable vt = resp.getResults()[0];
        boolean foundResponse = false;
        while (vt.advanceRow()) {
            String profCtlResult = vt.getString("Result");
            if ("SAMPLER_START".equalsIgnoreCase(profCtlResult)) {
                foundResponse = true;
            }
            else {
                fail("Was not expecting @ProfCtl result: " + profCtlResult);
            }
        }
        assertTrue(foundResponse);

        //
        // GPERF_ENABLE
        //
        resp = client.callProcedure("@ProfCtl", "GPERF_ENABLE");
        vt = resp.getResults()[0];
        foundResponse = false;
        while (vt.advanceRow()) {
            String profCtlResult = vt.getString("Result");
            if ("GPERF_ENABLE".equalsIgnoreCase(profCtlResult)) {
                foundResponse = true;
            }
            else {
                assertTrue("GPERF_NOOP".equalsIgnoreCase(profCtlResult));
            }
        }
        assertTrue(foundResponse);

        //
        // GPERF_DISABLE
        //
        resp = client.callProcedure("@ProfCtl", "GPERF_DISABLE");
        vt = resp.getResults()[0];
        foundResponse = false;
        while (vt.advanceRow()) {
            String profCtlResult = vt.getString("Result");
            if ("GPERF_DISABLE".equalsIgnoreCase(profCtlResult)) {
                foundResponse = true;
            }
            else {
                assertTrue("GPERF_NOOP".equalsIgnoreCase(profCtlResult));
            }
        }
        assertTrue(foundResponse);

        //
        // garbage
        //
        resp = client.callProcedure("@ProfCtl", "MakeAPony");
        vt = resp.getResults()[0];
    }

    public void testPause() throws Exception {
        Client client = getClient();
        VoltTable[] results = client.callProcedure("@UpdateLogging", m_loggingConfig).getResults();
        for (VoltTable result : results) {
            assertEquals(0, result.asScalarLong());
        }

        ClientResponse resp = client.callProcedure("pauseTestInsert");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        client.callProcedure("@AdHoc", "INSERT INTO pause_test_tbl values (10);");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        // pause
        Client admin = getAdminClient();
        resp = admin.callProcedure("@Pause");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        try {
            client.callProcedure("@AdHoc", "INSERT INTO pause_test_tbl values (20);");
            fail("AdHoc insert did not fail in pause mode");
        }
        catch (ProcCallException ex) {
            assertEquals(ClientResponse.SERVER_UNAVAILABLE, ex.getClientResponse().getStatus());
        }
        try {
            client.callProcedure("@AdHoc", "CREATE TABLE ddl_test1 (fld1 integer NOT NULL);");
            fail("AdHoc create did not fail in pause mode");
        }
        catch (ProcCallException ex) {
            assertTrue(ex.getMessage().contains("Server is paused"));
            assertEquals(ClientResponse.SERVER_UNAVAILABLE, ex.getClientResponse().getStatus());
        }
        try {
            client.callProcedure("@AdHoc", "DROP TABLE pause_test_tbl;");
            fail("AdHoc drop did not fail in pause mode");
        }
        catch (ProcCallException ex) {
            assertTrue(ex.getMessage().contains("Server is paused"));
            assertEquals(ClientResponse.SERVER_UNAVAILABLE, ex.getClientResponse().getStatus());
        }
        try {
            client.callProcedure("@AdHoc", "CREATE PROCEDURE pause_test_proc AS SELECT * FROM pause_test_tbl;");
            fail("AdHoc create proc did not fail in pause mode");
        }
        catch (ProcCallException ex) {
            assertTrue(ex.getMessage().contains("Server is paused"));
            assertEquals(ClientResponse.SERVER_UNAVAILABLE, ex.getClientResponse().getStatus());
        }

        // admin should work fine
        admin.callProcedure("@AdHoc", "INSERT INTO pause_test_tbl values (20);");
        admin.callProcedure("@AdHoc", "CREATE TABLE ddl_test1 (fld1 integer NOT NULL);");
        admin.callProcedure("@AdHoc", "CREATE PROCEDURE pause_test_proc AS SELECT * FROM pause_test_tbl;");
        admin.callProcedure("@AdHoc", "DROP TABLE ddl_test1;");
        admin.callProcedure("@AdHoc", "DROP PROCEDURE pause_test_proc;");

        try {
            resp = client.callProcedure("@UpdateLogging", m_loggingConfig);
            fail();
        }
        catch (ProcCallException ex) {
            assertEquals(ClientResponse.SERVER_UNAVAILABLE, ex.getClientResponse().getStatus());
        }

        try {
            resp = client.callProcedure("pauseTestInsert");
            fail();
        }
        catch (ProcCallException ex) {
            assertEquals(ClientResponse.SERVER_UNAVAILABLE, ex.getClientResponse().getStatus());
        }

        resp = client.callProcedure("@Ping");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        resp = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM pause_test_tbl");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(3, resp.getResults()[0].asScalarLong());
        resp = client.callProcedure("pauseTestCount");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        assertEquals(3, resp.getResults()[0].asScalarLong());

        // resume
        resp = admin.callProcedure("@Resume");
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());

        results = client.callProcedure("@UpdateLogging", m_loggingConfig).getResults();
        for (VoltTable result : results) {
            assertEquals(0, result.asScalarLong());
        }
    }

    private static final Object[][] THE_SWAP_CONTENTS =
        {
            {"1", new TimestampType(), 1.0, 1, 1}
        };
    private static final int THE_SWAP_COUNT = 1;
    private static final Object[][] OTHER_SWAP_CONTENTS =
        {
            {"2", new TimestampType(), null, 2, 2},
            {"3", new TimestampType(), 3.0, 3, 3}
        };
    private static final int OTHER_SWAP_COUNT = 2;

    private void populateSwappyTables(Client client, String thisTable, String thatTable) throws Exception {
        for (Object[] row : THE_SWAP_CONTENTS) {
            client.callProcedure(thisTable + ".insert", row);
        }

        for (Object[] row : OTHER_SWAP_CONTENTS) {
            client.callProcedure(thatTable + ".insert", row);
        }
    }

    public void testSwapTables() throws Exception {
        Client client = getClient();

        for (int ii = 0; ii < SWAPPY_TABLES.length; ++ii) {
            String theTable = SWAPPY_PREFIX_PAIR[0] + SWAPPY_TABLES[ii][0];

            for (int jj = 0; jj < SWAPPY_TABLES.length; ++jj) {
                // Allow swapping only for identically defined tables
                // or those with minor variations in syntax.
                String otherTable = SWAPPY_PREFIX_PAIR[1] + SWAPPY_TABLES[jj][0];

                //*enable to debug*/ System.out.println("ii = " + ii + ", jj = " +jj);
                //*enable to debug*/ System.out.println("@SwapTables " + theTable + " " + otherTable);

                // These pairs of numbers are indexes into SWAPPY_TABLES that
                // represent definitions that are "close enough" for swapping
                // purposes. These were experimentally discovered and then
                // verified for reasonableness.
                if ((ii == jj) ||
                        (ii == 2 && jj == 3) || (ii == 3 && jj == 2) ||
                        (ii == 2 && jj == 4) || (ii == 4 && jj == 2) ||
                        (ii == 3 && jj == 4) || (ii == 4 && jj == 3) ||
                        (ii == 5 && jj == 6) || (ii == 6 && jj == 5) ||
                        (ii == 5 && jj == 7) || (ii == 7 && jj == 5) ||
                        (ii == 6 && jj == 7) || (ii == 7 && jj == 6) ||
                        (ii == 9 && jj == 10) || (ii == 10 && jj == 9) ||
                        (ii == 9 && jj == 11) || (ii == 11 && jj == 9) ||
                        (ii == 10 && jj == 11) || (ii == 11 && jj == 10)) {

                    VoltTable[] results;

                    populateSwappyTables(client, theTable, otherTable);

                    results = client.callProcedure("@SwapTables",
                            theTable, otherTable).getResults();
                    //*enable to debug*/ System.out.println(results[0]);
                    assertNotNull(results);
                    assertEquals(1, results.length);
                    assertEquals(3, results[0].asScalarLong());

                    VoltTable contents = client.callProcedure("@AdHoc", "select * from " + theTable + " order by id").getResults()[0];
                    assertContentOfTable(OTHER_SWAP_CONTENTS, contents);

                    contents = client.callProcedure("@AdHoc", "select * from " + otherTable + " order by id").getResults()[0];
                    assertContentOfTable(THE_SWAP_CONTENTS, contents);

                    // Just to verify the statistics are correct for normal case
                    // In some of the pairs we are involving partitioned tables where the
                    // statistics we get from some partitions may be zero.
                    if (ii == jj && ii == 0) {
                        // Hacky, we need to sleep long enough so the internal server tick
                        // updates the memory stats.
                        Thread.sleep(1000);
                        VoltTable stats = client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
                        while (stats.advanceRow()) {
                            if (stats.getString("TABLE_NAME").equals(theTable)) {
                                assertEquals(OTHER_SWAP_COUNT, stats.getLong("TUPLE_COUNT"));
                            }
                            else if (stats.getString("TABLE_NAME").equals(otherTable)) {
                                assertEquals(THE_SWAP_COUNT, stats.getLong("TUPLE_COUNT"));
                            }
                        }
                    }

                    // Swap again to restore the baseline populations.
                    results = client.callProcedure("@SwapTables",
                            otherTable, theTable).getResults();
                    //*enable to debug*/ System.out.println(results[0]);
                    assertNotNull(results);
                    assertEquals(1, results.length);
                    assertEquals(3, results[0].asScalarLong());

                    // Verify that baseline is restored
                    contents = client.callProcedure("@AdHoc", "select * from " + theTable + " order by id").getResults()[0];
                    assertContentOfTable(THE_SWAP_CONTENTS, contents);

                    contents = client.callProcedure("@AdHoc", "select * from " + otherTable + " order by id").getResults()[0];
                    assertContentOfTable(OTHER_SWAP_CONTENTS, contents);

                    // Just to verify the statistics are correct for normal case
                    if (ii == jj && ii == 0) {
                        // Hacky, we need to sleep long enough so the internal server tick
                        // updates the memory stats.
                        Thread.sleep(1000);
                        VoltTable stats = client.callProcedure("@Statistics", "TABLE", 0).getResults()[0];
                        while (stats.advanceRow()) {
                            if (stats.getString("TABLE_NAME").equals(theTable)) {
                                assertEquals(THE_SWAP_COUNT, stats.getLong("TUPLE_COUNT"));
                            }
                            else if (stats.getString("TABLE_NAME").equals(otherTable)) {
                                assertEquals(OTHER_SWAP_COUNT, stats.getLong("TUPLE_COUNT"));
                            }
                        }
                    }

                    results = client.callProcedure("@AdHoc",
                            "TRUNCATE TABLE " + theTable).getResults();
                    assertEquals(1, results[0].asScalarLong());

                    // Try a swap with one empty table.
                    results = client.callProcedure("@SwapTables",
                            otherTable, theTable).getResults();
                    assertNotNull(results);
                    assertEquals(1, results.length);
                    assertEquals(2, results[0].asScalarLong());

                    contents = client.callProcedure("@AdHoc", "select * from " + theTable + " order by id").getResults()[0];
                    assertContentOfTable(OTHER_SWAP_CONTENTS, contents);

                    contents = client.callProcedure("@AdHoc", "select * from " + otherTable + " order by id").getResults()[0];
                    assertContentOfTable(new Object[][] {}, contents);

                    results = client.callProcedure("@AdHoc",
                            "TRUNCATE TABLE " + theTable).getResults();
                    assertEquals(2, results[0].asScalarLong());

                    contents = client.callProcedure("@AdHoc", "select * from " + theTable + " order by id").getResults()[0];
                    assertContentOfTable(new Object[][] {}, contents);

                    contents = client.callProcedure("@AdHoc", "select * from " + otherTable + " order by id").getResults()[0];
                    assertContentOfTable(new Object[][] {}, contents);

                    // Try a swap with both empty tables.
                    results = client.callProcedure("@SwapTables",
                            otherTable, theTable).getResults();
                    assertNotNull(results);
                    assertEquals(1, results.length);
                    assertEquals(0, results[0].asScalarLong());

                    continue;
                }

                try {

                    client.callProcedure("@SwapTables",
                            theTable, otherTable);
                    fail("Swap should have conflicted on table definitions " +
                            ii + " and " + jj + " : " +
                            theTable + " with " + otherTable);
                }
                catch (ProcCallException ex) {
                    if (! ex.getMessage().contains("Swapping")) {
                        System.out.println("sup w/ incompatible tables " +
                                ii + " and " + jj + ":" + ex);
                    }
                    assertTrue("Expected message about swapping but got " + ex.getMessage(),
                            ex.getMessage().contains("Swapping"));
                }
            }
        }
    }

    /*
     * We used to allow client.callProcedure("@AdHoc", "@SwapTables a b");
     * This was in error, and we now have a guard for it.  This tests that
     * the guard actually does what we expect it to do.
     */
    public void testAdhocSwapTables() throws Exception {
        Client client = getClient();

        try {
            client.callProcedure("@AdHoc", "@SwapTables SWAP_THIS SWAP_THAT");
            fail("Unexpected @AdHoc success.");
        } catch (ProcCallException ex) {
            assertTrue(ex.getMessage().contains("Error in \"@SwapTables SWAP_THIS SWAP_THAT\" - unknown token"));
        }
    }

    public void testOneOffNegativeSwapTables() throws Exception {
        Client client = getClient();

        String tableA = SWAPPY_PREFIX_PAIR[0] + SWAPPY_TABLES[0][0];
        for (int ii = 0; ii < NONSWAPPY_TABLES.length; ++ii) {
            String tableB = NONSWAPPY_TABLES[ii][0];
            try {
                client.callProcedure("@SwapTables", tableA, tableB);
                fail("Swap should have conflicted on table definitions " +
                        tableA + " and " + tableB);
            }
            catch (ProcCallException ex) {
                if (! ex.getMessage().contains("Swapping")) {
                    System.out.println("sup w/ these incompatible tables(" +
                            tableA + " and " + tableB + "): " + ex);
                }
                assertTrue(ex.getMessage().contains("Swapping"));
            }

            // Try reversing the arguments. Incompatibility should be mutual.
            try {
                client.callProcedure("@SwapTables", tableB, tableA);
                fail("Swap should have conflicted on table definitions " +
                        tableB + " and " + tableA);
            }
            catch (ProcCallException ex) {
                if (! ex.getMessage().contains("Swapping")) {
                    System.out.println("sup w/ these incompatible tables(" +
                            tableB + " and " + tableA + "): " + ex);
                }
                assertTrue(ex.getMessage().contains("Swapping"));
            }
        }
    }

    public void testSwapTablesWithExport() throws Exception {
        Client client = getClient();
        try {
            client.callProcedure("@SwapTables", "MIGRATE1", "MIGRATE2");
            fail("Swap should not be allowed between migrate tables.");
        } catch (ProcCallException ex) {
            assertTrue(ex.getMessage().contains("exporting"));
        }
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws Exception {
        // Not really using TPCC functionality but need a schema.
        // The testLoadMultipartitionTable procedure assumes partitioning
        // on warehouse id.
        VoltProjectBuilder project = new VoltProjectBuilder();
        String literalSchema =
                "CREATE TABLE WAREHOUSE (\n" +
                "  W_ID SMALLINT DEFAULT 0 NOT NULL,\n" +
                "  W_NAME VARCHAR(16),\n" +
                "  W_STREET_1 VARCHAR(32),\n" +
                "  W_STREET_2 VARCHAR(32),\n" +
                "  W_CITY VARCHAR(32),\n" +
                "  W_STATE VARCHAR(2),\n" +
                "  W_ZIP VARCHAR(9),\n" +
                "  W_TAX FLOAT,\n" +
                "  W_YTD FLOAT,\n" +
                "  CONSTRAINT W_PK_TREE PRIMARY KEY (W_ID)\n" +
                ");\n" +
                "CREATE TABLE ITEM (\n" +
                "  I_ID INTEGER DEFAULT 0 NOT NULL,\n" +
                "  I_IM_ID INTEGER,\n" +
                "  I_NAME VARCHAR(32),\n" +
                "  I_PRICE FLOAT,\n" +
                "  I_DATA VARCHAR(64),\n" +
                "  CONSTRAINT I_PK_TREE PRIMARY KEY (I_ID)\n" +
                ");\n" +
                "CREATE TABLE NEW_ORDER (\n" +
                "  NO_W_ID SMALLINT DEFAULT 0 NOT NULL\n" +
                ");\n" +
                "CREATE TABLE PAUSE_TEST_TBL (\n" +
                "  TEST_ID SMALLINT DEFAULT 0 NOT NULL\n" +
                ");\n" +
                "CREATE table MIGRATE1 MIGRATE to TARGET FOO (" +
                        "PKEY          INTEGER          NOT NULL);\n" +
                "CREATE table MIGRATE2 MIGRATE to TARGET FOO (" +
                        "PKEY          INTEGER          NOT NULL);\n";
        project.addLiteralSchema(literalSchema);

        // testSwapTables needs lots of variations on the same table,
        // including duplicates, to test compatibility criteria.
        for (String prefix : SWAPPY_PREFIX_PAIR) {
            addSwappyTables(prefix, project);
        }
        addNonSwappyTables(project);

        project.setUseDDLSchema(true);
        project.addPartitionInfo("WAREHOUSE", "W_ID");
        project.addPartitionInfo("NEW_ORDER", "NO_W_ID");
        project.addProcedure(GoSleep.class);
        project.addStmtProcedure("pauseTestCount", "SELECT COUNT(*) FROM pause_test_tbl");
        project.addStmtProcedure("pauseTestInsert", "INSERT INTO pause_test_tbl VALUES (1)");

        MultiConfigSuiteBuilder builder =
                new MultiConfigSuiteBuilder(TestSystemProcedureSuite.class);
        Map<String, String> additionalEnv = new HashMap<String, String>();
        System.setProperty("TIME_TO_LIVE_DELAY", "60000");
        additionalEnv.put("TIME_TO_LIVE_DELAY", "60000");

        LocalCluster config = new LocalCluster("sysproc-cluster.jar", SITES, HOSTS, KFACTOR,
               BackendTarget.NATIVE_EE_JNI, additionalEnv);
        if ( ! hasLocalServer ) {
            config.setHasLocalServer(false);
        }
        // boolean success = config.compile(project);
        boolean success = config.compileWithAdminMode(project, -1, false);
        assertTrue(success);
        builder.addServerConfig(config);
        return builder;
    }
}
