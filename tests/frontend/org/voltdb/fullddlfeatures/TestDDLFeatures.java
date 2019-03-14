/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.fullddlfeatures;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.net.URLDecoder;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.utils.MiscUtils;

public class TestDDLFeatures extends AdhocDDLTestBase {

    String catalogJar = "DDLFeature.jar";
    String pathToCatalog = Configuration.getPathToCatalogForTest("DDLFeature.jar");
    String pathToDeployment = Configuration.getPathToCatalogForTest("DDLFeature.xml");

    VoltProjectBuilder builder = new VoltProjectBuilder();

    @Before
    public void setUp() throws Exception
    {
        final URL url = TestDDLFeatures.class.getResource("fullDDL.sql");
        String schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        builder.addSchema(schemaPath);
        builder.setUseDDLSchema(true);

        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);
    }

    @After
    public void tearDown() throws Exception
    {
        teardownSystem();
    }

    /**
     * A (public) method used to start the client, without also starting the
     * server; needed by FullDdlSqlTest.java, in the GEB/VMC tests.
     */
    public void startClient() throws Exception
    {
        startClient(null);
    }

    @Test
    public void testCreateUniqueIndex() throws Exception {
        assertTrue(findTableInSystemCatalogResults("T1"));
        assertTrue(findIndexInSystemCatalogResults("area"));
        assertTrue(verifyIndexUniqueness("area", true));
        assertEquals(indexedColumnCount("T1"), 3);
    }

    @Test
    public void testCreateAssumeUniqueIndex() throws Exception {
        assertTrue(findTableInSystemCatalogResults("T2"));
        assertTrue(findIndexInSystemCatalogResults("absVal"));
        assertTrue(verifyIndexUniqueness("absVal", true));
        assertEquals(indexedColumnCount("T2"), 2);
    }

    @Test
    public void testCreateHashIndex() throws Exception {
        assertTrue(findTableInSystemCatalogResults("T3"));
        assertTrue(findIndexInSystemCatalogResults("nomeaninghashweirdidx"));
        assertTrue(findIndexInSystemCatalogResults("abs_Hash_idx"));
        assertEquals(indexedColumnCount("T3"), 4);
    }

    @Test
    public void testCreateProcedureAsSQLStmt() throws Exception {
        assertTrue(findTableInSystemCatalogResults("User"));

        ClientResponse p1Result, p2Result;
        VoltTable vt1, vt2;

        p1Result = m_client.callProcedure("p1", "18");
        vt1 = p1Result.getResults()[0];
        assertEquals(vt1.getRowCount(), 0);

        p2Result = m_client.callProcedure("p2", 18, "Kevin Durant");
        vt2 = p2Result.getResults()[0];
        assertEquals(vt2.getRowCount(), 1);

        p1Result = m_client.callProcedure("p1", "18");
        vt1 = p1Result.getResults()[0];
        vt1.advanceToRow(0);

        assertEquals(vt1.getRowCount(), 1);
        assertEquals(vt1.getLong(0), 1);
        assertEquals(vt1.getString("NAME"), "Kevin Durant");

        // ENG-14210 more than 1025 parameters
        StringBuilder tooManyParmsProcBuilder = new StringBuilder();
        tooManyParmsProcBuilder.append("CREATE PROCEDURE ENG14210 AS SELECT * FROM T3 WHERE str IN (")
                               .append(String.join(",", Collections.nCopies(1200, "?")))
                               .append(");");

        RegressionSuite.verifyProcFails(m_client, "The statement's parameter count 1200 must not exceed the maximum 1025",
                "@AdHoc", tooManyParmsProcBuilder.toString());

        // ENG-14487 truncate statement is not allowed for single partitioned procedures.
        String ENG14487 = "CREATE PROCEDURE ENG14487 PARTITION ON TABLE T2 COLUMN area\n" +
                "   AS BEGIN\n" +
                "      select * from t2 where area=?;\n" +
                "      truncate table t2;\n" +
                "   END;";
        RegressionSuite.verifyProcFails(m_client,
                "Single partitioned procedure: ENG14487 has TRUNCATE statement: \"truncate table t2\"",
                "@AdHoc", ENG14487);

        ENG14487 = "CREATE PROCEDURE ENG14487\n" +
                "   AS BEGIN\n" +
                "      select * from t2 where area=?;\n" +
                "      truncate table t2;\n" +
                "   END;";
        ClientResponse cr = m_client.callProcedure("@AdHoc", ENG14487);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        RegressionSuite.verifyProcFails(m_client,
                "Single partitioned procedure: ENG14487 has TRUNCATE statement: \"truncate table t2\"",
                "@AdHoc", "PARTITION PROCEDURE ENG14487 ON TABLE T2 COLUMN area;");
    }

    @Test
    public void testCreateMultiStmtProcedureAsSQLStmt() throws Exception {
        assertTrue(findTableInSystemCatalogResults("T26"));
        assertTrue(isColumnPartitionColumn("T26", "age"));

        ClientResponse resp;
        VoltTable vt;

        m_client.callProcedure("@AdHoc", "DELETE FROM T26;");
        // multi partitioned query with 2 statements
        resp = m_client.callProcedure("msp1", 19, 0);
        vt = resp.getResults()[0];
        assertEquals(vt.getRowCount(), 1);
        vt = resp.getResults()[1];
        vt.advanceToRow(0);
        assertEquals(19l, vt.get(0, VoltType.BIGINT));
        assertEquals((byte)0, vt.get(1, VoltType.TINYINT));

        m_client.callProcedure("T26.insert", 19, 1);
        m_client.callProcedure("T26.insert", 19, 0);
        m_client.callProcedure("T26.insert", 20, 0);

        // single partitioned query with 3 statements
        resp = m_client.callProcedure("msp2", 0, 19, 20);
        vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(2l, vt.get(0, VoltType.BIGINT));
        vt = resp.getResults()[1];
        assertEquals(vt.getRowCount(), 1);
        vt = resp.getResults()[2];
        assertEquals(vt.getRowCount(), 3);
    }

    @Test
    public void testCreateProcedureFromClass() throws Exception {
        ClientResponse resp = m_client.callProcedure("testCreateProcFromClassProc", 1l, "Test", "Yuning He");
        VoltTable vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(0, VoltType.INTEGER), 1);

        // ENG-14210 more than 1025 parameters
        RegressionSuite.verifyProcFails(m_client, "The statement's parameter count 1200 must not exceed the maximum 1025",
                "@AdHoc", "CREATE PROCEDURE FROM CLASS org.voltdb_testprocs.fullddlfeatures.testJavaProcTooManyParams;");

        // ENG-14487 truncate statement is not allowed for single partitioned procedures.
        RegressionSuite.verifyProcFails(m_client,
                "Single partitioned procedure: org.voltdb_testprocs.fullddlfeatures.testSinglePartitionedTruncateProc has TRUNCATE statement: \"truncate table t2;\".",
                "@AdHoc",
                "CREATE PROCEDURE PARTITION ON TABLE T2 COLUMN area FROM CLASS org.voltdb_testprocs.fullddlfeatures.testSinglePartitionedTruncateProc;");

        ClientResponse cr = m_client.callProcedure("@AdHoc", "CREATE PROCEDURE FROM CLASS org.voltdb_testprocs.fullddlfeatures.testSinglePartitionedTruncateProc;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        RegressionSuite.verifyProcFails(m_client,
                ".*Single partitioned procedure: org.voltdb_testprocs.fullddlfeatures.testSinglePartitionedTruncateProc has TRUNCATE statement: \"truncate table t2;\"",
                "@AdHoc", "PARTITION PROCEDURE testSinglePartitionedTruncateProc ON TABLE T2 COLUMN area;");
    }

    @Test
    public void testCreateTableDataType() throws Exception {
        assertTrue(findTableInSystemCatalogResults("T4"));
        assertTrue(findTableInSystemCatalogResults("T5"));
        assertTrue(findTableInSystemCatalogResults("T6"));
        assertTrue(findTableInSystemCatalogResults("T7"));

        ClientResponse resp;
        VoltTable vt;

        m_client.callProcedure("@AdHoc", "insert into T4 values "
                + "(1, "
                + "2, "
                + "3, "
                + "4, "
                + "5.5, "
                + "6.6, "
                + "\'test\', "
                + "\'010101\', "
                + "1000, "
                + "1111, "
                + "pointfromtext('point(0 0)'),"
                + "pointfromtext('point(-2.5 0)'),"
                + "polygonfromtext('polygon((0 1, -1 1, -1 0, 0 0, 0 1))'),"
                + "polygonfromtext('polygon((0 2, -2 2, -2 0, 0 0, 0 2))')"
                + ");");

        resp = m_client.callProcedure("@AdHoc", "select * from T4;");
        vt = resp.getResults()[0];
        assertEquals(vt.getRowCount(), 1);
        vt.advanceToRow(0);
        byte ret1 = 1;
        assertEquals(vt.get(0, VoltType.TINYINT), ret1);
        assertEquals(vt.get(2, VoltType.INTEGER), 3);
        assertEquals(vt.get(4, VoltType.FLOAT), 5.5);

        m_client.callProcedure("T5.insert", "test");
        resp = m_client.callProcedure("@AdHoc", "select * from T5;");
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        m_client.callProcedure("T5.insert", "hahahaha");
        resp = m_client.callProcedure("@AdHoc", "select * from T5 order by c;");
        vt = resp.getResults()[0];
        assertEquals(vt.getRowCount(), 2);
        vt.advanceToRow(0);
        assertEquals(vt.get(0, VoltType.STRING), "hahahaha");
        vt.advanceToRow(1);
        assertEquals(vt.get(0, VoltType.STRING), "test");

        m_client.callProcedure("T6.insert", "test");
        resp = m_client.callProcedure("@AdHoc", "select * from T6;");
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        m_client.callProcedure("@AdHoc", "insert into T7 values (\'101010101010\')");
        resp = m_client.callProcedure("@AdHoc", "select * from T7;");
        assertEquals(resp.getResults()[0].getRowCount(), 1);
    }

    @Test
    public void testCreateTableConstraint() throws Exception {
        ClientResponse resp;
        boolean threw;

        // Test for T9
        assertTrue(findTableInSystemCatalogResults("T9"));
        resp = m_client.callProcedure("T9.insert", 1, 2);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T9.insert", 1, 3);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate PRIMARY KEY constraint", threw);

        threw = false;
        try {
            m_client.callProcedure("T9.insert", 2, 2);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate UNIQUE constraint", threw);

        // Test for T10
        assertTrue(findTableInSystemCatalogResults("T10"));
        resp = m_client.callProcedure("T10.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T10.insert", 1);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate UNIQUE constraint", threw);

        // Test fot T11
        assertTrue(findTableInSystemCatalogResults("T11"));
        resp = m_client.callProcedure("T11.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T11.insert", 1);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate PRIMARY KEY constraint", threw);

        // Test for T12
        assertTrue(findTableInSystemCatalogResults("T12"));
        assertTrue(isColumnPartitionColumn("T12", "C1"));
        resp = m_client.callProcedure("T12.insert", 1, 2);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            // Attempt to add an identical row: using the same partition key
            // guarantees that this will go to the same partition as the earlier
            // insert; using the the same constraint key (in the same partition)
            // means that the ASSUMEUNIQUE constraint will be violoated, so an
            // exception should be thrown
            m_client.callProcedure("T12.insert", 1, 2);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate ASSUMEUNIQUE constraint", threw);

        // Test for T16
        assertTrue(findTableInSystemCatalogResults("T16"));
        resp = m_client.callProcedure("T16.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T16.insert", 2);
        } catch (ProcCallException pce) {
            pce.printStackTrace();
            threw = true;
        }
        assertTrue("Shouldn't violate LIMIT PARTITION ROW constraint", threw);

        // Test for T21
        assertTrue(findTableInSystemCatalogResults("T21"));
        assertEquals(indexedColumnCount("T21"), 3);
        assertTrue(isColumnPartitionColumn("T21", "C3"));

        // Test for T22
        assertTrue(findTableInSystemCatalogResults("T22"));
        assertEquals(10, indexedColumnCount("T22"));

        // Test for T23
        assertTrue(findTableInSystemCatalogResults("T23"));
        assertEquals(14, indexedColumnCount("T23"));
    }

    @Test
    public void testCreateTableConstraintWithoutKeyword() throws Exception {
        ClientResponse resp;
        boolean threw;

        // Test for T17
        assertTrue(findTableInSystemCatalogResults("T17"));
        resp = m_client.callProcedure("T17.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        // Test for T18
        assertTrue(findTableInSystemCatalogResults("T18"));
        resp = m_client.callProcedure("T18.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        // Test for T19
        assertTrue(findTableInSystemCatalogResults("T19"));
        resp = m_client.callProcedure("T19.insert", 1, 2);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        // Test for T20
        assertTrue(findTableInSystemCatalogResults("T20"));
        resp = m_client.callProcedure("T20.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T20.insert", 2);
        } catch (ProcCallException pce) {
            pce.printStackTrace();
            threw = true;
        }
        assertTrue("Shouldn't violate LIMIT PARTITION ROW constraint", threw);
    }

    @Test
    public void testCreateView() throws Exception
    {
        assertTrue(findTableInSystemCatalogResults("T24"));
        assertTrue(findTableInSystemCatalogResults("VT1"));
        assertEquals(getTableType("VT1"), "VIEW");

        m_client.callProcedure("T24.insert", 1, 2);
        m_client.callProcedure("T24.insert", 1, 2);
        ClientResponse resp = m_client.callProcedure("@AdHoc", "select * from VT1");
        VoltTable vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(2, VoltType.INTEGER), 2);

        // create single table view without count(*)
        assertTrue(findTableInSystemCatalogResults("VT1B"));
        assertEquals(getTableType("VT1B"), "VIEW");


        m_client.callProcedure("T24.insert", 2, 3);
        m_client.callProcedure("T24.insert", 2, 4);
        resp = m_client.callProcedure("@AdHoc", "select * from VT1B");
        vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(1, VoltType.INTEGER), 4);
    }

    @Test
    public void testCreateStream() throws Exception
    {
        assertTrue("Stream T25 should exist", findTableInSystemCatalogResults("T25"));
        assertEquals(getTableType("T25"), "EXPORT");
        assertTrue("Stream T25R should exist", findTableInSystemCatalogResults("T25R"));
        assertEquals(getTableType("T25R"), "EXPORT");
        assertTrue("Stream T25P should exist", findTableInSystemCatalogResults("T25P"));
        assertEquals(getTableType("T25P"), "EXPORT");
    }

    @Test
    public void testDropStream() throws Exception
    {
        assertFalse("Stream T25D should NOT exist; it was DROP-ed", findTableInSystemCatalogResults("T25D"));
    }

    @Test
    public void testStreamView() throws Exception
    {
        assertTrue("Stream T25P should exist", findTableInSystemCatalogResults("T25P"));
        assertEquals("Stream T25P has wrong type", getTableType("T25P"), "EXPORT");
        assertEquals("Stream View VT25P has wrong type", getTableType("VT25P"), "VIEW");
    }

//    @Test
//    public void testImportClass() throws Exception
//    {
//        LocalCluster cluster = new LocalCluster(catalogJar, 2, 1, 1, BackendTarget.NATIVE_EE_JNI);
//        cluster.setHasLocalServer(false);
//
//        boolean success = cluster.compile(builder);
//        assertTrue(success);
//
//        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);
//
//        cluster.startUp();
//
//        Client client = ClientFactory.createClient();
//        client.createConnection("localhost");
//
//        String classpath = "org/voltdb_testprocs/fullddlfeatures/NoMeaningClass.class";
//        Process p = Runtime.getRuntime().exec("jar tf " + pathToCatalog);
//
//        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
//        String file = null;
//        boolean exist = false;
//        while ((file = in.readLine()) != null)
//        {
//            if(file.equals(classpath))
//            {
//                exist = true;
//                break;
//            }
//        }
//        assertTrue(exist);
//
//        client.close();
//        cluster.shutDown();
//    }

    @Test
    public void testPartitionProcedure() throws Exception
    {
        assertTrue(findProcedureInSystemCatalog("p4"));
        assertTrue(isColumnPartitionColumn("T26", "age"));

        ClientResponse resp;
        VoltTable vt;
        resp = m_client.callProcedure("p4", 1, 18);
        vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(0, VoltType.BIGINT), 0l);

        m_client.callProcedure("T26.insert", 18, 1);

        resp = m_client.callProcedure("p4", 1, 18);
        vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(0, VoltType.BIGINT), 1l);
    }

    @Test
    public void testDropView() throws Exception
    {
        assertFalse(findTableInSystemCatalogResults("VT000"));
        assertFalse(findTableInSystemCatalogResults("VT30A"));
        assertFalse(findTableInSystemCatalogResults("VT30B"));
    }

    @Test
    public void testDropIndex() throws Exception
    {
        assertFalse(findTableInSystemCatalogResults("abs_T31A_idx"));
        assertFalse(findIndexInSystemCatalogResults("abs_T000_idx"));
    }

    @Test
    public void testDropProcedure() throws Exception
    {
        assertFalse(findProcedureInSystemCatalog("T32A"));
        assertFalse(findProcedureInSystemCatalog("T32B"));
    }

    @Test
    public void testDropTable() throws Exception
    {
        assertFalse(findIndexInSystemCatalogResults("T33"));
        assertFalse(findIndexInSystemCatalogResults("T34"));
        assertFalse(findTableInSystemCatalogResults("VT34A"));
        assertFalse(findIndexInSystemCatalogResults("abs_T34A_idx"));
    }

    @Test
    public void testAlterTableDropConstraint() throws Exception {
        ClientResponse resp;
        boolean threw;

        // Test for T35
        assertTrue(findTableInSystemCatalogResults("T35"));
        resp = m_client.callProcedure("T35.insert", 1, 2);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T35.insert", 1, 3);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertFalse("Shouldn't violate PRIMARY KEY constraint", threw);

        threw = false;
        try {
            m_client.callProcedure("T35.insert", 2, 2);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate UNIQUE constraint", threw);
        assertEquals(indexedColumnCount("T35"), 1);

        // Test for T35A
        assertTrue(findTableInSystemCatalogResults("T35A"));
        resp = m_client.callProcedure("T35A.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T35A.insert", 1);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertFalse("Shouldn't violate LIMIT PARTITION ROWS constraint", threw);
        assertEquals(indexedColumnCount("T35A"), 0);

        // Test for T36
        assertTrue(findTableInSystemCatalogResults("T36"));
        resp = m_client.callProcedure("T36.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T36.insert", 1);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertFalse("Shouldn't violate PRIMARY KEY constraint", threw);
        assertEquals(indexedColumnCount("T36"), 0);

        // Test for T37
        assertTrue(findTableInSystemCatalogResults("T37"));
        resp = m_client.callProcedure("T37.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T37.insert", 1);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertFalse("Shouldn't violate UNIQUE constraint", threw);
        assertEquals(indexedColumnCount("T37"), 0);

        // Test for T38
        assertTrue(findTableInSystemCatalogResults("T38"));
        resp = m_client.callProcedure("T38.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T38.insert", 1);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertFalse("Shouldn't violate UNIQUE constraint", threw);
        assertEquals(indexedColumnCount("T38"), 0);

        // Test for T39
        assertTrue(findTableInSystemCatalogResults("T39"));
        resp = m_client.callProcedure("T39.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T39.insert", 2);
        } catch (ProcCallException pce) {
            pce.printStackTrace();
            threw = true;
        }
        assertFalse("Shouldn't violate LIMIT PARTITION ROW constraint", threw);
        assertEquals(indexedColumnCount("T39"), 0);
    }

    @Test
    public void testAlterTableAddConstraint() throws Exception {
        ClientResponse resp;
        boolean threw;

        // Test for T40
        assertTrue(findTableInSystemCatalogResults("T40"));
        resp = m_client.callProcedure("T40.insert", 1, 2);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T40.insert", 1, 2);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate UNIQUE constraint", threw);

//      ENG-7321 - bug with PRIMARY KEY and verification of generated DDL
//        // Test for T41
//        assertTrue(findTableInSystemCatalogResults("T41"));
//        resp = m_client.callProcedure("T41.insert", 1);
//        assertEquals(resp.getResults()[0].getRowCount(), 1);
//
//        threw = false;
//        try {
//            resp = m_client.callProcedure("T41.insert", 1);
//        } catch (ProcCallException pce) {
//            pce.printStackTrace();
//            threw = true;
//        }
//        assertEquals(resp.getResults()[0].getRowCount(), 1);
//        assertTrue("Shouldn't violate PRIMARY KEY constraint", threw);
////        assertEquals(indexedColumnCount("T41"), 1);

        // Test for T42
        assertTrue(findTableInSystemCatalogResults("T42"));
        resp = m_client.callProcedure("T42.insert", 1, 2);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T42.insert", 1, 2);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate ASSUMEUNIQUE constraint", threw);

        // Test for T42A
        assertTrue(findTableInSystemCatalogResults("T42A"));
        resp = m_client.callProcedure("T42A.insert", 1, 2, 3);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T42A.insert", 1, 2, 3);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate ASSUMEUNIQUE constraint", threw);

        // Test for T43
        assertTrue(findTableInSystemCatalogResults("T43"));
        resp = m_client.callProcedure("T43.insert", 1);
        assertEquals(resp.getResults()[0].getRowCount(), 1);

        threw = false;
        try {
            m_client.callProcedure("T43.insert", 2);
        } catch (ProcCallException pce) {
            threw = true;
        }
        assertTrue("Shouldn't violate LIMIT PARTITION ROW constraint", threw);
        assertEquals(indexedColumnCount("T43"), 0);

    }

    @Test
    public void testAlterTableAddColumn() throws Exception
    {
        // Test for T44
        assertTrue(findTableInSystemCatalogResults("T44"));
        assertTrue(doesColumnExist("T44", "C1" ));
        assertTrue(doesColumnExist("T44", "C2" ));
        assertTrue(verifyTableColumnType("T44", "C1", "INTEGER" ));
        assertTrue(verifyTableColumnType("T44", "C2", "VARCHAR" ));

        // Test for T45
        assertTrue(findTableInSystemCatalogResults("T45"));
        assertTrue(doesColumnExist("T45", "C1" ));
        assertTrue(doesColumnExist("T45", "C2" ));
        assertTrue(verifyTableColumnType("T45", "C1", "INTEGER" ));
        assertTrue(verifyTableColumnType("T45", "C2", "INTEGER" ));

        // Test for T46
        assertTrue(findTableInSystemCatalogResults("T46"));
        assertTrue(doesColumnExist("T46", "C1" ));
        assertTrue(doesColumnExist("T46", "C2" ));
        assertTrue(verifyTableColumnType("T46", "C1", "INTEGER" ));
        assertTrue(verifyTableColumnType("T46", "C2", "INTEGER" ));
        assertEquals(indexedColumnCount("T46"), 1);

        // Test for T47
        assertTrue(findTableInSystemCatalogResults("T47"));
        assertTrue(doesColumnExist("T47", "C1" ));
        assertTrue(doesColumnExist("T47", "C2" ));
        assertTrue(verifyTableColumnType("T47", "C1", "INTEGER" ));
        assertTrue(verifyTableColumnType("T47", "C2", "INTEGER" ));
        assertEquals(indexedColumnCount("T47"), 1);

//        ENG-7321 - bug with PRIMARY KEY and verification of generated DDL
//        // Test for T48
//        assertTrue(findTableInSystemCatalogResults("T48"));
//        assertTrue(doesColumnExist("T48", "C1" ));
//        assertTrue(doesColumnExist("T48", "C2" ));
//        assertTrue(verifyTableColumnType("T48", "C1", "INTEGER" ));
//        assertTrue(verifyTableColumnType("T48", "C2", "INTEGER" ));
//        assertEquals(indexedColumnCount("T48"), 1);

    }

    @Test
    public void testAlterTableDropColumn() throws Exception
    {
        // Test for T49
        assertTrue(findTableInSystemCatalogResults("T49"));
        assertFalse(doesColumnExist("T49", "C1" ));
        assertTrue(doesColumnExist("T49", "C2" ));

        // Test for T50
        assertTrue(findTableInSystemCatalogResults("T50"));
        assertTrue(doesColumnExist("T50", "C1" ));
        assertFalse(doesColumnExist("T50", "C2" ));
        assertTrue(doesColumnExist("T50", "C3" ));
        assertEquals(indexedColumnCount("T50"), 1);
        assertFalse(findTableInSystemCatalogResults("VT50A"));
        assertFalse(findIndexInSystemCatalogResults("abs_T50A_idx"));
    }

    @Test
    public void testAlterTableAlterColumn() throws Exception
    {
        // Test for T51
        assertTrue(findTableInSystemCatalogResults("T51"));
        assertTrue(doesColumnExist("T51", "C1" ));
        assertTrue(doesColumnExist("T51", "C2" ));
        assertTrue(isColumnNullable("T51", "C1"));


        // Test for T52
        assertTrue(findTableInSystemCatalogResults("T52"));
        assertTrue(doesColumnExist("T52", "C1" ));
        assertTrue(doesColumnExist("T52", "C2" ));
        assertTrue(isColumnNullable("T52", "C2"));

        // Test for T53
        assertTrue(findTableInSystemCatalogResults("T53"));
        assertTrue(doesColumnExist("T53", "C1" ));
        assertTrue(doesColumnExist("T53", "C2" ));
        assertTrue(verifyTableColumnType("T53", "C1", "VARCHAR"));
        assertTrue(verifyTableColumnType("T53", "C2", "VARCHAR"));

        // Test for T54
        assertTrue(findTableInSystemCatalogResults("T54"));
        assertTrue(findIndexInSystemCatalogResults("abs_T54A_idx"));
        assertTrue(doesColumnExist("T54", "C1" ));
        assertTrue(doesColumnExist("T54", "C2" ));
        assertTrue(verifyTableColumnType("T54", "C1", "VARCHAR"));
        assertTrue(verifyTableColumnType("T54", "C2", "VARCHAR"));
        assertFalse(isColumnNullable("T54", "C1"));
        assertTrue(isColumnNullable("T54", "C2"));
    }

    @Test
    public void testAlterStreamDropColumn() throws Exception
    {
        // Tests of T25R, the "replicated" (non-partitioned) stream
        assertTrue("Stream T25R should exist", findTableInSystemCatalogResults("T25R"));
        assertColumnDoesNotExist("T25R", "D1");
        assertColumnDoesNotExist("T25R", "D6");
        assertColumnDoesNotExist("T25R", "D7");
        assertColumnDoesNotExist("T25R", "D14");

        // Tests of T25P, the partitioned stream (identical to the above tests for T25R)
        assertTrue("Stream T25P should exist", findTableInSystemCatalogResults("T25P"));
        assertColumnDoesNotExist("T25P", "D1");
        assertColumnDoesNotExist("T25P", "D6");
        assertColumnDoesNotExist("T25P", "D7");
        assertColumnDoesNotExist("T25P", "D14");
    }

    @Test
    public void testAlterStreamAddColumn() throws Exception
    {
        // Tests of T25R, the "replicated" (non-partitioned) stream
        assertTrue("Stream T25R should exist", findTableInSystemCatalogResults("T25R"));
        assertColumnExists("T25R", "A1");
        assertColumnExists("T25R", "A2");
        assertColumnExists("T25R", "A3");
        assertColumnExists("T25R", "A4");
        assertColumnExists("T25R", "A5");
        assertColumnExists("T25R", "A6");
        assertColumnExists("T25R", "A7");
        assertColumnExists("T25R", "A8");
        assertColumnExists("T25R", "A9");

        assertColumnTypeEquals("T25R", "A1", "VARCHAR");
        assertColumnTypeEquals("T25R", "A2", "INTEGER");
        assertColumnTypeEquals("T25R", "A3", "VARCHAR");
        assertColumnTypeEquals("T25R", "A4", "INTEGER");
        assertColumnTypeEquals("T25R", "A5", "INTEGER");
        assertColumnTypeEquals("T25R", "A6", "VARCHAR");
        assertColumnTypeEquals("T25R", "A7", "INTEGER");
        assertColumnTypeEquals("T25R", "A8", "VARCHAR");
        assertColumnTypeEquals("T25R", "A9", "INTEGER");

        assertColumnSizeEquals("T25R", "A1", 16);
        assertColumnSizeEquals("T25R", "A2", 31);
        assertColumnSizeEquals("T25R", "A3", 15);
        assertColumnSizeEquals("T25R", "A4", 31);
        assertColumnSizeEquals("T25R", "A5", 31);
        assertColumnSizeEquals("T25R", "A6", 63);
        assertColumnSizeEquals("T25R", "A7", 31);
        assertColumnSizeEquals("T25R", "A8", 2048);
        assertColumnSizeEquals("T25R", "A9", 31);

        assertColumnIsNullable   ("T25R", "A1");
        assertColumnIsNotNullable("T25R", "A2");
        assertColumnIsNullable   ("T25R", "A3");
        assertColumnIsNotNullable("T25R", "A4");
        assertColumnIsNullable   ("T25R", "A5");
        assertColumnIsNotNullable("T25R", "A6");
        assertColumnIsNullable   ("T25R", "A7");
        assertColumnIsNotNullable("T25R", "A8");
        assertColumnIsNotNullable("T25R", "A9");

        assertColumnDefaultValueEquals("T25R", "A1", null);
        assertColumnDefaultValueEquals("T25R", "A2", null);
        assertColumnDefaultValueEquals("T25R", "A3", "'def'");
        assertColumnDefaultValueEquals("T25R", "A4", "-2");
        assertColumnDefaultValueEquals("T25R", "A5", null);
        assertColumnDefaultValueEquals("T25R", "A6", null);
        assertColumnDefaultValueEquals("T25R", "A7", "-3");
        assertColumnDefaultValueEquals("T25R", "A8", "'ghi'");
        assertColumnDefaultValueEquals("T25R", "A9", "-4");

        assertColumnOrdinalPositionEquals("T25R", "A1", 14);
        assertColumnOrdinalPositionEquals("T25R", "A2", 15);
        assertColumnOrdinalPositionEquals("T25R", "A3", 16);
        assertColumnOrdinalPositionEquals("T25R", "A4", 19);
        assertColumnOrdinalPositionEquals("T25R", "A5", 2);
        assertColumnOrdinalPositionEquals("T25R", "A6", 12);
        assertColumnOrdinalPositionEquals("T25R", "A7", 1);
        assertColumnOrdinalPositionEquals("T25R", "A8", 18);
        assertColumnOrdinalPositionEquals("T25R", "A9", 17);

        assertColumnOrdinalPositionEquals("T25R", "C2", 3);
        assertColumnOrdinalPositionEquals("T25R", "C3", 4);
        assertColumnOrdinalPositionEquals("T25R", "C4", 5);
        assertColumnOrdinalPositionEquals("T25R", "C5", 6);
        assertColumnOrdinalPositionEquals("T25R", "C8", 7);
        assertColumnOrdinalPositionEquals("T25R", "C9", 8);
        assertColumnOrdinalPositionEquals("T25R", "C10", 9);
        assertColumnOrdinalPositionEquals("T25R", "C11", 10);
        assertColumnOrdinalPositionEquals("T25R", "C12", 11);
        assertColumnOrdinalPositionEquals("T25R", "C13", 13);

        // Tests of T25P, the partitioned stream
        // (identical to the above tests for T25R - mostly)
        assertTrue("Stream T25P should exist", findTableInSystemCatalogResults("T25P"));
        assertColumnExists("T25P", "A1");
        assertColumnExists("T25P", "A2");
        assertColumnExists("T25P", "A3");
        assertColumnExists("T25P", "A4");
        assertColumnExists("T25P", "A5");
        assertColumnExists("T25P", "A6");
        assertColumnExists("T25P", "A7");
        assertColumnExists("T25P", "A8");
        assertColumnExists("T25P", "A9");

        assertColumnTypeEquals("T25P", "A1", "VARCHAR");
        assertColumnTypeEquals("T25P", "A2", "INTEGER");
        assertColumnTypeEquals("T25P", "A3", "VARCHAR");
        assertColumnTypeEquals("T25P", "A4", "INTEGER");
        assertColumnTypeEquals("T25P", "A5", "INTEGER");
        assertColumnTypeEquals("T25P", "A6", "VARCHAR");
        assertColumnTypeEquals("T25P", "A7", "INTEGER");
        assertColumnTypeEquals("T25P", "A8", "VARCHAR");
        assertColumnTypeEquals("T25P", "A9", "INTEGER");

        assertColumnSizeEquals("T25P", "A1", 16);
        assertColumnSizeEquals("T25P", "A2", 31);
        assertColumnSizeEquals("T25P", "A3", 15);
        assertColumnSizeEquals("T25P", "A4", 31);
        assertColumnSizeEquals("T25P", "A5", 31);
        assertColumnSizeEquals("T25P", "A6", 63);
        assertColumnSizeEquals("T25P", "A7", 31);
        assertColumnSizeEquals("T25P", "A8", 2048);
        assertColumnSizeEquals("T25P", "A9", 31);

        assertColumnIsNullable   ("T25P", "A1");
        assertColumnIsNotNullable("T25P", "A2");
        assertColumnIsNullable   ("T25P", "A3");
        assertColumnIsNotNullable("T25P", "A4");
        assertColumnIsNullable   ("T25P", "A5");
        assertColumnIsNotNullable("T25P", "A6");
        assertColumnIsNullable   ("T25P", "A7");
        assertColumnIsNotNullable("T25P", "A8");
        assertColumnIsNotNullable("T25P", "A9");

        assertColumnDefaultValueEquals("T25P", "A1", null);
        assertColumnDefaultValueEquals("T25P", "A2", null);
        assertColumnDefaultValueEquals("T25P", "A3", "'def'");
        assertColumnDefaultValueEquals("T25P", "A4", "-2");
        assertColumnDefaultValueEquals("T25P", "A5", null);
        assertColumnDefaultValueEquals("T25P", "A6", null);
        assertColumnDefaultValueEquals("T25P", "A7", "-3");
        assertColumnDefaultValueEquals("T25P", "A8", "'ghi'");
        assertColumnDefaultValueEquals("T25P", "A9", "-4");

        // The remaining tests are not quite identical to the above tests of T25R,
        // because T25P includes the partitioning column P0, which T25R does not
        assertColumnOrdinalPositionEquals("T25P", "A1", 15);
        assertColumnOrdinalPositionEquals("T25P", "A2", 16);
        assertColumnOrdinalPositionEquals("T25P", "A3", 17);
        assertColumnOrdinalPositionEquals("T25P", "A4", 20);
        assertColumnOrdinalPositionEquals("T25P", "A5", 2);
        assertColumnOrdinalPositionEquals("T25P", "A6", 13);
        assertColumnOrdinalPositionEquals("T25P", "A7", 1);
        assertColumnOrdinalPositionEquals("T25P", "A8", 19);
        assertColumnOrdinalPositionEquals("T25P", "A9", 18);

        assertColumnOrdinalPositionEquals("T25P", "C2", 3);
        assertColumnOrdinalPositionEquals("T25P", "C3", 5);
        assertColumnOrdinalPositionEquals("T25P", "C4", 6);
        assertColumnOrdinalPositionEquals("T25P", "C5", 7);
        assertColumnOrdinalPositionEquals("T25P", "C8", 8);
        assertColumnOrdinalPositionEquals("T25P", "C9", 9);
        assertColumnOrdinalPositionEquals("T25P", "C10", 10);
        assertColumnOrdinalPositionEquals("T25P", "C11", 11);
        assertColumnOrdinalPositionEquals("T25P", "C12", 12);
        assertColumnOrdinalPositionEquals("T25P", "C13", 14);
    }

    @Test
    public void testAlterStreamAlterColumn() throws Exception
    {
        // Tests of T25R, the "replicated" (non-partitioned) stream
        assertTrue("Stream T25R should exist", findTableInSystemCatalogResults("T25R"));
        assertColumnExists("T25R", "C2");
        assertColumnExists("T25R", "C3");
        assertColumnExists("T25R", "C4");
        assertColumnExists("T25R", "C5");
        assertColumnExists("T25R", "C8");
        assertColumnExists("T25R", "C9");
        assertColumnExists("T25R", "C10");
        assertColumnExists("T25R", "C11");
        assertColumnExists("T25R", "C12");
        assertColumnExists("T25R", "C13");

        assertColumnTypeEquals("T25R", "C2", "VARCHAR");
        assertColumnTypeEquals("T25R", "C3", "INTEGER");
        assertColumnTypeEquals("T25R", "C4", "VARCHAR");
        assertColumnTypeEquals("T25R", "C5", "INTEGER");
        assertColumnTypeEquals("T25R", "C8", "VARCHAR");
        assertColumnTypeEquals("T25R", "C9", "INTEGER");
        assertColumnTypeEquals("T25R", "C10", "VARCHAR");
        assertColumnTypeEquals("T25R", "C11", "INTEGER");
        assertColumnTypeEquals("T25R", "C12", "VARCHAR");
        assertColumnTypeEquals("T25R", "C13", "INTEGER");

        assertColumnSizeEquals("T25R", "C2", 15);
        assertColumnSizeEquals("T25R", "C3", 31);
        assertColumnSizeEquals("T25R", "C4", 16);
        assertColumnSizeEquals("T25R", "C5", 31);
        assertColumnSizeEquals("T25R", "C8", 2048);
        assertColumnSizeEquals("T25R", "C9", 31);
        assertColumnSizeEquals("T25R", "C10", 2048);
        assertColumnSizeEquals("T25R", "C11", 31);
        assertColumnSizeEquals("T25R", "C12", 16);
        assertColumnSizeEquals("T25R", "C13", 31);

        assertColumnIsNullable   ("T25R", "C2");
        assertColumnIsNotNullable("T25R", "C3");
        assertColumnIsNullable   ("T25R", "C4");
        assertColumnIsNotNullable("T25R", "C5");
        assertColumnIsNotNullable("T25R", "C8");
        assertColumnIsNullable   ("T25R", "C9");
        assertColumnIsNullable   ("T25R", "C10");
        assertColumnIsNotNullable("T25R", "C11");
        assertColumnIsNullable   ("T25R", "C12");
        assertColumnIsNotNullable("T25R", "C13");

        assertColumnDefaultValueEquals("T25R", "C2", null);
        assertColumnDefaultValueEquals("T25R", "C3", null);
        assertColumnDefaultValueEquals("T25R", "C4", "'jkl'");
        assertColumnDefaultValueEquals("T25R", "C5", "-5");
        assertColumnDefaultValueEquals("T25R", "C8", null);
        assertColumnDefaultValueEquals("T25R", "C9", null);
        assertColumnDefaultValueEquals("T25R", "C10", "'mno'");
        assertColumnDefaultValueEquals("T25R", "C11", "-1");
        assertColumnDefaultValueEquals("T25R", "C12", "'abc'");
        assertColumnDefaultValueEquals("T25R", "C13", "-6");

        // Tests of T25P, the partitioned stream (identical to the tests above for T25R)
        assertTrue("Stream T25P should exist", findTableInSystemCatalogResults("T25P"));
        assertColumnExists("T25P", "C2");
        assertColumnExists("T25P", "C3");
        assertColumnExists("T25P", "C4");
        assertColumnExists("T25P", "C5");
        assertColumnExists("T25P", "C8");
        assertColumnExists("T25P", "C9");
        assertColumnExists("T25P", "C10");
        assertColumnExists("T25P", "C11");
        assertColumnExists("T25P", "C12");
        assertColumnExists("T25P", "C13");

        assertColumnTypeEquals("T25P", "C2", "VARCHAR");
        assertColumnTypeEquals("T25P", "C3", "INTEGER");
        assertColumnTypeEquals("T25P", "C4", "VARCHAR");
        assertColumnTypeEquals("T25P", "C5", "INTEGER");
        assertColumnTypeEquals("T25P", "C8", "VARCHAR");
        assertColumnTypeEquals("T25P", "C9", "INTEGER");
        assertColumnTypeEquals("T25P", "C10", "VARCHAR");
        assertColumnTypeEquals("T25P", "C11", "INTEGER");
        assertColumnTypeEquals("T25P", "C12", "VARCHAR");
        assertColumnTypeEquals("T25P", "C13", "INTEGER");

        assertColumnSizeEquals("T25P", "C2", 15);
        assertColumnSizeEquals("T25P", "C3", 31);
        assertColumnSizeEquals("T25P", "C4", 16);
        assertColumnSizeEquals("T25P", "C5", 31);
        assertColumnSizeEquals("T25P", "C8", 2048);
        assertColumnSizeEquals("T25P", "C9", 31);
        assertColumnSizeEquals("T25P", "C10", 2048);
        assertColumnSizeEquals("T25P", "C11", 31);
        assertColumnSizeEquals("T25P", "C12", 16);
        assertColumnSizeEquals("T25P", "C13", 31);

        assertColumnIsNullable   ("T25P", "C2");
        assertColumnIsNotNullable("T25P", "C3");
        assertColumnIsNullable   ("T25P", "C4");
        assertColumnIsNotNullable("T25P", "C5");
        assertColumnIsNotNullable("T25P", "C8");
        assertColumnIsNullable   ("T25P", "C9");
        assertColumnIsNullable   ("T25P", "C10");
        assertColumnIsNotNullable("T25P", "C11");
        assertColumnIsNullable   ("T25P", "C12");
        assertColumnIsNotNullable("T25P", "C13");

        assertColumnDefaultValueEquals("T25P", "C2", null);
        assertColumnDefaultValueEquals("T25P", "C3", null);
        assertColumnDefaultValueEquals("T25P", "C4", "'jkl'");
        assertColumnDefaultValueEquals("T25P", "C5", "-5");
        assertColumnDefaultValueEquals("T25P", "C8", null);
        assertColumnDefaultValueEquals("T25P", "C9", null);
        assertColumnDefaultValueEquals("T25P", "C10", "'mno'");
        assertColumnDefaultValueEquals("T25P", "C11", "-1");
        assertColumnDefaultValueEquals("T25P", "C12", "'abc'");
        assertColumnDefaultValueEquals("T25P", "C13", "-6");

        // Additional tests of T25P's partitioned column, P0
        assertColumnDoesNotExist      ("T25R", "P0");
        assertColumnExists            ("T25P", "P0" );
        assertColumnTypeEquals        ("T25P", "P0", "INTEGER");
        assertColumnSizeEquals        ("T25P", "P0", 31);
        assertColumnIsNotNullable     ("T25P", "P0");
        assertColumnDefaultValueEquals("T25P", "P0", "-7");
    }

    @Test
    public void testTableDR() throws Exception
    {
        // Test for T56 (DR table exists)
        assertTrue(findTableInSystemCatalogResults("T56"));
        assertTrue(doesColumnExist("T56", "C1" ));
        assertTrue(doesColumnExist("T56", "C2" ));
        assertTrue(isColumnNullable("T56", "C2"));
        assertTrue(isDRedTable("T56"));

        // Test for T57 (DR partitioned table exists)
        assertTrue(findTableInSystemCatalogResults("T57"));
        assertTrue(doesColumnExist("T57", "C1" ));
        assertTrue(doesColumnExist("T57", "C2" ));
        assertTrue(isColumnPartitionColumn("T57", "C2"));
        assertTrue(isDRedTable("T57"));

        // Test that T58 and T59 have been dropped
        assertFalse(findTableInSystemCatalogResults("T58"));
        assertFalse(findTableInSystemCatalogResults("T59"));

        // Test that the DR flag is false for T60 and T61 (after disable)
        assertTrue(findTableInSystemCatalogResults("T60"));
        assertTrue(doesColumnExist("T60", "C1" ));
        assertTrue(doesColumnExist("T60", "C2" ));
        assertTrue(doesColumnExist("T60", "C3" ));
        assertTrue(verifyTableColumnType("T60", "C3", "INTEGER"));
        assertFalse(isDRedTable("T60"));
        assertTrue(findTableInSystemCatalogResults("T61"));
        assertTrue(doesColumnExist("T61", "C1" ));
        assertTrue(doesColumnExist("T61", "C2" ));
        assertTrue(doesColumnExist("T61", "C3" ));
        assertTrue(isColumnPartitionColumn("T61", "C3"));
        assertTrue(verifyTableColumnType("T61", "C3", "INTEGER"));
        assertFalse(isDRedTable("T61"));
    }

    @Test
    public void testINETFunctions() throws Exception {
        assertTrue(findTableInSystemCatalogResults("T22"));

        assertTrue(findIndexInSystemCatalogResults("ENG_8168_INDEX_USES_INET_ATON"));
        assertTrue(findIndexInSystemCatalogResults("ENG_8168_INDEX_USES_INET_NTOA"));
        assertTrue(findIndexInSystemCatalogResults("ENG_8168_INDEX_USES_INET6_ATON"));
        assertTrue(findIndexInSystemCatalogResults("ENG_8168_INDEX_USES_INET6_NTOA"));
    }

    @Test
    public void testGEOIndex() throws Exception {
        assertTrue(findTableInSystemCatalogResults("GEO"));
        assertTrue(findTableInSystemCatalogResults("T4"));

        assertTrue(findIndexInSystemCatalogResults("GEOINDEX_GEOGRAPHY"));
        assertTrue(findIndexInSystemCatalogResults("GEOINDEX_REASONS"));
        assertTrue(findIndexInSystemCatalogResults("INDEX_USES_GEO_ASTEXT_POINT"));
        assertTrue(findIndexInSystemCatalogResults("INDEX_USES_GEO_ASTEXT_POLYGON"));
        assertTrue(findIndexInSystemCatalogResults("INDEX_USES_GEO_LATITUDE"));
        assertTrue(findIndexInSystemCatalogResults("INDEX_USES_GEO_DISTANCE_POLYGON_POINT"));
        assertTrue(findIndexInSystemCatalogResults("INDEX_USES_GEO_DISTANCE_POINT_POINT"));
        assertTrue(findIndexInSystemCatalogResults("PARTIAL_INDEX_USES_GEO_DISTANCE_POLYGON_POINT"));
        assertTrue(findIndexInSystemCatalogResults("PARTIAL_INDEX_USES_GEO_AREA"));
        // GEO has three index columns.  Two for IDX
        // and one for the primary key,
        // plus a geospatial index on geography column region1.
        assertEquals(4, indexedColumnCount("GEO"));
    }

}
