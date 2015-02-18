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

package org.voltdb.fullddlfeatures;

import java.net.URL;
import java.net.URLDecoder;

import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestDDLFeatures extends AdhocDDLTestBase {

    String catalogJar = "DDLFeature.jar";
    String pathToCatalog = Configuration.getPathToCatalogForTest("DDLFeature.jar");
    String pathToDeployment = Configuration.getPathToCatalogForTest("DDLFeature.xml");

    VoltProjectBuilder builder = new VoltProjectBuilder();

    @Override
    public void setUp() throws Exception
    {
        final URL url = TestDDLFeatures.class.getResource("fullDDL.sql");
        String schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        builder.addSchema(schemaPath);

        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);
    }

    @Override
    public void tearDown() throws Exception
    {
        teardownSystem();
    }


    @Test
    public void testCreateUniqueIndex() throws Exception {
        assertTrue(findTableInSystemCatalogResults("T17"));
        assertTrue(findIndexInSystemCatalogResults("area"));
        assertTrue(verifyIndexUniqueness("area", true));
        assertEquals(indexedColumnCount("T1"), 2);
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
    }

    @Test
    public void testCreateProcedureFromClass() throws Exception {
        ClientResponse resp = m_client.callProcedure("testCreateProcFromClassProc", 1l, "Test", "Yuning He");
        VoltTable vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(0, VoltType.INTEGER), 1);
    }

    @Test
    public void testCreateTableDataType() throws Exception {
        assertTrue(findTableInSystemCatalogResults("T4"));
        assertTrue(findTableInSystemCatalogResults("T5"));
        assertTrue(findTableInSystemCatalogResults("T6"));
        assertTrue(findTableInSystemCatalogResults("T7"));

        ClientResponse resp;
        VoltTable vt;

        m_client.callProcedure("@AdHoc", "insert into T4 values (1, 2, 3, 4, 5.5, 6.6, \'test\', \'010101\', 1000, 1111);");

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
        VoltTable vt;

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
            m_client.callProcedure("T12.insert", 3, 2);
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
        assertEquals(indexedColumnCount("T22"), 4);

        // Test for T23
        assertTrue(findTableInSystemCatalogResults("T23"));
        assertEquals(indexedColumnCount("T23"), 5);
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
    }

    @Test
    public void testExportTable() throws Exception
    {
        assertTrue(findTableInSystemCatalogResults("T25"));
        assertEquals(getTableType("T25"), "EXPORT");
        //Export table created with STREAM syntax
        assertTrue(findTableInSystemCatalogResults("T25S"));
        assertEquals(getTableType("T25S"), "EXPORT");
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
        VoltTable vt;

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
        VoltTable vt;

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
}
