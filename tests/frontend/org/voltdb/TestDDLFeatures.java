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
 * EXpceSS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestDDLFeatures extends AdhocDDLTestBase {

    private String line = "===================================================\n";
    String catalogJar = "DDLFeature.jar";
    String pathToCatalog = Configuration.getPathToCatalogForTest("DDLFeature.jar");
    String pathToDeployment = Configuration.getPathToCatalogForTest("DDLFeature.xml");

    @Test
    public void testCreateUniqueIndex() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE T1(width INTEGER, length INTEGER, volume INTEGER);"
                + "CREATE UNIQUE INDEX area ON T1 (width * length);";
        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);
        assertTrue(findTableInSystemCatalogResults("T1"));

        // ClientResponse resp = m_client.callProcedure("@SystemCatalog", "INDEXINFO");
        // System.out.println(line + resp.getResults()[0] + line);
        assertTrue(findIndexInSystemCatalogResults("area"));
        assertTrue(verifyIndexUniqueness("area", true));
        assertEquals(indexedColumnCount("T1"), 2);

        teardownSystem();
    }

    @Test
    public void testCreateAssumeUniqueIndex() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE T2(width INTEGER, length INTEGER, area INTEGER NOT NULL, volume INTEGER);"
                + "PARTITION TABLE T2 ON COLUMN area;"
                + "CREATE ASSUMEUNIQUE INDEX absVal ON T2(ABS(area * 2), ABS(volume / 2));";
        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);
        assertTrue(findTableInSystemCatalogResults("T2"));

        // ClientResponse resp = m_client.callProcedure("@SystemCatalog", "PROCEDURES");
        // System.out.println(line + resp.getResults()[0] + line);
        assertTrue(findIndexInSystemCatalogResults("absVal"));
        assertTrue(verifyIndexUniqueness("absVal", true));
        assertEquals(indexedColumnCount("T2"), 2);

        teardownSystem();
    }

    @Test
    public void testCreateHashIndex() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE T3(val INTEGER, str VARCHAR(30), id INTEGER);"
                + "CREATE UNIQUE INDEX abs_Hash_idx ON T3 (ABS(val));"
                + "CREATE UNIQUE INDEX nomeaninghashweirdidx ON T3 (ABS(id));"
                + "CREATE INDEX strMatch ON T3 (FIELD (str, 'arbitrary'), id);";
        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);
        assertTrue(findTableInSystemCatalogResults("T3"));

        // ClientResponse resp = m_client.callProcedure("@SystemCatalog", "INDEXINFO");
        // System.out.println(line + resp.getResults()[0] + line);

        assertTrue(findIndexInSystemCatalogResults("nomeaninghashweirdidx"));
        assertTrue(findIndexInSystemCatalogResults("abs_Hash_idx"));
        assertEquals(indexedColumnCount("T3"), 4);

        teardownSystem();
    }

    @Test
    public void testCreateProcedureAsSQLStmt() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE User (age INTEGER, name VARCHAR(20));"
                + "CREATE ROLE admin WITH sysproc, adhoc;"
                + "CREATE PROCEDURE p1 ALLOW admin AS SELECT COUNT(*), name FROM User WHERE age = ? GROUP BY name;"
                + "CREATE PROCEDURE p2 ALLOW admin AS INSERT INTO User VALUES (?, ?);";

        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);
        assertTrue(findTableInSystemCatalogResults("User"));

        // ClientResponse resp = m_client.callProcedure("@SystemCatalog", "INDEXINFO");
        // System.out.println(line + resp.getResults()[0] + line);

        ClientResponse p1Result, p2Result;
        VoltTable vt1, vt2;

        p1Result = m_client.callProcedure("p1", "18");
        vt1 = p1Result.getResults()[0];
        assertEquals(vt1.m_rowCount, 0);

        p2Result = m_client.callProcedure("p2", 18, "Kevin Durant");
        vt2 = p2Result.getResults()[0];
        assertEquals(vt2.m_rowCount, 1);

        p1Result = m_client.callProcedure("p1", "18");
        // System.out.println(p1Result.getResults()[0] + line);
        vt1 = p1Result.getResults()[0];
        vt1.advanceToRow(0);

        assertEquals(vt1.m_rowCount, 1);
        assertEquals(vt1.getLong(0), 1);
        assertEquals(vt1.getString("NAME"), "Kevin Durant");

        teardownSystem();
    }

    @Test
    public void testCreateProcedureFromClass() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE ROLE admin WITH sysproc, adhoc, defaultproc;"
                + "CREATE PROCEDURE ALLOW admin FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedEmptyProcedure;";

        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        ClientResponse resp = m_client.callProcedure("NotAnnotatedEmptyProcedure", 1l, "Test", "Yuning He");
        // System.out.println(line + resp.getResults()[0] + line);
        VoltTable vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(0, VoltType.INTEGER), 1);

        teardownSystem();
    }

    @Test
    public void testCreateTableDataType() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE T4 (C1 TINYINT DEFAULT 127 NOT NULL, C2 SMALLINT DEFAULT 32767 NOT NULL, "
                + "C3 INTEGER DEFAULT 2147483647 NOT NULL, C4 BIGINT NOT NULL, C5 FLOAT NOT NULL, C6 DECIMAL NOT NULL, "
                + "C7 VARCHAR(32) NOT NULL, C8 VARBINARY(32) NOT NULL, C9 TIMESTAMP DEFAULT NOW NOT NULL, "
                + "C10 TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (C1, C9));"
                + "CREATE TABLE T5 (C VARCHAR(1048576 BYTES));"
                + "CREATE TABLE T6 (C VARCHAR(262144));"
                + "CREATE TABLE T7 (C VARBINARY(1048576));";

        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        assertTrue(findTableInSystemCatalogResults("T4"));
        assertTrue(findTableInSystemCatalogResults("T5"));
        assertTrue(findTableInSystemCatalogResults("T6"));
        assertTrue(findTableInSystemCatalogResults("T7"));

        ClientResponse resp;
        VoltTable vt;

        m_client.callProcedure("@AdHoc", "insert into T4 values (1, 2, 3, 4, 5.5, 6.6, \'test\', \'010101\', 1000, 1111);");

        resp = m_client.callProcedure("@AdHoc", "select * from T4;");
        vt = resp.getResults()[0];
        assertEquals(vt.m_rowCount, 1);
        vt.advanceToRow(0);
        byte ret1 = 1;
        assertEquals(vt.get(0, VoltType.TINYINT), ret1);
        assertEquals(vt.get(2, VoltType.INTEGER), 3);
        assertEquals(vt.get(4, VoltType.FLOAT), 5.5);

        m_client.callProcedure("T5.insert", "test");
        resp = m_client.callProcedure("@AdHoc", "select * from T5;");
        assertEquals(resp.getResults()[0].m_rowCount, 1);

        m_client.callProcedure("T5.insert", "hahahaha");
        resp = m_client.callProcedure("@AdHoc", "select * from T5;");
        vt = resp.getResults()[0];
        assertEquals(vt.m_rowCount, 2);
        System.out.println(line + resp.getResults()[0] + line);
        vt.advanceToRow(0);
        assertEquals(vt.get(0, VoltType.STRING), "test");
        vt.advanceToRow(1);
        assertEquals(vt.get(0, VoltType.STRING), "hahahaha");

        m_client.callProcedure("T6.insert", "test");
        resp = m_client.callProcedure("@AdHoc", "select * from T6;");
        assertEquals(resp.getResults()[0].m_rowCount, 1);

        m_client.callProcedure("@AdHoc", "insert into T7 values (\'101010101010\')");
        resp = m_client.callProcedure("@AdHoc", "select * from T7;");
        assertEquals(resp.getResults()[0].m_rowCount, 1);

        teardownSystem();
    }

    @Test
    public void testCreateTableConstraint() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE T9 (C1 INTEGER PRIMARY KEY NOT NULL, C2 SMALLINT UNIQUE NOT NULL);"
                +
                "CREATE TABLE T10 (C INTEGER DEFAULT 123 NOT NULL, CONSTRAINT con UNIQUE(C));"
                +
                "CREATE TABLE T11 (C INTEGER DEFAULT 123 NOT NULL, CONSTRAINT pk1 PRIMARY KEY(C));"
                +
                "PARTITION TABLE T12 ON COLUMN C1;"
                + "CREATE TABLE T12 (C1 INTEGER NOT NULL, C2 INTEGER DEFAULT 123 NOT NULL, CONSTRAINT au ASSUMEUNIQUE(C2));"
                +
                "CREATE TABLE T16 (C INTEGER, CONSTRAINT lpr1 LIMIT PARTITION ROWS 1);"
                +
                "PARTITION TABLE T21 ON COLUMN C3;"
                + "CREATE TABLE T21 (C1 TINYINT DEFAULT 127 NOT NULL, C2 SMALLINT DEFAULT 32767 NOT NULL, "
                + "C3 INTEGER DEFAULT 2147483647 NOT NULL, C4 BIGINT NOT NULL, C5 FLOAT NOT NULL, C6 DECIMAL ASSUMEUNIQUE NOT NULL, "
                + "C7 VARCHAR(32) NOT NULL, C8 VARBINARY(32) NOT NULL, C9 TIMESTAMP DEFAULT NOW NOT NULL, "
                + "C10 TIMESTAMP DEFAULT CURRENT_TIMESTAMP, ASSUMEUNIQUE (C1, C9));"
                +
                "CREATE TABLE T22 (C1 TINYINT DEFAULT 127 NOT NULL UNIQUE, C2 SMALLINT DEFAULT 32767 NOT NULL, "
                + "C3 INTEGER DEFAULT 2147483647 NOT NULL, C4 BIGINT NOT NULL, C5 FLOAT NOT NULL, C6 DECIMAL UNIQUE NOT NULL, "
                + "C7 VARCHAR(32) NOT NULL, C8 VARBINARY(32) NOT NULL, C9 TIMESTAMP DEFAULT NOW NOT NULL, "
                + "C10 TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE (C1, C9));"
                +
                "CREATE TABLE T23 (C1 INTEGER NOT NULL, C2 SMALLINT UNIQUE, C3 VARCHAR(32) NOT NULL, "
                + "C4 TINYINT NOT NULL, C5 TIMESTAMP NOT NULL, C6 BIGINT NOT NULL, C7 FLOAT NOT NULL, C8 DECIMAL NOT NULL, "
                + "C9 INTEGER, CONSTRAINT hash_pk PRIMARY KEY (C1, C5), CONSTRAINT uni2 UNIQUE(C1, C7), "
                + "CONSTRAINT lpr2 LIMIT PARTITION ROWS 123);";
        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        ClientResponse resp;
        boolean threw;
        VoltTable vt;

        // Test for T9
        assertTrue(findTableInSystemCatalogResults("T9"));
        resp = m_client.callProcedure("T9.insert", 1, 2);
        assertEquals(resp.getResults()[0].m_rowCount, 1);

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
        assertEquals(resp.getResults()[0].m_rowCount, 1);

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
        assertEquals(resp.getResults()[0].m_rowCount, 1);

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
        assertEquals(resp.getResults()[0].m_rowCount, 1);

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
        assertEquals(resp.getResults()[0].m_rowCount, 1);

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

        teardownSystem();
    }

    @Test
    public void testCreateView() throws Exception
    {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE T24 (C1 INTEGER, C2 INTEGER);" +
                "CREATE VIEW VT (C1, C2, TOTAL) AS SELECT C1, C2, COUNT(*) FROM T24 GROUP BY C1, C2;";
        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);
        assertTrue(findTableInSystemCatalogResults("T24"));
        assertTrue(findTableInSystemCatalogResults("VT"));
        assertEquals(getTableType("VT"), "VIEW");

        m_client.callProcedure("T24.insert", 1, 2);
        m_client.callProcedure("T24.insert", 1, 2);
        ClientResponse resp = m_client.callProcedure("@AdHoc", "select * from VT");
        VoltTable vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(2, VoltType.INTEGER), 2);

        teardownSystem();
    }

    @Test
    public void testExportTable() throws Exception
    {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE T25 (id INTEGER NOT NULL);" +
                "EXPORT TABLE T25;";
        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);
        assertTrue(findTableInSystemCatalogResults("T25"));
        assertEquals(getTableType("T25"), "EXPORT");

        teardownSystem();
    }

    @Test
    public void testImportClass() throws Exception
    {

        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE T25 (id INTEGER NOT NULL); " +
                "IMPORT CLASS org.voltdb_testprocs.fullddlfeatures.NoMeaningClass;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb_testprocs.fullddlfeatures.testImportProc;";
        builder.addLiteralSchema(schema);

        LocalCluster cluster = new LocalCluster(catalogJar, 2, 1, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.setHasLocalServer(false);

        boolean success = cluster.compile(builder);
        assertTrue(success);

        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        cluster.startUp();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost", cluster.port(0));

        String classpath = "org/voltdb_testprocs/fullddlfeatures/NoMeaningClass.class";
        Process p = Runtime.getRuntime().exec("jar tf " + pathToCatalog);

        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String file = null;
        boolean exist = false;
        while ((file = in.readLine()) != null)
        {
            if(file.equals(classpath))
            {
                exist = true;
                break;
            }
        }
        assertTrue(exist);

//        ClientResponse resp = client.callProcedure("testImportProc");
//        System.out.println(line + resp.getResults()[0] + line);

        client.close();
        cluster.shutDown();
    }

    @Test
    public void testPartitionProcedure() throws Exception
    {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "CREATE TABLE T26 (age BIGINT NOT NULL, gender TINYINT);" +
                "CREATE PROCEDURE proc AS SELECT COUNT(*) FROM T26 WHERE age = ?;" +
                "PARTITION TABLE T26 ON COLUMN age;" +
                "PARTITION PROCEDURE proc ON TABLE T26 COLUMN age PARAMETER 0;";
        builder.addLiteralSchema(schema);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        assertTrue(findProcedureInSystemCatalog("proc"));
        assertTrue(isColumnPartitionColumn("T26", "age"));

        ClientResponse resp;
        VoltTable vt;
        resp = m_client.callProcedure("proc", 18);
        vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(0, VoltType.BIGINT), 0l);

        m_client.callProcedure("T26.insert", 18, 1);

        resp = m_client.callProcedure("proc", 18);
        vt = resp.getResults()[0];
        vt.advanceToRow(0);
        assertEquals(vt.get(0, VoltType.BIGINT), 1l);

        teardownSystem();
    }
}
