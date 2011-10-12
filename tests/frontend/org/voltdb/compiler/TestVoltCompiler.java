/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.compiler;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.voltdb.ProcInfoData;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler.Feedback;
import org.voltdb.regressionsuites.TestSQLTypesSuite;
import org.voltdb.types.IndexType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;

public class TestVoltCompiler extends TestCase {

    String nothing_jar;
    String testout_jar;

    @Override
    public void setUp() {
        nothing_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "nothing.jar";
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "testout.jar";
    }

    @Override
    public void tearDown() {
        File njar = new File(nothing_jar);
        njar.delete();
        File tjar = new File(testout_jar);
        tjar.delete();
    }

    public void testBrokenLineParsing() throws IOException {
        final String simpleSchema1 =
            "create table table1r_el  (pkey integer, column2_integer integer, PRIMARY KEY(pkey));\n" +
            "create view v_table1r_el (column2_integer, num_rows) as\n" +
            "select column2_integer as column2_integer,\n" +
                   "count(*) as num_rows\n" +
            "from table1r_el\n" +
            "group by column2_integer;\n" +
            "create view v_table1r_el2 (column2_integer, num_rows) as\n" +
            "select column2_integer as column2_integer,\n" +
                   "count(*) as num_rows\n" +
            "from table1r_el\n" +
            "group by column2_integer\n;\n";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema1);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas>" +
            "<schema path='" + schemaPath + "' />" +
            "</schemas>" +
            "<procedures>" +
            "<procedure class='Foo'>" +
            "<sql>select * from table1r_el;</sql>" +
            "</procedure>" +
            "</procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertTrue(success);
    }

    public void testUTF8XMLFromHSQL() throws IOException {
        final String simpleSchema =
                "create table blah  (pkey integer not null, strval varchar(200), PRIMARY KEY(pkey));\n";
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(simpleSchema);
        pb.addStmtProcedure("utf8insert", "insert into blah values(1, 'něco za nic')");
        pb.addPartitionInfo("blah", "pkey");
        boolean success = pb.compile(Configuration.getPathToCatalogForTest("utf8xml.jar"));
        assertTrue(success);
    }

    private boolean isFeedbackPresent(String expectedError,
            ArrayList<Feedback> fbs) {
        for (Feedback fb : fbs) {
            if (fb.getStandardFeedbackLine().contains(expectedError)) {
                return true;
            }
        }
        return false;
    }

    public void testMismatchedPartitionParams() throws IOException {
        String expectedError;
        ArrayList<Feedback> fbs;


        fbs = checkPartitionParam("CREATE TABLE PKEY_BIGINT ( PKEY BIGINT NOT NULL, PRIMARY KEY (PKEY) );",
                                  "org.voltdb.compiler.procedures.PartitionParamBigint", "PKEY_BIGINT");
        expectedError =
            "Mismatch between partition column and partition parameter for procedure " +
            "org.voltdb.compiler.procedures.PartitionParamBigint\n" +
            "Partition column is type VoltType.BIGINT and partition parameter is type VoltType.STRING";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );",
                "org.voltdb.compiler.procedures.PartitionParamInteger",
                "PKEY_INTEGER");
        expectedError =
                    "Mismatch between partition column and partition parameter for procedure " +
                    "org.voltdb.compiler.procedures.PartitionParamInteger\n" +
                    "Partition column is type VoltType.INTEGER and partition parameter " +
                    "is type VoltType.BIGINT";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_SMALLINT ( PKEY SMALLINT NOT NULL, PRIMARY KEY (PKEY) );",
                "org.voltdb.compiler.procedures.PartitionParamSmallint",
                "PKEY_SMALLINT");
        expectedError =
                    "Mismatch between partition column and partition parameter for procedure " +
                    "org.voltdb.compiler.procedures.PartitionParamSmallint\n" +
                    "Partition column is type VoltType.SMALLINT and partition parameter " +
                    "is type VoltType.BIGINT";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_TINYINT ( PKEY TINYINT NOT NULL, PRIMARY KEY (PKEY) );",
                "org.voltdb.compiler.procedures.PartitionParamTinyint",
                "PKEY_TINYINT");
        expectedError =
                    "Mismatch between partition column and partition parameter for procedure " +
                    "org.voltdb.compiler.procedures.PartitionParamTinyint\n" +
                    "Partition column is type VoltType.TINYINT and partition parameter " +
                    "is type VoltType.SMALLINT";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_STRING ( PKEY VARCHAR(32) NOT NULL, PRIMARY KEY (PKEY) );",
                "org.voltdb.compiler.procedures.PartitionParamString",
                "PKEY_STRING");
        expectedError =
                    "Mismatch between partition column and partition parameter for procedure " +
                    "org.voltdb.compiler.procedures.PartitionParamString\n" +
                    "Partition column is type VoltType.STRING and partition parameter " +
                    "is type VoltType.INTEGER";
        assertTrue(isFeedbackPresent(expectedError, fbs));
    }


    private ArrayList<Feedback> checkPartitionParam(String ddl, String procedureClass, String table) {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas>" +
            "<schema path='" + schemaPath + "' />" +
            "</schemas>" +
            "<procedures>" +
            "<procedure class='" + procedureClass + "' />" +
            "</procedures>" +
            "<partitions>" +
            "<partition table='" + table + "' column='PKEY' />" +
            "</partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, null, null);
        assertFalse(success);
        return compiler.m_errors;
    }

    public void testSnapshotSettings() throws IOException {
        String schemaPath = "";
        try {
            final URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.addProcedures(org.voltdb.compiler.procedures.TPCCTestProc.class);
        builder.setSnapshotSettings("32m", 5, "/tmp", "woobar");
        builder.addSchema(schemaPath);
        try {
            assertTrue(builder.compile("/tmp/snapshot_settings_test.jar"));
            final String catalogContents =
                VoltCompiler.readFileFromJarfile("/tmp/snapshot_settings_test.jar", "catalog.txt");
            final Catalog cat = new Catalog();
            cat.execute(catalogContents);
            CatalogUtil.compileDeploymentAndGetCRC(cat, builder.getPathToDeployment(), true);
            SnapshotSchedule schedule =
                cat.getClusters().get("cluster").getDatabases().
                    get("database").getSnapshotschedule().get("default");
            assertEquals(32, schedule.getFrequencyvalue());
            assertEquals("m", schedule.getFrequencyunit());
            //Will be empty because the deployment file initialization is what sets this value
            assertEquals("/tmp", schedule.getPath());
            assertEquals("woobar", schedule.getPrefix());
        } finally {
            final File jar = new File("/tmp/snapshot_settings_test.jar");
            jar.delete();
        }
    }

    // TestExportSuite tests most of these options are tested end-to-end; however need to test
    // that a disabled connector is really disabled and that auth data is correct.
    public void testExportSetting() throws IOException {
        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-nonulls-ddl.sql"));

        // note that Insert inherits from InsertBase (testing this feature too)
        project.addProcedures(org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertBase.class);
        project.addProcedures(org.voltdb_testprocs.regressionsuites.sqltypesprocs.Insert.class);

        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addExport("org.voltdb.export.processors.RawProcessor", false, null);
        project.setTableAsExportOnly("ALLOW_NULLS");   // persistent table
        project.setTableAsExportOnly("WITH_DEFAULTS");  // streamed table
        try {
            boolean success = project.compile("/tmp/exportsettingstest.jar");
            assertTrue(success);
            final String catalogContents =
                VoltCompiler.readFileFromJarfile("/tmp/exportsettingstest.jar", "catalog.txt");
            final Catalog cat = new Catalog();
            cat.execute(catalogContents);

            Connector connector = cat.getClusters().get("cluster").getDatabases().
                get("database").getConnectors().get("0");
            assertFalse(connector.getEnabled());

        } finally {
            final File jar = new File("/tmp/exportsettingstest.jar");
            jar.delete();
        }

    }

    // test that Export configuration is insensitive to the case of the table name
    public void testExportTableCase() throws IOException {
        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ExportTester-ddl.sql"));
        project.addStmtProcedure("Dummy", "insert into a values (?, ?, ?);",
                                 "a.a_id: 0");
        project.addPartitionInfo("A", "A_ID");
        project.addPartitionInfo("B", "B_ID");
        project.addPartitionInfo("e", "e_id");
        project.addPartitionInfo("f", "f_id");
        project.addExport("org.voltdb.export.processors.RawProcessor", true, null);
        project.setTableAsExportOnly("A"); // uppercase DDL, uppercase export
        project.setTableAsExportOnly("b"); // uppercase DDL, lowercase export
        project.setTableAsExportOnly("E"); // lowercase DDL, uppercase export
        project.setTableAsExportOnly("f"); // lowercase DDL, lowercase export
        try {
            assertTrue(project.compile("/tmp/exportsettingstest.jar"));
            final String catalogContents =
                VoltCompiler.readFileFromJarfile("/tmp/exportsettingstest.jar", "catalog.txt");
            final Catalog cat = new Catalog();
            cat.execute(catalogContents);
            CatalogUtil.compileDeploymentAndGetCRC(cat, project.getPathToDeployment(), true);
            Connector connector = cat.getClusters().get("cluster").getDatabases().
                get("database").getConnectors().get("0");
            assertTrue(connector.getEnabled());
            // Assert that all tables exist in the connector section of catalog
            assertNotNull(connector.getTableinfo().getIgnoreCase("a"));
            assertNotNull(connector.getTableinfo().getIgnoreCase("b"));
            assertNotNull(connector.getTableinfo().getIgnoreCase("e"));
            assertNotNull(connector.getTableinfo().getIgnoreCase("f"));
        } finally {
            final File jar = new File("/tmp/exportsettingstest.jar");
            jar.delete();
        }
    }

    // test that the source table for a view is not export only
    public void testViewSourceNotExportOnly() throws IOException {
        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ExportTesterWithView-ddl.sql"));
        project.addStmtProcedure("Dummy", "select * from v_table1r_el_only");
        project.addExport("org.voltdb.export.processors.RawProcessor", true, null);
        project.setTableAsExportOnly("table1r_el_only");
        try {
            assertFalse(project.compile("/tmp/exporttestview.jar"));
        }
        finally {
            final File jar = new File("/tmp/exporttestview.jar");
            jar.delete();
        }
    }

    // test that a view is not export only
    public void testViewNotExportOnly() throws IOException {
        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ExportTesterWithView-ddl.sql"));
        project.addStmtProcedure("Dummy", "select * from table1r_el_only");
        project.addExport("org.voltdb.export.processors.RawProcessor", true, null);
        project.setTableAsExportOnly("v_table1r_el_only");
        try {
            assertFalse(project.compile("/tmp/exporttestview.jar"));
        }
        finally {
            final File jar = new File("/tmp/exporttestview.jar");
            jar.delete();
        }
    }

    public void testBadPath() {
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile("invalidnonsense", nothing_jar, System.out, null);

        assertFalse(success);
    }

    public void testXSDSchemaOrdering() throws IOException {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile("create table T(ID INTEGER);");
        final String schemaPath = schemaFile.getPath();
        final String project = "<?xml version=\"1.0\"?>\n" +
            "<project>" +
              "<database>" +
                "<schemas>" +
                   "<schema path='" +  schemaPath  + "'/>" +
                "</schemas>" +
                "<procedures>" +
                   "<procedure class='proc'><sql>select * from T</sql></procedure>" +
                "</procedures>" +
              "</database>" +
              "<security enabled='true'/>" +
            "</project>";
        final File xmlFile = VoltProjectBuilder.writeStringToTempFile(project);
        final String path = xmlFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        boolean success = compiler.compile(path, nothing_jar, System.out, null);
        assertTrue(success);
    }


    public void testXMLFileWithInvalidSchemaReference() {
        final String simpleXML =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='my schema file.sql' /></schemas>" +
            "<procedures><procedure class='procedures/procs.jar' /></procedures>" +
            "</database>" +
            "</project>";

        final File xmlFile = VoltProjectBuilder.writeStringToTempFile(simpleXML);
        final String path = xmlFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(path, nothing_jar, System.out, null);

        assertFalse(success);
    }

    public void testXMLFileWithSchemaError() {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile("create table T(ID INTEGER);");
        final String simpleXML =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='baddbname'>" +
            "<schemas>" +
            "<schema path='" +  schemaFile.getAbsolutePath()  + "'/>" +
            "</schemas>" +
            // invalid project file: no procedures
            // "<procedures>" +
            // "<procedure class='proc'><sql>select * from T</sql></procedure>" +
            //"</procedures>" +
            "</database>" +
            "</project>";
        final File xmlFile = VoltProjectBuilder.writeStringToTempFile(simpleXML);
        final String path = xmlFile.getPath();
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile(path, nothing_jar, System.out, null);
        assertFalse(success);
    }

    public void testXMLFileWithWrongDBName() {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile("create table T(ID INTEGER);");
        final String simpleXML =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='baddbname'>" +
            "<schemas>" +
            "<schema path='" +  schemaFile.getAbsolutePath()  + "'/>" +
            "</schemas>" +
            "<procedures>" +
            "<procedure class='proc'><sql>select * from T</sql></procedure>" +
            "</procedures>" +
            "</database>" +
            "</project>";
        final File xmlFile = VoltProjectBuilder.writeStringToTempFile(simpleXML);
        final String path = xmlFile.getPath();
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile(path, nothing_jar, System.out, null);
        assertFalse(success);
    }


    public void testXMLFileWithDefaultDBName() {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile("create table T(ID INTEGER);");
        final String simpleXML =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database>" +
            "<schemas>" +
            "<schema path='" +  schemaFile.getAbsolutePath()  + "'/>" +
            "</schemas>" +
            "<procedures>" +
            "<procedure class='proc'><sql>select * from T</sql></procedure>" +
            "</procedures>" +
            "</database>" +
            "</project>";
        final File xmlFile = VoltProjectBuilder.writeStringToTempFile(simpleXML);
        final String path = xmlFile.getPath();
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile(path, nothing_jar, System.out, null);
        assertTrue(success);
        assertTrue(compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database") != null);
    }

    public void testBadClusterConfig() throws IOException {
        // check no hosts
        ClusterConfig cluster_config = new ClusterConfig(0, 1, 0);
        assertFalse(cluster_config.validate());

        // check no sites-per-hosts
        cluster_config = new ClusterConfig(1, 0, 0);
        assertFalse(cluster_config.validate());
    }

    public void testXMLFileWithDDL() throws IOException {
        final String simpleSchema1 =
            "create table books (cash integer default 23 NOT NULL, title varchar(3) default 'foo', PRIMARY KEY(cash));";
        // newline inserted to test catalog friendliness
        final String simpleSchema2 =
            "create table books2\n (cash integer default 23 NOT NULL, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile1 = VoltProjectBuilder.writeStringToTempFile(simpleSchema1);
        final String schemaPath1 = schemaFile1.getPath();
        final File schemaFile2 = VoltProjectBuilder.writeStringToTempFile(simpleSchema2);
        final String schemaPath2 = schemaFile2.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<!-- xml comment check -->" +
            "<database name='database'>" +
            "<!-- xml comment check -->" +
            "<schemas>" +
            "<!-- xml comment check -->" +
            "<schema path='" + schemaPath1 + "' />" +
            "<schema path='" + schemaPath2 + "' />" +
            "<!-- xml comment check -->" +
            "</schemas>" +
            "<!-- xml comment check -->" +
            "<procedures>" +
            "<!-- xml comment check -->" +
            "<procedure class='org.voltdb.compiler.procedures.AddBook' />" +
            "<procedure class='Foo'>" +
            "<sql>select * from books;</sql>" +
            "</procedure>" +
            "</procedures>" +
            "  <partitions><partition table='books' column='cash'/></partitions> " +
            "<!-- xml comment check -->" +
            "</database>" +
            "<!-- xml comment check -->" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);

        assertTrue(success);

        final Catalog c1 = compiler.getCatalog();

        final String catalogContents = VoltCompiler.readFileFromJarfile(testout_jar, "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        assertTrue(c2.serialize().equals(c1.serialize()));
    }

    public void testProcWithBoxedParam() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas>" +
            "<schema path='" + schemaPath + "' />" +
            "</schemas>" +
            "<procedures>" +
            "<procedure class='org.voltdb.compiler.procedures.AddBookBoxed' />" +
            "</procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertFalse(success);
    }

    public void testDDLWithNoLengthString() throws IOException {

        // DO NOT COPY PASTE THIS INVALID EXAMPLE!
        final String simpleSchema1 =
            "create table books (cash integer default 23, title varchar default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema1);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas>" +
            "<schema path='" + schemaPath + "' />" +
            "</schemas>" +
            "<procedures>" +
            "<procedure class='org.voltdb.compiler.procedures.AddBook' />" +
            "<procedure class='Foo'>" +
            "<sql>select * from books;</sql>" +
            "</procedure>" +
            "</procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertFalse(success);
    }

    public void testNullablePartitionColumn() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.AddBook'/></procedures>" +
            "<partitions><partition table='BOOKS' column='CASH' /></partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);

        assertFalse(success);

        boolean found = false;
        for (final VoltCompiler.Feedback fb : compiler.m_errors) {
            if (fb.message.indexOf("Partition column") > 0)
                found = true;
        }
        assertTrue(found);
    }

    public void testXMLFileWithBadDDL() throws IOException {
        final String simpleSchema =
            "create table books (id integer default 0, strval varchar(33000) default '', PRIMARY KEY(id));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertFalse(success);
    }

    // NOTE: TPCCTest proc also tests whitespaces regressions in SQL literals
    public void testWithTPCCDDL() {
        String schemaPath = "";
        try {
            final URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertTrue(success);
    }

    public void testSeparateCatalogCompilation() throws IOException {
        String schemaPath = "";
        try {
            final URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler1 = new VoltCompiler();
        final VoltCompiler compiler2 = new VoltCompiler();

        // TODO: temporary fix so this would compile was to remove second argument (cluster_config)
        final Catalog catalog = compiler1.compileCatalog(projectPath);

        final String cat1 = catalog.serialize();
        final boolean success = compiler2.compile(projectPath, testout_jar, System.out, null);
        final String cat2 = VoltCompiler.readFileFromJarfile(testout_jar, "catalog.txt");

        assertTrue(success);
        assertTrue(cat1.compareTo(cat2) == 0);
    }

    public void testDDLTableTooManyColumns() throws IOException {
        String schemaPath = "";
        try {
            final URL url = TestVoltCompiler.class.getResource("toowidetable-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertFalse(success);

        boolean found = false;
        for (final VoltCompiler.Feedback fb : compiler.m_errors) {
            if (fb.message.startsWith("Table MANY_COLUMNS has"))
                found = true;
        }
        assertTrue(found);
    }

    public void testExtraFilesExist() throws IOException {
        String schemaPath = "";
        try {
            final URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (final UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.TPCCTestProc' /></procedures>" +
            "</database>" +
            "</project>";

        //System.out.println(simpleProject);

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertTrue(success);

        final String sql = VoltCompiler.readFileFromJarfile(testout_jar, "tpcc-ddl.sql");
        assertNotNull(sql);
    }

    public void testXMLFileWithELEnabled() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23 NOT NULL, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            " <database name='database'>" +
            "  <partitions><partition table='books' column='cash'/></partitions> " +
            "  <schemas><schema path='" + schemaPath + "' /></schemas>" +
            "  <procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "  <export>" +
            "    <tables><table name='books'/></tables>" +
            "  </export>" +
            " </database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);

        assertTrue(success);

        final Catalog c1 = compiler.getCatalog();
        //System.out.println("PRINTING Catalog 1");
        //System.out.println(c1.serialize());

        final String catalogContents = VoltCompiler.readFileFromJarfile(testout_jar, "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        assertTrue(c2.serialize().equals(c1.serialize()));
    }

    public void testOverrideProcInfo() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23 not null, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "<partitions><partition table='BOOKS' column='CASH' /></partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final ProcInfoData info = new ProcInfoData();
        info.singlePartition = true;
        info.partitionInfo = "BOOKS.CASH: 0";
        final Map<String, ProcInfoData> overrideMap = new HashMap<String, ProcInfoData>();
        overrideMap.put("AddBook", info);

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, overrideMap);

        assertTrue(success);

        final String catalogContents = VoltCompiler.readFileFromJarfile(testout_jar, "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        final Database db = c2.getClusters().get("cluster").getDatabases().get("database");
        final Procedure addBook = db.getProcedures().get("AddBook");
        assertEquals(true, addBook.getSinglepartition());
    }

    public void testBadStmtProcName() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23 not null, title varchar(10) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='@Foo'><sql>select * from books;</sql></procedure></procedures>" +
            "<partitions><partition table='BOOKS' column='CASH' /></partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertFalse(success);
    }

    public void testGoodStmtProcName() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23 not null, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='Foo'><sql>select * from books;</sql></procedure></procedures>" +
            "<partitions><partition table='BOOKS' column='CASH' /></partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertTrue(success);
    }

    public void testMaterializedView() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23 NOT NULL, title varchar(10) default 'foo', PRIMARY KEY(cash));\n" +
            "create view matt (title, num, foo) as select title, count(*), sum(cash) from books group by title;";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<partitions><partition table='books' column='cash'/></partitions> " +
            "<procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        // final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertTrue(success);
        final Catalog c1 = compiler.getCatalog();
        final String catalogContents = VoltCompiler.readFileFromJarfile(testout_jar, "catalog.txt");
        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);
        assertTrue(c2.serialize().equals(c1.serialize()));
    }


    public void testVarbinary() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23 NOT NULL, title varbinary(10) default NULL, PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<partitions><partition table='books' column='cash'/></partitions> " +
            "<procedures>" +
            "<procedure class='get'><sql>select * from books;</sql></procedure>" +
            "<procedure class='i1'><sql>insert into books values(5, 'AA');</sql></procedure>" +
            "<procedure class='i2'><sql>insert into books values(5, ?);</sql></procedure>" +
            "<procedure class='s1'><sql>update books set title = 'bb';</sql></procedure>" +
            "</procedures>" +
            //"<procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        // final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertTrue(success);
        final Catalog c1 = compiler.getCatalog();
        final String catalogContents = VoltCompiler.readFileFromJarfile(testout_jar, "catalog.txt");
        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);
        assertTrue(c2.serialize().equals(c1.serialize()));
    }


    //
    // There are DDL tests a number of places. TestDDLCompiler seems more about
    // verifying HSQL behaviour. Additionally, there are users of PlannerAideDeCamp
    // that verify plans for various DDL/SQL combinations.
    //
    // I'm going to add some DDL parsing validation tests here, as they seem to have
    // more to do with compiling a catalog.. and there are some related tests already
    // in this file.
    //

    private VoltCompiler compileForDDLTest(String schemaPath, boolean expectSuccess) {
        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<procedures><procedure class='sample'><sql>select * from t</sql></procedure></procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        projectFile.deleteOnExit();
        final String projectPath = projectFile.getPath();
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertEquals(expectSuccess, success);
        return compiler;
    }

    private String getPathForSchema(String s) {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(s);
        schemaFile.deleteOnExit();
        return schemaFile.getPath();
    }

    public void testDDLCompilerLeadingGarbage() throws IOException {
        final String s =
            "-- a valid comment\n" +
            "- an invalid comment\n" +
            "create table t(id integer);";

        VoltCompiler c = compileForDDLTest(getPathForSchema(s), false);
        assertTrue(c.hasErrors());
    }

    public void testDDLCompilerLeadingWhitespace() throws IOException {
        final String s =
            "     \n" +
            "\n" +
            "create table t(id integer);";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }

    public void testDDLCompilerLeadingComment() throws IOException {
        final String s =
            "-- this is a leading comment\n" +
            "  -- with some leading whitespace\n" +
            "     create table t(id integer);";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }

    public void testDDLCompilerNoNewlines() throws IOException {
        final String s =
            "create table t(id integer); create table r(id integer);";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 2);
    }

    public void testDDLCompilerSplitLines() throws IOException {
        final String s =
            "create\n" +
            "table\n" +
            "t(id\n" +
            "integer);";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }

    public void testDDLCompilerTrailingComment1() throws IOException {
        final String s =
            "create table t(id integer) -- this is a trailing comment\n" +
            "-- and a line full of comments\n" +
            ";\n";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }

    public void testDDLCompilerTrailingComment2() throws IOException {
        final String s =
            "create table t(id integer) -- this is a trailing comment\n" +
            ";\n";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }

    public void testDDLCompilerTrailingComment3() throws IOException {
        final String s =
            "create table t(id integer) -- this is a trailing comment\n" +
            "-- and a line full of comments\n" +
            ";";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }

    public void testDDLCompilerTrailingComment4() throws IOException {
        final String s =
            "create table t(id integer) -- this is a trailing comment\n" +
            ";";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }

    public void testDDLCompilerTrailingComment5() throws IOException {
        final String s =
            "create table t(id integer) -- this is a trailing comment\n" +
            "-- and a line full of comments\n" +
            "    ;\n";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }

    public void testDDLCompilerTrailingComment6() throws IOException {
        final String s =
            "create table t(id integer) -- this is a trailing comment\n" +
            "-- and a line full of comments\n" +
            "    ;\n" +
            "-- ends with a comment\n";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }


    public void testDDLCompilerInvalidStatement() throws IOException {
        final String s =
            "create table t for justice -- with a comment\n";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), false);
        assertTrue(c.hasErrors());
    }

    public void testDDLCompilerCommentThatLooksLikeStatement() throws IOException {
        final String s =
            "create table t(id integer); -- create table r(id integer);";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
    }

    public void testDDLCompilerLeadingSemicolon() throws IOException {
        final String s = "; create table t(id integer);";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), false);
        assertTrue(c.hasErrors());
    }

    public void testDDLCompilerMultipleStatementsOnMultipleLines() throws IOException {
        final String s =
            "create table t(id integer); create\n" +
            "table r(id integer); -- second table";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 2);
    }

    public void testDDLCompilerStringLiteral() throws IOException {
        final String s =
            "create table t(id varchar(3) default 'abc');";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);

        Table tbl = c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().getIgnoreCase("t");
        String defaultvalue = tbl.getColumns().getIgnoreCase("id").getDefaultvalue();
        assertTrue(defaultvalue.equalsIgnoreCase("abc"));
    }

    public void testDDLCompilerSemiColonInStringLiteral() throws IOException {
        final String s =
            "create table t(id varchar(5) default 'a;bc');";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);

        Table tbl = c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().getIgnoreCase("t");
        String defaultvalue = tbl.getColumns().getIgnoreCase("id").getDefaultvalue();
        assertTrue(defaultvalue.equalsIgnoreCase("a;bc"));
    }

    public void testDDLCompilerDashDashInStringLiteral() throws IOException {
        final String s =
            "create table t(id varchar(5) default 'a--bc');";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);

        Table tbl = c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().getIgnoreCase("t");
        String defaultvalue = tbl.getColumns().getIgnoreCase("id").getDefaultvalue();
        assertTrue(defaultvalue.equalsIgnoreCase("a--bc"));
    }

    public void testDDLCompilerNewlineInStringLiteral() throws IOException {
        final String s =
            "create table t(id varchar(5) default 'a\n" + "bc');";

        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
        Table tbl = c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().getIgnoreCase("t");
        String defaultvalue = tbl.getColumns().getIgnoreCase("id").getDefaultvalue();

        // In the debugger, this looks valid at parse time but is mangled somewhere
        // later, perhaps in HSQL or in the catalog assembly?
        // ENG-681
        System.out.println(defaultvalue);
        // assertTrue(defaultvalue.equalsIgnoreCase("a\nbc"));
    }

    public void testDDLCompilerEscapedStringLiterals() throws IOException {
        final String s =
            "create table t(id varchar(10) default 'a''b''''c');";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        assertTrue(c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().size() == 1);
        Table tbl = c.m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables().getIgnoreCase("t");
        String defaultvalue = tbl.getColumns().getIgnoreCase("id").getDefaultvalue();
        assertTrue(defaultvalue.equalsIgnoreCase("a'b''c"));
    }

    // Test that DDLCompiler's index creation adheres to the rules implicit in
    // the EE's tableindexfactory.  Currently (10/3/2010) these are:
    // All column types can be used in a tree array.  Only int types can
    // be used in hash tables or array indexes

    String[] column_types = {"tinyint", "smallint", "integer", "bigint",
                             "float", "varchar(10)", "timestamp", "decimal"};

    IndexType[] default_index_types = {IndexType.BALANCED_TREE,
                                       IndexType.BALANCED_TREE,
                                       IndexType.BALANCED_TREE,
                                       IndexType.BALANCED_TREE,
                                       IndexType.BALANCED_TREE,
                                       IndexType.BALANCED_TREE,
                                       IndexType.BALANCED_TREE,
                                       IndexType.BALANCED_TREE};

    boolean[] can_be_hash = {true, true, true, true, false, false, true, false};
    boolean[] can_be_array = {true, true, true, true, false, false, true, false};
    boolean[] can_be_tree = {true, true, true, true, true, true, true, true};

    public void testDDLCompilerIndexDefaultTypes()
    {
        for (int i = 0; i < column_types.length; i++)
        {
            String s =
                "create table t(id " + column_types[i] + " not null, num integer not null);\n" +
                "create index idx_t_id on t(id);\n" +
                "create index idx_t_idnum on t(id,num);";
            VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
            assertFalse(c.hasErrors());
            Database d = c.m_catalog.getClusters().get("cluster").getDatabases().get("database");
            assertEquals(default_index_types[i].getValue(),
                         d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t_id").getType());
            assertEquals(default_index_types[i].getValue(),
                         d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t_idnum").getType());
        }
    }

    public void testDDLCompilerHashIndexAllowed()
    {
        for (int i = 0; i < column_types.length; i++)
        {
            final String s =
                "create table t(id " + column_types[i] + " not null, num integer not null);\n" +
                "create index idx_t_id_hash on t(id);\n" +
                "create index idx_t_idnum_hash on t(id,num);";
            VoltCompiler c = compileForDDLTest(getPathForSchema(s), can_be_hash[i]);
            if (can_be_hash[i])
            {
                // do appropriate index exists checks
                assertFalse(c.hasErrors());
                Database d = c.m_catalog.getClusters().get("cluster").getDatabases().get("database");
                assertEquals(IndexType.HASH_TABLE.getValue(),
                             d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t_id_hash").getType());
                assertEquals(IndexType.HASH_TABLE.getValue(),
                             d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t_idnum_hash").getType());
            }
            else
            {
                assertTrue(c.hasErrors());
            }
        }
    }

    public void testDDLCompilerArrayIndexAllowed()
    {
        for (int i = 0; i < column_types.length; i++)
        {
            final String s =
                "create table t(id " + column_types[i] + " not null, num integer not null);\n" +
                "create index idx_t_id_array on t(id);\n" +
                "create index idx_t_idnum_array on t(id,num);";
            VoltCompiler c = compileForDDLTest(getPathForSchema(s), can_be_array[i]);
            if (can_be_array[i])
            {
                // do appropriate index exists checks
                assertFalse(c.hasErrors());
                Database d = c.m_catalog.getClusters().get("cluster").getDatabases().get("database");
                assertEquals(IndexType.ARRAY.getValue(),
                             d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t_id_array").getType());
                assertEquals(IndexType.ARRAY.getValue(),
                             d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t_idnum_array").getType());
            }
            else
            {
                assertTrue(c.hasErrors());
            }
        }
    }

    public void testUniqueIndexAllowed()
    {
        final String s =
                "create table t(id integer not null, num integer not null);\n" +
                "create unique index idx_t_unique on t(id,num);\n" +
                "create index idx_t on t(num);";
        VoltCompiler c = compileForDDLTest(getPathForSchema(s), true);
        assertFalse(c.hasErrors());
        Database d = c.m_catalog.getClusters().get("cluster").getDatabases().get("database");
        assertTrue(d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t_unique").getUnique());
        assertFalse(d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t").getUnique());
    }

    public void testDDLCompilerVarcharTreeIndexAllowed()
    {
        for (int i = 0; i < column_types.length; i++)
        {
            final String s =
                "create table t(id " + column_types[i] + " not null, num integer not null);\n" +
                "create index idx_t_id_tree on t(id);\n" +
                "create index idx_t_idnum_tree on t(id,num);";
            VoltCompiler c = compileForDDLTest(getPathForSchema(s), can_be_tree[i]);
            assertFalse(c.hasErrors());
            Database d = c.m_catalog.getClusters().get("cluster").getDatabases().get("database");
            assertEquals(IndexType.BALANCED_TREE.getValue(),
                         d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t_id_tree").getType());
            assertEquals(IndexType.BALANCED_TREE.getValue(),
                         d.getTables().getIgnoreCase("t").getIndexes().getIgnoreCase("idx_t_idnum_tree").getType());
        }
    }

    public void testPartitionOnBadType() {
        final String simpleSchema =
            "create table books (cash float default 0.0 NOT NULL, title varchar(10) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database>" +
            "<schemas><schema path='" + schemaPath + "' /></schemas>" +
            "<partitions><partition table='books' column='cash'/></partitions> " +
            "<procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile(projectPath, testout_jar, System.out, null);
        assertFalse(success);
    }
}
