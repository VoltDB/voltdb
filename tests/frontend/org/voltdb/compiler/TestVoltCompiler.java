/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;

import junit.framework.TestCase;

import org.voltdb.ProcInfoData;
import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.catalog.*;
import org.voltdb.compiler.VoltCompiler.Feedback;
import org.voltdb.regressionsuites.TestExportSuite;
import org.voltdb.utils.JarReader;
public class TestVoltCompiler extends TestCase {

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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);
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


    private static ArrayList<Feedback> checkPartitionParam(String ddl, String procedureClass, String table) {
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

        final boolean success = compiler.compile(projectPath, "testout.jar", null, null);
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
                JarReader.readFileFromJarfile("/tmp/snapshot_settings_test.jar", "catalog.txt");
            final Catalog cat = new Catalog();
            cat.execute(catalogContents);
            SnapshotSchedule schedule =
                cat.getClusters().get("cluster").getDatabases().
                    get("database").getSnapshotschedule().get("default");
            assertEquals(32, schedule.getFrequencyvalue());
            assertEquals("m", schedule.getFrequencyunit());
            assertEquals("/tmp", schedule.getPath());
            assertEquals("woobar", schedule.getPrefix());
        } finally {
            final File jar = new File("/tmp/snapshot_settings_test.jar");
            jar.delete();
        }
    }

    // TestELTSuite tests most of these options are tested end-to-end; however need to test
    // that a disabled connector is really disabled and that auth data is correct.
    public void testELTSetting() throws IOException {
        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestExportSuite.class.getResource("sqltypessuite-ddl.sql"));
        project.addSchema(TestExportSuite.class.getResource("sqltypessuite-nonulls-ddl.sql"));
        project.addProcedures(org.voltdb.regressionsuites.sqltypesprocs.Insert.class);
        project.addPartitionInfo("NO_NULLS", "PKEY");
        project.addPartitionInfo("ALLOW_NULLS", "PKEY");
        project.addPartitionInfo("WITH_DEFAULTS", "PKEY");
        project.addPartitionInfo("WITH_NULL_DEFAULTS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_WITH_NULLS", "PKEY");
        project.addPartitionInfo("EXPRESSIONS_NO_NULLS", "PKEY");
        project.addPartitionInfo("JUMBO_ROW", "PKEY");
        project.addELT("org.voltdb.elt.processors.RawProcessor", false, null);
        project.addELTTable("ALLOW_NULLS", false);   // persistent table
        project.addELTTable("WITH_DEFAULTS", true);  // streamed table
        try {
            assertTrue(project.compile("/tmp/eltsettingstest.jar"));
            final String catalogContents =
                JarReader.readFileFromJarfile("/tmp/eltsettingstest.jar", "catalog.txt");
            final Catalog cat = new Catalog();
            cat.execute(catalogContents);

            Connector connector = cat.getClusters().get("cluster").getDatabases().
                get("database").getConnectors().get("0");
            assertFalse(connector.getEnabled());

        } finally {
            final File jar = new File("/tmp/eltsettingstest.jar");
            jar.delete();
        }

    }

    // test that ELT configuration is insensitive to the case of the table name
    public void testELTTableCase() throws IOException {
        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ELTTester-ddl.sql"));
        project.addStmtProcedure("Dummy", "insert into a values (?, ?, ?);",
                                 "a.a_id: 0");
        project.addPartitionInfo("A", "A_ID");
        project.addPartitionInfo("B", "B_ID");
        project.addPartitionInfo("e", "e_id");
        project.addPartitionInfo("f", "f_id");
        project.addELT("org.voltdb.elt.processors.RawProcessor", true, null);
        project.addELTTable("A", true); // uppercase DDL, uppercase export
        project.addELTTable("b", true); // uppercase DDL, lowercase export
        project.addELTTable("E", true); // lowercase DDL, uppercase export
        project.addELTTable("f", true); // lowercase DDL, lowercase export
        try {
            assertTrue(project.compile("/tmp/eltsettingstest.jar"));
            final String catalogContents =
                JarReader.readFileFromJarfile("/tmp/eltsettingstest.jar", "catalog.txt");
            final Catalog cat = new Catalog();
            cat.execute(catalogContents);
            Connector connector = cat.getClusters().get("cluster").getDatabases().
                get("database").getConnectors().get("0");
            assertTrue(connector.getEnabled());
            // Assert that all tables exist in the connector section of catalog
            assertNotNull(connector.getTableinfo().getIgnoreCase("a"));
            assertNotNull(connector.getTableinfo().getIgnoreCase("b"));
            assertNotNull(connector.getTableinfo().getIgnoreCase("e"));
            assertNotNull(connector.getTableinfo().getIgnoreCase("f"));
        } finally {
            final File jar = new File("/tmp/eltsettingstest.jar");
            jar.delete();
        }
    }

    // test that the source table for a view is not export only
    public void testViewSourceNotExportOnly() throws IOException {
        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ELTTesterWithView-ddl.sql"));
        project.addStmtProcedure("Dummy", "select * from v_table1r_el_only");
        project.addELT("org.voltdb.elt.processors.RawProcessor", true, null);
        project.addELTTable("table1r_el_only", true);
        try {
            assertFalse(project.compile("/tmp/elttestview.jar"));
        }
        finally {
            final File jar = new File("/tmp/elttestview.jar");
            jar.delete();
        }
    }

    // test that a view is not export only
    public void testViewNotExportOnly() throws IOException {
        final VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ELTTesterWithView-ddl.sql"));
        project.addStmtProcedure("Dummy", "select * from table1r_el_only");
        project.addELT("org.voltdb.elt.processors.RawProcessor", true, null);
        project.addELTTable("v_table1r_el_only", true);
        try {
            assertFalse(project.compile("/tmp/elttestview.jar"));
        }
        finally {
            final File jar = new File("/tmp/elttestview.jar");
            jar.delete();
        }
    }

    public void testBadPath() {
        final VoltCompiler compiler = new VoltCompiler();
        final boolean success = compiler.compile("invalidnonsense", "nothing", System.out, null);

        assertFalse(success);
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

        final boolean success = compiler.compile(path, "nothing", System.out, null);

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
        final boolean success = compiler.compile(path, "nothing", System.out, null);
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
        final boolean success = compiler.compile(path, "nothing", System.out, null);
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
        final boolean success = compiler.compile(path, "nothing", System.out, null);
        assertTrue(success);
        assertTrue(compiler.m_catalog.getClusters().get("cluster").getDatabases().get("database") != null);
    }

    public void testBadClusterConfig() throws IOException {
        // check no hosts
        ClusterConfig cluster_config = new ClusterConfig(0, 1, 0, "localhost");
        assertFalse(cluster_config.validate());

        // check no sites-per-hosts
        cluster_config = new ClusterConfig(1, 0, 0, "localhost");
        assertFalse(cluster_config.validate());
    }

    public void testXMLFileWithDDL() throws IOException {
        final String simpleSchema1 =
            "create table books (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));";
        // newline inserted to test catalog friendliness
        final String simpleSchema2 =
            "create table books2\n (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));";

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
            "<!-- xml comment check -->" +
            "</database>" +
            "<!-- xml comment check -->" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);

        assertTrue(success);

        final Catalog c1 = compiler.getCatalog();

        final String catalogContents = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        assertTrue(c2.serialize().equals(c1.serialize()));

        final File jar = new File("testout.jar");
        jar.delete();
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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);

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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);
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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);

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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);

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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);

        assertTrue(success);

        final File jar = new File("testout.jar");
        jar.delete();
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
        final boolean success = compiler2.compile(projectPath, "testout.jar", System.out, null);
        final String cat2 = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        assertTrue(success);
        assertTrue(cat1.compareTo(cat2) == 0);

        final File jar = new File("testout.jar");
        jar.delete();
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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);
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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);
        assertTrue(success);

        final String sql = JarReader.readFileFromJarfile("testout.jar", "tpcc-ddl.sql");
        assertNotNull(sql);

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testXMLFileWithELEnabled() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            " <database name='database'>" +
            "  <schemas><schema path='" + schemaPath + "' /></schemas>" +
            "  <procedures><procedure class='org.voltdb.compiler.procedures.AddBook' /></procedures>" +
            "  <exports><connector class='org.voltdb.VerticaLoader'> " +
            "             <tables><table name='books' exportonly='true'/></tables>" +
            "           </connector>" +
            "  </exports>" +
            " </database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);

        assertTrue(success);

        final Catalog c1 = compiler.getCatalog();
        //System.out.println("PRINTING Catalog 1");
        //System.out.println(c1.serialize());

        final String catalogContents = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        assertTrue(c2.serialize().equals(c1.serialize()));

        final File jar = new File("testout.jar");
        jar.delete();
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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, overrideMap);

        assertTrue(success);

        final String catalogContents = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        final Database db = c2.getClusters().get("cluster").getDatabases().get("database");
        final Procedure addBook = db.getProcedures().get("AddBook");
        assertEquals(true, addBook.getSinglepartition());

        final File jar = new File("testout.jar");
        jar.delete();
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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);

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

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);

        assertTrue(success);

        final File jar = new File("testout.jar");
        jar.delete();
    }

    public void testMaterializedView() throws IOException {
        final String simpleSchema =
            "create table books (cash integer default 23, title varchar(10) default 'foo', PRIMARY KEY(cash));\n" +
            "create view matt (title, num, foo) as select title, count(*), sum(cash) from books group by title;";

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
        // final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);

        assertTrue(success);

        final Catalog c1 = compiler.getCatalog();

        final String catalogContents = JarReader.readFileFromJarfile("testout.jar", "catalog.txt");

        final Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        assertTrue(c2.serialize().equals(c1.serialize()));

        final File jar = new File("testout.jar");
        jar.delete();
    }


    /*
     * There are DDL tests a number of places. TestDDLCompiler seems more about
     * verifying HSQL behaviour. Additionally, there are users of PlannerAideDeCamp
     * that verify plans for various DDL/SQL combinations.
     *
     * I'm going to add some DDL parsing validation tests here, as they seem to have
     * more to do with compiling a catalog.. and there are some related tests already
     * in this file.
     */

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
        final boolean success = compiler.compile(projectPath, "testout.jar", System.out, null);
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

    /*public void testForeignKeys() {
        String schemaPath = "";
        try {
            final URL url = TPCCClient.class.getResource("tpcc-ddl-fkeys.sql");
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
        final ClusterConfig cluster_config = new ClusterConfig(1, 1, 0, "localhost");

        final Catalog catalog = compiler.compileCatalog(projectPath, cluster_config);
        assertNotNull(catalog);

        // Now check that CUSTOMER correctly references DISTRICT
        //  (1) Make sure CUSTOMER has a fkey constraint
        //  (2) Make sure that each source column in CUSTOMER points to the constraint
        //  (3) Make sure that the fkey constraint points to DISTRICT
        final Database catalog_db = catalog.getClusters().get("cluster").getDatabases().get("database");
        assertNotNull(catalog_db);

        final Table cust_table = catalog_db.getTables().get("CUSTOMER");
        assertNotNull(cust_table);
        final Table dist_table = catalog_db.getTables().get("DISTRICT");
        assertNotNull(dist_table);

        // In the code below, we will refer to the column that is pointed to by another column
        // in the dependency as the parent, and the column with the fkey constraint as the child
        boolean found = false;
        for (final Constraint catalog_const : cust_table.getConstraints()) {
            final ConstraintType const_type = ConstraintType.get(catalog_const.getType());
            if (const_type == ConstraintType.FOREIGN_KEY) {
                found = true;
                assertEquals(dist_table, catalog_const.getForeignkeytable());
                assertEquals(catalog_const.getForeignkeycols().size(), 2);

                for (final ColumnRef catalog_parent_colref : catalog_const.getForeignkeycols()) {

                    // We store the name of the child column in the name of the ColumnRef catalog
                    // object to the parent
                    final Column catalog_child_col = cust_table.getColumns().get(catalog_parent_colref.getTypeName());
                    assertNotNull(catalog_child_col);
                    // Lame
                    boolean found_const_for_child = false;
                    for (final ConstraintRef catalog_constref : catalog_child_col.getConstraints()) {
                        if (catalog_constref.getConstraint().equals(catalog_const)) {
                            found_const_for_child = true;
                            break;
                        }
                    }
                    assertTrue(found_const_for_child);

                    final Column catalog_parent_col = catalog_parent_colref.getColumn();
                    assertEquals(dist_table, catalog_parent_col.getParent());
                }
                break;
            }
        }
        assertTrue(found);
    }*/
}
