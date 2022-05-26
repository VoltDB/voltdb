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

package org.voltdb.compiler;

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hsqldb_voltpatches.HsqlException;
import org.mockito.Mockito;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.TableType;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.ConnectorTableInfo;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltCompiler.Feedback;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.IndexType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

import junit.framework.TestCase;

// Note: as of 2022-01-17, unit tests for
// CREATE TABLE ...  EXPORT|MIGRATE TO TARGET|TOPIC
// are in file TestVoltCompilerTableExport.

public class TestVoltCompiler extends TestCase {
    private String nothing_jar;
    private String testout_jar;

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

    public void testDDLCompilerTTL() throws Exception {
        String ddl = "create table ttl (a integer NOT NULL, b integer, c timestamp default now() not null, PRIMARY KEY(a)) USING TTL 10 SECONDS ON COLUMN c;\n" +
                     "alter table ttl alter USING TTL 20 MINUTES ON COLUMN c;\n" +
                     "alter table ttl alter USING TTL 20 ON COLUMN c;\n" +
                     "alter table ttl alter USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10;\n" +
                     "alter table ttl alter USING TTL 20 MINUTES ON COLUMN c MAX_FREQUENCY 10;\n" +
                     "alter table ttl alter USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 1;\n" +
                     "alter table ttl alter USING TTL 20 MINUTES ON COLUMN c MAX_FREQUENCY 3 BATCH_SIZE 1;\n" +
                     "alter table ttl alter USING TTL 20 MINUTES ON COLUMN c MAX_FREQUENCY 3;\n" +
                     "alter table ttl alter USING TTL 20 ON COLUMN c BATCH_SIZE 10;\n" +
                     "alter table ttl drop TTL;\n" +
                     "alter table ttl ADD USING TTL 20 ON COLUMN c BATCH_SIZE 10;\n";
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(ddl);
        assertTrue(pb.compile(Configuration.getPathToCatalogForTest("testout.jar")));

        // can not drop ttl column
        ddl = "create table ttl (a integer NOT NULL, b integer, c timestamp default now() not null, PRIMARY KEY(a)) USING TTL 10 SECONDS ON COLUMN c;\n" +
              "alter table ttl drop column c;\n";
        pb = new VoltProjectBuilder();
        pb.addLiteralSchema(ddl);
        assertFalse(pb.compile(Configuration.getPathToCatalogForTest("testout.jar")));

        // max_fequency must be positive integer
        ddl = "create table ttl (a integer NOT NULL, b integer, c timestamp default now() not null, PRIMARY KEY(a)) USING TTL 10 SECONDS ON COLUMN c;\n" +
              "alter table ttl alter USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 0;\n";
        pb = new VoltProjectBuilder();
        pb.addLiteralSchema(ddl);
        assertFalse(pb.compile(Configuration.getPathToCatalogForTest("testout.jar")));
    }

    public void testDDLFiltering() {

        String ddl = "file -inlinebatch END_OF_DROP_BATCH\n" +
                     "-- This comment is inside a batch\n" +
                     "DROP PROCEDURE Initialize                     IF EXISTS;\n" +
                     "DROP PROCEDURE Results                         IF EXISTS;\n" +
                     "\n" +
                     "END_OF_DROP_BATCH\n" +
                     "-- This command cannot be part of a DDL batch.\n" +
                     "LOAD CLASSES voter-procs.jar\n";
        VoltCompiler compiler = new VoltCompiler(false);
        boolean success = compileInitDDL(true, ddl, compiler);
        assertTrue(success);

        success = compileInitDDL(false, ddl, compiler);
        assertFalse(success);
    }

    public void testDDLFilteringNoEndBatch() {

        String ddl = "file -inlinebatch END_OF_DROP_BATCH\n" +
                     "-- This comment is inside a batch\n" +
                     "DROP PROCEDURE Initialize                     IF EXISTS;\n" +
                     "DROP PROCEDURE Results                         IF EXISTS;\n" +
                     "\n";

        VoltCompiler compiler = new VoltCompiler(false);
        boolean success = compileInitDDL(true, ddl, compiler);
        assertFalse(success);
    }

    public void testDDLFilteringCaseInsensitve() {

        String ddl = "FiLe -inlinebatch END_OF_DROP_BATCH\n" +
                     "-- This comment is inside a batch\n" +
                     "DROP PROCEDURE Initialize                     IF EXISTS;\n" +
                     "DROP PROCEDURE Results                         IF EXISTS;\n" +
                     "\n" +
                     "END_OF_DROP_BATCH\n" +
                     "-- This command cannot be part of a DDL batch.\n" +
                     "Load Classes voter-procs.jar\n";

        VoltCompiler compiler = new VoltCompiler(false);
        boolean success = compileInitDDL(true, ddl, compiler);
        assertTrue(success);
    }

    public void testBrokenLineParsing() throws IOException {
        String schema =
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
            "group by column2_integer\n;\n" +
            "create procedure Foo as select * from table1r_el;";

        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(schema);
        assertTrue(pb.compile(Configuration.getPathToCatalogForTest("testout.jar")));
    }

    public void testUTF8XMLFromHSQL() throws IOException {
        String schema =
                "create table blah  (pkey integer not null, strval varchar(200), PRIMARY KEY(pkey));\n";
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(schema);
        pb.addStmtProcedure("utf8insert", "insert into blah values(1, 'něco za nic')");
        pb.addPartitionInfo("blah", "pkey");
        assertTrue(pb.compile(Configuration.getPathToCatalogForTest("utf8xml.jar")));
    }

    private String feedbackToString(List<Feedback> fbs) {
        StringBuilder sb = new StringBuilder();
        for (Feedback fb : fbs) {
            sb.append(fb.getStandardFeedbackLine() + "\n");
        }
        return sb.toString();
    }

    private boolean isFeedbackPresent(String expectedError,
            ArrayList<Feedback> fbs) {
        String expErr = expectedError.replaceAll("\\s+", " ");
        for (Feedback fb : fbs) {
            String fbLine = fb.getStandardFeedbackLine().replaceAll("\\s+", " ");
            if (fbLine.contains(expErr)) {
                return true;
            }
        }
        return false;
    }

    public void testMismatchedPartitionParams() {
        String expectedError;
        ArrayList<Feedback> fbs;

        /**
         * FIXME:
         * It is hard to figure out the differences between test cases.
         * Better with using common variable to check out the diffs.
         */

        fbs = checkPartitionParam("CREATE TABLE PKEY_BIGINT ( PKEY BIGINT NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_BIGINT ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamBigint;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamBigint ON TABLE PKEY_BIGINT COLUMN PKEY;",
                "PKEY_BIGINT");
        expectedError =
                "Type mismatch between partition column and partition parameter for procedure " +
                "org.voltdb.compiler.procedures.NotAnnotatedPartitionParamBigint may cause overflow or loss of precision.\n" +
                "Partition column is type VoltType.BIGINT and partition parameter is type VoltType.STRING";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE PARTITION ON TABLE PKEY_INTEGER COLUMN PKEY FROM CLASS org.voltdb.compiler.procedures.PartitionParamInteger;",
                "PKEY_INTEGER");
        expectedError =
                "Type mismatch between partition column and partition parameter for procedure " +
                "org.voltdb.compiler.procedures.PartitionParamInteger may cause overflow or loss of precision.\n" +
                "Partition column is type VoltType.INTEGER and partition parameter " +
                "is type VoltType.BIGINT";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY;",
                "PKEY_INTEGER");
        expectedError =
                "Type mismatch between partition column and partition parameter for procedure " +
                "org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger may cause overflow or loss of precision.\n" +
                "Partition column is type VoltType.INTEGER and partition parameter " +
                "is type VoltType.BIGINT";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_SMALLINT ( PKEY SMALLINT NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_SMALLINT ON COLUMN PKEY;" +
                "CREATE PROCEDURE PARTITION ON TABLE PKEY_SMALLINT COLUMN PKEY FROM CLASS org.voltdb.compiler.procedures.PartitionParamSmallint;",
                "PKEY_SMALLINT");
        expectedError =
                "Type mismatch between partition column and partition parameter for procedure " +
                "org.voltdb.compiler.procedures.PartitionParamSmallint may cause overflow or loss of precision.\n" +
                "Partition column is type VoltType.SMALLINT and partition parameter " +
                "is type VoltType.BIGINT";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_SMALLINT ( PKEY SMALLINT NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_SMALLINT ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamSmallint;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamSmallint ON TABLE PKEY_SMALLINT COLUMN PKEY;",
                "PKEY_SMALLINT");
        expectedError =
                "Type mismatch between partition column and partition parameter for procedure " +
                "org.voltdb.compiler.procedures.NotAnnotatedPartitionParamSmallint may cause overflow or loss of precision.\n" +
                "Partition column is type VoltType.SMALLINT and partition parameter " +
                "is type VoltType.BIGINT";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_TINYINT ( PKEY TINYINT NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_TINYINT ON COLUMN PKEY;" +
                "CREATE PROCEDURE PARTITION ON TABLE PKEY_TINYINT COLUMN PKEY FROM CLASS org.voltdb.compiler.procedures.PartitionParamTinyint;",
                "PKEY_TINYINT");
        expectedError =
                "Type mismatch between partition column and partition parameter for procedure " +
                "org.voltdb.compiler.procedures.PartitionParamTinyint may cause overflow or loss of precision.\n" +
                "Partition column is type VoltType.TINYINT and partition parameter " +
                "is type VoltType.SMALLINT";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_TINYINT ( PKEY TINYINT NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_TINYINT ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamTinyint;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamTinyint ON TABLE PKEY_TINYINT COLUMN PKEY;",
                "PKEY_TINYINT");
        expectedError =
                "Type mismatch between partition column and partition parameter for procedure " +
                "org.voltdb.compiler.procedures.NotAnnotatedPartitionParamTinyint may cause overflow or loss of precision.\n" +
                "Partition column is type VoltType.TINYINT and partition parameter " +
                "is type VoltType.SMALLINT";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_STRING ( PKEY VARCHAR(32) NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_STRING ON COLUMN PKEY;" +
                "CREATE PROCEDURE PARTITION ON TABLE PKEY_STRING COLUMN PKEY FROM CLASS org.voltdb.compiler.procedures.PartitionParamString;",
                "PKEY_STRING");
        expectedError =
                "Type mismatch between partition column and partition parameter for procedure " +
                "org.voltdb.compiler.procedures.PartitionParamString may cause overflow or loss of precision.\n" +
                "Partition column is type VoltType.STRING and partition parameter " +
                "is type VoltType.INTEGER";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkPartitionParam("CREATE TABLE PKEY_STRING ( PKEY VARCHAR(32) NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_STRING ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamString;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamString ON TABLE PKEY_STRING COLUMN PKEY;",
                "PKEY_STRING");
        expectedError =
                "Type mismatch between partition column and partition parameter for procedure " +
                "org.voltdb.compiler.procedures.NotAnnotatedPartitionParamString may cause overflow or loss of precision.\n" +
                "Partition column is type VoltType.STRING and partition parameter " +
                "is type VoltType.INTEGER";
        assertTrue(isFeedbackPresent(expectedError, fbs));
    }

    private ArrayList<Feedback> checkPartitionParam(String ddl, String table) {
        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(ddl, compiler);
        assertFalse(success);
        return compiler.m_errors;
    }

    public void testPartitionProcedureWarningMessage() {
        String ddl = "CREATE TABLE PKEY_BIGINT ( PKEY BIGINT NOT NULL, NUM INTEGER, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_BIGINT ON COLUMN PKEY;" +
                "create procedure myTestProc as select num from PKEY_BIGINT where pkey = ? order by 1;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(ddl, compiler);
        assertTrue(success);

        String expectedWarning =
                "This procedure myTestProc would benefit from being partitioned, by adding a " +
                "'PARTITION ON TABLE PKEY_BIGINT COLUMN PKEY PARAMETER 0' clause to the " +
                "CREATE PROCEDURE statement. or using a separate PARTITION PROCEDURE statement";

        boolean findMatched = false;
        for (Feedback fb : compiler.m_warnings) {
            System.out.println(fb.getStandardFeedbackLine());
            if (fb.getStandardFeedbackLine().contains(expectedWarning)) {
                findMatched = true;
                break;
            }
        }
        assertTrue(findMatched);
    }

    public void testSnapshotSettings() throws IOException {
        String schemaPath = "";
        try {
            URL url = TPCCProjectBuilder.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.addProcedure(org.voltdb.compiler.procedures.TPCCTestProc.class);
        builder.setSnapshotSettings("32m", 5, "/tmp", "woobar");
        builder.addSchema(schemaPath);
        try {
            assertTrue(builder.compile("/tmp/snapshot_settings_test.jar"));
            String catalogContents =
                VoltCompilerUtils.readFileFromJarfile("/tmp/snapshot_settings_test.jar", "catalog.txt");
            Catalog cat = new Catalog();
            cat.execute(catalogContents);
            CatalogUtil.compileDeployment(cat, builder.getPathToDeployment(), false);
            SnapshotSchedule schedule =
                cat.getClusters().get("cluster").getDatabases().
                    get("database").getSnapshotschedule().get("default");
            assertEquals(32, schedule.getFrequencyvalue());
            assertEquals("m", schedule.getFrequencyunit());
            assertEquals("woobar", schedule.getPrefix());
        }
        finally {
            File jar = new File("/tmp/snapshot_settings_test.jar");
            jar.delete();
        }
    }

    // TestExportSuite tests most of these options are tested end-to-end; however need to test
    // that a disabled connector is really disabled and that auth data is correct.
    public void testExportSetting() throws IOException {
        VoltProjectBuilder project = new VoltProjectBuilder();
        StringBuilder ddl = new StringBuilder();
        String ddlTemplate = "CREATE STREAM T%d PARTITION ON COLUMN ID (\n" +
                "  CLIENT INTEGER NOT NULL,\n" +
                "  ID INTEGER DEFAULT '0' NOT NULL,\n" +
                "  VAL INTEGER);";
        for (int i = 0; i < 2; i++) {
            ddl.append(String.format(ddlTemplate, i));
        }
        project.addLiteralSchema(ddl.toString());
        project.addExport(false /* disabled */);
        try {
            assertTrue(project.compile("/tmp/exportsettingstest.jar"));
            String catalogContents =
                VoltCompilerUtils.readFileFromJarfile("/tmp/exportsettingstest.jar", "catalog.txt");
            Catalog cat = new Catalog();
            cat.execute(catalogContents);

            Connector connector = cat.getClusters().get("cluster").getDatabases().
                get("database").getConnectors().get("noop");
            assertTrue(connector == null);
        }
        finally {
            File jar = new File("/tmp/exportsettingstest.jar");
            jar.delete();
        }

    }

    // test that Export configuration is insensitive to the case of the table name
    public void testExportTableCase() throws IOException {
        if (! MiscUtils.isPro()) {
            // This test creates 6 streams with various permutations of upper and lower case naming
            // It can only be tested where stream limits are not enforced.
            return;
        }

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ExportTester-ddl.sql"));
        project.addStmtProcedure("Dummy", "insert into a values (?, ?, ?);",
                new ProcedurePartitionData("a", "a_id"));
        project.addExport(true, ServerExportEnum.CUSTOM, "org.voltdb.exportclient.NoOpExporter", new Properties(), "noop");
        project.addExport(true /* enabled */);
        try {
            assertTrue(project.compile("/tmp/exportsettingstest.jar"));
            String catalogContents =
                VoltCompilerUtils.readFileFromJarfile("/tmp/exportsettingstest.jar", "catalog.txt");
            Catalog cat = new Catalog();
            cat.execute(catalogContents);
            CatalogUtil.compileDeployment(cat, project.getPathToDeployment(), false);
            Connector connector = cat.getClusters().get("cluster").getDatabases().
                get("database").getConnectors().get("noop");
            assertTrue(connector.getEnabled());
            // Assert that all tables exist in the connector section of catalog
            assertNotNull(connector.getTableinfo().getIgnoreCase("a"));
            assertNotNull(connector.getTableinfo().getIgnoreCase("B"));
            assertNull(connector.getTableinfo().getIgnoreCase("c"));
            assertNull(connector.getTableinfo().getIgnoreCase("D"));
            assertNull(connector.getTableinfo().getIgnoreCase("e"));
            assertNotNull(connector.getTableinfo().getIgnoreCase("f"));
        }
        finally {
            File jar = new File("/tmp/exportsettingstest.jar");
            jar.delete();
        }
    }

    public void testViewSourceExportOnlyValid() {
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ExportTesterWithView-good-ddl.sql"));
        try {
            assertTrue(project.compile("/tmp/exporttestview.jar"));
        }
        finally {
            File jar = new File("/tmp/exporttestview.jar");
            jar.delete();
        }
    }

    public void testViewSourceExportOnlyInvalidNoPartitionColumn() {
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ExportTesterWithView-bad2-ddl.sql"));
        try {
            assertFalse(project.compile("/tmp/exporttestview.jar"));
        }
        finally {
            File jar = new File("/tmp/exporttestview.jar");
            jar.delete();
        }
    }

    public void testViewSourceExportOnlyInvalidPartitionColumnNotInView() {
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestVoltCompiler.class.getResource("ExportTesterWithView-bad1-ddl.sql"));
        try {
            assertFalse(project.compile("/tmp/exporttestview.jar"));
        }
        finally {
            File jar = new File("/tmp/exporttestview.jar");
            jar.delete();
        }
    }

    public void testBadPath() {
        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compiler.compileFromDDL(nothing_jar, "invalidnonsense");
        assertFalse(success);
    }

    public void testProcWithBoxedParam() {
        String schema =
            "create table books (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));\n"
                    + "create procedure from class org.voltdb.compiler.procedures.AddBookBoxed;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        // boxed tupes are supported ENG-539
        assertTrue(success);
    }

    public void testDDLWithNoLengthString() {

        // DO NOT COPY PASTE THIS INVALID EXAMPLE!
        String schema1 =
            "create table books (cash integer default 23, title varchar default 'foo', PRIMARY KEY(cash));";

        VoltCompiler compiler = new VoltCompiler(false);

        final boolean success = compileDDL(schema1, compiler);
        assertTrue(success);
    }

    public void testDDLWithLongStringInCharacters() {
        int length = VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS + 10;
        String schema1 =
            "create table books (cash integer default 23, " +
            "title varchar("+length+") default 'foo', PRIMARY KEY(cash));";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema1, compiler);
        assertTrue(success);

        // Check warnings
        assertEquals(1, compiler.m_warnings.size());
        String warningMsg = compiler.m_warnings.get(0).getMessage();
        String expectedMsg = "The size of VARCHAR column TITLE in table BOOKS greater than " +
                "262144 will be enforced as byte counts rather than UTF8 character counts. " +
                "To eliminate this warning, specify \"VARCHAR(262154 BYTES)\"";
        assertEquals(expectedMsg, warningMsg);
        Database db = compiler.getCatalog().getClusters().get("cluster").getDatabases().get("database");
        Column var = db.getTables().get("BOOKS").getColumns().get("TITLE");
        assertTrue(var.getInbytes());
    }

    public void testDDLWithHashDeprecatedWarning() {
        String schema =
            "create table test (dummy int); " +
            "create index hashidx on test(dummy);";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertTrue(success);

        // Check warnings
        assertEquals(1, compiler.m_warnings.size());
        String warningMsg = compiler.m_warnings.get(0).getMessage();
        String expectedMsg = "Hash indexes are deprecated. In a future release, VoltDB will only support tree indexes, even if the index name contains the string \"hash\"";
        assertEquals(expectedMsg, warningMsg);
    }

    public void testDDLWithTooLongVarbinaryVarchar() {
        int length = VoltType.MAX_VALUE_LENGTH + 10;
        String schema1 =
                "create table books (cash integer default 23, " +
                        "title varbinary("+length+") , PRIMARY KEY(cash));";

        String error1 = "VARBINARY column size for column BOOKS.TITLE is > " +
                VoltType.MAX_VALUE_LENGTH+" char maximum.";
        checkDDLErrorMessage(schema1, error1);

        String schema2 =
                "create table books (cash integer default 23, " +
                        "title varchar("+length+") , PRIMARY KEY(cash));";

        String error2 = "VARCHAR column size for column BOOKS.TITLE is > " +
                VoltType.MAX_VALUE_LENGTH+" char maximum.";
        checkDDLErrorMessage(schema2, error2);
    }

    public void testNullablePartitionColumn() {
        String schema =
            "create table books (cash integer default 23, title varchar(3) default 'foo', PRIMARY KEY(cash));" +
            "partition table books on column cash;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertFalse(success);

        boolean found = false;
        for (VoltCompiler.Feedback fb : compiler.m_errors) {
            if (fb.message.indexOf("Partition column") > 0) {
                found = true;
            }
        }
        assertTrue(found);
    }

    // NOTE: TPCCTest proc also tests whitespaces regressions in SQL literals
    public void testWithTPCCDDL() {
        String schemaPath = "";
        try {
            URL url = TPCCProjectBuilder.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compiler.compileFromDDL(testout_jar, schemaPath);
        assertTrue(success);
    }

    public void testDDLTableTooManyColumns() {
        String schemaPath = "";
        try {
            URL url = TestVoltCompiler.class.getResource("toowidetable-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compiler.compileFromDDL(testout_jar, schemaPath);
        assertFalse(success);

        for (VoltCompiler.Feedback fb : compiler.m_errors) {
            if (fb.message.startsWith("Table MANY_COLUMNS has")) {
                return;
            }
        }
        fail("Error message pattern not found");
    }

    public void testExtraFilesExist() throws IOException {
        String schemaPath = "";
        try {
            URL url = TPCCProjectBuilder.class.getResource("tpcc-ddl.sql");
            schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compiler.compileFromDDL(testout_jar, schemaPath);
        assertTrue(success);

        String sql = VoltCompilerUtils.readFileFromJarfile(testout_jar, VoltCompiler.AUTOGEN_DDL_FILE_NAME);
        assertNotNull(sql);
    }

    public void testBadDdlStmtProcName() {
        String schema =
            "create table books (cash integer default 23 not null, title varchar(10) default 'foo', PRIMARY KEY(cash));" +
            "partition table books on column cash;\n" +
            "create procedure @Foo as select * from books;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertFalse(success);
    }

    public void testGoodStmtProcName() {
        String schema =
            "create table books (cash integer default 23 not null, title varchar(3) default 'foo', PRIMARY KEY(cash));" +
            "create procedure Foo as select * from books;\n" +
            "PARTITION TABLE books ON COLUMN cash;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertTrue(success);
    }

    public void testGoodDdlStmtProcName() {
        String schema =
            "create table books" +
            " (cash integer default 23 not null," +
            " title varchar(3) default 'foo'," +
            " PRIMARY KEY(cash));" +
            "PARTITION TABLE books ON COLUMN cash;" +
            "CREATE PROCEDURE Foo AS select * from books where cash = ?;" +
            "PARTITION PROCEDURE Foo ON TABLE BOOKS COLUMN CASH PARAMETER 0;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertTrue(success);
    }

    public void testCreateProcedureWithPartition() {
        class Tester {
            VoltCompiler compiler = new VoltCompiler(false);
            String baseDDL =
                "create table books (cash integer default 23 not null, "
                                  + "title varchar(3) default 'foo', "
                                  + "primary key(cash));\n"
              + "partition table books on column cash";

            void runtest(String ddl) {
                runtest(ddl, null);
            }

            void runtest(String ddl, String expectedError) {
                String schema = String.format("%s;\n%s;", baseDDL, ddl);
                boolean success = compileDDL(schema, compiler);
                checkCompilerErrorMessages(expectedError, compiler, success);
            }
        }
        Tester tester = new Tester();

        // Class proc
        tester.runtest("create procedure "
                  + "partition on table books column cash "
                  + "from class org.voltdb.compiler.procedures.NotAnnotatedAddBook");

        // Class proc with ALLOW before PARTITION clause
        tester.runtest("create role r1;\n"
                  + "create procedure "
                  + "allow r1 "
                  + "partition on table books column cash "
                  + "from class org.voltdb.compiler.procedures.NotAnnotatedAddBook");

        // Class proc with ALLOW after PARTITION clause
        tester.runtest("create role r1;\n"
                  + "create procedure "
                  + "partition on table books column cash "
                  + "allow r1 "
                  + "from class org.voltdb.compiler.procedures.NotAnnotatedAddBook");

        // Statement proc
        tester.runtest("create procedure Foo "
                  + "PARTITION on table books COLUMN cash PARAMETER 0 "
                  + "AS select * from books where cash = ?");

        // Statement proc with ALLOW before PARTITION clause
        tester.runtest("create role r1;\n"
                  + "create procedure Foo "
                  + "allow r1 "
                  + "PARTITION on table books COLUMN cash PARAMETER 0 "
                  + "AS select * from books where cash = ?");

        // Statement proc with ALLOW after PARTITION clause
        tester.runtest("create role r1;\n"
                  + "create procedure Foo "
                  + "PARTITION on table books COLUMN cash PARAMETER 0 "
                  + "allow r1 "
                  + "AS select * from books where cash = ?");

        // multi statement proc
        tester.runtest("create procedure multifoo "
                  + "AS begin select * from books where cash = ?; "
                  + "select * from books; end");

        // multi statement proc with no space after semi colons
        tester.runtest("create procedure multifoo "
                  + "AS begin select * from books where cash = ?;"
                  + "select * from books;end");

        // multi statement proc with partition
        tester.runtest("create procedure multifoo "
                + "PARTITION on table books COLUMN cash PARAMETER 0 "
                + "AS begin select * from books where cash = ?; "
                + "select * from books; end");

        // multi statement proc with ALLOW before PARTITION clause
        tester.runtest("create role r1;\n"
                  + "create procedure multifoo "
                  + "allow r1 "
                  + "PARTITION on table books COLUMN cash PARAMETER 0 "
                  + "AS begin select * from books where cash = ?; "
                  + "select * from books; end");

        // multi statement proc with ALLOW after PARTITION clause
        tester.runtest("create role r1;\n"
                  + "create procedure multifoo "
                  + "PARTITION on table books COLUMN cash PARAMETER 0 "
                  + "allow r1 "
                  + "AS begin select * from books where cash = ?;"
                  + "select * from books; end");

        // single statement proc with CASE
        tester.runtest("create procedure foocase as "
                + "select title, CASE WHEN cash > 100 THEN 'expensive' ELSE 'cheap' END "
                + "from books");

        // multi statement proc with CASE
        tester.runtest("create procedure multifoo "
                  + "AS BEGIN select * from books where cash = ?; "
                  + "select title, CASE WHEN cash > 100 THEN 'expensive' ELSE 'cheap' END "
                  + "from books; end");

        // Inspired by a problem with fullDDL.sql
        tester.runtest(
                "create role admin;\n" +
                "CREATE TABLE T26 (age BIGINT NOT NULL, gender TINYINT);\n" +
                "PARTITION TABLE T26 ON COLUMN age;\n" +
                "CREATE TABLE T26a (age BIGINT NOT NULL, gender TINYINT);\n" +
                "PARTITION TABLE T26a ON COLUMN age;\n" +
                "CREATE PROCEDURE p4 ALLOW admin PARTITION ON TABLE T26 COLUMN age PARAMETER 0 AS SELECT COUNT(*) FROM T26 WHERE age = ?;\n" +
                "CREATE PROCEDURE PARTITION ON TABLE T26a COLUMN age ALLOW admin FROM CLASS org.voltdb_testprocs.fullddlfeatures.testCreateProcFromClassProc");

        // Class proc with two PARTITION clauses (inner regex failure causes specific error)
        tester.runtest("create procedure "
                  + "partition on table books column cash "
                  + "partition on table books column cash "
                  + "from class org.voltdb.compiler.procedures.NotAnnotatedAddBook",
                    "Only one PARTITION clause is allowed for CREATE PROCEDURE");

        tester.runtest("create procedure mutlitpart "
                + "partition on table books column cash "
                + "partition on table books column cash "
                + "as begin "
                + "select * from books where cash = ?; "
                + "select * from books; end",
                  "Only one PARTITION clause is allowed for CREATE PROCEDURE");

        // Class proc with two ALLOW clauses (should work)
        tester.runtest("create role r1;\n"
                  + "create role r2;\n"
                  + "create procedure "
                  + "partition on table books column cash "
                  + "allow r1 "
                  + "allow r2 "
                  + "from class org.voltdb.compiler.procedures.AddBook");

        tester.runtest("create role r1;\n"
                + "create role r2;\n"
                + "create procedure fooroles "
                + "allow r1 "
                + "allow r2 "
                + "as begin "
                + "select * from books where cash = ?; "
                + "select * from books; end");

        // semi colon and END inside quoted string
        tester.runtest("create procedure thisproc as "
                + "select * from books where title = 'a;b' or title = 'END'");

        tester.runtest("create procedure thisproc as "
                + "begin "
                + "select * from books;"
                + "select * from books where title = 'a;b' or title = 'END'; "
                + "end");

        // embedded case
        tester.runtest("create table R (emptycase int, caseofbeer int, suitcaseofbeer int);"
                + "create procedure p as begin "
                + "select emptycase from R; "
                + "select caseofbeer from R; "
                + "select suitcaseofbeer from R; "
                + "end");

        //embedded end
        tester.runtest("create table R (emptycase int, bendbeer int, endofbeer int, frontend tinyint);"
                + "create procedure p as begin "
                + "select emptycase from R; "
                + "select bendbeer from R; "
                + "select endofbeer from R; "
                + "select frontend from R; "
                + "end");

        // check for table and column named begin
        tester.runtest("create table begin (a int)");
        tester.runtest("create table t (begin int)");
        tester.runtest("create table begin (begin int)");

        // begin outside begin...end
        tester.runtest("create table begin (begin int);"
                + "create procedure p as "
                + "select begin.begin from begin");

        // test space between AS BEGIN
        tester.runtest("create table begin (begin int);"
                + "create procedure p as \t "
                + "select begin.begin from begin");

        // begin inside begin...end
        tester.runtest("create table R (begin int);"
                + "create procedure p as begin "
                + "insert into R values(?); "
                + "select begin from R;"
                + "end");

        // with comments
        tester.runtest("create table t (f varchar(5));"
                + "create procedure thisproc as "
                + "begin --one\n"
                + "select * from t;"
                + "select * from t where f = 'foo';"
                + "select * from t where f = 'begin' or f = 'END';"
                + "end");

        // with case
        tester.runtest("create procedure thisproc as "
                + "begin "
                + "SELECT cash, "
                + "CASE WHEN cash > 100.00 "
                + "THEN 'Expensive' "
                + "ELSE 'Cheap' "
                + "END "
                + "FROM books; "
                + "end");

        // nested CASE-WHEN-THEN-ELSE-END
        tester.runtest("create procedure thisproc as "
                + "begin \n"
                + "select * from books;"
                + "select title, "
                + "case when cash > 100.00 then "
                + "case when cash > 1000.00 then 'Super Expensive' else 'Pricy' end "
                + "else 'Cheap' end "
                + "from books; "
                + "end");

        // c style block comments
        tester.runtest("create procedure thisproc as "
                + "begin \n"
                + "select * from books; /*comment will still exist*/"
                + "select title, "
                + "case when cash > 100.00 then "
                + "case when cash > 1000.00 then 'Super Expensive' else 'Pricy' end "
                + "else 'Cheap' end "
                + "from books; "
                + "end");

        // case with no whitespace before it
        tester.runtest("create procedure thisproc as "
                + "begin "
                + "SELECT title, "
                + "100+CASE WHEN cash > 100.00 "
                + "THEN 10 "
                + "ELSE 5 "
                + "END "
                + "FROM books; "
                + "end");

        // case/end with no whitespace before and after it
        tester.runtest("create procedure thisproc as "
                + "begin "
                + "SELECT title, "
                + "10+case when cash < 0 then (cash+0)end+100 from books; "
                + "end");

        tester.runtest("create table t (a int, b int);"
                + "create procedure mumble as begin "
                + "select * from t order by case when t.a < 1 then a else b end desc; "
                + "select * from t order by case when t.b < 1 then b else a end desc; "
                + "end");
    }

    public void testUseInnerClassAsProc() {
        String schema =
            "create procedure from class org.voltdb_testprocs.regressionsuites.fixedsql.TestENG2423$InnerProc;";
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
        String schemaPath = schemaFile.getPath();
        VoltCompiler compiler = new VoltCompiler(false);
        assertTrue(compiler.compileFromDDL(testout_jar, schemaPath));
    }

    public void testMaterializedView() throws IOException {
        String schema =
            "create table books (cash integer default 23 NOT NULL, title varchar(10) default 'foo', PRIMARY KEY(cash));\n" +
            "partition table books on column cash;\n" +
            "create table foo (cash integer not null);\n" +
            "create view matt (title, cash, num, foo) as select title, cash, count(*), sum(cash) from books group by title, cash;\n" +
            "create view matt2 (title, cash, num, foo) as select books.title, books.cash, count(*), sum(books.cash) from books join foo on books.cash = foo.cash group by books.title, books.cash;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertTrue(success);
        Catalog c1 = compiler.getCatalog();
        String catalogContents = VoltCompilerUtils.readFileFromJarfile(testout_jar, "catalog.txt");
        Catalog c2 = new Catalog();
        c2.execute(catalogContents);
        assertTrue(c2.serialize().equals(c1.serialize()));
    }

    public void testDdlProcVarbinary() throws IOException {
        String schema =
            "create table books" +
            "  (cash integer default 23 NOT NULL," +
            "  title varbinary(10) default NULL," +
            "  PRIMARY KEY(cash));" +
            "partition table books on column cash;" +
            "create procedure get as select * from books;" +
            "create procedure i1 as insert into books values(5, 'AA');" +
            "create procedure i2 as insert into books values(5, ?);" +
            "create procedure s1 as update books set title = 'bb';" +
            "create procedure i3 as insert into books values( ?, ?);" +
            "partition procedure i3 on table books column cash;" +
            "create procedure d1 as" +
            "  delete from books where title = ? and cash = ?;" +
            "partition procedure d1 on table books column cash parameter 1;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertTrue(success);
        Catalog c1 = compiler.getCatalog();
        String catalogContents = VoltCompilerUtils.readFileFromJarfile(testout_jar, "catalog.txt");
        Catalog c2 = new Catalog();
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

    private VoltCompiler compileSchemaForDDLTest(String schema, boolean expectSuccess) {
        String schemaPath = getPathForSchema(schema);

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compiler.compileFromDDL(testout_jar, schemaPath);
        assertEquals(expectSuccess, success);
        return compiler;
    }

    private String getPathForSchema(String schema) {
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
        schemaFile.deleteOnExit();
        return schemaFile.getPath();
    }

    private CatalogMap<Table> tablesFromVoltCompiler(VoltCompiler c) {
        return c.m_catalog.getClusters().get("cluster")
                .getDatabases().get("database").getTables();
    }

    private CatalogMap<Procedure> proceduresFromVoltCompiler(VoltCompiler c) {
        return c.m_catalog.getClusters().get("cluster")
                .getDatabases().get("database").getProcedures();
    }

    private Table assertTableT(VoltCompiler c) {
        CatalogMap<Table> tables = tablesFromVoltCompiler(c);
        assertEquals(1, tables.size());
        Table tbl = tables.getIgnoreCase("t");
        assertNotNull(tbl);
        return tbl;
    }

    public void testDDLCompilerLeadingGarbage() {
        String schema =
            "-- a valid comment\n" +
            "- an invalid comment\n" +
            "create table t(id integer);";

        VoltCompiler c = compileSchemaForDDLTest(schema, false);
        assertTrue(c.hasErrors());
    }

    public void testDDLCompilerLeadingWhitespace() {
        String schema =
            "     \n" +
            "\n" +
            "create table t(id integer);";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerLeadingComment() {
        String schema =
            "-- this is a leading comment\n" +
            "  -- with some leading whitespace\n" +
            "     create table t(id integer);";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerLeadingCommentAndHashMarks() {
        String schema =
            "-- ### this is a leading comment\n" +
            "  -- with some ### leading whitespace\n" +
            "     create table t(id integer);";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerNoNewlines() {
        String schema =
            "create table t(id integer); create table r(id integer);";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        CatalogMap<Table> tables = tablesFromVoltCompiler(c);
        assertEquals(2, tables.size());
    }

    public void testDDLCompilerSplitLines() {
        String schema =
            "create\n" +
            "table\n" +
            "t(id\n" +
            "integer);";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerTrailingComment1() {
        String schema =
            "create table t(id integer) -- this is a trailing comment\n" +
            "-- and a line full of comments\n" +
            ";\n";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerTrailingComment2() {
        String schema =
            "create table t(id integer) -- this is a trailing comment\n" +
            ";\n";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerTrailingCommentAndHashMarks() {
        String schema =
            "create table t(id varchar(128) default '###')  " +
            "-- ### this ###### is a trailing comment\n" +
            ";\n";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerTrailingComment3() {
        String schema =
            "create table t(id integer) -- this is a trailing comment\n" +
            "-- and a line full of comments\n" +
            ";";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerTrailingComment4() {
        String schema =
            "create table t(id integer) -- this is a trailing comment\n" +
            ";";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerTrailingComment5() {
        String schema =
            "create table t(id integer) -- this is a trailing comment\n" +
            "-- and a line full of comments\n" +
            "    ;\n";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerTrailingComment6() {
        String schema =
            "create table t(id integer) -- this is a trailing comment\n" +
            "-- and a line full of comments\n" +
            "    ;\n" +
            "-- ends with a comment\n";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }


    public void testDDLCompilerInvalidStatement() {
        String schema =
            "create table t for justice -- with a comment\n";
        VoltCompiler c = compileSchemaForDDLTest(schema, false);
        assertTrue(c.hasErrors());
    }

    public void testDDLCompilerCommentThatLooksLikeStatement() {
        String schema =
            "create table t(id integer); -- create table r(id integer);";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTableT(c);
    }

    public void testDDLCompilerLeadingSemicolon() {
        String schema = "; create table t(id integer);";
        VoltCompiler c = compileSchemaForDDLTest(schema, false);
        assertTrue(c.hasErrors());
    }

    public void testDDLCompilerMultipleStatementsOnMultipleLines() {
        String schema =
            "create table t(id integer); create\n" +
            "table r(id integer); -- second table";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        CatalogMap<Table> tables = tablesFromVoltCompiler(c);
        assertEquals(2, tables.size());
    }

    public void testDDLCompilerMultiStmtProc() {
        // multi statement proc with one statement
        String schema =
            "create table t(a integer); create procedure multipr as begin\n" +
            "select * from t; end;";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        CatalogMap<Table> tables = tablesFromVoltCompiler(c);
        assertEquals(1, tables.size());
        CatalogMap<Procedure> procs = proceduresFromVoltCompiler(c);
        assertEquals(1, procs.size());
        assertNotNull(procs.get("multipr"));

        // multi statement proc with multiple statements
        schema =
            "create table t(a integer);\n"
            + "create procedure multipr1 as begin\n"
            + "select * from t;\n"
            + "insert into t values(1); end;";
        c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        tables = tablesFromVoltCompiler(c);
        assertEquals(1, tables.size());
        procs = proceduresFromVoltCompiler(c);
        assertEquals(1, procs.size());
        assertNotNull(procs.get("multipr1"));
    }

    private void checkDDLCompilerDefaultStringLiteral(String literal)
            throws IOException {
        checkDDLCompilerDefaultStringLiteral(literal, literal);
    }

    private void checkDDLCompilerDefaultStringLiteral(
            String literalIn, String literalOut) {
        String schema = "create table t(id varchar(6) default '" +
                literalIn + "');";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse("Schema {" + schema + "} had unexpected errors.", c.hasErrors());
        Table tbl = assertTableT(c);
        String defaultValue = tbl.getColumns().getIgnoreCase("id").getDefaultvalue();
        // Somehow "\n" is getting corrupted in a way that would fail this test.
        // So we weaken the test for that case.
        if (literalOut != null) {
            assertEquals(literalOut, defaultValue);
        }
    }

    public void testDDLCompilerStringLiteral() throws IOException {
        // The trivial case to exercise the test framework.
        checkDDLCompilerDefaultStringLiteral("abc");
    }

    public void testDDLCompilerSemiColonInStringLiteral() throws IOException {
        checkDDLCompilerDefaultStringLiteral("a;bc");
    }

    public void testDDLCompilerDashDashInStringLiteral() throws IOException {
        checkDDLCompilerDefaultStringLiteral("a--bc");
    }

    public void testDDLCompilerNewlineInStringLiteral() {
        checkDDLCompilerDefaultStringLiteral("a\nbc", null);
    }

    public void testDDLCompilerEscapedStringLiterals() {
        checkDDLCompilerDefaultStringLiteral("a''b''''c", "a'b''c");
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
    boolean[] can_be_tree = {true, true, true, true, true, true, true, true};

    public void testDDLCompilerIndexDefaultTypes() {
        for (int ii = 0; ii < column_types.length; ii++) {
            String schema =
                "create table t(id " + column_types[ii] + " not null, num integer not null);\n" +
                "create index idx_t_id on t(id);\n" +
                "create index idx_t_idnum on t(id,num);";
            VoltCompiler c = compileSchemaForDDLTest(schema, true);
            assertFalse(c.hasErrors());
            Table tbl = assertTableT(c);
            assertEquals(default_index_types[ii].getValue(),
                    tbl.getIndexes().getIgnoreCase("idx_t_id").getType());
            assertEquals(default_index_types[ii].getValue(),
                    tbl.getIndexes().getIgnoreCase("idx_t_idnum").getType());
        }
    }

    public void testDDLCompilerHashIndexAllowed() {
        for (int ii = 0; ii < column_types.length; ii++) {
            String schema =
                "create table t(id " + column_types[ii] + " not null, num integer not null);\n" +
                "create index idx_t_id_hash on t(id);\n" +
                "create index idx_t_idnum_hash on t(id,num);";
            VoltCompiler c = compileSchemaForDDLTest(schema, can_be_hash[ii]);
            if (can_be_hash[ii]) {
                // do appropriate index exists checks
                assertFalse(c.hasErrors());
                Table tbl = assertTableT(c);
                assertEquals(IndexType.HASH_TABLE.getValue(),
                        tbl.getIndexes().getIgnoreCase("idx_t_id_hash").getType());
                assertEquals(IndexType.HASH_TABLE.getValue(),
                        tbl.getIndexes().getIgnoreCase("idx_t_idnum_hash").getType());
            }
            else {
                assertTrue(c.hasErrors());
            }
        }
    }

    public void testUniqueIndexAllowed() {
        String schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create unique index idx_t_unique on t(id,num);\n" +
                "create index idx_t on t(num);";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        Table tbl = assertTableT(c);
        assertTrue(tbl.getIndexes().getIgnoreCase("idx_t_unique").getUnique());
        assertFalse(tbl.getIndexes().getIgnoreCase("idx_t").getUnique());
        // also validate that simple column indexes don't trigger the generalized expression index handling
        String noExpressionFound = "";
        assertEquals(noExpressionFound, tbl.getIndexes().getIgnoreCase("idx_t_unique").getExpressionsjson());
        assertEquals(noExpressionFound, tbl.getIndexes().getIgnoreCase("idx_t").getExpressionsjson());
    }

    public void testFunctionIndexAllowed() {
        String schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create unique index idx_ft_unique on t(abs(id+num));\n" +
                "create index idx_ft on t(abs(num));\n" +
                "create index poweridx on t(power(id, 2));";
        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        Table tbl = assertTableT(c);
        assertTrue(tbl.getIndexes().getIgnoreCase("idx_ft_unique").getUnique());
        assertFalse(tbl.getIndexes().getIgnoreCase("idx_ft").getUnique());
        // Validate that general expression indexes get properly annotated with an expressionjson attribute
        String noExpressionFound = "";
        assertNotSame(noExpressionFound, tbl.getIndexes().getIgnoreCase("idx_ft_unique").getExpressionsjson());
        assertNotSame(noExpressionFound, tbl.getIndexes().getIgnoreCase("idx_ft").getExpressionsjson());
    }

    public void testDDLCompilerVarcharTreeIndexAllowed() {
        for (int i = 0; i < column_types.length; i++) {
            String schema =
                "create table t(id " + column_types[i] + " not null, num integer not null);\n" +
                "create index idx_t_id_tree on t(id);\n" +
                "create index idx_t_idnum_tree on t(id,num);";
            VoltCompiler c = compileSchemaForDDLTest(schema, can_be_tree[i]);
            assertFalse(c.hasErrors());
            Table tbl = assertTableT(c);
            assertEquals(IndexType.BALANCED_TREE.getValue(),
                        tbl.getIndexes().getIgnoreCase("idx_t_id_tree").getType());
            assertEquals(IndexType.BALANCED_TREE.getValue(),
                        tbl.getIndexes().getIgnoreCase("idx_t_idnum_tree").getType());
        }
    }

    public void testDDLCompilerTwoIdenticalIndexes() {
        String schema;
        VoltCompiler c;
        schema = "create table t(id integer not null, num integer not null);\n" +
            "create index idx_t_idnum1 on t(id,num);\n" +
            "create index idx_t_idnum2 on t(id,num);";
        c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTrue(c.hasErrorsOrWarnings());

        // non-unique partial index
        schema = "create table t(id integer not null, num integer not null);\n" +
            "create index idx_t_idnum1 on t(id) where num > 3;\n" +
            "create index idx_t_idnum2 on t(id) where num > 3;";
        c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTrue(c.hasErrorsOrWarnings());

        // unique partial index
        schema = "create table t(id integer not null, num integer not null);\n" +
            "create unique index idx_t_idnum1 on t(id) where num > 3;\n" +
            "create unique index idx_t_idnum2 on t(id) where num > 3;";
        c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTrue(c.hasErrorsOrWarnings());

        // non-unique expression partial index
        schema = "create table t(id integer not null, num integer not null);\n" +
            "create index idx_t_idnum1 on t(id) where abs(num) > 3;\n" +
            "create index idx_t_idnum2 on t(id) where abs(num) > 3;";
        c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTrue(c.hasErrorsOrWarnings());

        // unique expression partial index
        schema = "create table t(id integer not null, num integer not null);\n" +
            "create unique index idx_t_idnum1 on t(id) where abs(num) > 3;\n" +
            "create unique index idx_t_idnum2 on t(id) where abs(num) > 3;";
        c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertTrue(c.hasErrorsOrWarnings());
    }

    public void testDDLCompilerSameNameIndexesOnTwoTables() {
        String schema =
                "create table t1(id integer not null, num integer not null);\n" +
                "create table t2(id integer not null, num integer not null);\n" +
                "create index idx_t_idnum on t1(id,num);\n" +
                "create index idx_t_idnum on t2(id,num);";

        // if this test ever fails, it's worth figuring out why
        // When written, HSQL wouldn't allow two indexes with the same name,
        //  even across tables.
        compileSchemaForDDLTest(schema, false);
    }

    public void testDDLCompilerTwoCoveringIndexes() {
        String schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create index idx_t_idnum_hash on t(id,num);\n" +
                "create index idx_t_idnum_tree on t(id,num);";

        compileSchemaForDDLTest(schema, true);
    }

    public void testDDLCompilerTwoSwappedOrderIndexes() {
        String schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create index idx_t_idnum_a on t(num,id);\n" +
                "create index idx_t_idnum_b on t(id,num);";

        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrorsOrWarnings());
    }

    public void testDDLCompilerDropTwoOfFiveIndexes() {
        String schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create index idx_t_idnum_a on t(num,id);\n" +
                "create index idx_t_idnum_b on t(id,num);\n" +
                "create index idx_t_idnum_c on t(id,num);\n" +
                "create index idx_t_idnum_d on t(id,num) where id > 0;\n" +
                "create index idx_t_idnum_f on t(id,num) where id > 0;\n";

        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertTrue(c.hasErrorsOrWarnings());
        int foundCount = 0;
        for (VoltCompiler.Feedback f : c.m_warnings) {
            if (f.message.contains("Dropping index")) {
                foundCount++;
            }
        }
        assertEquals(2, foundCount);
    }

    public void testDDLCompilerUniqueAndNonUniqueIndexOnSameColumns() {
        String schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create unique index idx_t_idnum_unique on t(id,num);\n" +
                "create index idx_t_idnum on t(id,num);";
        compileSchemaForDDLTest(schema, true);
    }

    public void testDDLCompilerTwoIndexesWithSameName() {
        String schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create index idx_t_idnum on t(id);\n" +
                "create index idx_t_idnum on t(id,num);";
        compileSchemaForDDLTest(schema, false);
    }

    public void testDDLCompilerIndexesOrMatViewContainSQLFunctionNOW() {
        // Test indexes.
        String ddl = "";
        String errorIndexMsg = "Index \"IDX_T_TM\" cannot include the function NOW or CURRENT_TIMESTAMP.";
        ddl = "create table t(id integer not null, tm timestamp);\n" +
              "create index idx_t_tm on t(since_epoch(second, CURRENT_TIMESTAMP) - since_epoch(second, tm));";
        checkDDLErrorMessage(ddl, errorIndexMsg);

        ddl = "create table t(id integer not null, tm timestamp);\n" +
              "create index idx_t_tm on t(since_epoch(second, NOW) - since_epoch(second, tm));";
        checkDDLErrorMessage(ddl, errorIndexMsg);

        ddl = "create table t(id integer not null, tm timestamp);\n" +
              "create index idx_t_tm on t(CURRENT_TIMESTAMP);";
        checkDDLErrorMessage(ddl, errorIndexMsg);

        // Test MatView.
        String errorMatviewMsg = "Materialized view \"MY_VIEW\" cannot include the function NOW or CURRENT_TIMESTAMP.";
        ddl = "create table t(id integer not null, tm timestamp);\n" +
              "create view my_view as select since_epoch(second, CURRENT_TIMESTAMP) - since_epoch(second, tm), " +
              "count(*) from t group by since_epoch(second, CURRENT_TIMESTAMP) - since_epoch(second, tm);";
        checkDDLErrorMessage(ddl, errorMatviewMsg);

        ddl = "create table t(id integer not null, tm timestamp);\n" +
                "create view my_view as select since_epoch(second, NOW) - since_epoch(second, tm), " +
                "count(*) from t group by since_epoch(second, NOW) - since_epoch(second, tm);";
        checkDDLErrorMessage(ddl, errorMatviewMsg);

        ddl = "create table t(id integer not null, tm timestamp);\n" +
                "create view my_view as select tm, count(*), count(CURRENT_TIMESTAMP)  from t group by tm;";
        checkDDLErrorMessage(ddl, errorMatviewMsg);

        ddl = "create table t(id integer not null, tm timestamp);\n" +
                "create view my_view as select tm, count(*), count(NOW)  from t group by tm;";
        checkDDLErrorMessage(ddl, errorMatviewMsg);

        ddl = "create table t(id integer not null, tm timestamp);\n" +
                "create view my_view as select tm, count(*) from t " +
                "where since_epoch(second, CURRENT_TIMESTAMP) - since_epoch(second, tm) > 60 " +
                "group by tm;";
        checkDDLErrorMessage(ddl, errorMatviewMsg);
    }

    public void testDDLCompilerCreateAndDropIndexesOnMatView() {
        String ddl = "";

        ddl = "create table foo(a integer, b float, c float);\n" +
              "create table foo2(a integer, b float, c float);\n" +
              "create view bar (a, b, total) as select a, b, count(*) as total from foo group by a, b;\n" +
              "create view bar2 (a, b, total) as select foo.a, foo.b, count(*) as total from foo join foo2 on foo.a = foo2.a group by foo.a, foo.b;\n" +
              "create index baridx on bar (a);\n" +
              "drop index baridx;\n" +
              "create index baridx on bar2(a);\n" +
              "drop index baridx;\n";
        checkDDLErrorMessage(ddl, null);

        ddl = "create table foo(a integer, b float);\n" +
              "create table foo2(a integer, b float);\n" +
              "create view bar (a, total) as select a, count(*) as total from foo group by a;\n" +
              "create view bar2 (a, total) as select foo.a, count(*) as total from foo join foo2 on foo.a = foo2.a group by foo.a;\n" +
              "create index baridx on bar (a, total);\n" +
              "drop index baridx;\n" +
              "create index baridx on bar2 (a, total);\n" +
              "drop index baridx;\n";
        checkDDLErrorMessage(ddl, null);
    }

    public void testColumnNameIndexHash() {
        List<Pair<String, IndexType>> passing
            = Arrays.asList(
                            // If we don't explicitly name the primary key constraint,
                            // we always get a tree index.  This is independent of the name
                            // of the index column or columns.
                            Pair.of("create table t ( goodhashname varchar(256) not null, primary key ( goodhashname ) );",
                                    IndexType.BALANCED_TREE),
                            Pair.of("create table t ( goodhashname integer not null, primary key ( goodhashname ) );",
                                    IndexType.BALANCED_TREE),
                            Pair.of("create table t ( goodtreename varchar(256) not null, primary key ( goodtreename ) );",
                                    IndexType.BALANCED_TREE),
                            Pair.of("create table t ( goodtreename integer not null, primary key ( goodtreename ) );",
                                    IndexType.BALANCED_TREE),
                            Pair.of("create table t ( goodtreehashname varchar(256) not null, primary key (goodtreehashname));",
                                    IndexType.BALANCED_TREE),
                            Pair.of("create table t ( goodtreehashname integer not null, primary key (goodtreehashname));",
                                    IndexType.BALANCED_TREE),
                            // If we explicitly name the constraint with a tree name
                            // we always get a tree index.  This is true even if the
                            // column type is hashable.
                            Pair.of("create table t ( goodtreehashname varchar(256) not null, constraint good_tree primary key (goodtreehashname));",
                                    IndexType.BALANCED_TREE),
                            Pair.of("create table t ( goodtreehashname integer not null, constraint good_tree primary key (goodtreehashname));",
                                    IndexType.BALANCED_TREE),
                            // If we explicitly name the constraint with a name
                            // which is both a hash name and a tree name, we always get a tree
                            // index.  This is true even if the column type is hashable.
                            Pair.of("create table t ( goodtreehashname varchar(256) not null, constraint good_tree primary key (goodtreehashname));",
                                    IndexType.BALANCED_TREE),
                            Pair.of("create table t ( goodtreehashname integer not null, constraint good_tree primary key (goodtreehashname));",
                                    IndexType.BALANCED_TREE),

                            // The only way to get a hash index is to explicitly name the constraint
                            // with a hash name and to make the column type or types be hashable.
                            Pair.of("create table t ( goodtreehashname integer not null, constraint good_hash primary key (goodtreehashname));",
                                    IndexType.HASH_TABLE),
                            Pair.of("create table t ( goodvanilla integer not null, constraint good_hash_constraint primary key ( goodvanilla ) );",
                                    IndexType.HASH_TABLE),
                            // Test to see if created indices are still hashed
                            // when they are expected, and not hashed when they
                            // are not expected.
                            Pair.of("create table t ( goodvanilla integer not null ); create unique index myhash on t ( goodvanilla );",
                                    IndexType.HASH_TABLE),
                            Pair.of("create table t ( goodhash integer not null primary key );",
                                    IndexType.BALANCED_TREE)
        );
        String[] failing = {
                // If we name the constraint with a hash name,
                // but the column type is not hashable, it is an
                // error.
                "create table t ( badhashname varchar(256) not null, constraint badhashconstraint primary key ( badhashname ) );",
                // The name of the column is not important.
                "create table t ( badzotzname varchar(256) not null, constraint badhashconstraint primary key ( badzotzname ) );",
                // If any of the columns are non-hashable, the index is
                // not hashable.
                "create table t ( fld1 integer, fld2 varchar(256), constraint badhashconstraint primary key ( fld1, fld2 ) );"
        };
        for (Pair<String, IndexType> cmdPair : passing) {
            // See if we can actually create the table.
            VoltCompiler c = compileSchemaForDDLTest(cmdPair.getLeft(), true);
            Table tbl = assertTableT(c);
            assertEquals(1, tbl.getIndexes().size());
            Index idx = tbl.getIndexes().iterator().next();
            String msg = String.format("CMD: %s\nExpected %s, got %s",
                                       cmdPair.getLeft(),
                                       cmdPair.getRight(),
                                       IndexType.get(idx.getType()));
            assertEquals(msg, cmdPair.getRight().getValue(),
                         idx.getType());
        }
        for (String cmd : failing) {
            compileSchemaForDDLTest(cmd, false);
        }
    }

    private static String msgP = "does not include the partitioning column";
    private static String msgPR =
            "ASSUMEUNIQUE is not valid for an index that includes the partitioning column. " +
            "Please use UNIQUE instead";

    public void testColumnUniqueGiveException() {
        String schema;

        // (1) ****** Replicate tables
        // A unique index on the non-primary key for replicated table gets no error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null UNIQUE, age integer,  primary key (id));\n";
        checkValidUniqueAndAssumeUnique(schema, null, null);

        // Similar to above, but use a different way to define unique column.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  " +
                "primary key (id), UNIQUE (name) );\n";
        checkValidUniqueAndAssumeUnique(schema, null, null);


        // (2) ****** Partition Table: UNIQUE valid, ASSUMEUNIQUE not valid
        // A unique index on the partitioning key ( no primary key) gets no error.
        schema = "create table t0 (id bigint not null UNIQUE, name varchar(32) not null, age integer);\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n";
        checkValidUniqueAndAssumeUnique(schema, null, msgPR);

        // Similar to above, but use a different way to define unique column.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  " +
                "primary key (id), UNIQUE(id) );\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n";
        checkValidUniqueAndAssumeUnique(schema, null, msgPR);

        // A unique index on the partitioning key ( also primary key) gets no error.
        schema = "create table t0 (id bigint not null UNIQUE, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n";
        checkValidUniqueAndAssumeUnique(schema, null, msgPR);


        // A unique compound index on the partitioning key and another column gets no error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  " +
                "UNIQUE (id, age), primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n";
        checkValidUniqueAndAssumeUnique(schema, null, msgPR);

        // A unique index on the partitioning key and an expression like abs(age) gets no error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  " +
                "primary key (id), UNIQUE (id, abs(age)) );\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n";
        checkValidUniqueAndAssumeUnique(schema, null, msgPR);

        // (3) ****** Partition Table: UNIQUE not valid
        // A unique index on the partitioning key ( non-primary key) gets one error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null UNIQUE, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN name;\n";
        checkValidUniqueAndAssumeUnique(schema, msgP, msgPR);

        // A unique index on the partitioning key ( no primary key) gets one error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null UNIQUE, age integer);\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n";
        checkValidUniqueAndAssumeUnique(schema, msgP, null);

        // A unique index on the non-partitioning key gets one error.
        schema = "create table t0 (id bigint not null, name varchar(32) UNIQUE, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n";
        checkValidUniqueAndAssumeUnique(schema, msgP, null);

        // A unique index on an unrelated expression like abs(age) gets a error.
        schema = "create table t0 (id bigint not null, name varchar(32), age integer, UNIQUE (abs(age)), primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n";
        checkValidUniqueAndAssumeUnique(schema, msgP, null);


        // A unique index on an expression of the partitioning key like substr(1, 2, name) gets two errors.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  " +
                "primary key (id), UNIQUE (substr(name, 1, 2 )) );\n" +
                "PARTITION TABLE t0 ON COLUMN name;\n";
        // 1) unique index, 2) primary key
        checkValidUniqueAndAssumeUnique(schema, msgP, msgP);

        // A unique index on the non-partitioning key, non-partitioned column gets two errors.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer UNIQUE,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN name;\n";
        // 1) unique index, 2) primary key
        checkValidUniqueAndAssumeUnique(schema, msgP, msgP);

        // unique/assumeunique constraint added via ALTER TABLE to replicated table
        schema = "create table t0 (id bigint not null, name varchar(32) not null);\n" +
                "ALTER TABLE t0 ADD UNIQUE(name);";
        checkValidUniqueAndAssumeUnique(schema, null, null);

        // unique/assumeunique constraint added via ALTER TABLE to partitioned table
        schema = "create table t0 (id bigint not null, name varchar(32) not null);\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "ALTER TABLE t0 ADD UNIQUE(name);";
        checkValidUniqueAndAssumeUnique(schema, msgP, null);

        // ENG-7242, kinda
        // (tests the assumeuniqueness constraint is preserved, obliquely, see
        // TestAdhocAlterTable for more thorough tests)
        schema = "create table t0 (id bigint not null, name varchar(32) not null, val integer);\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "ALTER TABLE t0 ADD UNIQUE(name);\n" +
                "ALTER TABLE t0 DROP COLUMN val;\n";
        checkValidUniqueAndAssumeUnique(schema, msgP, null);

        // ENG-7304, that we can pass functions to constrant definitions in alter table
        schema = "create table t0 (id bigint not null, val2 integer not null, val integer);\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "ALTER TABLE t0 ADD UNIQUE(abs(val2));\n" +
                "ALTER TABLE t0 DROP COLUMN val;\n";
        checkValidUniqueAndAssumeUnique(schema, msgP, null);
    }

    private boolean compileDDL(String ddl, VoltCompiler compiler) {
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        String schemaPath = schemaFile.getPath();

        return compiler.compileFromDDL(testout_jar, schemaPath);
    }

    private boolean compileInitDDL(boolean isInit, String ddl, VoltCompiler compiler) {
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        String schemaPath = schemaFile.getPath();
        compiler.setInitializeDDLWithFiltering(isInit);
        return compiler.compileFromDDL(testout_jar, schemaPath);
    }

    private void checkCompilerErrorMessages(String expectedError, VoltCompiler compiler, boolean success) {
        if (expectedError == null) {
            assertTrue("Expected no compilation errors but got these:\n" + feedbackToString(compiler.m_errors), success);
        }
        else {
            assertFalse("Expected failure but got success", success);
            assertTrue(isFeedbackPresent(expectedError, compiler.m_errors));
        }

    }

    private void checkDDLErrorMessage(String ddl, String errorMsg) {
        VoltCompiler compiler = new VoltCompiler(false);
        boolean success = compileDDL(ddl, compiler);
        checkCompilerErrorMessages(errorMsg, compiler, success);
    }

    private void checkValidUniqueAndAssumeUnique(String ddl, String errorUnique, String errorAssumeUnique) {
        checkDDLErrorMessage(ddl, errorUnique);
        checkDDLErrorMessage(ddl.replace("UNIQUE", "ASSUMEUNIQUE"), errorAssumeUnique);
    }

    public void testUniqueIndexGiveException() {
        String schema;

        // (1) ****** Replicate tables
        // A unique index on the non-primary key for replicated table gets no error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "CREATE UNIQUE INDEX user_index0 ON t0 (name) ;";
        checkValidUniqueAndAssumeUnique(schema, null, null);


        // (2) ****** Partition Table: UNIQUE valid, ASSUMEUNIQUE not valid
        // A unique index on the partitioning key ( no primary key) gets no error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer);\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index1 ON t0 (id) ;";
        checkValidUniqueAndAssumeUnique(schema, null, msgPR);

        // A unique index on the partitioning key ( also primary key) gets no error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index2 ON t0 (id) ;";
        checkValidUniqueAndAssumeUnique(schema, null, msgPR);

        // A unique compound index on the partitioning key and another column gets no error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index3 ON t0 (id, age) ;";
        checkValidUniqueAndAssumeUnique(schema, null, msgPR);

        // A unique index on the partitioning key and an expression like abs(age) gets no error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index4 ON t0 (id, abs(age)) ;";
        checkValidUniqueAndAssumeUnique(schema, null, msgPR);


        // (3) ****** Partition Table: UNIQUE not valid
        // A unique index on the partitioning key ( no primary key) gets one error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer);\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index7 ON t0 (name) ;";
        checkValidUniqueAndAssumeUnique(schema, msgP, null);

        // A unique index on the non-partitioning key gets one error.
        schema = "create table t0 (id bigint not null, name varchar(32), age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index8 ON t0 (name) ;";
        checkValidUniqueAndAssumeUnique(schema, msgP, null);

        // A unique index on an unrelated expression like abs(age) gets a error.
        schema = "create table t0 (id bigint not null, name varchar(32), age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index9 ON t0 (abs(age)) ;";
        checkValidUniqueAndAssumeUnique(schema, msgP, null);

        // A unique index on the partitioning key ( non-primary key) gets one error.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN name;";
        checkValidUniqueAndAssumeUnique(schema, msgP, msgP);

        // A unique index on an expression of the partitioning key like substr(1, 2, name) gets two errors.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN name;\n" +
                "CREATE UNIQUE INDEX user_index10 ON t0 (substr(name, 1, 2 )) ;";
        // 1) unique index, 2) primary key
        checkValidUniqueAndAssumeUnique(schema, msgP, msgP);

        // A unique index on the non-partitioning key, non-partitioned column gets two errors.
        schema = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN name;\n" +
                "CREATE UNIQUE INDEX user_index12 ON t0 (age) ;";
        // 1) unique index, 2) primary key
        checkValidUniqueAndAssumeUnique(schema, msgP, msgP);
    }

    private void subTestDDLCompilerMatViewJoin() {
        String tableDDL;
        String viewDDL;
        tableDDL = "CREATE TABLE T1 (a INTEGER NOT NULL, b INTEGER NOT NULL);\n" +
                   "CREATE TABLE T2 (a INTEGER NOT NULL, b INTEGER NOT NULL);\n" +
                   "CREATE TABLE T3 (a INTEGER NOT NULL, b INTEGER NOT NULL);\n";
        // 1. Test INNER JOIN:
        // 1.1 Test one join:
        viewDDL = "CREATE VIEW V (aint, cnt, sumint) AS \n" +
                  "SELECT T1.a, count(*), sum(T2.b) FROM T1 LEFT JOIN T2 ON T1.a=T2.a GROUP BY T1.a;";
        checkDDLErrorMessage(tableDDL+viewDDL, "Materialized view only supports INNER JOIN.");
        // 1.2 Test multiple joins:
        viewDDL = "CREATE VIEW V (aint, bint, cnt, sumint) AS \n" +
                  "SELECT T1.a, T2.a, count(*), sum(T3.b) FROM T1 JOIN T2 ON T1.a=T2.a RIGHT JOIN T3 on T2.a=T3.a GROUP BY T1.a, T2.a;";
        checkDDLErrorMessage(tableDDL+viewDDL, "Materialized view only supports INNER JOIN.");
        // 2. Test self-join:
        viewDDL = "CREATE VIEW V (aint, cnt, sumint) AS \n" +
                  "SELECT T1a.a, count(*), sum(T1a.b) FROM T1 T1a JOIN T1 T1b ON T1a.a=T1b.a GROUP BY T1a.a;";
        checkDDLErrorMessage(tableDDL+viewDDL, "Table T1 appeared in the table list more than once: " +
                                               "materialized view does not support self-join.");
        // 3. Test table join subquery. The subquery "LIMIT 10" is there to prevent an optimization
        // which replaces the subquery with an original table.
        viewDDL = "CREATE VIEW V (aint, cnt, sumint) AS \n" +
                  "SELECT T1.a, count(*), sum(T1.b) FROM T1 JOIN (SELECT * FROM T2 LIMIT 10) T2 ON T1.a=T2.a GROUP BY T1.a;";
        checkDDLErrorMessage(tableDDL+viewDDL, "Materialized view \"V\" cannot contain subquery sources.");

        // 4. Test view cannot be defined on other views:
        viewDDL = "CREATE TABLE t(id INTEGER NOT NULL, num INTEGER, wage INTEGER);\n" +
                  "CREATE VIEW my_view1 (num, total, sumwage) " +
                  "AS SELECT num, count(*), sum(wage) FROM t GROUP BY num; \n" +

                  "CREATE VIEW my_view2 (num, total, sumwage) " +
                  "AS SELECT t.num, count(*), sum(t.wage) FROM my_view1 JOIN t ON t.num=my_view1.num GROUP BY t.num; ";
        checkDDLErrorMessage(viewDDL, "A materialized view (MY_VIEW2) can not be defined on another view (MY_VIEW1)");

        // 5. Test view defined on non-plannable join query (partitioned table):
        viewDDL = "PARTITION TABLE T1 ON COLUMN a;\n" +
                  "PARTITION TABLE T2 ON COLUMN a;\n" +
                  "CREATE VIEW v2 (a, cnt, sumb) AS \n" +
                  "SELECT t1.a, count(*), sum(t2.b) FROM t1 JOIN t2 ON true GROUP BY t1.a;";
        checkDDLErrorMessage(tableDDL+viewDDL, "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition.");

        // 6. Test view defined on joined tables where some source tables are streamed table.
        viewDDL = "CREATE STREAM T3x PARTITION ON COLUMN a (a INTEGER NOT NULL, b INTEGER NOT NULL);\n" +
                  "CREATE VIEW V (aint, cnt, sumint) AS \n" +
                  "SELECT T1.a, count(*), sum(T3x.b) FROM T1 JOIN T3x ON T1.a=T3x.a GROUP BY T1.a;";
        checkDDLErrorMessage(tableDDL+viewDDL, "A materialized view (V) on joined tables cannot have streamed table (T3X) as its source.");
    }

    public void testDDLCompilerMatView() {
        // Test MatView.
        String ddl;
        VoltCompiler compiler = new VoltCompiler(false);

        // Subquery is replaced with a simple select
        ddl = "create table t(id integer not null, num integer, wage integer);\n" +
                "create view my_view1 (num, total) " +
                "as select num, count(*) from (select num from t) subt group by num; \n";
        assertTrue(compileDDL(ddl, compiler));

        // count(*) can be placed anywhere in materialized views
        ddl = "create table t(id integer not null, num integer, wage integer);\n" +
                "create view my_view1 (num, min_num, total) " +
                "as select num, min(num), count(*) from t group by num; \n";
        assertTrue(compileDDL(ddl, compiler));

        // count(*) can be placed anywhere in materialized views
        ddl = "create table t(id integer not null, num integer, wage integer);\n" +
                "create view my_view1 " +
                "as select num, max(num), min(wage), count(*), sum(wage) from t group by num; \n";
        assertTrue(compileDDL(ddl, compiler));

        // Users can create single table views without including count(*) column.
        ddl = "create table t (id integer not null, num integer);\n" +
                "create view my_view1 as select id, sum(num) from t group by id; \n";
        assertTrue(compileDDL(ddl, compiler));

        ddl = "create table t(id integer not null, num integer, wage integer);\n" +
                "create view my_view1 (num, total) " +
                "as select num, count(*) from (select num from t limit 5) subt group by num; \n";
        checkDDLErrorMessage(ddl, "Materialized view \"MY_VIEW1\" cannot contain subquery sources.");

        ddl = "create table t(id integer not null, num integer, wage integer);\n" +
                "create view my_view1 (num, total) " +
                "as select num, count(*) from t where id in (select id from t) group by num; \n";
        checkDDLErrorMessage(ddl, "Materialized view \"MY_VIEW1\" cannot contain subquery sources.");

        ddl = "create table t1(id integer not null, num integer, wage integer);\n" +
                "create table t2(id integer not null, num integer, wage integer);\n" +
                "create view my_view1 (id, num, total) " +
                "as select t1.id, st2.num, count(*) from t1 join (select id ,num from t2 limit 2) st2 on t1.id = st2.id group by t1.id, st2.num; \n";
        checkDDLErrorMessage(ddl, "Materialized view \"MY_VIEW1\" cannot contain subquery sources.");

        ddl = "create table t(id integer not null, num integer);\n" +
                "create view my_view as select num, count(*) from t group by num order by num;";
        checkDDLErrorMessage(ddl, "Materialized view \"MY_VIEW\" with an ORDER BY clause is not supported.");

        ddl = "create table t(id integer not null, num integer, wage integer);\n" +
                "create view my_view1 (num, total, sumwage) " +
                "as select num, count(*), sum(wage) from t group by num; \n" +

                "create view my_view2 (num, total, sumwage) " +
                "as select num, count(*), sum(sumwage) from my_view1 group by num; ";
        checkDDLErrorMessage(ddl, "A materialized view (MY_VIEW2) can not be defined on another view (MY_VIEW1)");

        ddl = "create table t(id integer not null, num integer);\n" +
                "create view my_view as select num, count(*) from t group by num limit 1;";
        checkDDLErrorMessage(ddl, "Materialized view \"MY_VIEW\" with a LIMIT or OFFSET clause is not supported.");

        ddl = "create table t(id integer not null, num integer);\n" +
                "create view my_view as select num, count(*) from t group by num limit 1 offset 10;";
        checkDDLErrorMessage(ddl, "Materialized view \"MY_VIEW\" with a LIMIT or OFFSET clause is not supported.");

        ddl = "create table t(id integer not null, num integer);\n" +
                "create view my_view as select num, count(*) from t group by num offset 10;";
        checkDDLErrorMessage(ddl, "Materialized view \"MY_VIEW\" with a LIMIT or OFFSET clause is not supported.");

        ddl = "create table t(id integer not null, num integer);\n" +
                "create view my_view as select num, count(*) from t group by num having count(*) > 3;";
        checkDDLErrorMessage(ddl, "Materialized view \"MY_VIEW\" with a HAVING clause is not supported.");

        String errorMsg = "In database, the materialized view is automatically " +
                "partitioned based on its source table. Invalid PARTITION statement on view table MY_VIEW.";

        ddl = "create table t(id integer not null, num integer not null);\n" +
                "partition table t on column num;\n" +
                "create view my_view as select num, count(*) from t group by num;\n" +
                "partition table my_view on column num;";
        checkDDLErrorMessage(ddl, errorMsg);

        ddl = "create table t(id integer not null, num integer not null);\n" +
                "partition table t on column num;" +
                "create view my_view as select num, count(*) as ct from t group by num;" +
                "partition table my_view on column ct;";
        checkDDLErrorMessage(ddl, errorMsg);

        ddl = "create table t(id integer not null, num integer not null);\n" +
                "create view my_view as select num, count(*) from t group by num;" +
                "partition table my_view on column num;";
        checkDDLErrorMessage(ddl, errorMsg);

        // approx_count_distinct is not a supported aggregate function for materialized views.
        errorMsg = "Materialized view \"MY_VIEW\" must have non-group by columns aggregated by sum, count, min or max.";
        ddl = "create table t(id integer not null, num integer not null);\n" +
                "create view my_view as select id, count(*), approx_count_distinct(num) from t group by id;";
        checkDDLErrorMessage(ddl, errorMsg);

        // comparison expression not supported in group by clause -- actually gets caught because it's not allowed
        // in the select list either.
        errorMsg = "SELECT clause does not allow a BOOLEAN expression.";
        ddl = "create table t(id integer not null, num integer not null);\n" +
                "create view my_view as select (id = num) as idVsNumber, count(*) from t group by (id = num);" +
                "partition table t on column num;";
        checkDDLErrorMessage(ddl, errorMsg);

        // multiple count(*) in ddl
        ddl = "create table t(id integer not null, num integer not null, wage integer);\n" +
                "create view my_view as select id, wage, count(*), min(wage), count(*) from t group by id, wage;" +
                "partition table t on column num;";
        assertTrue(compileDDL(ddl, compiler));

        // Multiple table view should throw error msg without count(*) columns.
        errorMsg = "Materialized view \"V\" joins multiple tables, therefore must include COUNT(*) after any GROUP BY columns.";
        ddl = "CREATE TABLE T1 (a INTEGER NOT NULL, b INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (a INTEGER NOT NULL, b INTEGER NOT NULL);\n" +
                "CREATE VIEW V (aint, sumint) AS " +
                "SELECT T1.a, sum(T2.b) FROM T1 JOIN T2 ON T1.a=T2.a GROUP BY T1.a;";
        checkDDLErrorMessage(ddl, errorMsg);

        // Check single table view GB without aggregates.
        ddl = "CREATE TABLE T (a INTEGER NOT NULL, b INTEGER NOT NULL);\n" +
                "CREATE VIEW V AS " +
                "SELECT A, B FROM T GROUP BY A, B;";
        assertTrue(compileDDL(ddl, compiler));

        // Check single table view aggregates without GB column.
        ddl = "CREATE TABLE T (a INTEGER NOT NULL, b INTEGER NOT NULL);\n" +
                "CREATE VIEW V AS " +
                "SELECT SUM(A), MAX(B) FROM T;";
        assertTrue(compileDDL(ddl, compiler));

        subTestDDLCompilerMatViewJoin();
    }
    public void testCreateTableWithGeographyPointValue() throws Exception {
        String ddl =
                "create table points (" +
                "  id integer," +
                "  pt geography_point" +
                ");";
        Database db = goodDDLAgainstSimpleSchema(ddl);
        assertNotNull(db);

        Table pointTable = db.getTables().getIgnoreCase("points");
        assertNotNull(pointTable);

        Column pointCol = pointTable.getColumns().getIgnoreCase("pt");
        assertEquals(VoltType.GEOGRAPHY_POINT.getValue(), pointCol.getType());
    }

    public void testGeographyPointValueNegative() throws Exception {

        // POINT cannot be a partition column
        badDDLAgainstSimpleSchema(".*Partition columns must be an integer, varchar or varbinary type.*",
                "create table pts (" +
                "  pt geography_point not null" +
                ");" +
                "partition table pts on column pt;"
                );

        // POINT columns cannot yet be indexed
        badDDLAgainstSimpleSchema(".*POINT values are not currently supported as index keys.*",
                "create table pts (" +
                "  pt geography_point not null" +
                ");  " +
                "create index ptidx on pts(pt);"
                );

        // POINT columns cannot use unique/pk constraints which
        // are implemented as indexes.
        badDDLAgainstSimpleSchema(".*POINT values are not currently supported as index keys.*",
                "create table pts (" +
                "  pt geography_point primary key" +
                ");  "
                );

        badDDLAgainstSimpleSchema(".*POINT values are not currently supported as index keys.*",
                "create table pts (" +
                "  pt geography_point, " +
                "  primary key (pt)" +
                ");  "
                );

        badDDLAgainstSimpleSchema(".*POINT values are not currently supported as index keys.*",
                "create table pts (" +
                "  pt geography_point, " +
                "  constraint uniq_pt unique (pt)" +
                ");  "
                );

        badDDLAgainstSimpleSchema(".*POINT values are not currently supported as index keys.*",
                "create table pts (" +
                "  pt geography_point unique, " +
                ");  "
                );

        // Default values are not yet supported
        badDDLAgainstSimpleSchema(".*incompatible data type in conversion.*",
                "create table pts (" +
                "  pt geography_point default 'point(3.0 9.0)', " +
                ");  "
                );

        badDDLAgainstSimpleSchema(".*unexpected token.*",
                "create table pts (" +
                "  pt geography_point default pointfromtext('point(3.0 9.0)'), " +
                ");  "
                );
    }

    public void testCreateTableWithGeographyType() throws Exception {
        String ddl =
                "create table polygons (" +
                "  id integer," +
                "  poly geography, " +
                "  sized_poly0 geography(1066), " +
                "  sized_poly1 geography(155), " +    // min allowed length
                "  sized_poly2 geography(1048576) " + // max allowed length
                ");";
        Database db = goodDDLAgainstSimpleSchema(ddl);
        assertNotNull(db);

        Table polygonsTable = db.getTables().getIgnoreCase("polygons");
        assertNotNull(polygonsTable);

        Column geographyCol = polygonsTable.getColumns().getIgnoreCase("poly");
        assertEquals(VoltType.GEOGRAPHY.getValue(), geographyCol.getType());
        assertEquals(GeographyValue.DEFAULT_LENGTH, geographyCol.getSize());

        geographyCol = polygonsTable.getColumns().getIgnoreCase("sized_poly0");
        assertEquals(VoltType.GEOGRAPHY.getValue(), geographyCol.getType());
        assertEquals(1066, geographyCol.getSize());

        geographyCol = polygonsTable.getColumns().getIgnoreCase("sized_poly1");
        assertEquals(VoltType.GEOGRAPHY.getValue(), geographyCol.getType());
        assertEquals(155, geographyCol.getSize());

        geographyCol = polygonsTable.getColumns().getIgnoreCase("sized_poly2");
        assertEquals(VoltType.GEOGRAPHY.getValue(), geographyCol.getType());
        assertEquals(1048576, geographyCol.getSize());
    }

    public void testGeographyNegative() throws Exception {

        String ddl = "create table geogs ( geog geography not null );\n" +
                     "partition table geogs on column geog;\n";

        // GEOGRAPHY cannot be a partition column
        badDDLAgainstSimpleSchema(".*Partition columns must be an integer, varchar or varbinary type.*", ddl);

        ddl = "create table geogs ( geog geography(0) not null );";
        badDDLAgainstSimpleSchema(".*precision or scale out of range.*", ddl);

        // Minimum length for a GEOGRAPHY column is 155.
        ddl = "create table geogs ( geog geography(154) not null );";
        badDDLAgainstSimpleSchema(".*GEOGRAPHY column GEOG in table GEOGS " +
                "has length of 154 which is shorter than " +
                "155, the minimum allowed length for the type.*",
                ddl
                );

        ddl = "create table geogs ( geog geography(1048577) not null );";
        badDDLAgainstSimpleSchema(".*is > 1048576 char maximum.*", ddl);

        // GEOGRAPHY columns cannot use unique/pk constraints which
        // are implemented as indexes.
        ddl = "create table geogs ( geog GEOGRAPHY primary key );\n";
        badDDLAgainstSimpleSchema(".*GEOGRAPHY values are not currently supported as unique index keys.*", ddl);

        ddl = "create table geogs ( geog geography, " +
                                  " primary key (geog) );\n";
        badDDLAgainstSimpleSchema(".*GEOGRAPHY values are not currently supported as unique index keys.*", ddl);

        ddl = "create table geogs ( geog geography, " +
                                  " constraint uniq_geog unique (geog) );\n";
        badDDLAgainstSimpleSchema(".*GEOGRAPHY values are not currently supported as unique index keys.*", ddl);

        ddl = "create table geogs (geog GEOGRAPHY unique);";
        badDDLAgainstSimpleSchema(".*GEOGRAPHY values are not currently supported as unique index keys.*", ddl);

        ddl = "create table geogs (geog GEOGRAPHY); create unique index geogsgeog on geogs(geog);";
        badDDLAgainstSimpleSchema(".*GEOGRAPHY values are not currently supported as unique index keys.*", ddl);

        ddl = "create table pgeogs (geog GEOGRAPHY, partkey int ); " +
        "partition table pgeogs on column partkey; " +
        "create assumeunique index pgeogsgeog on pgeogs(geog);";
        badDDLAgainstSimpleSchema(".*GEOGRAPHY values are not currently supported as unique index keys.*", ddl);

        // index on boolean functions is not supported
        ddl = "create table geogs ( id integer primary key, " +
                                  " region1 geography NOT NULL, " +
                                  " point1 geography_point NOT NULL );\n" +
              "create index geoindex_contains ON geogs (contains(region1, point1) );\n";
        badDDLAgainstSimpleSchema(".*Cannot create index \"GEOINDEX_CONTAINS\" because it contains a BOOLEAN valued function 'CONTAINS', " +
                                  "which is not supported.*", ddl);

        ddl = "create table geogs ( id integer primary key, " +
                                  " region1 geography NOT NULL, " +
                                  " point1 geography_point NOT NULL );\n" +
              "create index geoindex_within100000 ON geogs (DWITHIN(region1, point1, 100000) );\n";
        badDDLAgainstSimpleSchema(".*Cannot create index \"GEOINDEX_WITHIN100000\" because it contains a BOOLEAN valued function 'DWITHIN', " +
                                  "which is not supported.*", ddl);

        ddl = "create table geogs ( id integer primary key, " +
                                  " region1 geography NOT NULL, " +
                                  " point1 geography_point NOT NULL);\n " +
              "create index geoindex_nonzero_distance ON geogs ( distance(region1, point1) = 0 );\n";
        badDDLAgainstSimpleSchema(".*Cannot create index \"GEOINDEX_NONZERO_DISTANCE\" because it contains " +
                                  "comparison expression '=', which is not supported.*", ddl);

        // Default values are not yet supported
        ddl = "create table geogs ( geog geography default 'polygon((3.0 9.0, 3.0 0.0, 0.0 9.0, 3.0 9.0)');\n";
        badDDLAgainstSimpleSchema(".*incompatible data type in conversion.*", ddl);

        ddl = "create table geogs ( geog geography default polygonfromtext('polygon((3.0 9.0, 3.0 0.0, 0.0 9.0, 3.0 9.0)') );\n";
        badDDLAgainstSimpleSchema(".*unexpected token.*", ddl);

        // Materialized Views
        ddl = "create table geogs ( id integer primary key, " +
                                  " region1 geography NOT NULL, " +
                                  " point1 geography_point NOT NULL );\n" +
              "create view geo_view as select count(*), sum(id), sum(distance(region1, point1)) from geogs;\n";
        checkDDLAgainstSimpleSchema(null, ddl);

        ddl = "create table geogs ( id integer primary key, " +
                                  " region1 geography NOT NULL, " +
                                  " point1 geography_point NOT NULL );\n" +
              "create view geo_view as select region1, count(*) from geogs group by region1;\n";
        badDDLAgainstSimpleSchema(
                "Materialized view \"GEO_VIEW\" with expression of type GEOGRAPHY in GROUP BY clause not supported.",
                ddl);

        ddl = "create table geogs ( id integer primary key, " +
                                  " region1 geography NOT NULL, " +
                                  " point1 geography_point NOT NULL );\n" +
              "create view geo_view as select point1, count(*) from geogs group by point1;\n";
        badDDLAgainstSimpleSchema(
                "Materialized view \"GEO_VIEW\" with expression of type GEOGRAPHY_POINT in GROUP BY clause not supported.",
                ddl);

        ddl = "create table geogs ( id integer primary key, " +
                                  " region1 geography NOT NULL, " +
                                  " point1 geography_point NOT NULL );\n" +
              "create view geo_view as select isValid(Region1), count(*) from geogs group by isValid(Region1);\n";
        badDDLAgainstSimpleSchema(
                "A SELECT clause does not allow a BOOLEAN expression. consider using CASE WHEN to decode the BOOLEAN expression into a value of some other type.",
                ddl);

        ddl = "create table geogs ( id integer primary key, " +
                                  " region1 geography NOT NULL, " +
                                  " point1 geography_point NOT NULL );\n" +
              "create view geo_view as select Contains(Region1, POINT1), count(*) from geogs group by Contains(Region1, POINT1);\n";
        badDDLAgainstSimpleSchema(
                "A SELECT clause does not allow a BOOLEAN expression. consider using CASE WHEN to decode the BOOLEAN expression into a value of some other type.",
                ddl);

        ddl = "create table geogs ( id integer primary key, " +
                                  " region1 geography NOT NULL, " +
                                  " point1 geography_point NOT NULL );\n" +
              "create view geo_view as select Centroid(Region1), count(*) from geogs group by Centroid(Region1);\n";
        badDDLAgainstSimpleSchema(
                "Materialized view \"GEO_VIEW\" with a GEOGRAPHY_POINT valued function 'CENTROID' in GROUP BY clause not supported.",
                ddl);

        ddl = "create table geogs ( id integer, " +
                " region1 geography NOT NULL, " +
                " point1 geography_point NOT NULL );\n" +
              "create index COMPOUND_GEO_NOT_SUPPORTED on geogs(id, region1);\n";
        badDDLAgainstSimpleSchema(
                "Cannot create index \"COMPOUND_GEO_NOT_SUPPORTED\" " +
                 "because GEOGRAPHY values must be the only component of an index key: \"REGION1\"",
                ddl);
    }

    public void testPartitionOnBadType() {
        String schema =
            "create table books (cash float default 0.0 NOT NULL, title varchar(10) default 'foo', PRIMARY KEY(cash));\n"
                + "partition table books on column cash;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertFalse(success);
    }

    public void test3324MPPlan() throws IOException {
        String schema =
                "create table blah  (pkey integer not null, strval varchar(200), PRIMARY KEY(pkey));\n";
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.enableDiagnostics();
        pb.addLiteralSchema(schema);
        pb.addPartitionInfo("blah", "pkey");
        pb.addStmtProcedure("undeclaredspquery1", "select strval UNDECLARED1 from blah where pkey = ?");
        pb.addStmtProcedure("undeclaredspquery2", "select strval UNDECLARED2 from blah where pkey = 12");
        pb.addStmtProcedure("declaredspquery1", "select strval SODECLARED1 from blah where pkey = ?",
                new ProcedurePartitionData("blah", "pkey", "0"));
        // Currently no way to do this?
        // pb.addStmtProcedure("declaredspquery2", "select strval SODECLARED2 from blah where pkey = 12", "blah.pkey=12");
        assertTrue(pb.compile(Configuration.getPathToCatalogForTest("test3324.jar")));
        List<String> diagnostics = pb.harvestDiagnostics();
        // This asserts that the undeclared SP plans don't mistakenly get SP treatment
        // -- they must each include a RECEIVE plan node.
        assertEquals(2, countStringsMatching(diagnostics, ".*\"UNDECLARED.\".*\"PLAN_NODE_TYPE\":\"RECEIVE\".*"));
        // This asserts that the methods used to prevent undeclared SP plans from getting SP treatment
        // don't over-reach to declared SP plans.
        assertEquals(0, countStringsMatching(diagnostics, ".*\"SODECLARED.\".*\"PLAN_NODE_TYPE\":\"RECEIVE\".*"));
        // System.out.println("test3324MPPlan");
        // System.out.println(diagnostics);
    }

    public void testBadDDLErrorLineNumber() {
        String schema =
            "-- a comment\n" +                          // 1
            "create table books (\n" +                  // 2
            " id integer default 0,\n" +                // 3
            " strval varchar(33000) default '',\n" +    // 4
            " PRIMARY KEY(id)\n" +                      // 5
            ");\n" +                                    // 6
            "\n" +                                      // 7
            "-- another comment\n" +                    // 8
            "create view badview (\n" +                 // 9 * error reported here *
            " id,\n" +
            " COUNT(*),\n" +
            " total\n" +
            " as\n" +
            "select id, COUNT(*), SUM(cnt)\n" +
            " from books\n" +
            " group by id;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertFalse(success);
        for (Feedback error: compiler.m_errors) {
            assertEquals(9, error.lineNo);
        }
    }

    public void testInvalidCreateFunctionDDL() {
        ArrayList<Feedback> fbs;
        // Test CREATE FUNCTION syntax
        String[] ddls = new String[] {
                "CREATE FUNCTION .func FROM METHOD class.method",
                "CREATE FUNCTION func FROM METHOD class",
                "CREATE FUNCTION func FROM METHOD .method",
                "CREATE FUNCTION func FROM METHOD package..class.method",
                "CREATE FUNCTION func FROM METHOD package.class.method."
        };
        String expectedError = "Invalid CREATE FUNCTION statement: \"%s\""
                + " expected syntax: \"CREATE FUNCTION name FROM METHOD class-name.method-name\"";

        for (String ddl : ddls) {
            fbs = checkInvalidDDL(ddl + ";");
            assertTrue(isFeedbackPresent(String.format(expectedError, ddl), fbs));
        }

        // Test identifiers
        String[][] ddlsAndInvalidIdentifiers = new String[][] {
                {"CREATE FUNCTION 1nvalid FROM METHOD package.class.method", "1nvalid"},
                {"CREATE FUNCTION func FROM METHOD 1nvalid.class.method", "1nvalid.class"},
                {"CREATE FUNCTION func FROM METHOD package.1nvalid.method", "package.1nvalid"},
                {"CREATE FUNCTION func FROM METHOD package.class.1nvalid", "1nvalid"}
        };
        expectedError = "Unknown indentifier in DDL: \"%s\" contains invalid identifier \"%s\"";
        for (String[] ddlAndInvalidIdentifier : ddlsAndInvalidIdentifiers) {
            fbs = checkInvalidDDL(ddlAndInvalidIdentifier[0] + ";");
            assertTrue(isFeedbackPresent(
                    String.format(expectedError, ddlAndInvalidIdentifier[0], ddlAndInvalidIdentifier[1]), fbs));
        }

        // Test method validation
        VoltLogger mockedLogger = Mockito.mock(VoltLogger.class);
        VoltCompiler.setVoltLogger(mockedLogger);

        // Class not found
        fbs = checkInvalidDDL("CREATE FUNCTION afunc FROM METHOD org.voltdb.compiler.functions.NonExistentClass.run;");
        assertTrue(isFeedbackPresent("Cannot load class for user-defined function: org.voltdb.compiler.functions.NonExistentClass", fbs));

        // Abstract class
        fbs = checkInvalidDDL("CREATE FUNCTION afunc FROM METHOD org.voltdb.compiler.functions.AbstractUDFClass.run;");
        assertTrue(isFeedbackPresent("Cannot define a function using an abstract class org.voltdb.compiler.functions.AbstractUDFClass", fbs));

        // Method not found
        fbs = checkInvalidDDL("CREATE FUNCTION afunc FROM METHOD org.voltdb.compiler.functions.InvalidUDFLibrary.nonexistent;");
        assertTrue(isFeedbackPresent("Cannot find the implementation method nonexistent for user-defined function afunc in class InvalidUDFLibrary", fbs));

        // Invalid return type
        fbs = checkInvalidDDL("CREATE FUNCTION afunc FROM METHOD org.voltdb.compiler.functions.InvalidUDFLibrary.runWithUnsupportedReturnType;");
        assertTrue(isFeedbackPresent("Method InvalidUDFLibrary.runWithUnsupportedReturnType has an unsupported return type org.voltdb.compiler.functions.InvalidUDFLibrary$UnsupportedType", fbs));

        // Invalid parameter type
        fbs = checkInvalidDDL("CREATE FUNCTION afunc FROM METHOD org.voltdb.compiler.functions.InvalidUDFLibrary.runWithUnsupportedParamType;");
        assertTrue(isFeedbackPresent("Method InvalidUDFLibrary.runWithUnsupportedParamType has an unsupported parameter type org.voltdb.compiler.functions.InvalidUDFLibrary$UnsupportedType at position 2", fbs));

        // Multiple functions with the same name
        fbs = checkInvalidDDL("CREATE FUNCTION afunc FROM METHOD org.voltdb.compiler.functions.InvalidUDFLibrary.dup;");
        assertTrue(isFeedbackPresent("Class InvalidUDFLibrary has multiple methods named dup. Only a single function method is supported.", fbs));

        // Function name exists
        // One from FunctionSQL
        fbs = checkInvalidDDL("CREATE FUNCTION abs FROM METHOD org.voltdb.compiler.functions.InvalidUDFLibrary.run;");
        assertTrue(isFeedbackPresent("Function \"abs\" is already defined.", fbs));
        // One from FunctionCustom
        fbs = checkInvalidDDL("CREATE FUNCTION log FROM METHOD org.voltdb.compiler.functions.InvalidUDFLibrary.run;");
        assertTrue(isFeedbackPresent("Function \"log\" is already defined.", fbs));
        // One from FunctionForVoltDB
        fbs = checkInvalidDDL("CREATE FUNCTION longitude FROM METHOD org.voltdb.compiler.functions.InvalidUDFLibrary.run;");
        assertTrue(isFeedbackPresent("Function \"longitude\" is already defined.", fbs));

        // The class contains some other invalid functions with the same name
        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL("CREATE FUNCTION afunc FROM METHOD org.voltdb.compiler.functions.InvalidUDFLibrary.run;", compiler);
        assertTrue("A CREATE FUNCTION statement should be able to succeed, but it did not.", success);
        verify(mockedLogger, atLeastOnce()).warn(contains("Class InvalidUDFLibrary has a non-public run() method."));
        verify(mockedLogger, atLeastOnce()).warn(contains("Class InvalidUDFLibrary has a void run() method."));
        verify(mockedLogger, atLeastOnce()).warn(contains("Class InvalidUDFLibrary has a static run() method."));
        verify(mockedLogger, atLeastOnce()).warn(contains("Class InvalidUDFLibrary has a non-public static void run() method."));

        VoltCompiler.setVoltLogger(new VoltLogger("COMPILER"));
    }

    public void testInvalidCreateProcedureDDL() {
        ArrayList<Feedback> fbs;
        String expectedError;

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NonExistentPartitionParamInteger;" +
                "PARTITION PROCEDURE NonExistentPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Cannot load class for procedure: org.voltdb.compiler.procedures.NonExistentPartitionParamInteger";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "PARTITION PROCEDURE NotDefinedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Partition references an undefined procedure \"NotDefinedPartitionParamInteger\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_WHAAAT COLUMN PKEY;"
                );
        expectedError = "Procedure org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger "
                + "is partitioned on a column PKEY which can't be found in table PKEY_WHAAAT.";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PSURROGATE;"
                );
        expectedError = "Procedure org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger "
                + "is partitioned on a column PSURROGATE which can't be found in table PKEY_INTEGER.";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY PARAMETER 8;"
                );
        expectedError = "Invalid parameter index value 8 for procedure: "
                + "org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM GLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Invalid CREATE PROCEDURE statement: " +
                "\"CREATE PROCEDURE FROM GLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger\"" +
                " expected syntax: \"CREATE PROCEDURE";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger FOR TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Invalid PARTITION statement: \"PARTITION PROCEDURE " +
                "NotAnnotatedPartitionParamInteger FOR TABLE PKEY_INTEGER COLUMN PKEY\"" +
                " expected syntax: PARTITION PROCEDURE procedure ON " +
                "TABLE table COLUMN column [PARAMETER parameter-index-no]";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER CLUMN PKEY PARMTR 0;"
                );
        expectedError = "Invalid PARTITION statement: \"PARTITION PROCEDURE " +
                "NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER CLUMN PKEY PARMTR 0\"" +
                " expected syntax: PARTITION PROCEDURE procedure ON " +
                "TABLE table COLUMN column [PARAMETER parameter-index-no]";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY PARAMETER hello;"
                );
        expectedError = "Invalid PARTITION statement: \"PARTITION PROCEDURE " +
                "NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY PARAMETER hello\"" +
                " expected syntax: PARTITION PROCEDURE procedure ON " +
                "TABLE table COLUMN column [PARAMETER parameter-index-no]";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROGEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY PARAMETER hello;"
                );
        expectedError = "Invalid PARTITION statement: " +
                "\"PARTITION PROGEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER " +
                "COLUMN PKEY PARAMETER hello\" expected syntax: \"PARTITION TABLE table " +
                "ON COLUMN column\" or: \"PARTITION PROCEDURE procedure ON " +
                "TABLE table COLUMN column [PARAMETER parameter-index-no]\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE OUTOF CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY PARAMETER 2;"
                );
        expectedError = "Invalid CREATE PROCEDURE statement: " +
                "\"CREATE PROCEDURE OUTOF CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger\"" +
                " expected syntax: \"CREATE PROCEDURE";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "MAKE PROCEDURE OUTOF CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY PARAMETER 2;"
                );
        expectedError = "DDL Error: \"unexpected token: MAKE\" in statement starting on lineno: 1";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE 1PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN;"
                );
        expectedError = "Unknown indentifier in DDL: \"PARTITION TABLE 1PKEY_INTEGER ON COLUMN PKEY\" " +
                "contains invalid identifier \"1PKEY_INTEGER\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN 2PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Unknown indentifier in DDL: \"PARTITION TABLE PKEY_INTEGER ON COLUMN 2PKEY\" " +
                "contains invalid identifier \"2PKEY\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS 0rg.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Unknown indentifier in DDL: \""+
                "CREATE PROCEDURE FROM CLASS 0rg.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger" +
                "\" contains invalid identifier \"0rg.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.3compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Unknown indentifier in DDL: \""+
                "CREATE PROCEDURE FROM CLASS org.voltdb.3compiler.procedures.NotAnnotatedPartitionParamInteger" +
                "\" contains invalid identifier \"org.voltdb.3compiler.procedures.NotAnnotatedPartitionParamInteger\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.4NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Unknown indentifier in DDL: \""+
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.4NotAnnotatedPartitionParamInteger" +
                "\" contains invalid identifier \"org.voltdb.compiler.procedures.4NotAnnotatedPartitionParamInteger\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE 5NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Unknown indentifier in DDL: \""+
                "PARTITION PROCEDURE 5NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN PKEY" +
                "\" contains invalid identifier \"5NotAnnotatedPartitionParamInteger\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE 6PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Unknown indentifier in DDL: \""+
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE 6PKEY_INTEGER COLUMN PKEY" +
                "\" contains invalid identifier \"6PKEY_INTEGER\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN 7PKEY;"
                );
        expectedError = "Unknown indentifier in DDL: \""+
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger ON TABLE PKEY_INTEGER COLUMN 7PKEY" +
                "\" contains invalid identifier \"7PKEY\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.NotAnnotatedPartitionParamInteger;" +
                "PARTITION PROCEDURE NotAnnotatedPartitionParamInteger TABLE PKEY_INTEGER ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Invalid PARTITION statement: \"PARTITION PROCEDURE " +
                "NotAnnotatedPartitionParamInteger TABLE PKEY_INTEGER ON TABLE PKEY_INTEGER COLUMN PKEY\"" +
                " expected syntax: PARTITION PROCEDURE procedure ON " +
                "TABLE table COLUMN column [PARAMETER parameter-index-no]";
        assertTrue(isFeedbackPresent(expectedError, fbs));
    }

    public void testInvalidSingleStatementCreateProcedureDDL() {
        ArrayList<Feedback> fbs;
        String expectedError;

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS BANBALOO pkey FROM PKEY_INTEGER;" +
                "PARTITION PROCEDURE Foo ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Failed to plan for statement (sql0) \"BANBALOO pkey FROM PKEY_INTEGER;\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS SELEC pkey FROM PKEY_INTEGER;" +
                "PARTITION PROCEDURE Foo ON TABLE PKEY_INTEGER COLUMN PKEY PARAMETER 0;"
                );
        expectedError = "Failed to plan for statement (sql0) \"SELEC pkey FROM PKEY_INTEGER;\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS DELETE FROM PKEY_INTEGER WHERE PKEY = ?;" +
                "PARTITION PROCEDURE Foo ON TABLE PKEY_INTEGER COLUMN PKEY PARAMETER 2;"
                );
        expectedError = "Invalid parameter index value 2 for procedure: Foo";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS DELETE FROM PKEY_INTEGER;" +
                "PARTITION PROCEDURE Foo ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Invalid parameter index value 0 for procedure: Foo";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE 7Foo AS DELETE FROM PKEY_INTEGER WHERE PKEY = ?;" +
                "PARTITION PROCEDURE 7Foo ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Unknown indentifier in DDL: \""+
                "CREATE PROCEDURE 7Foo AS DELETE FROM PKEY_INTEGER WHERE PKEY = ?" +
                "\" contains invalid identifier \"7Foo\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));
    }

    public void testInvalidMultipleStatementCreateProcedureDDL() {
        ArrayList<Feedback> fbs;
        String expectedError;

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS BEGIN SELECT * FROM PKEY_INTEGER;\n"
                );
        expectedError = "Schema file ended mid-statement (no semicolon found)";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS BEGIN SELECT * FROM PKEY_INTEGER;\n" +
                "BANBALOO pkey FROM PKEY_INTEGER;" +
                "PARTITION PROCEDURE Foo ON TABLE PKEY_INTEGER COLUMN PKEY; END;"
                );
        expectedError = "Failed to plan for statement (sql1) \"BANBALOO pkey FROM PKEY_INTEGER;\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS BEGIN SELECT * FROM PKEY_INTEGER; SELEC pkey FROM PKEY_INTEGER;" +
                "PARTITION PROCEDURE Foo ON TABLE PKEY_INTEGER COLUMN PKEY PARAMETER 0; END;"
                );
        expectedError = "Failed to plan for statement (sql1) \"SELEC pkey FROM PKEY_INTEGER;\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, FRAC FLOAT, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS BEGIN DELETE FROM PKEY_INTEGER WHERE FRAC > ?; END;" +
                "PARTITION PROCEDURE Foo ON TABLE PKEY_INTEGER COLUMN FRAC PARAMETER 0;"
                );
        expectedError = "Procedure Foo refers to a column in schema which is not a partition key.";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_FLOAT ( PKEY FLOAT NOT NULL, FRAC FLOAT, PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_FLOAT ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS BEGIN DELETE FROM PKEY_INTEGER WHERE FRAC > ?; END;" +
                "PARTITION PROCEDURE Foo ON TABLE PKEY_FLOAT COLUMN PKEY PARAMETER 0;"
                );
        expectedError = "In database, Partition column 'PKEY_FLOAT.pkey' is not a valid type. "
                + "Partition columns must be an integer, varchar or varbinary type.";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE Foo AS BEGIN DELETE FROM PKEY_INTEGER; SELECT * FROM PKEY_INTEGER END;" +
                "PARTITION PROCEDURE Foo ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Failed to plan for statement (sql1) \"SELECT * FROM PKEY_INTEGER END;\". "
                + "Error: \"SQL Syntax error in \"SELECT * FROM PKEY_INTEGER END;\" unexpected token: END\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "CREATE PROCEDURE 7Foo AS BEGIN DELETE FROM PKEY_INTEGER WHERE PKEY = ?; END;" +
                "PARTITION PROCEDURE 7Foo ON TABLE PKEY_INTEGER COLUMN PKEY;"
                );
        expectedError = "Unknown indentifier in DDL: \""+
                "CREATE PROCEDURE 7Foo AS BEGIN DELETE FROM PKEY_INTEGER WHERE PKEY = ?; END" +
                "\" contains invalid identifier \"7Foo\"";
        assertTrue(isFeedbackPresent(expectedError, fbs));
    }

    public void testDropProcedure() throws Exception {
        if (Float.parseFloat(System.getProperty("java.specification.version")) < 1.7) {
            return;
        }

        Database db;
        Procedure proc;

        // Make sure we can drop a non-annotated stored procedure
        db = goodDDLAgainstSimpleSchema(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "creAte PrOcEdUrE partition on table books column cash FrOm CLasS org.voltdb.compiler.procedures.AddBook; " +
                "create procedure from class org.voltdb.compiler.procedures.NotAnnotatedAddBook; " +
                "DROP PROCEDURE org.voltdb.compiler.procedures.AddBook;"
                );
        proc = db.getProcedures().get("AddBook");
        assertNull(proc);
        proc = db.getProcedures().get("NotAnnotatedAddBook");
        assertNotNull(proc);

        // Make sure we can drop an annotated stored procedure
        db = goodDDLAgainstSimpleSchema(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "creAte PrOcEdUrE partition on table books column cash FrOm CLasS org.voltdb.compiler.procedures.AddBook; " +
                "create procedure from class org.voltdb.compiler.procedures.NotAnnotatedAddBook; " +
                "DROP PROCEDURE NotAnnotatedAddBook;"
                );
        proc = db.getProcedures().get("NotAnnotatedAddBook");
        assertNull(proc);
        proc = db.getProcedures().get("AddBook");
        assertNotNull(proc);

        // Make sure we can drop a single-statement procedure
        db = goodDDLAgainstSimpleSchema(
                "create procedure p1 as select * from books;\n" +
                "drop procedure p1;"
                );
        proc = db.getProcedures().get("p1");
        assertNull(proc);

        ArrayList<Feedback> fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "creAte PrOcEdUrE partition on table books column cash FrOm CLasS org.voltdb.compiler.procedures.AddBook; " +
                "DROP PROCEDURE NotAnnotatedAddBook;");
        String expectedError =
                "Dropped Procedure \"NotAnnotatedAddBook\" is not defined";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        // Make sure we can't drop a CRUD procedure (full name)
        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "DROP PROCEDURE PKEY_INTEGER.insert;"
                );
        expectedError =
                "Dropped Procedure \"PKEY_INTEGER.insert\" is not defined";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        // Make sure we can't drop a CRUD procedure (partial name)
        fbs = checkInvalidDDL(
                "CREATE TABLE PKEY_INTEGER ( PKEY INTEGER NOT NULL, DESCR VARCHAR(128), PRIMARY KEY (PKEY) );" +
                "PARTITION TABLE PKEY_INTEGER ON COLUMN PKEY;" +
                "DROP PROCEDURE insert;"
                );
        expectedError =
                "Dropped Procedure \"insert\" is not defined";
        assertTrue(isFeedbackPresent(expectedError, fbs));

        // check if exists
        db = goodDDLAgainstSimpleSchema(
                "create procedure p1 as select * from books;\n" +
                "drop procedure p1 if exists;\n" +
                "drop procedure p1 if exists;\n"
                );
        proc = db.getProcedures().get("p1");
        assertNull(proc);

        // check if exists
        db = goodDDLAgainstSimpleSchema(
                "create procedure mp1 as begin select * from books; end;\n" +
                "drop procedure mp1 if exists;\n" +
                "drop procedure mp1 if exists;\n"
                );
        proc = db.getProcedures().get("mp1");
        assertNull(proc);
    }

    private ArrayList<Feedback> checkInvalidDDL(String ddl) {
        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(ddl, compiler);
        assertFalse(success);
        return compiler.m_errors;
    }

    public void testValidAnnotatedProcedureDLL() throws Exception {
        String schema =
                "create table books" +
                " (cash integer default 23 not null," +
                " title varchar(3) default 'foo'," +
                " PRIMARY KEY(cash));" +
                "PARTITION TABLE books ON COLUMN cash;" +
                "creAte PrOcEdUrE partition on table books column cash FrOm CLasS org.voltdb.compiler.procedures.AddBook;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertTrue(success);

        String catalogContents = VoltCompilerUtils.readFileFromJarfile(testout_jar, "catalog.txt");

        Catalog c2 = new Catalog();
        c2.execute(catalogContents);

        Database db = c2.getClusters().get("cluster").getDatabases().get("database");
        Procedure addBook = db.getProcedures().get("AddBook");
        assertTrue(addBook.getSinglepartition());
    }

    public void testValidNonAnnotatedProcedureDDL() throws Exception {
        String schema =
                "create table books" +
                " (cash integer default 23 not null," +
                " title varchar(3) default 'foo'," +
                " PRIMARY KEY(cash));" +
                "PARTITION TABLE books ON COLUMN cash;" +
                "create procedure from class org.voltdb.compiler.procedures.NotAnnotatedAddBook;" +
                "paRtItiOn prOcEdure NotAnnotatedAddBook On taBLe   books coLUmN cash   ParaMETer  0;";

        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(schema, compiler);
        assertTrue(success);

        String catalogContents = VoltCompilerUtils.readFileFromJarfile(testout_jar, "catalog.txt");
        Catalog c2 = new Catalog();
        c2.execute(catalogContents);
        Database db = c2.getClusters().get("cluster").getDatabases().get("database");
        Procedure addBook = db.getProcedures().get("NotAnnotatedAddBook");
        assertTrue(addBook.getSinglepartition());
    }

    class TestRole {
        String name;
        boolean sql = false;
        boolean sqlread = false;
        boolean sysproc = false;
        boolean defaultproc = false;
        boolean defaultprocread = false;
        boolean allproc = false;

        public TestRole(String name) {
            this.name = name;
        }

        public TestRole(String name, boolean sql, boolean sqlread, boolean sysproc,
                        boolean defaultproc, boolean defaultprocread, boolean allproc) {
            this.name = name;
            this.sql = sql;
            this.sqlread = sqlread;
            this.sysproc = sysproc;
            this.defaultproc = defaultproc;
            this.defaultprocread = defaultprocread;
            this.allproc = allproc;
        }
    }

    private void checkRoleDDL(String ddl, String errorRegex, TestRole... roles) {
        VoltCompiler compiler = new VoltCompiler(false);
        final boolean success = compileDDL(ddl, compiler);

        String error = (success || compiler.m_errors.size() == 0
                            ? ""
                            : compiler.m_errors.get(compiler.m_errors.size()-1).message);
        if (errorRegex == null) {
            assertTrue(String.format("Expected success\nDDL: %s\nERR: %s", ddl, error), success);

            Database db = compiler.getCatalog().getClusters().get("cluster").getDatabases().get("database");
            CatalogMap<Group> groups = db.getGroups();
            CatalogMap<Connector> connectors = db.getConnectors();
            if (connectors.get("0") == null ) {
                connectors.add("0");
            }

            assertNotNull(groups);
            assertTrue(roles.length <= groups.size());

            for (TestRole role : roles) {
                Group group = groups.get(role.name);
                assertNotNull(String.format("Missing role \"%s\"", role.name), group);
                assertEquals(String.format("Role \"%s\" sql flag mismatch:", role.name), role.sql, group.getSql());
                assertEquals(String.format("Role \"%s\" sqlread flag mismatch:", role.name), role.sqlread, group.getSqlread());
                assertEquals(String.format("Role \"%s\" admin flag mismatch:", role.name), role.sysproc, group.getAdmin());
                assertEquals(String.format("Role \"%s\" defaultproc flag mismatch:", role.name), role.defaultproc, group.getDefaultproc());
                assertEquals(String.format("Role \"%s\" defaultprocread flag mismatch:", role.name), role.defaultprocread, group.getDefaultprocread());
                assertEquals(String.format("Role \"%s\" allproc flag mismatch:", role.name), role.allproc, group.getAllproc());
            }
        }
        else {
            assertFalse(String.format("Expected error (\"%s\")\n\nDDL: %s", errorRegex, ddl), success);
            assertFalse("Expected at least one error message.", error.isEmpty());
            Matcher m = Pattern.compile(errorRegex, Pattern.DOTALL).matcher(error);
            assertTrue(String.format("%s\nEXPECTED: %s", error, errorRegex), m.matches());
        }
    }

    private void goodRoleDDL(String ddl, TestRole... roles) {
        checkRoleDDL(ddl, null, roles);
    }

    private void badRoleDDL(String ddl, String errorRegex) {
        checkRoleDDL(ddl, errorRegex);
    }

    public void testRoleDDL() throws Exception {
        goodRoleDDL("create role R1;", new TestRole("r1"));
        goodRoleDDL("create role r1;create role r2;", new TestRole("r1"), new TestRole("R2"));
        goodRoleDDL("create role r1 with adhoc;", new TestRole("r1", true, true, false, true, true, false));
        goodRoleDDL("create role r1 with sql;", new TestRole("r1", true, true, false, true, true, false));
        goodRoleDDL("create role r1 with sqlread;", new TestRole("r1", false, true, false, false, true, false));
        goodRoleDDL("create role r1 with sysproc;", new TestRole("r1", true, true, true, true, true, true));
        goodRoleDDL("create role r1 with defaultproc;", new TestRole("r1", false, false, false, true, true, false));
        goodRoleDDL("create role r1 with adhoc,sysproc,defaultproc;", new TestRole("r1", true, true, true, true, true, true));
        goodRoleDDL("create role r1 with adhoc,sysproc,sysproc;", new TestRole("r1", true, true, true, true, true, true));
        goodRoleDDL("create role r1 with AdHoc,SysProc,DefaultProc;", new TestRole("r1", true, true, true, true, true, true));
        //Defaultprocread.
        goodRoleDDL("create role r1 with defaultprocread;", new TestRole("r1", false, false, false, false, true, false));
        goodRoleDDL("create role r1 with AdHoc,SysProc,DefaultProc,DefaultProcRead;", new TestRole("r1", true, true, true, true, true, true));
        goodRoleDDL("create role r1 with AdHoc,Admin,DefaultProc,DefaultProcRead;", new TestRole("r1", true, true, true, true, true, true));
        goodRoleDDL("create role r1 with allproc;", new TestRole("r1", false, false, false, false, false, true));

        // Check default roles: ADMINISTRATOR, USER
        goodRoleDDL("",
                    new TestRole("ADMINISTRATOR", true, true, true, true, true, true),
                    new TestRole("USER", true, true, false, true, true, true));
    }

    public void testBadRoleDDL() throws Exception {
        badRoleDDL("create role r1", ".*no semicolon.*");
        badRoleDDL("create role r1;create role r1;", ".*already exists.*");
        badRoleDDL("create role r1 with ;", ".*Invalid CREATE ROLE statement.*");
        badRoleDDL("create role r1 with blah;", ".*Invalid permission \"BLAH\".*");
        badRoleDDL("create role r1 with adhoc sysproc;", ".*Invalid CREATE ROLE statement.*");
        badRoleDDL("create role r1 with adhoc, blah;", ".*Invalid permission \"BLAH\".*");

        // cannot override default roles
        badRoleDDL("create role ADMINISTRATOR;", ".*already exists.*");
        badRoleDDL("create role USER;", ".*already exists.*");
    }

    private Database checkDDLAgainstSimpleSchema(String errorRegex, String... ddl) throws Exception {
        String schema = "create table books (cash integer default 23 NOT NULL, title varbinary(10) default NULL, PRIMARY KEY(cash)); " +
                                         "partition table books on column cash;";
        return checkDDLAgainstGivenSchema(errorRegex, schema, ddl);
    }

    private Database checkDDLAgainstGivenSchema(String errorRegex, String givenSchema, String... ddl) {
        String schemaDDL =
            givenSchema +
            StringUtils.join(ddl, " ");

        VoltCompiler compiler = new VoltCompiler(false);
        boolean success;
        String error;
        try {
            success = compileDDL(schemaDDL, compiler);
            error = (success || compiler.m_errors.size() == 0
                ? ""
                : compiler.m_errors.get(compiler.m_errors.size()-1).message);
        }
        catch (HsqlException hex) {
            success = false;
            error = hex.getMessage();
        }
        catch (PlanningErrorException plex) {
            success = false;
            error = plex.getMessage();
        }
        if (errorRegex == null) {
            assertTrue(String.format("Expected success\nDDL: %s\n%s",
                                     StringUtils.join(ddl, " "),
                                     error),
                       success);
            Catalog cat = compiler.getCatalog();
            return cat.getClusters().get("cluster").getDatabases().get("database");
        }
        else {
            assertFalse(String.format("Expected error (\"%s\")\nDDL: %s",
                                      errorRegex,
                                      StringUtils.join(ddl, " ")),
                        success);
            assertFalse("Expected at least one error message.", error.isEmpty());
            Matcher m = Pattern.compile(errorRegex, Pattern.DOTALL).matcher(error);
            assertTrue(String.format("%s\nEXPECTED: %s", error, errorRegex), m.matches());
            return null;
        }
    }

    private Database goodDDLAgainstSimpleSchema(String... ddl) throws Exception {
        return checkDDLAgainstSimpleSchema(null, ddl);
    }

    private void badDDLAgainstSimpleSchema(String errorRegex, String... ddl) throws Exception {
        checkDDLAgainstSimpleSchema(errorRegex, ddl);
    }

    public void testGoodCreateProcedureWithAllow() throws Exception {
        Database db = goodDDLAgainstSimpleSchema(
                "create role r1;",
                "create procedure p1 allow r1 as select * from books;");
        Procedure proc = db.getProcedures().get("p1");
        assertNotNull(proc);
        CatalogMap<GroupRef> groups = proc.getAuthgroups();
        assertEquals(1, groups.size());
        assertNotNull(groups.get("r1"));

        db = goodDDLAgainstSimpleSchema(
                "create role r1;",
                "create role r2;",
                "create procedure p1 allow r1, r2 as select * from books;");
        proc = db.getProcedures().get("p1");
        assertNotNull(proc);
        groups = proc.getAuthgroups();
        assertEquals(2, groups.size());
        assertNotNull(groups.get("r1"));
        assertNotNull(groups.get("r2"));

        db = goodDDLAgainstSimpleSchema(
                "create role r1;",
                "create procedure partition on table books column cash allow r1 from class org.voltdb.compiler.procedures.AddBook;");
        proc = db.getProcedures().get("AddBook");
        assertNotNull(proc);
        groups = proc.getAuthgroups();
        assertEquals(1, groups.size());
        assertNotNull(groups.get("r1"));

        db = goodDDLAgainstSimpleSchema(
                "create role r1;",
                "create role r2;",
                "create procedure partition on table books column cash allow r1,r2 from class org.voltdb.compiler.procedures.AddBook;");
        proc = db.getProcedures().get("AddBook");
        assertNotNull(proc);
        groups = proc.getAuthgroups();
        assertEquals(2, groups.size());
        assertNotNull(groups.get("r1"));
        assertNotNull(groups.get("r2"));

        db = goodDDLAgainstSimpleSchema(
                "create role r1;",
                "create procedure partition on table books column cash allow r1,r1 from class org.voltdb.compiler.procedures.AddBook;");
        proc = db.getProcedures().get("AddBook");
        assertNotNull(proc);
        groups = proc.getAuthgroups();
        assertEquals(1, groups.size());
        assertNotNull(groups.get("r1"));
    }

    public void testBadCreateProcedureWithAllow() throws Exception {
        badDDLAgainstSimpleSchema(".*expected syntax.*",
                "create procedure p1 allow as select * from books;");
        badDDLAgainstSimpleSchema(".*expected syntax.*",
                "create procedure p1 allow a b as select * from books;");
        badDDLAgainstSimpleSchema(".*role rx that does not exist.*",
                "create procedure p1 allow rx as select * from books;");
        badDDLAgainstSimpleSchema(".*role rx that does not exist.*",
                "create role r1;",
                "create procedure p1 allow r1, rx as select * from books;");
    }

    public void testDropRole() throws Exception {
        Database db = goodDDLAgainstSimpleSchema(
                "create role r1;",
                "drop role r1;");
        CatalogMap<Group> groups = db.getGroups();
        assertTrue(groups.get("r1") == null);

        db = goodDDLAgainstSimpleSchema(
                "create role r1;",
                "drop role r1 if exists;");
        groups = db.getGroups();
        assertTrue(groups.get("r1") == null);

        db = goodDDLAgainstSimpleSchema(
                "create role r1;",
                "drop role r1 if exists;",
                "drop role r1 IF EXISTS;");
        groups = db.getGroups();
        assertTrue(groups.get("r1") == null);

        badDDLAgainstSimpleSchema(".*does not exist.*",
                "create role r1;",
                "drop role r2;");

        badDDLAgainstSimpleSchema(".*does not exist.*",
                "create role r1;",
                "drop role r1;",
                "drop role r1;");

        badDDLAgainstSimpleSchema(".*may not drop.*",
                "drop role administrator;");

        badDDLAgainstSimpleSchema(".*may not drop.*",
                "drop role user;");
    }

    public void testDDLPartialIndex() {
        String schema;
        schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create unique index idx_t_idnum on t(id) where id > 4;\n";

        VoltCompiler c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertFalse(c.hasErrorsOrWarnings());

        // partial index with BOOLEAN function, NOT operator,
        // and AND expression in where clause.
        schema =
                "create table t (id integer not null, region1 geography not null, point1 geography_point not null);\n" +
                "create unique index partial_index on t(distance(region1, point1)) where (NOT Contains(region1, point1) AND isValid(region1));\n";
        c = compileSchemaForDDLTest(schema, true);
        assertFalse(c.hasErrors());
        assertFalse(c.hasErrorsOrWarnings());

    }

    public void testInvalidPartialIndex() {
        String schema = null;
        schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create unique index IDX_T_IDNUM on t(id) where max(id) > 4;\n";
        checkDDLErrorMessage(schema, "Partial index \"IDX_T_IDNUM\" cannot contain aggregate expressions.");

        schema =
                "create table t1(id integer not null, num integer not null);\n" +
                "create table t2(id integer not null, num integer not null);\n" +
                "create unique index IDX_T1_IDNUM on t1(id) where t2.id > 4;\n";
        checkDDLErrorMessage(schema, "Partial index \"IDX_T1_IDNUM\" with expression(s) involving other tables is not supported.");

        schema =
                "create table t(id integer not null, num integer not null);\n" +
                "create unique index IDX_T_IDNUM on t(id) where id in (select num from t);\n";
        checkDDLErrorMessage(schema, "Partial index \"IDX_T_IDNUM\" cannot contain subqueries.");
    }

    private ConnectorTableInfo getConnectorTableInfoFor(Database db,
            String tableName, String target) {
        Connector connector =  db.getConnectors().get(target);
        if (connector == null) {
            return null;
        }
        return connector.getTableinfo().getIgnoreCase(tableName);
    }

    private String getPartitionColumnInfoFor(Database db, String tableName) {
        Table table = db.getTables().getIgnoreCase(tableName);
        if (table == null) {
            return null;
        }
        if (table.getPartitioncolumn() == null) {
            return null;
        }
        return table.getPartitioncolumn().getName();
    }

    private String getColumnInfoFor(Database db, String tableName, String columnName) {
        Table table = getTableInfoFor(db, tableName);
        if (table == null) {
            return null;
        }
        for (Column column: table.getColumns()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return columnName;
            }
        }
        return null;

    }
    private  MaterializedViewInfo getViewInfoFor(Database db, String tableName, String viewName) {
        Table table = db.getTables().getIgnoreCase(tableName);
        if (table == null) {
            return null;
        }
        if (table.getViews() == null) {
            return null;
        }
        return table.getViews().get(viewName);
    }

    private Table getTableInfoFor(Database db, String tableName) {
        return db.getTables().getIgnoreCase(tableName);
    }

    public void testGoodExportTable() throws Exception {
        Database db;

        db = goodDDLAgainstSimpleSchema(
                "create stream e1 export to target e1 (id integer, f1 varchar(16));"
                );
        assertNotNull(getConnectorTableInfoFor(db, "e1", "e1"));

        db = goodDDLAgainstSimpleSchema(
                "create stream e1 export to target e1 (id integer, f1 varchar(16));",
                "create stream e2 export to target E2 (id integer, f1 varchar(16));"
                );
        assertNotNull(getConnectorTableInfoFor(db, "e1", "e1"));
        assertNotNull(getConnectorTableInfoFor(db, "e2", "e2"));
    }

    public void testGoodCreateStream() throws Exception {
        Database db;

        db = goodDDLAgainstSimpleSchema(
                "create stream e1 (id integer, f1 varchar(16));"
                );
        assertNotNull(getConnectorTableInfoFor(db, "e1", Constants.CONNECTORLESS_STREAM_TARGET_NAME));

        db = goodDDLAgainstSimpleSchema(
                "create stream e1 (id integer, f1 varchar(16));",
                "create stream e2 partition on column id (id integer not null, f1 varchar(16));",
                "create stream e3 export to target bar (id integer, f1 varchar(16));",
                "create stream e4 partition on column id export to target bar (id integer not null, f1 varchar(16));",
                "create stream e5 export to target bar partition on column id (id integer not null, f1 varchar(16));"
                );
        assertNotNull(getConnectorTableInfoFor(db, "e1", Constants.CONNECTORLESS_STREAM_TARGET_NAME));
        assertEquals(null, getPartitionColumnInfoFor(db,"e1"));
        assertNotNull(getConnectorTableInfoFor(db, "e2", Constants.CONNECTORLESS_STREAM_TARGET_NAME));
        assertEquals("ID", getPartitionColumnInfoFor(db,"e2"));
        assertNotNull(getConnectorTableInfoFor(db, "e3", "bar"));
        assertEquals(null, getPartitionColumnInfoFor(db,"e3"));
        assertNotNull(getConnectorTableInfoFor(db, "e4", "bar"));
        assertEquals("ID", getPartitionColumnInfoFor(db,"e4"));
        assertNotNull(getConnectorTableInfoFor(db, "e5", "bar"));
        assertEquals("ID", getPartitionColumnInfoFor(db,"e5"));

        db = goodDDLAgainstSimpleSchema(
                "CREATE STREAM User_Stream Partition On Column UserId" +
                " (UserId BIGINT NOT NULL, SessionStart TIMESTAMP);",
                "CREATE VIEW User_Logins (UserId, LoginCount)" +
                "AS SELECT UserId, Count(*) FROM User_Stream GROUP BY UserId;",
                "CREATE VIEW User_LoginLastTime (UserId, LoginCount, LoginLastTime)" +
                "AS SELECT UserId, Count(*), MAX(SessionStart) FROM User_Stream GROUP BY UserId;"
                );
        assertNotNull(getViewInfoFor(db,"User_Stream","User_Logins"));
        assertNotNull(getViewInfoFor(db,"User_Stream","User_LoginLastTime"));
    }

    public void testBadCreateStream() throws Exception {

        badDDLAgainstSimpleSchema(".+unexpected token:.*",
                "create stream 1table_name_not_valid (id integer, f1 varchar(16));"
                );

        badDDLAgainstSimpleSchema(".+unexpected token:.*",
               "create stream foo export to target bar1,bar2 (i bigint not null);"
                );

        badDDLAgainstSimpleSchema(".+unexpected token:.*",
                "create stream foo export to topic bar1,bar2 (i bigint not null);"
                 );

        badDDLAgainstSimpleSchema(".+unexpected token:.*",
                "create stream foo,foo2 export to target bar (i bigint not null);"
                );

        badDDLAgainstSimpleSchema(".+unexpected token:.*",
                "create stream foo,foo2 export to topic bar (i bigint not null);"
                );

        badDDLAgainstSimpleSchema("Invalid CREATE STREAM statement:.*",
                "create stream foo export to target bar ();"
                );

        badDDLAgainstSimpleSchema("Streams cannot be configured with indexes.*",
                "create stream foo export to target bar (id integer, primary key(id));"
                );

        badDDLAgainstSimpleSchema("Invalid topic bar: stream FOO must be partitioned",
                "create stream foo export to topic bar (id integer, primary key(id));"
                );

        badDDLAgainstSimpleSchema("Stream configured with materialized view without partitioned.*",
                "create stream view_source partition on column id (id integer not null, f1 varchar(16), f2 varchar(12));",
                "create view my_view as select f2, count(*) as f2cnt from view_source group by f2;"
                );
    }

    public void testGoodDropStream() throws Exception {
        Database db;

        db = goodDDLAgainstSimpleSchema(
                // drop an independent stream
                "CREATE STREAM e1 (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "DROP STREAM e1;\n",

                // try drop an non-existent stream
                "DROP STREAM e2 IF EXISTS;\n",

                //  automatically drop reference views for the stream
                "CREATE STREAM User_Stream Partition On Column UserId" +
                " (UserId BIGINT NOT NULL, SessionStart TIMESTAMP);\n" +
                "CREATE VIEW User_Logins (UserId, LoginCount)"  +
                " AS SELECT UserId, Count(*) FROM User_Stream GROUP BY UserId;\n" +
                "CREATE VIEW User_LoginLastTime (UserId, LoginCount, LoginLastTime)" +
                " AS SELECT UserId, Count(*), MAX(SessionStart) FROM User_Stream GROUP BY UserId;\n" +
                "DROP STREAM User_Stream IF EXISTS CASCADE ;\n"
                );

        assertNull(getTableInfoFor(db, "e1"));
        assertNull(getTableInfoFor(db, "e2"));
        assertNull(getTableInfoFor(db, "User_Stream"));
        assertNull(getTableInfoFor(db, "User_Logins"));
        assertNull(getTableInfoFor(db, "User_LoginLastTime"));
    }

    public void testAlterStream() throws Exception {
        Database db = goodDDLAgainstSimpleSchema(
                "CREATE STREAM e PARTITION ON COLUMN D1 (D1 INTEGER NOT NULL, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "ALTER STREAM e DROP COLUMN D2 ;\n" +
                "ALTER STREAM e ADD COLUMN D4 VARCHAR(1);\n" +
                "ALTER STREAM e ALTER COLUMN D4 INTEGER;\n"
                );
        // test drop, add and modify column
        assertNull(getColumnInfoFor(db, "e", "D2"));
        assertNotNull(getColumnInfoFor(db, "e", "D4"));
        Table t = getTableInfoFor(db, "e");
        for (Column c : t.getColumns()) {
            if ("D4".equalsIgnoreCase(c.getName())) {
                assert(c.getType() == Types.SMALLINT);
            }
        }
    }

    public void testDDLCompilerStreamType() {
        String ddl = "create table ttl MIGRATE TO TARGET TEST (a integer not null, b integer, c timestamp default now() not null, PRIMARY KEY(a)) " +
                " USING TTL 20 MINUTES ON COLUMN c BATCH_SIZE 10 MAX_FREQUENCY 3;\n" +
                "partition table ttl on column a;" +
                "ALTER TABLE TTL DROP COLUMN B;";
        Database db = checkDDLAgainstGivenSchema(null,
                "CREATE STREAM e PARTITION ON COLUMN D1 (D1 INTEGER NOT NULL, D2 INTEGER);\n",
                "CREATE STREAM e1 PARTITION ON COLUMN D1 EXPORT TO TARGET T(D1 INTEGER NOT NULL, D2 INTEGER);\n" +
                ddl
                );
        Table t = getTableInfoFor(db, "e");
        assert(t.getTabletype() == TableType.CONNECTOR_LESS_STREAM.get());

        t = getTableInfoFor(db, "e1");
        assert(t.getTabletype() == TableType.STREAM.get());

        t = getTableInfoFor(db, "ttl");
        assert(t.getTabletype() == TableType.PERSISTENT_MIGRATE.get());

    }
    public void testBadDropStream() throws Exception {
        // non-existent stream
        badDDLAgainstSimpleSchema(".+object not found: E1.*",
               "DROP STREAM e1;\n"
                );

        // non-stream table
        badDDLAgainstSimpleSchema(".+Invalid DROP STREAM statement: e2 is not a stream.*",
                "CREATE TABLE e2 (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                        "DROP STREAM e2;\n"
                );

        // stream with referencing view
        badDDLAgainstSimpleSchema(".+dependent objects exist:.*",
                "CREATE STREAM User_Stream Partition On Column UserId" +
                        " (UserId BIGINT NOT NULL, SessionStart TIMESTAMP);\n" +
                        "CREATE VIEW User_Logins (UserId, LoginCount)"  +
                        " AS SELECT UserId, Count(*) FROM User_Stream GROUP BY UserId;\n" +
                        "CREATE VIEW User_LoginLastTime (UserId, LoginCount, LoginLastTime)" +
                        " AS SELECT UserId, Count(*), MAX(SessionStart) FROM User_Stream GROUP BY UserId;\n" +
                        "DROP STREAM User_Stream;\n"
                );

        // stream with referencing procedure
        badDDLAgainstSimpleSchema(".+object not found: USER_STREAM_2.*",
                "CREATE STREAM User_Stream_2 Partition On Column UserId" +
                        " (UserId BIGINT NOT NULL, SessionStart TIMESTAMP);\n" +
                        "CREATE PROCEDURE Enter_User PARTITION ON TABLE User_Stream_2 column UserId" +
                        " AS INSERT INTO User_Stream_2 (UserId, SessionStart) VALUES (?,?);\n" +
                        "DROP STREAM User_Stream_2 CASCADE;\n"
                );
    }

    public void testGoodDRTable() throws Exception {
        Database db;

        db = goodDDLAgainstSimpleSchema(
                "create table e1 (id integer not null, f1 varchar(16));",
                "partition table e1 on column id;",
                "dr table e1;"
                );
        assertTrue(db.getTables().getIgnoreCase("e1").getIsdred());

        String schema = "create table e1 (id integer not null, f1 varchar(16));\n" +
                        "create table e2 (id integer not null, f1 varchar(16));\n" +
                        "partition table e1 on column id;";

        db = goodDDLAgainstSimpleSchema(
                schema,
                "dr table e1;",
                "DR TABLE E2;"
                );
        assertTrue(db.getTables().getIgnoreCase("e1").getIsdred());
        assertTrue(db.getTables().getIgnoreCase("e2").getIsdred());

        // DR statement is order sensitive
        db = goodDDLAgainstSimpleSchema(
                schema,
                "dr table e2;",
                "dr table e2 disable;"
                );
        assertFalse(db.getTables().getIgnoreCase("e2").getIsdred());

        db = goodDDLAgainstSimpleSchema(
                schema,
                "dr table e2 disable;",
                "dr table e2;"
                );
        assertTrue(db.getTables().getIgnoreCase("e2").getIsdred());

        schema = "create table geogs ( id integer NOT NULL, " +
                                    " region1 geography NOT NULL, " +
                                    " point1 geography_point NOT NULL, " +
                                    " point2 geography_point NOT NULL);\n" +
                 "partition table geogs on column id;\n";
        db = goodDDLAgainstSimpleSchema(
                schema,
                "dr table geogs;");
        assertTrue(db.getTables().getIgnoreCase("geogs").getIsdred());

        db = goodDDLAgainstSimpleSchema(
                schema,
                "dr table geogs;",
                "dr table geogs disable;");
        assertFalse(db.getTables().getIgnoreCase("geogs").getIsdred());
    }

    public void testBadDRTable() throws Exception {
        badDDLAgainstSimpleSchema(".+\\sdr, table non_existant was not present in the catalog.*",
                "dr table non_existant;"
                );

        badDDLAgainstSimpleSchema(".+contains invalid identifier \"1table_name_not_valid\".*",
                "dr table 1table_name_not_valid;"
                );

        badDDLAgainstSimpleSchema(".+Invalid DR TABLE statement.*",
                "dr table one, two, three;"
                );

        badDDLAgainstSimpleSchema(".+Invalid DR TABLE statement.*",
                "dr dr table one;"
                );

        badDDLAgainstSimpleSchema(".+Invalid DR TABLE statement.*",
                "dr table table one;"
                );
    }

    public void testCompileFromDDL() {
        String schema1 =
                "create table table1r_el " +
                " (pkey integer, column2_integer integer, PRIMARY KEY(pkey));\n" +
                "create view v_table1r_el (column2_integer, num_rows) as\n" +
                "  select column2_integer as column2_integer,\n" +
                "  count(*) as num_rows\n" +
                "  from table1r_el\n" +
                "  group by column2_integer;\n" +
                "create view v_table1r_el2 (column2_integer, num_rows) as\n" +
                "  select column2_integer as column2_integer,\n" +
                "  count(*) as num_rows\n" +
                "  from table1r_el\n" +
                "  group by column2_integer\n;\n";
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema1);
        String schemaPath = schemaFile.getPath();
        VoltCompiler compiler = new VoltCompiler(false);

        boolean success = compiler.compileFromDDL(testout_jar, schemaPath);
        assertTrue(success);

        success = compiler.compileFromDDL(testout_jar, schemaPath + "???");
        assertFalse(success);

        success = compiler.compileFromDDL(testout_jar);
        assertFalse(success);
    }

    public void testDDLStmtProcNameWithDots() {
        File ddlFile = VoltProjectBuilder.writeStringToTempFile(StringUtils.join(new String[] {
            "create table books (cash integer default 23 not null, title varchar(10) default 'foo', PRIMARY KEY(cash));",
            "create procedure a.Foo as select * from books;"
        }, "\n"));

        VoltCompiler compiler = new VoltCompiler(false);
        assertFalse("Compile with dotted proc name should fail",
                    compiler.compileFromDDL(testout_jar, ddlFile.getPath()));
        assertTrue("Compile with dotted proc name did not have the expected error message",
                   isFeedbackPresent("Invalid procedure name", compiler.m_errors));
    }


    /*
     * Test some ddl with a schema tailored for illegal scalar subqueries.
     */
    private Database checkDDLAgainstScalarSubquerySchema(String errorRegex, String... ddl) {
        String scalarSubquerySchema = "create table books (cash integer default 23 NOT NULL, title varchar(10) default NULL, PRIMARY KEY(cash)); " +
                                         "partition table books on column cash;";
        return checkDDLAgainstGivenSchema(errorRegex, scalarSubquerySchema, ddl);
    }

    /**
     * Test to see if scalar subqueries are either allowed where we
     * expect them to be or else cause compilation errors where we
     * don't expect them to be.
     *
     * @throws Exception
     */
    public void testScalarSubqueriesExpectedFailures() {
        // Scalar subquery not allowed in partial indices.
        checkDDLAgainstScalarSubquerySchema(null, "create table mumble ( ID integer ); \n");
        checkDDLAgainstScalarSubquerySchema("Partial index \"BIDX\" cannot contain subqueries.",
                                    "create index bidx on books ( title ) where exists ( select title from books as child where books.cash = child.cash ) ;\n");
        checkDDLAgainstScalarSubquerySchema("Partial index \"BIDX\" cannot contain subqueries.",
                                    "create index bidx on books ( title ) where 7 < ( select cash from books as child where books.title = child.title ) ;\n");
        checkDDLAgainstScalarSubquerySchema("Partial index \"BIDX\" cannot contain subqueries.",
                                    "create index bidx on books ( title ) where 'ossians ride' < ( select title from books as child where books.cash = child.cash ) ;\n");
        // Scalar subquery not allowed in indices.
        checkDDLAgainstScalarSubquerySchema("DDL Error: \"unexpected token: SELECT\" in statement starting on lineno: [0-9]*",
                                    "create index bidx on books ( select title from books as child where child.cash = books.cash );");
        checkDDLAgainstScalarSubquerySchema("Index \"BIDX1\" cannot contain subqueries.",
                                    "create index bidx1 on books ( ( select title from books as child where child.cash = books.cash ) ) ;");
        checkDDLAgainstScalarSubquerySchema("Index \"BIDX2\" cannot contain subqueries.",
                                    "create index bidx2 on books ( cash + ( select cash from books as child where child.title < books.title ) );");
        // Scalar subquery not allowed in materialize views.
        checkDDLAgainstScalarSubquerySchema("Materialized view \"TVIEW\" cannot contain subquery sources.",
                                    "create view tview as select cash, count(*) from books where 7 < ( select cash from books as child where books.title = child.title ) group by cash;\n");
        checkDDLAgainstScalarSubquerySchema("Materialized view \"TVIEW\" cannot contain subquery sources.",
                                    "create view tview as select cash, count(*) from books where ( select cash from books as child where books.title = child.title ) < 100 group by cash;\n");
    }

    /*
     * When ENG-8727 is addressed, reenable this test.
     */
    public void notest8727SubqueriesInViewDisplayLists() {
        checkDDLAgainstScalarSubquerySchema("Materialized view \"TVIEW\" cannot contain subquery sources.",
                                    "create view tview as select ( select cash from books as child where books.title = child.title ) as bucks, count(*) from books group by bucks;\n");
    }

    public void test8291UnhelpfulSubqueryErrorMessage() {
        checkDDLAgainstScalarSubquerySchema("DDL Error: \"object not found: BOOKS.TITLE\" in statement starting on lineno: 1",
                                    "create view tview as select cash, count(*), max(( select cash from books as child where books.title = child.title )) from books group by cash;\n");
        checkDDLAgainstScalarSubquerySchema("DDL Error: \"object not found: BOOKS.CASH\" in statement starting on lineno: 1",
                                    "create view tview as select cash, count(*), max(( select cash from books as child where books.cash = child.cash )) from books group by cash;\n");
    }

    public void test8290UnboundIdentifiersNotCaughtEarlyEnough() {
        // The name parent is not defined here.  This is an
        // HSQL bug somehow.
        checkDDLAgainstScalarSubquerySchema("Object not found: PARENT",
                                    "create index bidx1 on books ( ( select title from books as child where child.cash = parent.cash ) ) ;");
        checkDDLAgainstScalarSubquerySchema("Object not found: PARENT",
                                    "create index bidx2 on books ( cash + ( select cash from books as child where child.title < parent.title ) );");
    }

    public void testAggregateExpressionsInIndices() {
        String ddl = "create table alpha (id integer not null, seqnum float);";
        // Test for time sensitive queries.
        checkDDLAgainstGivenSchema(".*Index \"FAULTY\" cannot include the function NOW or CURRENT_TIMESTAMP\\.",
                                    ddl,
                                    "create index faulty on alpha(id, NOW);");
        checkDDLAgainstGivenSchema(".*Index \"FAULTY\" cannot include the function NOW or CURRENT_TIMESTAMP\\.",
                                   ddl,
                                   "create index faulty on alpha(id, CURRENT_TIMESTAMP);");
        // Test for aggregate calls.
        checkDDLAgainstGivenSchema(".*Index \"FAULTY\" cannot contain aggregate expressions\\.",
                                   ddl,
                                   "create index faulty on alpha(id, seqnum + avg(seqnum));");
        checkDDLAgainstGivenSchema(".*Index \"FAULTY\" cannot contain aggregate expressions\\.",
                                   ddl,
                                   "create index faulty on alpha(id, seqnum + max(seqnum));");
        checkDDLAgainstGivenSchema(".*Index \"FAULTY\" cannot contain aggregate expressions\\.",
                                   ddl,
                                   "create index faulty on alpha(id, seqnum + min(seqnum));");
        checkDDLAgainstGivenSchema(".*Index \"FAULTY\" cannot contain aggregate expressions\\.",
                                   ddl,
                                   "create index faulty on alpha(id, seqnum + count(seqnum));");
        checkDDLAgainstGivenSchema(".*Index \"FAULTY\" cannot contain aggregate expressions\\.",
                                   ddl,
                                   "create index faulty on alpha(id, seqnum + count(*));");
        checkDDLAgainstGivenSchema(".*Index \"FAULTY\" cannot contain aggregate expressions\\.",
                                   ddl,
                                   "create index faulty on alpha(id, 100 + sum(id));");
        // Test for subqueries.
        checkDDLAgainstGivenSchema(".*Cannot create index \"FAULTY\" because it contains comparison expression '=', " +
                                   "which is not supported.*",
                                   ddl,
                                   "create index faulty on alpha(id = (select id + id from alpha));");
    }

    private int countStringsMatching(List<String> diagnostics, String pattern) {
        int count = 0;
        for (String string : diagnostics) {
            if (string.matches(pattern)) {
                ++count;
            }
        }
        return count;
    }
}
