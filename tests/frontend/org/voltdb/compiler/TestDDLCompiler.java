/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;

import junit.framework.TestCase;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.DatabaseConfiguration;
import org.voltdb.catalog.IndexRef;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.utils.CatalogUtil;

public class TestDDLCompiler extends TestCase {

    public void testSimpleDDLCompiler() throws HSQLParseException {
        String ddl1 =
            "CREATE TABLE warehouse ( " +
            "w_id integer default '0' NOT NULL, " +
            "w_name varchar(16) default NULL, " +
            "w_street_1 varchar(32) default NULL, " +
            "w_street_2 varchar(32) default NULL, " +
            "w_city varchar(32) default NULL, " +
            "w_state varchar(2) default NULL, " +
            "w_zip varchar(9) default NULL, " +
            "w_tax float default NULL, " +
            "PRIMARY KEY  (w_id) " +
            ");";

        HSQLInterface hsql = HSQLInterface.loadHsqldb();

        hsql.runDDLCommand(ddl1);

        VoltXMLElement xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);

    }

    public void testCharIsNotAllowed() {
        String ddl1 =
            "CREATE TABLE warehouse ( " +
            "w_street_1 char(32) default NULL, " +
            ");";

        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        try {
            hsql.runDDLCommand(ddl1);
        }
        catch (HSQLParseException e) {
            assertTrue(true);
            return;
        }
        fail();
    }

    //
    // Note, this should succeed as HSQL doesn't have a hard limit
    // on the number of columns. The test in TestVoltCompiler will
    // fail on 1025 columns.
    // @throws HSQLParseException
    //
    public void testTooManyColumnTable() throws IOException, HSQLParseException {
        String schemaPath = "";
        URL url = TestVoltCompiler.class.getResource("toowidetable-ddl.sql");
        schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        FileReader fr = new FileReader(new File(schemaPath));
        BufferedReader br = new BufferedReader(fr);
        String ddl1 = "";
        String line;
        while ((line = br.readLine()) != null) {
            ddl1 += line + "\n";
        }

        br.close();
        HSQLInterface hsql = HSQLInterface.loadHsqldb();

        hsql.runDDLCommand(ddl1);

        VoltXMLElement xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);

    }

    //
    // Before the fix for ENG-912, the following schema would work:
    //  create table tmc (name varchar(32), user varchar(32));
    // but this wouldn't:
    //  create table tmc (name varchar(32), user varchar(32), primary key (name, user));
    //
    // Changes in HSQL's ParserDQL and ParserBase make this more consistent
    //
    public void testENG_912() throws HSQLParseException {
        String schema = "create table tmc (name varchar(32), user varchar(32), primary key (name, user));";
        HSQLInterface hsql = HSQLInterface.loadHsqldb();

        hsql.runDDLCommand(schema);
        VoltXMLElement xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);

    }

    //
    // Before fixing ENG-2345, the VIEW definition wouldn't compile if it were
    // containing single quote characters.
    //
    public void testENG_2345() throws HSQLParseException {
        String table = "create table tmc (name varchar(32), user varchar(32), primary key (name, user));";
        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        hsql.runDDLCommand(table);

        String view = "create view v (name , user ) as select name , user from tmc where name = 'name';";
        hsql.runDDLCommand(view);

        VoltXMLElement xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);

    }

    //
    // ENG-2643: Ensure VoltDB can compile DDL with check and fk constrants,
    // but warn the user, rather than silently ignoring the stuff VoltDB
    // doesn't support.
    //
    public void testFKsAndChecksGiveWarnings() throws HSQLParseException {
        // ensure the test cleans up
        File jarOut = new File("checkCompilerWarnings.jar");
        jarOut.deleteOnExit();

        // schema with a foreign key constraint and a check constraint
        String schema1 =  "create table t0 (id bigint not null, primary key (id));\n";
               schema1 += "create table t1 (name varchar(32), username varchar(32), " +
                          "id bigint references t0, primary key (name, username), CHECK (id>0));";

        // similar schema with not null and unique constraints (should have no warnings)
        String schema2 =  "create table t0 (id bigint not null, primary key (id));\n";

        // boilerplate for making a project
        final String simpleProject =
                "<?xml version=\"1.0\"?>\n" +
                "<project><database><schemas>" +
                "<schema path='%s' />" +
                "</schemas></database></project>";

        // RUN EXPECTING WARNINGS
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema1);
        String schemaPath = schemaFile.getPath();

        File projectFile = VoltProjectBuilder.writeStringToTempFile(
                String.format(simpleProject, schemaPath));
        String projectPath = projectFile.getPath();

        // compile successfully (but with two warnings hopefully)
        VoltCompiler compiler = new VoltCompiler();
        boolean success = compiler.compileWithProjectXML(projectPath, jarOut.getPath());
        assertTrue(success);

        // verify the warnings exist
        int foundCheckWarnings = 0;
        int foundFKWarnings = 0;
        for (VoltCompiler.Feedback f : compiler.m_warnings) {
            if (f.message.toLowerCase().contains("check")) {
                foundCheckWarnings++;
            }
            if (f.message.toLowerCase().contains("foreign")) {
                foundFKWarnings++;
            }
        }
        assertEquals(1, foundCheckWarnings);
        assertEquals(1, foundFKWarnings);

        // cleanup after the test
        jarOut.delete();

        // RUN EXPECTING NO WARNINGS
        schemaFile = VoltProjectBuilder.writeStringToTempFile(schema2);
        schemaPath = schemaFile.getPath();

        projectFile = VoltProjectBuilder.writeStringToTempFile(
                String.format(simpleProject, schemaPath));
        projectPath = projectFile.getPath();

        // don't reinitialize the compiler to test that it can be re-called
        //compiler = new VoltCompiler();

        // compile successfully with no warnings
        success = compiler.compileWithProjectXML(projectPath, jarOut.getPath());
        assertTrue(success);

        // verify no warnings
        assertEquals(0, compiler.m_warnings.size());

        // cleanup after the test
        jarOut.delete();
    }

    boolean checkImportValidity(String importStmt) {
        File jarOut = new File("checkImportValidity.jar");
        jarOut.deleteOnExit();

        String schema = String.format("IMPORT CLASS %s;", importStmt);

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
        schemaFile.deleteOnExit();

        // compile and fail on bad import
        VoltCompiler compiler = new VoltCompiler();
        try {
            return compiler.compileFromDDL(jarOut.getPath(), schemaFile.getPath());
        }
        catch (VoltCompilerException e) {
            e.printStackTrace();
            fail();
            return false;
        }
    }

    public void testExtraClasses() {
        assertFalse(checkImportValidity("org.1oltdb.**"));
        assertTrue(checkImportValidity("org.voltdb_testprocs.a**"));
        assertFalse(checkImportValidity("$.1oltdb.**"));
        assertFalse(checkImportValidity("org.voltdb.** org.bolt"));
        assertTrue(checkImportValidity("org.voltdb_testprocs.a*"));
        assertTrue(checkImportValidity("你rg.voltdb_testprocs.a*"));
        assertTrue(checkImportValidity("org.我不爱你.V*"));
        assertFalse(checkImportValidity("org.1我不爱你.V*"));
        assertFalse(checkImportValidity("org"));
        assertTrue(checkImportValidity("org.**.executeSQLMP"));
        assertTrue(checkImportValidity("org.vol*_testprocs.adhoc.executeSQLMP"));
        assertTrue(checkImportValidity("org.voltdb_testprocs.adhoc.executeSQLMP"));
        assertFalse(checkImportValidity("org."));
        assertFalse(checkImportValidity("org.."));
        assertFalse(checkImportValidity("org.v_dt"));
        assertTrue(checkImportValidity("org.voltdb.compiler.dummy_test_underscore"));
    }

    boolean checkMultiDDLImportValidity(String importStmt1, String importStmt2, boolean checkWarn) {
        File jarOut = new File("checkImportValidity.jar");
        jarOut.deleteOnExit();

        String schema1 = String.format("IMPORT CLASS %s;", importStmt1);
        File schemaFile1 = VoltProjectBuilder.writeStringToTempFile(schema1);
        schemaFile1.deleteOnExit();

        String schema2 = String.format("IMPORT CLASS %s;", importStmt2);
        File schemaFile2 = VoltProjectBuilder.writeStringToTempFile(schema2);
        schemaFile2.deleteOnExit();

        // compile and fail on bad import
        VoltCompiler compiler = new VoltCompiler();
        try {
            boolean rslt = compiler.compileFromDDL(jarOut.getPath(), schemaFile1.getPath(), schemaFile2.getPath());
            assertTrue(checkWarn^compiler.m_warnings.isEmpty());
            return rslt;
        }
        catch (VoltCompilerException e) {
            e.printStackTrace();
            fail();
            assertTrue(checkWarn^compiler.m_warnings.isEmpty());
            return false;
        }
    }

    public void testExtraClassesFrom2Ddls() {
        assertTrue(checkMultiDDLImportValidity("org.voltdb_testprocs.a**", "org.voltdb_testprocs.a**", false));
        assertTrue(checkMultiDDLImportValidity("org.woltdb_testprocs.a**", "org.voltdb_testprocs.a**", true));
        assertTrue(checkMultiDDLImportValidity("org.voltdb_testprocs.a**", "org.woltdb_testprocs.a**", true));
        assertTrue(checkMultiDDLImportValidity("org.woltdb_testprocs.*", "org.voltdb_testprocs.a**", true));
        assertTrue(checkMultiDDLImportValidity("org.voltdb_testprocs.a**", "org.woltdb_testprocs.*", true));
        assertFalse(checkMultiDDLImportValidity("org.vol*db_testprocs.adhoc.executeSQLMP", "org.voltdb_testprocs.", false));
        assertTrue(checkMultiDDLImportValidity("org.vol*db_testprocs.adhoc.executeSQLMP", "org.voltdb_testprocs.adhoc.*", false));
        assertFalse(checkMultiDDLImportValidity("org.voltdb_testprocs.adhoc.executeSQLMP", "org.woltdb", false));
        assertTrue(checkMultiDDLImportValidity("org.vol*db_testprocs.adhoc.executeSQLMP", "org.voltdb_testprocs.adhoc.executeSQLMP", false));
        assertTrue(checkMultiDDLImportValidity("org.voltdb_testprocs.adhoc.executeSQLMP", "org.voltdb_testprocs.adhoc.executeSQLMP", false));
    }

    public void testIndexedMinMaxViews() {
        File jarOut = new File("indexedMinMaxViews.jar");
        jarOut.deleteOnExit();

        String schema[] = {
                // no indexes (should produce warnings)
                "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T " +
                "GROUP BY D1, D2, D3;\n" +
                "CREATE VIEW VT2 (V_D1_D2, V_D3, CNT, MIN_VAL1, SUM_VAL2, MAX_VAL3) " +
                "AS SELECT D1 + D2, ABS(D3), COUNT(*), MIN(VAL1), SUM(VAL2), MAX(VAL3) " +
                "FROM T " +
                "GROUP BY D1 + D2, ABS(D3);" +
                "CREATE VIEW VT3 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T WHERE D1 > 3 " +
                "GROUP BY D1, D2, D3;\n" +
                "CREATE VIEW VT4 (V_D1_D2, V_D3, CNT, MIN_VAL1, SUM_VAL2, MAX_VAL3) " +
                "AS SELECT D1 + D2, ABS(D3), COUNT(*), MIN(VAL1), SUM(VAL2), MAX(VAL3) " +
                "FROM T WHERE D1 > 3 " +
                "GROUP BY D1 + D2, ABS(D3);",

                // schema with indexes (should have no warnings)
               "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
               "CREATE INDEX T_TREE_1 ON T(D1);\n" +
               "CREATE INDEX T_TREE_2 ON T(D1, D2);\n" +
               "CREATE INDEX T_TREE_3 ON T(D1+D2, ABS(D3));\n" +
               "CREATE INDEX T_TREE_4 ON T(D1, D2, D3);\n" +
               "CREATE INDEX T_TREE_5 ON T(D1, D2, D3) WHERE D1 > 3;\n" +
               "CREATE INDEX T_TREE_6 ON T(D1+D2, ABS(D3)) WHERE D1 > 3;\n" +
               "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
               "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
               "FROM T " +
               "GROUP BY D1, D2, D3;\n" +
               "CREATE VIEW VT2 (V_D1_D2, V_D3, CNT, MIN_VAL1, SUM_VAL2, MAX_VAL3) " +
               "AS SELECT D1 + D2, ABS(D3), COUNT(*), MIN(VAL1), SUM(VAL2), MAX(VAL3) " +
               "FROM T " +
               "GROUP BY D1 + D2, ABS(D3);" +
               "CREATE VIEW VT3 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
               "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
               "FROM T WHERE D1 > 3 " +
               "GROUP BY D1, D2, D3;\n" +
               "CREATE VIEW VT4 (V_D1_D2, V_D3, CNT, MIN_VAL1, SUM_VAL2, MAX_VAL3) " +
               "AS SELECT D1 + D2, ABS(D3), COUNT(*), MIN(VAL1), SUM(VAL2), MAX(VAL3) " +
               "FROM T WHERE D1 > 3 " +
               "GROUP BY D1 + D2, ABS(D3);",

               // schema with no indexes and mat view with no min / max
               "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
               "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT) " +
               "AS SELECT D1, D2, D3, COUNT(*) " +
               "FROM T " +
               "GROUP BY D1, D2, D3;\n" +
               "CREATE VIEW VT2 (V_D1_D2, V_D3, CNT) " +
               "AS SELECT D1 + D2, ABS(D3), COUNT(*) " +
               "FROM T " +
               "GROUP BY D1 + D2, ABS(D3);",

                // schema with index but can not be used for mat view with min / max
                "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "CREATE INDEX T_TREE_1 ON T(D1, D2 + D3);\n" +
                "CREATE INDEX T_TREE_2 ON T(D1, D2 + D3, D3);\n" +
                "CREATE INDEX T_TREE_3 ON T(D1, D2);\n" +
                "CREATE INDEX T_TREE_4 ON T(D1, D2, D3) WHERE D1 > 0;\n" +
                "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T WHERE D2 > 0 " +
                "GROUP BY D1, D2, D3;\n" +
                "CREATE VIEW VT2 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T " +
                "GROUP BY D1, D2, D3;\n",

                // schemas with index but can not be used for mat view with min / max
                "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "CREATE INDEX T_TREE_1 ON T(D1, D2 + D3);\n" +
                "CREATE INDEX T_TREE_2 ON T(D1, D2 + D3, D3);\n" +
                "CREATE INDEX T_TREE_3 ON T(D1, D2);\n" +
                "CREATE INDEX T_TREE_4 ON T(D1, D2, D3, VAL1);\n" +
                "CREATE INDEX T_TREE_5 ON T(D1, D2, D3, ABS(VAL1));\n" +
                "CREATE INDEX T_TREE_6 ON T(D1, D2-D3);\n" +
                "CREATE INDEX T_TREE_7 ON T(D1, D2-D3, D3, D2);\n" +
                "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T " +
                "GROUP BY D1, D2, D3;\n" +
                "CREATE VIEW VT2 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2-D3, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T " +
                "GROUP BY D1, D2-D3, D3;",

                // schemas with index but not all min/max columns in the view can have a usable index (ENG-8512)
                "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "CREATE INDEX T_TREE_1 ON T(D1, D2 + D3);\n" +
                "CREATE INDEX T_TREE_2 ON T(D1, D2 + D3, D3);\n" +
                "CREATE INDEX T_TREE_3 ON T(D1, D2);\n" +
                "CREATE INDEX T_TREE_4 ON T(D1, D2, D3, VAL1);\n" +
                "CREATE INDEX T_TREE_5 ON T(D1, D2, D3, ABS(VAL1));\n" +
                "CREATE INDEX T_TREE_6 ON T(D1, D2-D3, D3, ABS(VAL3));\n" +
                "CREATE INDEX T_TREE_7 ON T(D1, D2, D3, VAL1 + VAL2);\n" +
                "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T " +
                "GROUP BY D1, D2, D3;\n" +
                "CREATE VIEW VT2 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2-D3, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T " +
                "GROUP BY D1, D2-D3, D3;",
        };

        int expectWarning[] = { 4, 0, 0, 2, 2, 2 };
        // boilerplate for making a project
        final String simpleProject =
                "<?xml version=\"1.0\"?>\n" +
                "<project><database><schemas>" +
                "<schema path='%s' />" +
                "</schemas></database></project>";

        VoltCompiler compiler = new VoltCompiler();
        for (int ii = 0; ii < schema.length; ++ii) {
            File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema[ii]);
            String schemaPath = schemaFile.getPath();

            File projectFile = VoltProjectBuilder.writeStringToTempFile(
                    String.format(simpleProject, schemaPath));
            String projectPath = projectFile.getPath();

            // compile successfully (but with two warnings hopefully)
            boolean success = compiler.compileWithProjectXML(projectPath, jarOut.getPath());
            assertTrue(success);

            // verify the warnings exist
            int foundWarnings = 0;
            for (VoltCompiler.Feedback f : compiler.m_warnings) {
                if (f.message.toLowerCase().contains("min")) {
                    System.out.println(f.message);
                    foundWarnings++;
                }
            }
            if (expectWarning[ii] != foundWarnings) {
                if (expectWarning[ii] > foundWarnings) {
                    System.out.println("Missed expected warning(s) for schema:");
                } else {
                    System.out.println("Unexpected warning(s) for schema:");
                }
                System.out.println(schema[ii]);
            }
            assertEquals(expectWarning[ii], foundWarnings);

            // cleanup after the test
            jarOut.delete();
        }
    }

    private void assertIndexSelectionResult(CatalogMap<IndexRef> indexRefs, String... indexNames) {
        assertEquals(indexRefs.size(), indexNames.length);
        int i = 0;
        for (IndexRef idx : indexRefs) {
            assertEquals(idx.getName(), indexNames[i++]);
        }
    }

    // ENG-6511
    public void testMinMaxViewIndexSelection() {
        File jarOut = new File("minMaxViewIndexSelection.jar");
        jarOut.deleteOnExit();

        // boilerplate for making a project
        final String simpleProject =
                "<?xml version=\"1.0\"?>\n" +
                "<project><database><schemas>" +
                "<schema path='%s' />" +
                "</schemas></database></project>";
        String schema =

                "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "CREATE INDEX T_TREE_01 ON T(      D1,       D2                         );\n" +
                "CREATE INDEX T_TREE_02 ON T(      D1,       D2,       VAL1             );\n" +
                "CREATE INDEX T_TREE_03 ON T(      D1,       D2,  VAL1+VAL2             ) WHERE D1 > 3;\n" +
                "CREATE INDEX T_TREE_04 ON T(      D1,       D2,         D3             );\n" +
                "CREATE INDEX T_TREE_05 ON T(      D1,       D2,         D3,  VAL1+VAL2 );\n" +
                "CREATE INDEX T_TREE_06 ON T(      D1,       D2,         D3,  VAL1+VAL2 ) WHERE D2 > 4;\n" +
                "CREATE INDEX T_TREE_07 ON T(   D1+D2,  ABS(D3)                         );\n" +
                "CREATE INDEX T_TREE_08 ON T(   D1+D2,  ABS(D3)                         ) WHERE D1 > 3;\n" +
                "CREATE INDEX T_TREE_09 ON T(   D1+D2,  ABS(D3),       VAL1             );\n" +
                "CREATE INDEX T_TREE_10 ON T(   D1+D2                                   );\n" +
                "CREATE INDEX T_TREE_11 ON T( ABS(D3)                                   );\n" +

                // Test no min/max
                "CREATE VIEW VT01 (V_D1, V_D2, CNT, SUM_VAL1_VAL2, COUNT_VAL3) " +           // should have no index for min/max
                "AS SELECT D1, D2, COUNT(*), SUM(VAL1 + VAL2), COUNT(VAL3) " +
                "FROM T " +
                "GROUP BY D1, D2;\n" +

                // Test one single min/max
                "CREATE VIEW VT02 (V_D1, V_D2, V_D3, CNT, MIN_VAL1) " +                      // should choose T_TREE_04
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1) " +
                "FROM T " +
                "GROUP BY D1, D2, D3;\n" +

                // Test repeated min/max, single aggCol
                "CREATE VIEW VT03 (V_D1, V_D2, CNT, MIN_VAL1, MAX_VAL1, MIN_VAL1_DUP) " +    // should choose T_TREE_02, T_TREE_02, T_TREE_02
                "AS SELECT D1, D2, COUNT(*), MIN(VAL1), MAX(VAL1), MIN(VAL1) " +
                "FROM T " +
                "GROUP BY D1, D2;\n" +

                // Test min/max with different aggCols
                "CREATE VIEW VT04 (V_D1, V_D2, CNT, MIN_VAL1, MAX_VAL1, MIN_VAL2) " +        // should choose T_TREE_02, T_TREE_02, T_TREE_01
                "AS SELECT D1, D2, COUNT(*), MIN(VAL1), MAX(VAL1), MIN(VAL2) " +
                "FROM T " +
                "GROUP BY D1, D2;\n" +

                // Test min/max with single arithmetic aggExpr
                "CREATE VIEW VT05 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +   // should choose T_TREE_05, T_TREE_05
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(VAL1 + VAL2) " +
                "FROM T " +
                "GROUP BY D1, D2, D3;\n" +

                // Test min/max with different aggExprs
                "CREATE VIEW VT06 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +   // should choose T_TREE_05, T_TREE_04
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX( ABS(VAL3) ) " +
                "FROM T " +
                "GROUP BY D1, D2, D3;\n" +

                // Test min/max with expression in group-by, single aggCol
                "CREATE VIEW VT07 (V_D1_D2, V_D3, CNT, MIN_VAL1, SUM_VAL2, MAX_VAL3) " +     // should choose T_TREE_09, T_TREE_09
                "AS SELECT D1 + D2, ABS(D3), COUNT(*), MIN(VAL1), SUM(VAL1), MAX(VAL1) " +
                "FROM T " +
                "GROUP BY D1 + D2, ABS(D3);\n" +

                // Test min/max with predicate (partial index)
                "CREATE VIEW VT08 (V_D1, V_D2, CNT, MIN_VAL1_VAL2) " +                       // should choose T_TREE_03
                "AS SELECT D1, D2, COUNT(*), MIN(VAL1 + VAL2)" +
                "FROM T WHERE D1 > 3 " +
                "GROUP BY D1, D2;\n" +

                // Test min/max with predicate, with expression in group-by
                "CREATE VIEW VT09 (V_D1_D2, V_D3, CNT, MIN_VAL1, SUM_VAL2, MAX_VAL3) " +     // should choose T_TREE_09, T_TREE_08
                "AS SELECT D1 + D2, ABS(D3), COUNT(*), MIN(VAL1), SUM(VAL2), MAX(VAL3) " +
                "FROM T WHERE D1 > 3 " +
                "GROUP BY D1 + D2, ABS(D3);\n" +

                "CREATE VIEW VT10 (V_D1, V_D2, CNT, MIN_VAL1, SUM_VAL2, MAX_VAL1) " +        // should choose T_TREE_02, T_TREE_02
                "AS SELECT D1, D2, COUNT(*), MIN(VAL1), SUM(VAL2), MAX(VAL1) " +
                "FROM T " +
                "GROUP BY D1, D2;" +

                // Test min/max with no group by.
                "CREATE VIEW VT11 (CNT, MIN_D1_D2, MAX_ABS_VAL3) " +                         // should choose T_TREE_10, T_TREE_11
                "AS SELECT COUNT(*), MIN(D1+D2), MAX(ABS(D3)) " +
                "FROM T;";

        VoltCompiler compiler = new VoltCompiler();
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
        String schemaPath = schemaFile.getPath();

        File projectFile = VoltProjectBuilder.writeStringToTempFile(
                String.format(simpleProject, schemaPath));
        String projectPath = projectFile.getPath();

        // compile successfully
        boolean success = compiler.compileWithProjectXML(projectPath, jarOut.getPath());
        assertTrue(success);

        CatalogMap<Table> tables = compiler.getCatalogDatabase().getTables();
        Table t = tables.get("T");
        CatalogMap<MaterializedViewInfo> views = t.getViews();
        assertIndexSelectionResult( views.get("VT01").getIndexforminmax() );
        assertIndexSelectionResult( views.get("VT02").getIndexforminmax(), "T_TREE_04" );
        assertIndexSelectionResult( views.get("VT03").getIndexforminmax(), "T_TREE_02", "T_TREE_02", "T_TREE_02" );
        assertIndexSelectionResult( views.get("VT04").getIndexforminmax(), "T_TREE_02", "T_TREE_02", "T_TREE_01" );
        assertIndexSelectionResult( views.get("VT05").getIndexforminmax(), "T_TREE_05", "T_TREE_05" );
        assertIndexSelectionResult( views.get("VT06").getIndexforminmax(), "T_TREE_05", "T_TREE_04" );
        assertIndexSelectionResult( views.get("VT07").getIndexforminmax(), "T_TREE_09", "T_TREE_09" );
        assertIndexSelectionResult( views.get("VT08").getIndexforminmax(), "T_TREE_03" );
        assertIndexSelectionResult( views.get("VT09").getIndexforminmax(), "T_TREE_09", "T_TREE_08" );
        assertIndexSelectionResult( views.get("VT10").getIndexforminmax(), "T_TREE_02", "T_TREE_02" );
        assertIndexSelectionResult( views.get("VT11").getIndexforminmax(), "T_TREE_10", "T_TREE_11" );

        // cleanup after the test
        jarOut.delete();
    }

    public void testExportTables() {
        File jarOut = new File("exportTables.jar");
        jarOut.deleteOnExit();

        String schema[] = {
                // export table w/o group
                "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "EXPORT TABLE T;",

                // export table w/ group
                "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "EXPORT TABLE T TO STREAM FOO;",

                // export table w/ and w/o group
                "CREATE TABLE T (T_D1 INTEGER, T_D2 INTEGER, T_D3 INTEGER, T_VAL1 INTEGER, T_VAL2 INTEGER, T_VAL3 INTEGER);\n" +
                "CREATE TABLE S (S_D1 INTEGER, S_D2 INTEGER, S_D3 INTEGER, S_VAL1 INTEGER, S_VAL2 INTEGER, S_VAL3 INTEGER);\n" +
                "EXPORT TABLE T;\n" +
                "EXPORT TABLE S TO STREAM FOO;"
        };

        VoltCompiler compiler = new VoltCompiler();
        for (int ii = 0; ii < schema.length; ++ii) {
            File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema[ii]);
            String schemaPath = schemaFile.getPath();

            // compile successfully
            boolean success = false;
            try {
                success = compiler.compileFromDDL(jarOut.getPath(), schemaPath);
            }
            catch (Exception e) {
                // do nothing
            }
            assertTrue(success);

            // cleanup after the test
            jarOut.delete();
        }
    }

    public void testExportDRTable() {
        File jarOut = new File("exportDrTables.jar");
        jarOut.deleteOnExit();

        VoltCompiler compiler = new VoltCompiler();
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(
        "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
        "DR TABLE T;\n" +
        "EXPORT TABLE T;");
        String schemaPath = schemaFile.getPath();

        try {
            assertFalse(compiler.compileFromDDL(jarOut.getPath(), schemaPath));
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // cleanup after the test
        jarOut.delete();
    }

    public void testSetDatabaseConfig() {
        File jarOut = new File("setDatabaseConfig.jar");
        jarOut.deleteOnExit();

        VoltCompiler compiler = new VoltCompiler();
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(
        "SET " + DatabaseConfiguration.DR_MODE_NAME + "=" + DatabaseConfiguration.ACTIVE_ACTIVE + ";\n" +
        "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
        "DR TABLE T;");
        String schemaPath = schemaFile.getPath();

        try {
            assertTrue(compiler.compileFromDDL(jarOut.getPath(), schemaPath));
        } catch (Exception e) {
            fail(e.getMessage());
        }

        schemaFile = VoltProjectBuilder.writeStringToTempFile(
        "SET DR_MOD=ACTIVE_ACTIVE;\n" +
        "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
        "DR TABLE T;");
        schemaPath = schemaFile.getPath();

        try {
            assertFalse(compiler.compileFromDDL(jarOut.getPath(), schemaPath));
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // cleanup after the test
        jarOut.delete();
    }

    public void testNullAnnotation() throws IOException {

        Catalog catalog  = new TPCCProjectBuilder().createTPCCSchemaCatalog();
        Database catalog_db = catalog.getClusters().get("cluster").getDatabases().get("database");

        for(Table t : catalog_db.getTables()) {
            assertNotNull(((TableAnnotation)t.getAnnotation()).ddl);
        }
    }

    public void testQuotedNameIsNotAllowed() {
        class Tester {
            HSQLInterface hsql = HSQLInterface.loadHsqldb();
            void testSuccess(String ddl) {
                try {
                    hsql.runDDLCommand(ddl);
                }
                catch (HSQLParseException e) {
                    fail(String.format("Expected DDL to succeed: %s", ddl));
                }
            }
            void testFailure(String ddl) {
                try {
                    hsql.runDDLCommand(ddl);
                }
                catch (HSQLParseException e) {
                    return;
                }
                fail(String.format("Expected DDL to fail: %s", ddl));
            }
        }
        Tester tester = new Tester();
        tester.testFailure("create table \"a_quoted_table_without_spaces\" (an_unquoted_column integer)");
        tester.testFailure("create table \"a quoted table with spaces\" (an_unquoted_column integer)");
        tester.testFailure("create table an_unquoted_table (\"a_quoted_column_without_spaces\" integer)");
        tester.testFailure("create table an_unquoted_table (\"a quoted column with spaces\" integer)");
        tester.testSuccess("create table an_unquoted_table (an_unquoted_column integer)");
    }

    public void testAutogenDRConflictTable() {
        File jarOut = new File("setDatabaseConfig.jar");
        jarOut.deleteOnExit();

        VoltCompiler compiler = new VoltCompiler();
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(
        "SET " + DatabaseConfiguration.DR_MODE_NAME + "=" + DatabaseConfiguration.ACTIVE_ACTIVE + ";\n" +
        "CREATE TABLE T (D1 INTEGER NOT NULL, D2 INTEGER, D3 VARCHAR(32), VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER, PRIMARY KEY (D1), LIMIT PARTITION ROWS 1000);\n" +
        "DR TABLE T;\n" +
        "PARTITION TABLE T ON COLUMN D1;\n");
        String schemaPath = schemaFile.getPath();

        try {
            assertTrue(compiler.compileFromDDL(jarOut.getPath(), schemaPath));
            verifyDRConflictTableSchema(compiler, CatalogUtil.DR_CONFLICTS_PARTITIONED_EXPORT_TABLE, true);
            verifyDRConflictTableSchema(compiler, CatalogUtil.DR_CONFLICTS_REPLICATED_EXPORT_TABLE, false);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        // cleanup after the test
        jarOut.delete();
    }

    private static void verifyDRConflictTableSchema(VoltCompiler compiler, String name, boolean partitioned) {
        Table t = compiler.getCatalogDatabase().getTables().get(name);
        assertNotNull(t);

        if (partitioned) {
            assertNotNull(t.getPartitioncolumn());
        } else {
            assertNull(t.getPartitioncolumn());
        }

        // verify table schema
        assertTrue(t.getColumns().size() == 10);
        Column c1 = t.getColumns().get(DDLCompiler.DR_ROW_TYPE_COLUMN_NAME);
        assertNotNull(c1);
        assertTrue(c1.getType() == VoltType.STRING.getValue());
        Column c2 = t.getColumns().get(DDLCompiler.DR_LOG_ACTION_COLUMN_NAME);
        assertNotNull(c2);
        assertTrue(c2.getType() == VoltType.STRING.getValue());
        Column c3 = t.getColumns().get(DDLCompiler.DR_CONFLICT_COLUMN_NAME);
        assertNotNull(c3);
        assertTrue(c3.getType() == VoltType.STRING.getValue());
        Column c4 = t.getColumns().get(DDLCompiler.DR_CONFLICTS_ON_PK_COLUMN_NAME);
        assertNotNull(c4);
        assertTrue(c4.getType() == VoltType.TINYINT.getValue());
        Column c5 = t.getColumns().get(DDLCompiler.DR_DECISION_COLUMN_NAME);
        assertNotNull(c5);
        assertTrue(c5.getType() == VoltType.STRING.getValue());
        Column c6 = t.getColumns().get(DDLCompiler.DR_CLUSTER_ID_COLUMN_NAME);
        assertNotNull(c6);
        assertTrue(c6.getType() == VoltType.TINYINT.getValue());
        Column c7 = t.getColumns().get(DDLCompiler.DR_TIMESTAMP_COLUMN_NAME);
        assertNotNull(c7);
        assertTrue(c7.getType() == VoltType.BIGINT.getValue());
        Column c8 = t.getColumns().get(DDLCompiler.DR_DIVERGENCE_COLUMN_NAME);
        assertNotNull(8);
        assertTrue(c8.getType() == VoltType.STRING.getValue());
        Column c9 = t.getColumns().get(DDLCompiler.DR_TABLE_NAME_COLUMN_NAME);
        assertNotNull(9);
        assertTrue(c9.getType() == VoltType.STRING.getValue());
        Column c10 = t.getColumns().get(DDLCompiler.DR_TUPLE_COLUMN_NAME);
        assertNotNull(10);
        assertTrue(c10.getType() == VoltType.STRING.getValue());
    }
}
