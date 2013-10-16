/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

public class TestDDLCompiler extends TestCase {

    public void testSimpleDDLCompiler() throws HSQLParseException {
        String ddl1 =
            "CREATE TABLE \"warehouse\" ( " +
            "\"w_id\" integer default '0' NOT NULL, " +
            "\"w_name\" varchar(16) default NULL, " +
            "\"w_street_1\" varchar(32) default NULL, " +
            "\"w_street_2\" varchar(32) default NULL, " +
            "\"w_city\" varchar(32) default NULL, " +
            "\"w_state\" varchar(2) default NULL, " +
            "\"w_zip\" varchar(9) default NULL, " +
            "\"w_tax\" float default NULL, " +
            "PRIMARY KEY  (\"w_id\") " +
            ");";

        HSQLInterface hsql = HSQLInterface.loadHsqldb();

        hsql.runDDLCommand(ddl1);

        VoltXMLElement xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);

    }

    public void testCharIsNotAllowed() {
        String ddl1 =
            "CREATE TABLE \"warehouse\" ( " +
            "\"w_street_1\" char(32) default NULL, " +
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
    // ENG-4865
    // @throws HSQLParseException
    //
    public void testUniqueIndexGiveWarnings() throws HSQLParseException {
        // ensure the test cleans up
        File jarOut = new File("checkCompilerWarnings.jar");
        jarOut.deleteOnExit();

        // boilerplate for making a project
        final String simpleProject =
                "<?xml version=\"1.0\"?>\n" +
                "<project><database><schemas>" +
                "<schema path='%s' />" +
                "</schemas></database></project>";

        // A unique index on the non-primary key for replicated table gets no warning.
        String schema0 = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "CREATE UNIQUE INDEX user_index0 ON t0 (name) ;";

        // A unique index on the partitioning key ( no primary key) gets no warning.
        String schema1 = "create table t0 (id bigint not null, name varchar(32) not null, age integer);\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index1 ON t0 (id) ;";

        // A unique index on the partitioning key ( also primary key) gets no warning.
        String schema2 = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index2 ON t0 (id) ;";

        // A unique compound index on the partitioning key and another column gets no warning.
        String schema3 = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index3 ON t0 (id, age) ;";

        // A unique index on the partitioning key and an expression like abs(age) gets no warning.
        String schema4 = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index4 ON t0 (id, abs(age)) ;";


        // A unique index on the partitioning key ( non-primary key) gets one warning.
        String schema6 = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN name;\n" +
                "CREATE UNIQUE INDEX user_index6 ON t0 (name) ;";

        // A unique index on the partitioning key ( no primary key) gets one warning.
        String schema7 = "create table t0 (id bigint not null, name varchar(32) not null, age integer);\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index7 ON t0 (name) ;";

        // A unique index on the non-partitioning key gets one warning.
        String schema8 = "create table t0 (id bigint not null, name varchar(32), age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index8 ON t0 (name) ;";

        // A unique index on an unrelated expression like abs(age) gets a warning.
        String schema9 = "create table t0 (id bigint not null, name varchar(32), age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN id;\n" +
                "CREATE UNIQUE INDEX user_index9 ON t0 (abs(age)) ;";


        // A unique index on an expression of the partitioning key like substr(1, 2, name) gets two warnings.
        String schema10 = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN name;\n" +
                "CREATE UNIQUE INDEX user_index10 ON t0 (substr(name, 1, 2 )) ;";

        // A unique index on the non-partitioning key, non-partitioned column gets two warnings.
        String schema12 = "create table t0 (id bigint not null, name varchar(32) not null, age integer,  primary key (id));\n" +
                "PARTITION TABLE t0 ON COLUMN name;\n" +
                "CREATE UNIQUE INDEX user_index12 ON t0 (age) ;";


        String[] schemas = {schema0, schema1, schema2, schema3, schema4, schema6, schema7, schema8, schema9, schema10, schema12};
        int[] expected =   {   0   ,    0   ,    0   ,    0   ,    0   ,    1   ,    1   ,    1   ,     1,      2   ,     2    };

        for (int i =0; i< schemas.length; i++) {
            String schema = schemas[i];
            // RUN EXPECTING WARNINGS
            File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
            String schemaPath = schemaFile.getPath();

            File projectFile = VoltProjectBuilder.writeStringToTempFile(
                    String.format(simpleProject, schemaPath));
            String projectPath = projectFile.getPath();

            // compile successfully (but with a warning hopefully)
            VoltCompiler compiler = new VoltCompiler();
            boolean success = compiler.compileWithProjectXML(projectPath, jarOut.getPath());
            assertTrue(success);

            // verify the warnings exist
            int foundWarnings = 0;
            for (VoltCompiler.Feedback f : compiler.m_warnings) {
                if (f.message.toLowerCase().contains("does not include the partitioning column")) {
                    foundWarnings++;
                }
            }
            assertEquals(expected[i], foundWarnings);
            // cleanup after the test
            jarOut.delete();
        }
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
        return compiler.compileFromDDL(jarOut.getPath(), schemaFile.getPath());
    }

    public void testExtraClasses() {
        assertFalse(checkImportValidity("org.1oltdb.**"));
        assertTrue(checkImportValidity("org.voltdb.V**"));
        assertFalse(checkImportValidity("$.1oltdb.**"));
        assertFalse(checkImportValidity("org.voltdb.** org.bolt"));
        assertTrue(checkImportValidity("org.voltdb.V*"));
        assertTrue(checkImportValidity("你rg.voltdb.V*"));
        assertTrue(checkImportValidity("org.我不爱你.V*"));
        assertFalse(checkImportValidity("org.1我不爱你.V*"));
        assertTrue(checkImportValidity("org"));
        assertTrue(checkImportValidity("*org"));
        assertTrue(checkImportValidity("org.**.RealVoltDB"));
        assertTrue(checkImportValidity("org.vol*db.RealVoltDB"));
    }

    public void testIndexedMinMaxViews() {
        File jarOut = new File("indexedMinMaxViews.jar");
        jarOut.deleteOnExit();

        // no indexes (should produce warnings)
        String schema1 =
                "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T " +
                "GROUP BY D1, D2, D3;\n" +
                "CREATE VIEW VT2 (V_D1_D2, V_D3, CNT, MIN_VAL1, SUM_VAL2, MAX_VAL3) " +
                "AS SELECT D1 + D2, ABS(D3), COUNT(*), MIN(VAL1), SUM(VAL2), MAX(VAL3) " +
                "FROM T " +
                "GROUP BY D1 + D2, ABS(D3);";

        // schema with indexes (should have no warnings)
        String schema2 =
               "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
               "CREATE INDEX T_TREE_1 ON T(D1);\n" +
               "CREATE INDEX T_TREE_2 ON T(D1, D2);\n" +
               "CREATE INDEX T_TREE_3 ON T(D1 + D2, ABS(D3));\n" +
               "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
               "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
               "FROM T " +
               "GROUP BY D1, D2, D3;\n" +
               "CREATE VIEW VT2 (V_D1_D2, V_D3, CNT, MIN_VAL1, SUM_VAL2, MAX_VAL3) " +
               "AS SELECT D1 + D2, ABS(D3), COUNT(*), MIN(VAL1), SUM(VAL2), MAX(VAL3) " +
               "FROM T " +
               "GROUP BY D1 + D2, ABS(D3);";

        // schema with no indexes and mat view with no min / max
        String schema3 =
               "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
               "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT) " +
               "AS SELECT D1, D2, D3, COUNT(*) " +
               "FROM T " +
               "GROUP BY D1, D2, D3;\n" +
               "CREATE VIEW VT2 (V_D1_D2, V_D3, CNT) " +
               "AS SELECT D1 + D2, ABS(D3), COUNT(*) " +
               "FROM T " +
               "GROUP BY D1 + D2, ABS(D3);";

        // schema with index but can not be used for mat view with min / max
        String schema4 =
                "CREATE TABLE T (D1 INTEGER, D2 INTEGER, D3 INTEGER, VAL1 INTEGER, VAL2 INTEGER, VAL3 INTEGER);\n" +
                "CREATE INDEX T_TREE_1 ON T(D1, D2 + D3);\n" +
                "CREATE VIEW VT1 (V_D1, V_D2, V_D3, CNT, MIN_VAL1_VAL2, MAX_ABS_VAL3) " +
                "AS SELECT D1, D2, D3, COUNT(*), MIN(VAL1 + VAL2), MAX(ABS(VAL3)) " +
                "FROM T " +
                "GROUP BY D1, D2, D3;" ;

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
        int foundWarnings = 0;
        for (VoltCompiler.Feedback f : compiler.m_warnings) {
            if (f.message.toLowerCase().contains("min")) {
                System.out.println(f.message);
                foundWarnings++;
            }
        }
        assertEquals(2, foundWarnings);

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

        // RUN EXPECTING NO WARNINGS
        schemaFile = VoltProjectBuilder.writeStringToTempFile(schema3);
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

        // RUN EXPECTING WARNINGS
        schemaFile = VoltProjectBuilder.writeStringToTempFile(schema4);
        schemaPath = schemaFile.getPath();

        projectFile = VoltProjectBuilder.writeStringToTempFile(
                String.format(simpleProject, schemaPath));
        projectPath = projectFile.getPath();

        // don't reinitialize the compiler to test that it can be re-called
        //compiler = new VoltCompiler();

        // compile successfully with no warnings
        success = compiler.compileWithProjectXML(projectPath, jarOut.getPath());
        assertTrue(success);

        // verify the warnings exist
        foundWarnings = 0;
        for (VoltCompiler.Feedback f : compiler.m_warnings) {
            if (f.message.toLowerCase().contains("min")) {
                System.out.println(f.message);
                foundWarnings++;
            }
        }
        assertEquals(1, foundWarnings);

        // cleanup after the test
        jarOut.delete();
    }
}
