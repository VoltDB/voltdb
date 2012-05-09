/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

        hsql.close();
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

    /**
     * Note, this should succeed as HSQL doesn't have a hard limit
     * on the number of columns. The test in TestVoltCompiler will
     * fail on 1025 columns.
     * @throws HSQLParseException
     */
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

        hsql.close();
    }

    /**
     * Before the fix for ENG-912, the following schema would work:
     *  create table tmc (name varchar(32), user varchar(32));
     * but this wouldn't:
     *  create table tmc (name varchar(32), user varchar(32), primary key (name, user));
     *
     * Changes in HSQL's ParserDQL and ParserBase make this more consistent
     */
    public void testENG_912() throws HSQLParseException {
        String schema = "create table tmc (name varchar(32), user varchar(32), primary key (name, user));";
        HSQLInterface hsql = HSQLInterface.loadHsqldb();

        hsql.runDDLCommand(schema);
        VoltXMLElement xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);

        hsql.close();
    }

    /**
     * Before fixing ENG-2345, the VIEW definition wouldn't compile if it were
     * containing single quote characters.
     */
    public void testENG_2345() throws HSQLParseException {
        String table = "create table tmc (name varchar(32), user varchar(32), primary key (name, user));";
        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        hsql.runDDLCommand(table);

        String view = "create view v (name , user ) as select name , user from tmc where name = 'name';";
        hsql.runDDLCommand(view);

        VoltXMLElement xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);

        hsql.close();
    }

    /**
     * ENG-2643: Ensure VoltDB can compile DDL with check and fk constrants,
     * but warn the user, rather than silently ignoring the stuff VoltDB
     * doesn't support.
     */
    public void testFKsAndChecksGiveWarnings() throws HSQLParseException {
        // ensure the test cleans up
        File jarOut = new File("checkCompilerWarnings.jar");
        jarOut.deleteOnExit();

        // schema with a foreign key constraint and a check constraint
        String schema1 =  "create table t0 (id bigint not null, primary key (id));\n";
               schema1 += "create table t1 (name varchar(32), user varchar(32), " +
                          "id bigint references t0, primary key (name, user), CHECK (id>0));";

        // similar schema with not null and unique constraints (should have no warnings)
        String schema2 =  "create table t0 (id bigint not null, primary key (id));\n";
               //schema2 += "create table t1 (name varchar(32), user varchar(32), " +
               //                  "id bigint, primary key (name, user);";

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
        boolean success = compiler.compile(projectPath, jarOut.getPath());
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
        success = compiler.compile(projectPath, jarOut.getPath());
        assertTrue(success);

        // verify no warnings
        assertEquals(0, compiler.m_warnings.size());

        // cleanup after the test
        jarOut.delete();
    }
}
