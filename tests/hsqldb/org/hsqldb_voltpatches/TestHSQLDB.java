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

package org.hsqldb_voltpatches;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;

import org.apache.commons.lang3.tuple.Pair;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.voltdb.planner.ParameterizationInfo;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import junit.framework.TestCase;

public class TestHSQLDB extends TestCase {
    class VoltErrorHandler implements ErrorHandler {

        @Override
        public void error(SAXParseException exception) throws SAXException {
            throw exception;
        }

        @Override
        public void fatalError(SAXParseException exception) throws SAXException {
        }

        @Override
        public void warning(SAXParseException exception) throws SAXException {
            throw exception;
        }
    }

    public class StringInputStream extends InputStream {
        StringReader sr;
        public StringInputStream(String value) { sr = new StringReader(value); }
        @Override
        public int read() throws IOException { return sr.read(); }
    }

    public void testCatalogRead() {
        HSQLInterface hsql = HSQLInterface.loadHsqldb(ParameterizationInfo.getParamStateManager());

        try {
            hsql.runDDLCommand("create table test (cash integer default 23);");
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
            fail();
        }

        VoltXMLElement xml;
        try {
            xml = hsql.getXMLFromCatalog();
            assertNotNull(xml);
        } catch (HSQLParseException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            fail();
        }

        try {
            xml = hsql.getXMLCompiledStatement("select * from test;");
            assertNotNull(xml);
        } catch (HSQLParseException e) {
            e.printStackTrace();
            fail();
        }

        //* enable to debug */ System.out.println(xml);
    }

    public HSQLInterface setupTPCCDDL() {
        HSQLInterface hsql = HSQLInterface.loadHsqldb(ParameterizationInfo.getParamStateManager());
        URL url = getClass().getResource("hsqltest-ddl.sql");

        try {
            hsql.runDDLFile(URLDecoder.decode(url.getPath(), "UTF-8"));
        } catch (HSQLParseException e) {
            e.printStackTrace();
            fail();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            fail();
        }
        return hsql;
    }

    /*public void testWithTPCCDDL() throws HSQLParseException {
        HSQLInterface hsql = setupTPCCDDL();

        String xmlCatalog = hsql.getXMLFromCatalog();
        //* enable to debug *-/ System.out.println(xmlCatalog);
        StringInputStream xmlStream = new StringInputStream(xmlCatalog);

        Document doc = null;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(true);

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(xmlStream);
            assertNotNull(doc);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }

    public void testDMLFromTPCCNewOrder() {
        HSQLInterface hsql = setupTPCCDDL();

        URL url = getClass().getResource("hsqltest-dml.sql");

        HSQLFileParser.Statement[] stmts = null;
        try {
            String dmlPath = URLDecoder.decode(url.getPath(), "UTF-8");
            stmts = HSQLFileParser.getStatements(dmlPath);
            assertNotNull(stmts);
        } catch (HSQLParseException e) {
            e.printStackTrace();
            fail();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            fail();
        }

        for (HSQLFileParser.Statement stmt : stmts) {
            //* enable to debug *-/ System.out.println(stmt.statement);

            String xml = null;
            try {
                xml = hsql.getXMLCompiledStatement(stmt.statement);
            } catch (HSQLParseException e1) {
                e1.printStackTrace();
            }
            assertNotNull(xml);

            //* enable to debug *-/ System.out.println(xml);

            StringInputStream xmlStream = new StringInputStream(xml);

            Document doc = null;
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setValidating(true);

            try {
                DocumentBuilder builder = factory.newDocumentBuilder();
                builder.setErrorHandler(new VoltErrorHandler());
                doc = builder.parse(xmlStream);
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
            assertNotNull(doc);
        }

    }*/

    private static void expectFailStmt(HSQLInterface hsql, String stmt, String errorPart) {
        try {
            VoltXMLElement xml = hsql.getXMLCompiledStatement(stmt);
            System.out.println(xml.toString());
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains(errorPart));
        }
    }

    public void testSqlInToXML() throws HSQLParseException {
        HSQLInterface hsql = setupTPCCDDL();
        VoltXMLElement stmt;

        // The next few statements should work, also with a trivial test

        stmt = hsql.getXMLCompiledStatement("select * from new_order");
        assertTrue(stmt.toString().contains("NO_W_ID"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id = 5");
        assertTrue(stmt.toString().contains("equal"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in (5,7);");
        assertTrue(stmt.toString().contains("vector"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in (?);");
        assertTrue(stmt.toString().contains("vector"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in (?,5,3,?);");
        assertTrue(stmt.toString().contains("vector"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id not in (?,5,3,?);");
        assertTrue(stmt.toString().contains("vector"));

        stmt = hsql.getXMLCompiledStatement("select * from warehouse where w_name not in (?, 'foo');");
        assertTrue(stmt.toString().contains("vector"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in (no_d_id, no_o_id, ?, 7);");
        assertTrue(stmt.toString().contains("vector"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in (abs(-1), ?, 17761776);");
        assertTrue(stmt.toString().contains("vector"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in (abs(17761776), ?, 17761776) and no_d_id in (abs(-1), ?, 17761776);");
        assertTrue(stmt.toString().contains("vector"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in ?;");
        assertTrue(stmt.toString().contains("vector"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in (select w_id from warehouse);");
        assertTrue(stmt.toString().contains("tablesubquery"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where exists (select w_id from warehouse);");
        assertTrue(stmt.toString().contains("tablesubquery"));

        stmt = hsql.getXMLCompiledStatement("select * from new_order where not exists (select w_id from warehouse);");
        assertTrue(stmt.toString().contains("tablesubquery"));

        // The ones below here should continue to give sensible errors
        expectFailStmt(hsql, "select * from new_order where no_w_id <> (5, 7, 8);",
                "row column count mismatch");

        // Fixed as ENG-9869 bogus plan when ORDER BY of self-join uses wrong table alias for GROUP BY key
        expectFailStmt(hsql, "select x.no_w_id from new_order x, new_order y group by x.no_w_id order by y.no_w_id;",
                "expression not in aggregate or GROUP BY columns");

    }

    public void testVarbinary() {
        HSQLInterface hsql = HSQLInterface.loadHsqldb(ParameterizationInfo.getParamStateManager());
        URL url = getClass().getResource("hsqltest-varbinaryddl.sql");

        try {
            hsql.runDDLFile(URLDecoder.decode(url.getPath(), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        String sql = "SELECT * FROM BT;";
        VoltXMLElement xml = null;
        try {
            xml = hsql.getXMLCompiledStatement(sql);
            assertNotNull(xml);
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
            fail();
        }
        //* enable to debug */ System.out.println(xml);

        sql = "INSERT INTO BT VALUES (?, ?, ?);";
        xml = null;
        try {
            xml = hsql.getXMLCompiledStatement(sql);
            assertNotNull(xml);
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
            fail();
        }
        //* enable to debug */ System.out.println(xml);
    }

    public void testInsertIntoSelectFrom() {
        HSQLInterface hsql = setupTPCCDDL();

        String sql = "INSERT INTO new_order (NO_O_ID, NO_D_ID, NO_W_ID) SELECT O_ID, O_D_ID+1, CAST(? AS INTEGER) FROM ORDERS;";
        VoltXMLElement xml = null;
        try {
            xml = hsql.getXMLCompiledStatement(sql);
            assertNotNull(xml);
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
            fail();
        }
    }

    public void testSumStarFails() {
        HSQLInterface hsql = HSQLInterface.loadHsqldb(ParameterizationInfo.getParamStateManager());
        assertNotNull(hsql);

        // The elements of this ArrayList tells us the statement to
        // execute, and whether we expect an exception when we execute
        // the statement.  If the first element of the pair begins
        // with the string "Expected", we expect an error.  If
        // the First begins with something else, we don't expect errors.
        // In either case and the string tells what to complain about.
        ArrayList<Pair<String, String>> allddl = new ArrayList<Pair<String, String>>();
        allddl.add(Pair.of("Unexpected Table Creation Failure.", "CREATE TABLE t (i INTEGER, j INTEGER);"));
        allddl.add(Pair.of("Unexpected count(*) call failure.", "CREATE VIEW vw1 (sm) as SELECT count(*) from t group by i;"));
        allddl.add(Pair.of("Expected sum(*) call failure.", "CREATE VIEW vw (sm) AS SELECT sum(*) from t group by i;"));
        for (Pair<String, String> ddl : allddl) {
            boolean sawException = false;
            try {
                hsql.runDDLCommand(ddl.getRight());
            } catch (HSQLParseException e1) {
                sawException = true;
            }
            assertEquals(ddl.getLeft(), ddl.getLeft().startsWith("Expected", 0), sawException);
        }
    }

    /*public void testSimpleSQL() {
        HSQLInterface hsql = setupTPCCDDL();

        String sql = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1;";

        System.out.println(sql);

        String xml = null;
        try {
            xml = hsql.getXMLCompiledStatement(sql);
            assertNotNull(xml);
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
            fail();
        }
        //* enable to debug *-/ System.out.println(xml);

        StringInputStream xmlStream = new StringInputStream(xml);

        Document doc = null;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(true);

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new VoltErrorHandler());
            doc = builder.parse(xmlStream);
            assertNotNull(doc);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

    }*/

    /*public void testDeleteIssue() {
        HSQLInterface hsql = setupTPCCDDL();

        //String stmt = "delete from NEW_ORDER where NO_O_ID = 1;";
        //String stmt = "delete from NEW_ORDER where NO_O_ID = 1 and NO_D_ID = 1 and NO_W_ID = 1;";
        String stmt = "delete from NEW_ORDER where NO_O_ID = 1 and NO_D_ID = 1 and NO_W_ID = 1 and NO_W_ID = 3;";

        String xml = null;
        try {
            xml = hsql.getXMLCompiledStatement(stmt);
            assertNotNull(xml);
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
            fail();
        }
        //* enable to debug *-/ System.out.println(xml);
    }*/

    public static void main(String args[]) {
        //new TestHSQLDB().testWithTPCCDDL();
    }
}
