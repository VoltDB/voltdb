/* This file is part of VoltDB.
 * Copyright (C) 2008 Vertica Systems Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package org.hsqldb;

import java.io.*;
import java.net.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import org.hsqldb.HSQLInterface.HSQLParseException;
import junit.framework.*;

public class TestHSQLDB extends TestCase {
    class VoltErrorHandler implements ErrorHandler {

        public void error(SAXParseException exception) throws SAXException {
            throw exception;
        }

        public void fatalError(SAXParseException exception) throws SAXException {
        }

        public void warning(SAXParseException exception) throws SAXException {
            throw exception;
        }
    }

    public class StringInputStream extends InputStream {
        StringReader sr;
        public StringInputStream(String value) { sr = new StringReader(value); }
        public int read() throws IOException { return sr.read(); }
    }

    /*public void testCatalogRead() {
        String ddl = "create table test (cash integer default 23);";

        HSQLInterface hsql = HSQLInterface.loadHsqldb();

        try {
            hsql.runDDLCommand(ddl);
        } catch (HSQLInterface.HSQLParseException e1) {
            assertFalse(true);
        }

        String xml = hsql.getXMLFromCatalog();

        assertTrue(xml != null);

        String sql = "select * from test;";

        try {
            xml = hsql.getXMLCompiledStatement(sql);
        } catch (HSQLInterface.HSQLParseException e) {
            e.printStackTrace();
        }

        //System.out.println(xml);

        assertTrue(xml != null);
    }*/

    public HSQLInterface setupTPCCDDL() {
        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        URL url = getClass().getResource("hsqltest-ddl.sql");

        try {
            hsql.runDDLFile(URLDecoder.decode(url.getPath(), "UTF-8"));
        } catch (HSQLParseException e) {
            e.printStackTrace();
            return null;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return hsql;
    }

    public void testWithTPCCDDL() throws HSQLParseException {
        HSQLInterface hsql = setupTPCCDDL();
        assertTrue(hsql != null);

        String xmlCatalog = hsql.getXMLFromCatalog();
        System.out.println(xmlCatalog);
        StringInputStream xmlStream = new StringInputStream(xmlCatalog);

        Document doc = null;
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(true);

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(xmlStream);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue(doc != null);
    }

    public void testDMLFromTPCCNewOrder() {
        HSQLInterface hsql = setupTPCCDDL();
        assertFalse(hsql == null);

        URL url = getClass().getResource("hsqltest-dml.sql");

        HSQLFileParser.Statement[] stmts = null;
        try {
            String dmlPath = URLDecoder.decode(url.getPath(), "UTF-8");
            stmts = HSQLFileParser.getStatements(dmlPath);
        } catch (HSQLParseException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        assertFalse(stmts == null);

        for (HSQLFileParser.Statement stmt : stmts) {
            System.out.println(stmt.statement);

            String xml = null;
            try {
                xml = hsql.getXMLCompiledStatement(stmt.statement);
            } catch (HSQLParseException e1) {
                e1.printStackTrace();
            }
            assertFalse(xml == null);

            System.out.println(xml);

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
                //assertTrue(false);
            }
            assertFalse(doc == null);
        }

    }

    /*public void testSimpleSQL() {
        HSQLInterface hsql = setupTPCCDDL();
        assertFalse(hsql == null);

        String sql = "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1;";

        System.out.println(sql);

        String xml = null;
        try {
            xml = hsql.getXMLCompiledStatement(sql);
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
        }
        assertFalse(xml == null);

        System.out.println(xml);

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
            //assertTrue(false);
        }
        assertFalse(doc == null);

    }*/

    /*public void testDeleteIssue() {
        HSQLInterface hsql = setupTPCCDDL();
        assertFalse(hsql == null);

        //String stmt = "delete from NEW_ORDER where NO_O_ID = 1;";
        //String stmt = "delete from NEW_ORDER where NO_O_ID = 1 and NO_D_ID = 1 and NO_W_ID = 1;";
        String stmt = "delete from NEW_ORDER where NO_O_ID = 1 and NO_D_ID = 1 and NO_W_ID = 1 and NO_W_ID = 3;";

        String xml = null;
        try {
            xml = hsql.getXMLCompiledStatement(stmt);
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
        }
        assertFalse(xml == null);

        System.out.println(xml);
    }*/

    public static void main(String args[]) {
        //new TestHSQLDB().testWithTPCCDDL();
    }
}
