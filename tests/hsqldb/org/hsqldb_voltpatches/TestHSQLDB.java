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

import junit.framework.TestCase;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.lib.IntKeyHashMap;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

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

    /*public void testWithTPCCDDL() throws HSQLParseException {
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
        assertTrue(hsql != null);
        VoltXMLElement stmt;

        // The next few statements should work, also with a trivial test

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

        stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in (select w_id from warehouse);");
        //???assertTrue(stmt.toString().contains("vector"));
       // not supported yet
        //stmt = hsql.getXMLCompiledStatement("select * from new_order where no_w_id in ?;");
        //assertTrue(stmt.toString().contains("vector"));

        // The ones below here should continue to give sensible errors
        expectFailStmt(hsql, "select * from new_order where no_w_id <> (5, 7, 8);",
                "row column count mismatch");
        expectFailStmt(hsql, "select * from new_order where exists (select w_id from warehouse);",
                "Unsupported subquery");
        expectFailStmt(hsql, "select * from new_order where not exists (select w_id from warehouse);",
                "Unsupported subquery");
    }

    public void testVarbinary() {
        HSQLInterface hsql = HSQLInterface.loadHsqldb();
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
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
        }
        assertFalse(xml == null);
        System.out.println(xml);

        sql = "INSERT INTO BT VALUES (?, ?, ?);";
        xml = null;
        try {
            xml = hsql.getXMLCompiledStatement(sql);
        } catch (HSQLParseException e1) {
            e1.printStackTrace();
        }
        assertFalse(xml == null);
        System.out.println(xml);
    }


    final int MOD = 4;
    int counts[] = new int[MOD];
    public void testIntKeyHashMap() {
        Object x = null;
        IntKeyHashMap map = new IntKeyHashMap();
/*
        map = new IntKeyHashMap();
        // top 0 nulls 0 j 0
        map.put(0, new Integer(2)); //0
        // top 1 nulls 0 j 1
        map.put(1, new Integer(3)); //0
        // top 2 nulls 0 j 2
        map.put(2, new Integer(4)); //0
        // top 3 nulls 0 j 3
        x = map.put(1, new Integer(5)); //2
        // top 3 nulls 0 j 3
        x = map.put(1, new Integer(6)); //2
        // top 3 nulls 0 j 3
        x = map.remove(1); //1
        // top 3 nulls 1 j 2
        x = map.remove(1); //1
        // top 3 nulls 1 j 2
        map.put(2, new Integer(9)); //0
        // top 3 nulls 1 j 3
        
        checkMap(map, 3, 3, -1, -1);
*/
        int j = 0;
        int top = 0;
        boolean check = false;
        for (int i = 0; i < 100000; ++i) {
            check = false;
            int r = (int)(Math.random() * 10000);
            ++counts[r % MOD];
            switch(r % MOD) {
            default:
            case 0:
                int key = r / 500;
                x = map.get(key);
                if (x == null) {
                    System.out.println("        map.put(" + key + ", new Integer(" + i + ")); //0");
                    map.put(key, new Integer(i));
                    assertTrue(new Integer(i).equals(map.get(key)));
                    if (key >= top) {
                        top = key+1;
                    }
                    ++j;
                    check = true;
                }
                break;
            case 1:
                System.out.println("        x = map.remove(" + j/2 + "); //1") ;
                x = map.remove(j/2);
                assertTrue(null == map.get(j/2));
                assertTrue(null == map.remove(j/2));
                if (x != null) {
                    --j;
                }
                check = true;
                break;
            case 2:
                x = map.get(j/2);
                if (x == null) {
                    check = true;
                }
                break;
            case 3:
                x = map.get(j/3);
                System.out.println("        x = map.put(" + j/3 + ", new Integer(" + i + ")); //2") ;
                map.put(j/3, new Integer(i));
                assertTrue(new Integer(i).equals(map.get(j/3)));
                if (x == null) {
                    if (j/3 >= top) {
                        ++top;
                    }
                    ++j;
                }
                check = true;
                break;
            }

            if (check) {
                checkMap(map, j, top, i, r);
            }

            if (r % (MOD*MOD) == MOD) {
                System.out.println("        map = new IntKeyHashMap();") ;
                map = new IntKeyHashMap();
                j = 0;
                top = 0;
            }
            
        }
    }

    private void checkMap(IntKeyHashMap map, int j, int top, int i, int r) {
        int nulls = 0;
        for (int k = 0; k < top; ++k) {
            if (map.get(k) == null) {
                ++nulls;
            }
        }
        System.out.println("        // top " + top + " nulls " + nulls + " j " + j);
        if (top != nulls + j) {
            System.out.println("Action Counts:");
            for (int count : counts) {
                System.out.println(count);
            }
            System.out.println("Contents:");
            for (int k = 0; k < top; ++k) {
                Object got = map.get(k); 
                if (got == null) {
                    System.out.println(" " + k + ": null");
                } else {
                    System.out.println(" " + k + ": " + got.toString());
                }
            }
            System.out.println("iteration " + i + " value " + (r % MOD));
        }
        assertEquals(top, nulls + j);
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
