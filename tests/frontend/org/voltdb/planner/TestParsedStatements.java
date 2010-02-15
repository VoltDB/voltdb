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

package org.voltdb.planner;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Map.Entry;
import org.hsqldb.HSQLInterface;
import org.hsqldb.HSQLInterface.HSQLParseException;
import org.voltdb.AllTpccSQL;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.catalog.*;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.utils.BuildDirectoryUtils;
import junit.framework.TestCase;

public class TestParsedStatements extends TestCase {

    HSQLInterface m_hsql;
    Database m_db;
    AllTpccSQL m_allSQL;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Catalog catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
        m_db = catalog.getClusters().get("cluster").getDatabases().get("database");

        URL url = TPCCClient.class.getResource("tpcc-ddl.sql");
        m_hsql = HSQLInterface.loadHsqldb();
        try {
            m_hsql.runDDLFile(URLDecoder.decode(url.getPath(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        m_allSQL = new AllTpccSQL();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        m_hsql.close();
    }

    void runSQLTest(String stmtName, String stmtSQL) {
        // use HSQLDB to get XML that describes the semantics of the statement
        // this is much easier to parse than SQL and is checked against the catalog
        String xmlSQL = null;
        try {
            xmlSQL = m_hsql.getXMLCompiledStatement(stmtSQL);
        } catch (HSQLParseException e) {
            e.printStackTrace();
            assertTrue(false);
        }
        // output the xml from hsql to disk for debugging
        PrintStream xmlDebugOut = BuildDirectoryUtils.getDebugOutputPrintStream("statement-hsql-xml", stmtName + ".xml");
        xmlDebugOut.println(xmlSQL);
        xmlDebugOut.close();

        // get a parsed statement from the xml
        AbstractParsedStmt parsedStmt = AbstractParsedStmt.parse(stmtSQL, xmlSQL, m_db);
        // output a description of the parsed stmt
        PrintStream parsedDebugOut = BuildDirectoryUtils.getDebugOutputPrintStream("statement-hsql-parsed", stmtName + ".txt");
        parsedDebugOut.println(parsedStmt.toString());
        parsedDebugOut.close();

        int clausesFound = 0;
        clausesFound += parsedStmt.noTableSelectionList.size();
        for (Entry<Table, ArrayList<AbstractExpression>> pair : parsedStmt.tableFilterList.entrySet())
            clausesFound += pair.getValue().size();
        for (Entry<AbstractParsedStmt.TablePair, ArrayList<AbstractExpression>> pair : parsedStmt.joinSelectionList.entrySet())
            clausesFound += pair.getValue().size();
        clausesFound += parsedStmt.multiTableSelectionList.size();

        System.out.println(parsedStmt.toString());

        assertEquals(clausesFound, parsedStmt.whereSelectionList.size());
    }

    public void testParsedInsertStatements() {
        for (Entry<String, String> entry : m_allSQL.inserts.entrySet()) {
            runSQLTest(entry.getKey(), entry.getValue());
        }
    }

    public void testParsedSelectStatements() {
        for (Entry<String, String> entry : m_allSQL.selects.entrySet()) {
            runSQLTest(entry.getKey(), entry.getValue());
        }
    }

    public void testParsedDeleteStatements() {
        for (Entry<String, String> entry : m_allSQL.deletes.entrySet()) {
            runSQLTest(entry.getKey(), entry.getValue());
        }
    }

    public void testParsedUpdateStatements() {
        for (Entry<String, String> entry : m_allSQL.updates.entrySet()) {
            runSQLTest(entry.getKey(), entry.getValue());
        }
    }
}
