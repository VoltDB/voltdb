/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.AllTpccSQL;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.utils.BuildDirectoryUtils;

public class TestParsedStatements extends TestCase {

    HSQLInterface m_hsql;
    Database m_db;
    AllTpccSQL m_allSQL;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Catalog catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
        m_db = catalog.getClusters().get("cluster").getDatabases().get("database");

        URL url = TPCCProjectBuilder.class.getResource("tpcc-ddl.sql");
        m_hsql = HSQLInterface.loadHsqldb();
        try {
            m_hsql.runDDLFile(URLDecoder.decode(url.getPath(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        m_allSQL = new AllTpccSQL();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    void runSQLTest(String stmtName, String stmtSQL) {
        // use HSQLDB to get XML that describes the semantics of the statement
        // this is much easier to parse than SQL and is checked against the catalog
        VoltXMLElement xmlSQL = null;
        try {
            xmlSQL = m_hsql.getXMLCompiledStatement(stmtSQL);
        } catch (HSQLParseException e) {
            e.printStackTrace();
            assertTrue(false);
        }
        // output the xml from hsql to disk for debugging
        BuildDirectoryUtils.writeFile("statement-hsql-xml", stmtName + ".xml", xmlSQL.toString(), true);

        // get a parsed statement from the xml
        AbstractParsedStmt parsedStmt = AbstractParsedStmt.parse(stmtSQL, xmlSQL, null, m_db, null);
        // analyze expressions
        // except for "insert" statements that currently do without a joinTree.
        if (parsedStmt.m_joinTree != null) {
            parsedStmt.m_joinTree.analyzeJoinExpressions(parsedStmt.m_noTableSelectionList);
        }
        // output a description of the parsed stmt
        BuildDirectoryUtils.writeFile("statement-hsql-parsed", stmtName + ".txt", parsedStmt.toString(), true);

        assertTrue(parsedStmt.m_noTableSelectionList.isEmpty());

        System.out.println(parsedStmt.toString());
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

    public void testParsedInStatements() {
        runSQLTest("1", "select * from new_order where no_w_id in (5,7);");
        runSQLTest("2", "select * from new_order where no_w_id in (?);");
        runSQLTest("3", "select * from new_order where no_w_id in (?,5,3,?);");
        runSQLTest("4", "select * from new_order where no_w_id not in (?,5,3,?);");
        runSQLTest("5", "select * from warehouse where w_name not in (?, 'foo');");
        runSQLTest("6", "select * from new_order where no_w_id in (no_d_id, no_o_id, ?, 7);");
        runSQLTest("7", "select * from new_order where no_w_id in (abs(-1), ?, 17761776);");
        runSQLTest("8", "select * from new_order where no_w_id in (abs(17761776), ?, 17761776) and no_d_id in (abs(-1), ?, 17761776);");
    }
}
