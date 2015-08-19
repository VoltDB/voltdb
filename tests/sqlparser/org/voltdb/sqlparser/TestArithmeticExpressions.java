/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/*
 * This file has been created by elves.  If you make changes to it,
 * the elves will become annoyed, will overwrite your changes with
 * whatever odd notions they have of what should
 * be here, and ignore your plaintive bleatings.  So, don't edit this file,
 * Unless you want your work to disappear.
 */
package org.voltdb.sqlparser;

import org.junit.Test;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.hsqldb_voltpatches.HSQLInterface;
import org.voltdb.sqlparser.syntax.SQLKind;

import static org.voltdb.sqlparser.semantics.VoltXMLElementAssert.*;

public class TestArithmeticExpressions {
    HSQLInterface m_HSQLInterface = null;
    String        m_schema = null;
    public TestArithmeticExpressions() {
        m_HSQLInterface = HSQLInterface.loadHsqldb();
        String m_schema = "create table alpha ( id integer, beta integer );create table gamma ( id integer not null, zooba integer );create table fargle ( id integer not null, dooba integer )";
        try {
            m_HSQLInterface.processDDLStatementsUsingVoltSQLParser(m_schema, null);
        } catch (Exception ex) {
            System.err.printf("Error parsing ddl: %s\n", ex.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestSum() throws Exception {
        String sql    = "select * from alpha where alpha.id + alpha.id = 0";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName("select")
            .hasAllOf(
                withChildNamed("columns",
                    withChildNamed("columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute("alias", "ID"),
                        withAttribute("column", "ID"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA")),
                    withChildNamed("columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute("alias", "BETA"),
                        withAttribute("column", "BETA"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA"))),
                withChildNamed("parameters"),
                withChildNamed("tablescans",
                    withChildNamed("tablescan",
                                   "table", "ALPHA",
                        withAttribute("jointype", "inner"),
                        withAttribute("table", "ALPHA"),
                        withChildNamed("joincond",
                            withChildNamed("operation",
                                withIdAttribute(idTable),
                                withAttribute("optype", "equal"),
                                withChildNamed("operation",
                                    withIdAttribute(idTable),
                                    withAttribute("optype", "add"),
                                    withChildNamed("columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute("alias", "ID"),
                                        withAttribute("column", "ID"),
                                        withIdAttribute(idTable),
                                        withAttribute("table", "ALPHA")),
                                    withChildNamed("columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute("alias", "ID"),
                                        withAttribute("column", "ID"),
                                        withIdAttribute(idTable),
                                        withAttribute("table", "ALPHA"))),
                                withChildNamed("value",
                                    withIdAttribute(idTable),
                                    withAttribute("value", "0"),
                                    withAttribute("valuetype", "INTEGER")))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestDiff() throws Exception {
        String sql    = "select * from alpha where alpha.id - alpha.id = 0";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName("select")
            .hasAllOf(
                withChildNamed("columns",
                    withChildNamed("columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute("alias", "ID"),
                        withAttribute("column", "ID"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA")),
                    withChildNamed("columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute("alias", "BETA"),
                        withAttribute("column", "BETA"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA"))),
                withChildNamed("parameters"),
                withChildNamed("tablescans",
                    withChildNamed("tablescan",
                                   "table", "ALPHA",
                        withAttribute("jointype", "inner"),
                        withAttribute("table", "ALPHA"),
                        withChildNamed("joincond",
                            withChildNamed("operation",
                                withIdAttribute(idTable),
                                withAttribute("optype", "equal"),
                                withChildNamed("operation",
                                    withIdAttribute(idTable),
                                    withAttribute("optype", "subtract"),
                                    withChildNamed("columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute("alias", "ID"),
                                        withAttribute("column", "ID"),
                                        withIdAttribute(idTable),
                                        withAttribute("table", "ALPHA")),
                                    withChildNamed("columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute("alias", "ID"),
                                        withAttribute("column", "ID"),
                                        withIdAttribute(idTable),
                                        withAttribute("table", "ALPHA"))),
                                withChildNamed("value",
                                    withIdAttribute(idTable),
                                    withAttribute("value", "0"),
                                    withAttribute("valuetype", "INTEGER")))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestProd() throws Exception {
        String sql    = "select * from alpha where alpha.id * alpha.id = 0";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName("select")
            .hasAllOf(
                withChildNamed("columns",
                    withChildNamed("columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute("alias", "ID"),
                        withAttribute("column", "ID"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA")),
                    withChildNamed("columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute("alias", "BETA"),
                        withAttribute("column", "BETA"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA"))),
                withChildNamed("parameters"),
                withChildNamed("tablescans",
                    withChildNamed("tablescan",
                                   "table", "ALPHA",
                        withAttribute("jointype", "inner"),
                        withAttribute("table", "ALPHA"),
                        withChildNamed("joincond",
                            withChildNamed("operation",
                                withIdAttribute(idTable),
                                withAttribute("optype", "equal"),
                                withChildNamed("operation",
                                    withIdAttribute(idTable),
                                    withAttribute("optype", "multiply"),
                                    withChildNamed("columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute("alias", "ID"),
                                        withAttribute("column", "ID"),
                                        withIdAttribute(idTable),
                                        withAttribute("table", "ALPHA")),
                                    withChildNamed("columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute("alias", "ID"),
                                        withAttribute("column", "ID"),
                                        withIdAttribute(idTable),
                                        withAttribute("table", "ALPHA"))),
                                withChildNamed("value",
                                    withIdAttribute(idTable),
                                    withAttribute("value", "0"),
                                    withAttribute("valuetype", "INTEGER")))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestDiv() throws Exception {
        String sql    = "select * from alpha where alpha.id / alpha.id = 0";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName("select")
            .hasAllOf(
                withChildNamed("columns",
                    withChildNamed("columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute("alias", "ID"),
                        withAttribute("column", "ID"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA")),
                    withChildNamed("columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute("alias", "BETA"),
                        withAttribute("column", "BETA"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA"))),
                withChildNamed("parameters"),
                withChildNamed("tablescans",
                    withChildNamed("tablescan",
                                   "table", "ALPHA",
                        withAttribute("jointype", "inner"),
                        withAttribute("table", "ALPHA"),
                        withChildNamed("joincond",
                            withChildNamed("operation",
                                withIdAttribute(idTable),
                                withAttribute("optype", "equal"),
                                withChildNamed("operation",
                                    withIdAttribute(idTable),
                                    withAttribute("optype", "divide"),
                                    withChildNamed("columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute("alias", "ID"),
                                        withAttribute("column", "ID"),
                                        withIdAttribute(idTable),
                                        withAttribute("table", "ALPHA")),
                                    withChildNamed("columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute("alias", "ID"),
                                        withAttribute("column", "ID"),
                                        withIdAttribute(idTable),
                                        withAttribute("table", "ALPHA"))),
                                withChildNamed("value",
                                    withIdAttribute(idTable),
                                    withAttribute("value", "0"),
                                    withAttribute("valuetype", "INTEGER")))))));
    }
}
