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

import org.voltdb.sqlparser.assertions.semantics.VoltXMLElementAssert.IDTable;
import static org.voltdb.sqlparser.assertions.semantics.VoltXMLElementAssert.*;

public class TestBooleanOperators {
    HSQLInterface m_HSQLInterface = null;
    String        m_schema = null;
    public TestBooleanOperators() {
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
    public void TestAnd() throws Exception {
        String sql    = "select * from alpha where id = 0 and id = 1";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "and",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "and"),
                                withChildNamed(22, "operation",
                                               "optype", "equal",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "equal"),
                                    withChildNamed(25, "columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute(26, "alias", "ID"),
                                        withAttribute(27, "column", "ID"),
                                        withIdAttribute(28, idTable),
                                        withAttribute(29, "table", "ALPHA")),
                                    withChildNamed(30, "value",
                                        withIdAttribute(31, idTable),
                                        withAttribute(32, "value", "0"),
                                        withAttribute(33, "valuetype", "INTEGER"))),
                                withChildNamed(34, "operation",
                                               "optype", "equal",
                                    withIdAttribute(35, idTable),
                                    withAttribute(36, "optype", "equal"),
                                    withChildNamed(37, "columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute(38, "alias", "ID"),
                                        withAttribute(39, "column", "ID"),
                                        withIdAttribute(40, idTable),
                                        withAttribute(41, "table", "ALPHA")),
                                    withChildNamed(42, "value",
                                        withIdAttribute(43, idTable),
                                        withAttribute(44, "value", "1"),
                                        withAttribute(45, "valuetype", "INTEGER"))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestOr() throws Exception {
        String sql    = "select * from alpha where id != 0 or id = 1";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "or",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "or"),
                                withChildNamed(22, "operation",
                                               "optype", "notequal",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "notequal"),
                                    withChildNamed(25, "columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute(26, "alias", "ID"),
                                        withAttribute(27, "column", "ID"),
                                        withIdAttribute(28, idTable),
                                        withAttribute(29, "table", "ALPHA")),
                                    withChildNamed(30, "value",
                                        withIdAttribute(31, idTable),
                                        withAttribute(32, "value", "0"),
                                        withAttribute(33, "valuetype", "INTEGER"))),
                                withChildNamed(34, "operation",
                                               "optype", "equal",
                                    withIdAttribute(35, idTable),
                                    withAttribute(36, "optype", "equal"),
                                    withChildNamed(37, "columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute(38, "alias", "ID"),
                                        withAttribute(39, "column", "ID"),
                                        withIdAttribute(40, idTable),
                                        withAttribute(41, "table", "ALPHA")),
                                    withChildNamed(42, "value",
                                        withIdAttribute(43, idTable),
                                        withAttribute(44, "value", "1"),
                                        withAttribute(45, "valuetype", "INTEGER"))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestNot() throws Exception {
        String sql    = "select * from alpha where not id < 0";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "not",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "not"),
                                withChildNamed(22, "operation",
                                               "optype", "lessthan",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "lessthan"),
                                    withChildNamed(25, "columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute(26, "alias", "ID"),
                                        withAttribute(27, "column", "ID"),
                                        withIdAttribute(28, idTable),
                                        withAttribute(29, "table", "ALPHA")),
                                    withChildNamed(30, "value",
                                        withIdAttribute(31, idTable),
                                        withAttribute(32, "value", "0"),
                                        withAttribute(33, "valuetype", "INTEGER"))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestPrecedenceNotAndL() throws Exception {
        String sql    = "select * from alpha where not id < 0 and beta >                 0";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "and",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "and"),
                                withChildNamed(22, "operation",
                                               "optype", "not",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "not"),
                                    withChildNamed(25, "operation",
                                                   "optype", "lessthan",
                                        withIdAttribute(26, idTable),
                                        withAttribute(27, "optype", "lessthan"),
                                        withChildNamed(28, "columnref",
                                                       "alias", "ID",
                                                       "column", "ID",
                                                       "table", "ALPHA",
                                            withAttribute(29, "alias", "ID"),
                                            withAttribute(30, "column", "ID"),
                                            withIdAttribute(31, idTable),
                                            withAttribute(32, "table", "ALPHA")),
                                        withChildNamed(33, "value",
                                            withIdAttribute(34, idTable),
                                            withAttribute(35, "value", "0"),
                                            withAttribute(36, "valuetype", "INTEGER")))),
                                withChildNamed(37, "operation",
                                               "optype", "greaterthan",
                                    withIdAttribute(38, idTable),
                                    withAttribute(39, "optype", "greaterthan"),
                                    withChildNamed(40, "columnref",
                                                   "alias", "BETA",
                                                   "column", "BETA",
                                                   "table", "ALPHA",
                                        withAttribute(41, "alias", "BETA"),
                                        withAttribute(42, "column", "BETA"),
                                        withIdAttribute(43, idTable),
                                        withAttribute(44, "table", "ALPHA")),
                                    withChildNamed(45, "value",
                                        withIdAttribute(46, idTable),
                                        withAttribute(47, "value", "0"),
                                        withAttribute(48, "valuetype", "INTEGER"))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestPrecedenceNotOrL() throws Exception {
        String sql    = "select * from alpha where not id < 0 or beta > 0";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "or",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "or"),
                                withChildNamed(22, "operation",
                                               "optype", "not",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "not"),
                                    withChildNamed(25, "operation",
                                                   "optype", "lessthan",
                                        withIdAttribute(26, idTable),
                                        withAttribute(27, "optype", "lessthan"),
                                        withChildNamed(28, "columnref",
                                                       "alias", "ID",
                                                       "column", "ID",
                                                       "table", "ALPHA",
                                            withAttribute(29, "alias", "ID"),
                                            withAttribute(30, "column", "ID"),
                                            withIdAttribute(31, idTable),
                                            withAttribute(32, "table", "ALPHA")),
                                        withChildNamed(33, "value",
                                            withIdAttribute(34, idTable),
                                            withAttribute(35, "value", "0"),
                                            withAttribute(36, "valuetype", "INTEGER")))),
                                withChildNamed(37, "operation",
                                               "optype", "greaterthan",
                                    withIdAttribute(38, idTable),
                                    withAttribute(39, "optype", "greaterthan"),
                                    withChildNamed(40, "columnref",
                                                   "alias", "BETA",
                                                   "column", "BETA",
                                                   "table", "ALPHA",
                                        withAttribute(41, "alias", "BETA"),
                                        withAttribute(42, "column", "BETA"),
                                        withIdAttribute(43, idTable),
                                        withAttribute(44, "table", "ALPHA")),
                                    withChildNamed(45, "value",
                                        withIdAttribute(46, idTable),
                                        withAttribute(47, "value", "0"),
                                        withAttribute(48, "valuetype", "INTEGER"))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestPrecedenceNotAndR() throws Exception {
        String sql    = "select * from alpha where id < 0 and not beta >                 0";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "and",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "and"),
                                withChildNamed(22, "operation",
                                               "optype", "not",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "not"),
                                    withChildNamed(25, "operation",
                                                   "optype", "greaterthan",
                                        withIdAttribute(26, idTable),
                                        withAttribute(27, "optype", "greaterthan"),
                                        withChildNamed(28, "columnref",
                                                       "alias", "BETA",
                                                       "column", "BETA",
                                                       "table", "ALPHA",
                                            withAttribute(29, "alias", "BETA"),
                                            withAttribute(30, "column", "BETA"),
                                            withIdAttribute(31, idTable),
                                            withAttribute(32, "table", "ALPHA")),
                                        withChildNamed(33, "value",
                                            withIdAttribute(34, idTable),
                                            withAttribute(35, "value", "0"),
                                            withAttribute(36, "valuetype", "INTEGER")))),
                                withChildNamed(37, "operation",
                                               "optype", "lessthan",
                                    withIdAttribute(38, idTable),
                                    withAttribute(39, "optype", "lessthan"),
                                    withChildNamed(40, "columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute(41, "alias", "ID"),
                                        withAttribute(42, "column", "ID"),
                                        withIdAttribute(43, idTable),
                                        withAttribute(44, "table", "ALPHA")),
                                    withChildNamed(45, "value",
                                        withIdAttribute(46, idTable),
                                        withAttribute(47, "value", "0"),
                                        withAttribute(48, "valuetype", "INTEGER"))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestPrecedenceNotOrR() throws Exception {
        String sql    = "select * from alpha where id < 0 or not beta > 0";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "or",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "or"),
                                withChildNamed(22, "operation",
                                               "optype", "lessthan",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "lessthan"),
                                    withChildNamed(25, "columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute(26, "alias", "ID"),
                                        withAttribute(27, "column", "ID"),
                                        withIdAttribute(28, idTable),
                                        withAttribute(29, "table", "ALPHA")),
                                    withChildNamed(30, "value",
                                        withIdAttribute(31, idTable),
                                        withAttribute(32, "value", "0"),
                                        withAttribute(33, "valuetype", "INTEGER"))),
                                withChildNamed(34, "operation",
                                               "optype", "not",
                                    withIdAttribute(35, idTable),
                                    withAttribute(36, "optype", "not"),
                                    withChildNamed(37, "operation",
                                                   "optype", "greaterthan",
                                        withIdAttribute(38, idTable),
                                        withAttribute(39, "optype", "greaterthan"),
                                        withChildNamed(40, "columnref",
                                                       "alias", "BETA",
                                                       "column", "BETA",
                                                       "table", "ALPHA",
                                            withAttribute(41, "alias", "BETA"),
                                            withAttribute(42, "column", "BETA"),
                                            withIdAttribute(43, idTable),
                                            withAttribute(44, "table", "ALPHA")),
                                        withChildNamed(45, "value",
                                            withIdAttribute(46, idTable),
                                            withAttribute(47, "value", "0"),
                                            withAttribute(48, "valuetype", "INTEGER")))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestPrecedenceOrOr() throws Exception {
        String sql    = "select * from alpha where id < 0 or beta > 0 or beta                 != 1";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "or",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "or"),
                                withChildNamed(22, "operation",
                                               "optype", "or",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "or"),
                                    withChildNamed(25, "operation",
                                                   "optype", "lessthan",
                                        withIdAttribute(26, idTable),
                                        withAttribute(27, "optype", "lessthan"),
                                        withChildNamed(28, "columnref",
                                                       "alias", "ID",
                                                       "column", "ID",
                                                       "table", "ALPHA",
                                            withAttribute(29, "alias", "ID"),
                                            withAttribute(30, "column", "ID"),
                                            withIdAttribute(31, idTable),
                                            withAttribute(32, "table", "ALPHA")),
                                        withChildNamed(33, "value",
                                            withIdAttribute(34, idTable),
                                            withAttribute(35, "value", "0"),
                                            withAttribute(36, "valuetype", "INTEGER"))),
                                    withChildNamed(37, "operation",
                                                   "optype", "greaterthan",
                                        withIdAttribute(38, idTable),
                                        withAttribute(39, "optype", "greaterthan"),
                                        withChildNamed(40, "columnref",
                                                       "alias", "BETA",
                                                       "column", "BETA",
                                                       "table", "ALPHA",
                                            withAttribute(41, "alias", "BETA"),
                                            withAttribute(42, "column", "BETA"),
                                            withIdAttribute(43, idTable),
                                            withAttribute(44, "table", "ALPHA")),
                                        withChildNamed(45, "value",
                                            withIdAttribute(46, idTable),
                                            withAttribute(47, "value", "0"),
                                            withAttribute(48, "valuetype", "INTEGER")))),
                                withChildNamed(49, "operation",
                                               "optype", "notequal",
                                    withIdAttribute(50, idTable),
                                    withAttribute(51, "optype", "notequal"),
                                    withChildNamed(52, "columnref",
                                                   "alias", "BETA",
                                                   "column", "BETA",
                                                   "table", "ALPHA",
                                        withAttribute(53, "alias", "BETA"),
                                        withAttribute(54, "column", "BETA"),
                                        withIdAttribute(55, idTable),
                                        withAttribute(56, "table", "ALPHA")),
                                    withChildNamed(57, "value",
                                        withIdAttribute(58, idTable),
                                        withAttribute(59, "value", "1"),
                                        withAttribute(60, "valuetype", "INTEGER"))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestPrecedenceOrAnd() throws Exception {
        String sql    = "select * from alpha where id < 0 or beta > 0 and beta                 != 1";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "or",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "or"),
                                withChildNamed(22, "operation",
                                               "optype", "lessthan",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "lessthan"),
                                    withChildNamed(25, "columnref",
                                                   "alias", "ID",
                                                   "column", "ID",
                                                   "table", "ALPHA",
                                        withAttribute(26, "alias", "ID"),
                                        withAttribute(27, "column", "ID"),
                                        withIdAttribute(28, idTable),
                                        withAttribute(29, "table", "ALPHA")),
                                    withChildNamed(30, "value",
                                        withIdAttribute(31, idTable),
                                        withAttribute(32, "value", "0"),
                                        withAttribute(33, "valuetype", "INTEGER"))),
                                withChildNamed(34, "operation",
                                               "optype", "and",
                                    withIdAttribute(35, idTable),
                                    withAttribute(36, "optype", "and"),
                                    withChildNamed(37, "operation",
                                                   "optype", "greaterthan",
                                        withIdAttribute(38, idTable),
                                        withAttribute(39, "optype", "greaterthan"),
                                        withChildNamed(40, "columnref",
                                                       "alias", "BETA",
                                                       "column", "BETA",
                                                       "table", "ALPHA",
                                            withAttribute(41, "alias", "BETA"),
                                            withAttribute(42, "column", "BETA"),
                                            withIdAttribute(43, idTable),
                                            withAttribute(44, "table", "ALPHA")),
                                        withChildNamed(45, "value",
                                            withIdAttribute(46, idTable),
                                            withAttribute(47, "value", "0"),
                                            withAttribute(48, "valuetype", "INTEGER"))),
                                    withChildNamed(49, "operation",
                                                   "optype", "notequal",
                                        withIdAttribute(50, idTable),
                                        withAttribute(51, "optype", "notequal"),
                                        withChildNamed(52, "columnref",
                                                       "alias", "BETA",
                                                       "column", "BETA",
                                                       "table", "ALPHA",
                                            withAttribute(53, "alias", "BETA"),
                                            withAttribute(54, "column", "BETA"),
                                            withIdAttribute(55, idTable),
                                            withAttribute(56, "table", "ALPHA")),
                                        withChildNamed(57, "value",
                                            withIdAttribute(58, idTable),
                                            withAttribute(59, "value", "1"),
                                            withAttribute(60, "valuetype", "INTEGER")))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestPrecedenceAndAnd() throws Exception {
        String sql    = "select * from alpha where id < 0 and beta > 0 and                 beta != 1";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "and",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "and"),
                                withChildNamed(22, "operation",
                                               "optype", "and",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "and"),
                                    withChildNamed(25, "operation",
                                                   "optype", "notequal",
                                        withIdAttribute(26, idTable),
                                        withAttribute(27, "optype", "notequal"),
                                        withChildNamed(28, "columnref",
                                                       "alias", "BETA",
                                                       "column", "BETA",
                                                       "table", "ALPHA",
                                            withAttribute(29, "alias", "BETA"),
                                            withAttribute(30, "column", "BETA"),
                                            withIdAttribute(31, idTable),
                                            withAttribute(32, "table", "ALPHA")),
                                        withChildNamed(33, "value",
                                            withIdAttribute(34, idTable),
                                            withAttribute(35, "value", "1"),
                                            withAttribute(36, "valuetype", "INTEGER"))),
                                    withChildNamed(37, "operation",
                                                   "optype", "lessthan",
                                        withIdAttribute(38, idTable),
                                        withAttribute(39, "optype", "lessthan"),
                                        withChildNamed(40, "columnref",
                                                       "alias", "ID",
                                                       "column", "ID",
                                                       "table", "ALPHA",
                                            withAttribute(41, "alias", "ID"),
                                            withAttribute(42, "column", "ID"),
                                            withIdAttribute(43, idTable),
                                            withAttribute(44, "table", "ALPHA")),
                                        withChildNamed(45, "value",
                                            withIdAttribute(46, idTable),
                                            withAttribute(47, "value", "0"),
                                            withAttribute(48, "valuetype", "INTEGER")))),
                                withChildNamed(49, "operation",
                                               "optype", "greaterthan",
                                    withIdAttribute(50, idTable),
                                    withAttribute(51, "optype", "greaterthan"),
                                    withChildNamed(52, "columnref",
                                                   "alias", "BETA",
                                                   "column", "BETA",
                                                   "table", "ALPHA",
                                        withAttribute(53, "alias", "BETA"),
                                        withAttribute(54, "column", "BETA"),
                                        withIdAttribute(55, idTable),
                                        withAttribute(56, "table", "ALPHA")),
                                    withChildNamed(57, "value",
                                        withIdAttribute(58, idTable),
                                        withAttribute(59, "value", "0"),
                                        withAttribute(60, "valuetype", "INTEGER"))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void TestPrecedenceAndOr() throws Exception {
        String sql    = "select * from alpha where id < 0 and beta > 0 or beta                 != 1";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName(1, "select")
            .hasAllOf(
                withChildNamed(2, "columns",
                    withChildNamed(3, "columnref",
                                   "alias", "ID",
                                   "column", "ID",
                                   "table", "ALPHA",
                        withAttribute(4, "alias", "ID"),
                        withAttribute(5, "column", "ID"),
                        withIdAttribute(6, idTable),
                        withAttribute(7, "table", "ALPHA")),
                    withChildNamed(8, "columnref",
                                   "alias", "BETA",
                                   "column", "BETA",
                                   "table", "ALPHA",
                        withAttribute(9, "alias", "BETA"),
                        withAttribute(10, "column", "BETA"),
                        withIdAttribute(11, idTable),
                        withAttribute(12, "table", "ALPHA"))),
                withChildNamed(13, "parameters"),
                withChildNamed(14, "tablescans",
                    withChildNamed(15, "tablescan",
                        withAttribute(16, "jointype", "inner"),
                        withAttribute(17, "table", "ALPHA"),
                        withChildNamed(18, "joincond",
                            withChildNamed(19, "operation",
                                           "optype", "or",
                                withIdAttribute(20, idTable),
                                withAttribute(21, "optype", "or"),
                                withChildNamed(22, "operation",
                                               "optype", "and",
                                    withIdAttribute(23, idTable),
                                    withAttribute(24, "optype", "and"),
                                    withChildNamed(25, "operation",
                                                   "optype", "lessthan",
                                        withIdAttribute(26, idTable),
                                        withAttribute(27, "optype", "lessthan"),
                                        withChildNamed(28, "columnref",
                                                       "alias", "ID",
                                                       "column", "ID",
                                                       "table", "ALPHA",
                                            withAttribute(29, "alias", "ID"),
                                            withAttribute(30, "column", "ID"),
                                            withIdAttribute(31, idTable),
                                            withAttribute(32, "table", "ALPHA")),
                                        withChildNamed(33, "value",
                                            withIdAttribute(34, idTable),
                                            withAttribute(35, "value", "0"),
                                            withAttribute(36, "valuetype", "INTEGER"))),
                                    withChildNamed(37, "operation",
                                                   "optype", "greaterthan",
                                        withIdAttribute(38, idTable),
                                        withAttribute(39, "optype", "greaterthan"),
                                        withChildNamed(40, "columnref",
                                                       "alias", "BETA",
                                                       "column", "BETA",
                                                       "table", "ALPHA",
                                            withAttribute(41, "alias", "BETA"),
                                            withAttribute(42, "column", "BETA"),
                                            withIdAttribute(43, idTable),
                                            withAttribute(44, "table", "ALPHA")),
                                        withChildNamed(45, "value",
                                            withIdAttribute(46, idTable),
                                            withAttribute(47, "value", "0"),
                                            withAttribute(48, "valuetype", "INTEGER")))),
                                withChildNamed(49, "operation",
                                               "optype", "notequal",
                                    withIdAttribute(50, idTable),
                                    withAttribute(51, "optype", "notequal"),
                                    withChildNamed(52, "columnref",
                                                   "alias", "BETA",
                                                   "column", "BETA",
                                                   "table", "ALPHA",
                                        withAttribute(53, "alias", "BETA"),
                                        withAttribute(54, "column", "BETA"),
                                        withIdAttribute(55, idTable),
                                        withAttribute(56, "table", "ALPHA")),
                                    withChildNamed(57, "value",
                                        withIdAttribute(58, idTable),
                                        withAttribute(59, "value", "1"),
                                        withAttribute(60, "valuetype", "INTEGER"))))))));
    }
}
