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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *//* This file is part of VoltDB.
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

public class TestSimpleSQL {
    HSQLInterface m_HSQLInterface = null;
    String        m_schema = null;
    public TestSimpleSQL() {
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
    public void testCreateTable() throws Exception {
        String ddl    = "create table alpha ( id integer not null, beta integer)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getXMLFromCatalog();
        assertThat(element)
            .hasName("databaseschema")
            .hasAllOf(
                withAttribute("name", "databaseschema"),
                withChildNamed("table",
                    withAttribute("name", "ALPHA"),
                    withChildNamed("columns",
                        withAttribute("name", "columns"),
                        withChildByAttribute("column", "name", "ID",
                            withAttribute("index", "0"),
                            withAttribute("name", "ID"),
                            withAttribute("nullable", "false"),
                            withAttribute("size", "10"),
                            withAttribute("valuetype", "INTEGER")),
                        withChildByAttribute("column", "name", "BETA",
                            withAttribute("index", "1"),
                            withAttribute("name", "BETA"),
                            withAttribute("nullable", "true"),
                            withAttribute("size", "10"),
                            withAttribute("valuetype", "INTEGER"))),
                    withChildNamed("indexes",
                        withAttribute("name", "indexes"),
                        withChildByAttribute("index", "name", "VOLTDB_AUTOGEN_IDX_ALPHA",
                            withAttribute("assumeunique", "false"),
                            withAttribute("columns", ""),
                            withAttribute("name", "VOLTDB_AUTOGEN_IDX_ALPHA"),
                            withAttribute("unique", "true"))),
                    withChildNamed("constraints",
                        withAttribute("name", "constraints"),
                        withChildByAttribute("constraint", "name", "SYS_CT_10017",
                            withAttribute("assumeunique", "false"),
                            withAttribute("constrainttype", "NOT_NULL"),
                            withAttribute("name", "SYS_CT_10017"),
                            withAttribute("rowslimit", "2147483647")))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableWithDecimal() throws Exception {
        String ddl    = "create table alpha ( id integer not null, beta Decimal)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getXMLFromCatalog();
        assertThat(element)
            .hasName("databaseschema")
            .hasAllOf(
                withAttribute("name", "databaseschema"),
                withChildNamed("table",
                    withAttribute("name", "ALPHA"),
                    withChildNamed("columns",
                        withAttribute("name", "columns"),
                        withChildByAttribute("column", "name", "ID",
                            withAttribute("index", "0"),
                            withAttribute("name", "ID"),
                            withAttribute("nullable", "false"),
                            withAttribute("size", "10"),
                            withAttribute("valuetype", "INTEGER")),
                        withChildByAttribute("column", "name", "BETA",
                            withAttribute("index", "1"),
                            withAttribute("name", "BETA"),
                            withAttribute("nullable", "true"),
                            withAttribute("size", "100"),
                            withAttribute("valuetype", "DECIMAL"))),
                    withChildNamed("indexes",
                        withAttribute("name", "indexes"),
                        withChildByAttribute("index", "name", "VOLTDB_AUTOGEN_IDX_ALPHA",
                            withAttribute("assumeunique", "false"),
                            withAttribute("columns", ""),
                            withAttribute("name", "VOLTDB_AUTOGEN_IDX_ALPHA"),
                            withAttribute("unique", "true"))),
                    withChildNamed("constraints",
                        withAttribute("name", "constraints"),
                        withChildByAttribute("constraint", "name", "SYS_CT_10017",
                            withAttribute("assumeunique", "false"),
                            withAttribute("constrainttype", "NOT_NULL"),
                            withAttribute("name", "SYS_CT_10017"),
                            withAttribute("rowslimit", "2147483647")))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInsert1() throws Exception {
        String sql    = "insert into alpha values (1, 1)";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DML);
        assertThat(element)
            .hasName("insert")
            .hasAllOf(
                withAttribute("table", "ALPHA"),
                withChildNamed("columns",
                    withChildByAttribute("column", "name", "ID",
                        withAttribute("name", "ID"),
                        withChildNamed("value",
                            withIdAttribute(idTable),
                            withAttribute("value", "1"),
                            withAttribute("valuetype", "INTEGER"))),
                    withChildByAttribute("column", "name", "BETA",
                        withAttribute("name", "BETA"),
                        withChildNamed("value",
                            withIdAttribute(idTable),
                            withAttribute("value", "1"),
                            withAttribute("valuetype", "INTEGER")))),
                withChildNamed("parameters"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInsert2() throws Exception {
        String sql    = "insert into alpha (beta, id) values (100, 101)";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DML);
        assertThat(element)
            .hasName("insert")
            .hasAllOf(
                withAttribute("table", "ALPHA"),
                withChildNamed("columns",
                    withChildByAttribute("column", "name", "BETA",
                        withAttribute("name", "BETA"),
                        withChildNamed("value",
                            withIdAttribute(idTable),
                            withAttribute("value", "100"),
                            withAttribute("valuetype", "INTEGER"))),
                    withChildByAttribute("column", "name", "ID",
                        withAttribute("name", "ID"),
                        withChildNamed("value",
                            withIdAttribute(idTable),
                            withAttribute("value", "101"),
                            withAttribute("valuetype", "INTEGER")))),
                withChildNamed("parameters"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSelectId() throws Exception {
        String sql    = "select id from alpha";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName("select")
            .hasAllOf(
                withChildNamed("columns",
                    withChildByAttribute("columnref", "table", "ALPHA",
                        withAttribute("alias", "ID"),
                        withAttribute("column", "ID"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA"))),
                withChildNamed("parameters"),
                withChildNamed("tablescans",
                    withChildNamed("tablescan",
                        withAttribute("jointype", "inner"),
                        withAttribute("table", "ALPHA"))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSelectBeta() throws Exception {
        String sql    = "select beta from alpha";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName("select")
            .hasAllOf(
                withChildNamed("columns",
                    withChildByAttribute("columnref", "table", "ALPHA",
                        withAttribute("alias", "BETA"),
                        withAttribute("column", "BETA"),
                        withIdAttribute(idTable),
                        withAttribute("table", "ALPHA"))),
                withChildNamed("parameters"),
                withChildNamed("tablescans",
                    withChildNamed("tablescan",
                        withAttribute("jointype", "inner"),
                        withAttribute("table", "ALPHA"))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSelectFromFargle() throws Exception {
        String sql    = "select dooba from fargle";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName("select")
            .hasAllOf(
                withChildNamed("columns",
                    withChildByAttribute("columnref", "table", "FARGLE",
                        withAttribute("alias", "DOOBA"),
                        withAttribute("column", "DOOBA"),
                        withIdAttribute(idTable),
                        withAttribute("table", "FARGLE"))),
                withChildNamed("parameters"),
                withChildNamed("tablescans",
                    withChildNamed("tablescan",
                        withAttribute("jointype", "inner"),
                        withAttribute("table", "FARGLE"))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSelectFromGamma() throws Exception {
        String sql    = "select id from gamma";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName("select")
            .hasAllOf(
                withChildNamed("columns",
                    withChildByAttribute("columnref", "table", "GAMMA",
                        withAttribute("alias", "ID"),
                        withAttribute("column", "ID"),
                        withIdAttribute(idTable),
                        withAttribute("table", "GAMMA"))),
                withChildNamed("parameters"),
                withChildNamed("tablescans",
                    withChildNamed("tablescan",
                        withAttribute("jointype", "inner"),
                        withAttribute("table", "GAMMA"))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCaseSelect() throws Exception {
        String sql    = "select ID from GAMMA";
        IDTable idTable = new IDTable();
        VoltXMLElement element = m_HSQLInterface.getVoltXMLFromSQLUsingVoltSQLParser(sql, null, SQLKind.DQL);
        assertThat(element)
            .hasName("select")
            .hasAllOf(
                withChildNamed("columns",
                    withChildByAttribute("columnref", "table", "GAMMA",
                        withAttribute("alias", "ID"),
                        withAttribute("column", "ID"),
                        withIdAttribute(idTable),
                        withAttribute("table", "GAMMA"))),
                withChildNamed("parameters"),
                withChildNamed("tablescans",
                    withChildNamed("tablescan",
                        withAttribute("jointype", "inner"),
                        withAttribute("table", "GAMMA"))));
    }
}
