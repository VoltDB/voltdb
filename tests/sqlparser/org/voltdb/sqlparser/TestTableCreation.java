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

public class TestTableCreation {
    HSQLInterface m_HSQLInterface = null;
    String        m_schema = null;
    public TestTableCreation() {
        m_HSQLInterface = HSQLInterface.loadHsqldb();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableTinyInt() throws Exception {
        String ddl    = "create table alpha ( id TiNyInT not null, beta TINYINT)";
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
                            withAttribute("size", "3"),
                            withAttribute("valuetype", "TINYINT")),
                        withChildByAttribute("column", "name", "BETA",
                            withAttribute("index", "1"),
                            withAttribute("name", "BETA"),
                            withAttribute("nullable", "true"),
                            withAttribute("size", "3"),
                            withAttribute("valuetype", "TINYINT"))),
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
    public void testCreateTableSmallInt() throws Exception {
        String ddl    = "create table alpha ( id SmallInt not null, beta SMALLINT)";
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
                            withAttribute("size", "5"),
                            withAttribute("valuetype", "SMALLINT")),
                        withChildByAttribute("column", "name", "BETA",
                            withAttribute("index", "1"),
                            withAttribute("name", "BETA"),
                            withAttribute("nullable", "true"),
                            withAttribute("size", "5"),
                            withAttribute("valuetype", "SMALLINT"))),
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
    public void testCreateTableInteger() throws Exception {
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
    public void testCreateTableBigInt() throws Exception {
        String ddl    = "create table alpha ( id BiGiNt not null, beta bIgInT)";
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
                            withAttribute("size", "19"),
                            withAttribute("valuetype", "BIGINT")),
                        withChildByAttribute("column", "name", "BETA",
                            withAttribute("index", "1"),
                            withAttribute("name", "BETA"),
                            withAttribute("nullable", "true"),
                            withAttribute("size", "19"),
                            withAttribute("valuetype", "BIGINT"))),
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
        String ddl    = "create table alpha ( id integer not null, beta Decimal not null)";
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
                            withAttribute("nullable", "false"),
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
                            withAttribute("rowslimit", "2147483647")),
                        withChildByAttribute("constraint", "name", "SYS_CT_10018",
                            withAttribute("assumeunique", "false"),
                            withAttribute("constrainttype", "NOT_NULL"),
                            withAttribute("name", "SYS_CT_10018"),
                            withAttribute("rowslimit", "2147483647")))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableWithDecimalScalePrecision() throws Exception {
        String ddl    = "create table alpha ( id integer not null, beta Decimal (10, 100) not null)";
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
                            withAttribute("nullable", "false"),
                            withAttribute("size", "10"),
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
                            withAttribute("rowslimit", "2147483647")),
                        withChildByAttribute("constraint", "name", "SYS_CT_10018",
                            withAttribute("assumeunique", "false"),
                            withAttribute("constrainttype", "NOT_NULL"),
                            withAttribute("name", "SYS_CT_10018"),
                            withAttribute("rowslimit", "2147483647")))));
    }
}
