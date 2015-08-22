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

public class TestTableCreation {
    HSQLInterface m_HSQLInterface = null;
    String        m_schema = null;
    public TestTableCreation() {
        m_HSQLInterface = HSQLInterface.loadHsqldb();
    }
    /**
     * Test TINYINT type.
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableTinyInt() throws Exception {
        String ddl    = "create table alpha ( id TiNyInT not null, beta TINYINT)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "3"),
                            withAttribute(12, "valuetype", "TINYINT")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "index", "1"),
                            withAttribute(15, "name", "BETA"),
                            withAttribute(16, "nullable", "true"),
                            withAttribute(17, "size", "3"),
                            withAttribute(18, "valuetype", "TINYINT"))),
                    withChildNamed(19, "indexes",
                        withAttribute(20, "name", "indexes")),
                    withChildNamed(21, "constraints",
                        withAttribute(22, "name", "constraints"))));
    }
    /**
     * Test SMALLINT type.
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableSmallInt() throws Exception {
        String ddl    = "create table alpha ( id SmallInt not null, beta SMALLINT)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "5"),
                            withAttribute(12, "valuetype", "SMALLINT")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "index", "1"),
                            withAttribute(15, "name", "BETA"),
                            withAttribute(16, "nullable", "true"),
                            withAttribute(17, "size", "5"),
                            withAttribute(18, "valuetype", "SMALLINT"))),
                    withChildNamed(19, "indexes",
                        withAttribute(20, "name", "indexes")),
                    withChildNamed(21, "constraints",
                        withAttribute(22, "name", "constraints"))));
    }
    /**
     * Test INTEGER type.
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableInteger() throws Exception {
        String ddl    = "create table alpha ( id integer not null, beta integer)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "10"),
                            withAttribute(12, "valuetype", "INTEGER")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "index", "1"),
                            withAttribute(15, "name", "BETA"),
                            withAttribute(16, "nullable", "true"),
                            withAttribute(17, "size", "10"),
                            withAttribute(18, "valuetype", "INTEGER"))),
                    withChildNamed(19, "indexes",
                        withAttribute(20, "name", "indexes")),
                    withChildNamed(21, "constraints",
                        withAttribute(22, "name", "constraints"))));
    }
    /**
     * Test BIGINT type.
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableBigInt() throws Exception {
        String ddl    = "create table alpha ( id BiGiNt not null, beta bIgInT)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "19"),
                            withAttribute(12, "valuetype", "BIGINT")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "index", "1"),
                            withAttribute(15, "name", "BETA"),
                            withAttribute(16, "nullable", "true"),
                            withAttribute(17, "size", "19"),
                            withAttribute(18, "valuetype", "BIGINT"))),
                    withChildNamed(19, "indexes",
                        withAttribute(20, "name", "indexes")),
                    withChildNamed(21, "constraints",
                        withAttribute(22, "name", "constraints"))));
    }
    /**
     * Test DECIMAL type, default scale and precision.
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableWithDecimal() throws Exception {
        String ddl    = "create table alpha ( id integer not null, beta Decimal                 not null)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "10"),
                            withAttribute(12, "valuetype", "INTEGER")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "index", "1"),
                            withAttribute(15, "name", "BETA"),
                            withAttribute(16, "nullable", "false"),
                            withAttribute(17, "size", "100"),
                            withAttribute(18, "valuetype", "DECIMAL"))),
                    withChildNamed(19, "indexes",
                        withAttribute(20, "name", "indexes")),
                    withChildNamed(21, "constraints",
                        withAttribute(22, "name", "constraints"))));
    }
    /**
     * Test DECIMAL type with scale and precision.
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableWithDecimalScalePrecision() throws Exception {
        String ddl    = "create table alpha ( id integer not                 null, beta Decimal (10, 100) not null)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "10"),
                            withAttribute(12, "valuetype", "INTEGER")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "index", "1"),
                            withAttribute(15, "name", "BETA"),
                            withAttribute(16, "nullable", "false"),
                            withAttribute(17, "size", "10"),
                            withAttribute(18, "valuetype", "DECIMAL"))),
                    withChildNamed(19, "indexes",
                        withAttribute(20, "name", "indexes")),
                    withChildNamed(21, "constraints",
                        withAttribute(22, "name", "constraints"))));
    }
    /**
     * Test                 FLOAT type
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableFloat() throws Exception {
        String ddl    = "create table alpha ( id BiGiNt not null, beta FlOaT)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "19"),
                            withAttribute(12, "valuetype", "BIGINT")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "index", "1"),
                            withAttribute(15, "name", "BETA"),
                            withAttribute(16, "nullable", "true"),
                            withAttribute(17, "size", "0"),
                            withAttribute(18, "valuetype", "FLOAT"))),
                    withChildNamed(19, "indexes",
                        withAttribute(20, "name", "indexes")),
                    withChildNamed(21, "constraints",
                        withAttribute(22, "name", "constraints"))));
    }
    /**
     * Test VARCHAR type
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableVarchar() throws Exception {
        String ddl    = "create table alpha ( id BiGiNt not null, beta varchar(100))";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "19"),
                            withAttribute(12, "valuetype", "BIGINT")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "bytes", "false"),
                            withAttribute(15, "index", "1"),
                            withAttribute(16, "name", "BETA"),
                            withAttribute(17, "nullable", "true"),
                            withAttribute(18, "size", "100"),
                            withAttribute(19, "valuetype", "VARCHAR"))),
                    withChildNamed(20, "indexes",
                        withAttribute(21, "name", "indexes")),
                    withChildNamed(22, "constraints",
                        withAttribute(23, "name", "constraints"))));
    }
    /**
     * Test VARBINARY type
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableVarbinary() throws Exception {
        String ddl    = "create table alpha ( id BiGiNt not null, beta varbinary(100))";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "19"),
                            withAttribute(12, "valuetype", "BIGINT")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "index", "1"),
                            withAttribute(15, "name", "BETA"),
                            withAttribute(16, "nullable", "true"),
                            withAttribute(17, "size", "100"),
                            withAttribute(18, "valuetype", "VARBINARY"))),
                    withChildNamed(19, "indexes",
                        withAttribute(20, "name", "indexes")),
                    withChildNamed(21, "constraints",
                        withAttribute(22, "name", "constraints"))));
    }
    /**
     * Test TIMESTAMP type
     *
     * Throws: Exception
     */

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableTimestamp() throws Exception {
        String ddl    = "create table alpha ( id BiGiNt not null, beta timestamp)";
        IDTable idTable = new IDTable();
        HSQLInterface hif = HSQLInterface.loadHsqldb();
        hif.processDDLStatementsUsingVoltSQLParser(ddl, null);
        VoltXMLElement element = hif.getVoltCatalogXML(null, null);
        assertThat(element)
            .hasName(1, "databaseschema")
            .hasAllOf(
                withAttribute(2, "name", "databaseschema"),
                withChildNamed(3, "table",
                    withAttribute(4, "name", "ALPHA"),
                    withChildNamed(5, "columns",
                        withAttribute(6, "name", "columns"),
                        withChildNamed(7, "column",
                                       "name", "ID",
                            withAttribute(8, "index", "0"),
                            withAttribute(9, "name", "ID"),
                            withAttribute(10, "nullable", "false"),
                            withAttribute(11, "size", "19"),
                            withAttribute(12, "valuetype", "BIGINT")),
                        withChildNamed(13, "column",
                                       "name", "BETA",
                            withAttribute(14, "index", "1"),
                            withAttribute(15, "name", "BETA"),
                            withAttribute(16, "nullable", "true"),
                            withAttribute(17, "size", "8"),
                            withAttribute(18, "valuetype", "TIMESTAMP"))),
                    withChildNamed(19, "indexes",
                        withAttribute(20, "name", "indexes")),
                    withChildNamed(21, "constraints",
                        withAttribute(22, "name", "constraints"))));
    }
}
