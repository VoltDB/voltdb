/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.plannerv2;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

public class TestCalciteParser {

    private void assertSqlNodeKind(String sql, SqlKind expectedSqlKind) throws SqlParseException {
        SqlNode sqlNode = VoltSqlParser.parse(sql);
        assertEquals(expectedSqlKind, sqlNode.getKind());
    }

    @Test
    public void testSqlNodeKind() throws SqlParseException {
        assertSqlNodeKind("CREATE TABLE T (a INT)", SqlKind.CREATE_TABLE);
    }

    @Test
    public void testComputeDigest() throws SqlParseException {
        Catalog catalog = new Catalog();
        Cluster cluster = catalog.getClusters().add("cluster");
        Database database = cluster.getDatabases().add("database");
        Table table = database.getTables().add("testTable");
    }
}
