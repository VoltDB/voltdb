/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calcite;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;
import org.voltdb.parser.ParserFactory;

public class TestCalciteParser {

    private void assertSqlNodeKind(String sql, SqlKind expectedSqlKind) throws SqlParseException {
        SqlParser parser = ParserFactory.create(sql);
        SqlNode sqlNode = parser.parseStmt();
        assertEquals(expectedSqlKind, sqlNode.getKind());
    }

    @Test
    public void testSqlNodeKind() throws SqlParseException {
        assertSqlNodeKind("CREATE TABLE T (a INT)", SqlKind.CREATE_TABLE);
    }

}
