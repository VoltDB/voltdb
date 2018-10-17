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

package org.voltdb.calcite;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;
import org.voltdb.newplanner.NonDdlSqlTask;
import org.voltdb.newplanner.SqlTask;
import org.voltdb.newplanner.SqlTaskFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestSqlTaskFactory {
    @Test
    public void testSqlTaskFactory() throws SqlParseException {
        String ddlSql = "CREATE TABLE T (a INT)";
        String dqlSql = "SELECT a FROM T where a > 0";
        SqlTask task = SqlTaskFactory.createSqlTask(ddlSql);
        assertEquals(task.getClass(), SqlTask.class);
        task = SqlTaskFactory.createSqlTask(dqlSql);
        assertEquals(task.getClass(), NonDdlSqlTask.class);

        // before parameterizing
        NonDdlSqlTask nonDdlSqlTask = (NonDdlSqlTask) task;
        assertNull(nonDdlSqlTask.getSqlLiteralList());
        // parameterized
        nonDdlSqlTask.parameterize();
        assertEquals(1, nonDdlSqlTask.getSqlLiteralList().size());
    }
}
