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

package org.voltdb.utils;

import org.voltcore.utils.Pair;
import com.google_voltpatches.common.base.Optional;

import junit.framework.TestCase;

public class TestSQLCommandBatching extends TestCase {

    private static void assertIsValidDdlBatch(String batch) {
        Optional<Pair<Integer, String>> optPair = SQLCommand.findInvalidStatementInDdlBatch(batch);
        if (optPair.isPresent()) {
            fail("Expected no invalid statement in DDL batch, "
                    + "but found one at offset " + optPair.get().getFirst() + ": "
                    + optPair.get().getSecond());
        }
    }

    private static void assertIsInvalidDdlBatchAtOffset(String batch, int offset, String stmt) {
        Optional<Pair<Integer, String>> optPair = SQLCommand.findInvalidStatementInDdlBatch(batch);
        if (optPair.isPresent()) {
            int actualOffset = optPair.get().getFirst();
            String actualStmt = optPair.get().getSecond();
            assertEquals("Expected invalid statement in DDL batch at offset " + offset + ", "
                    + "but instead found invalid statement at offset " + actualOffset + ": "
                    + actualStmt,
                    offset, actualOffset);
            assertEquals("Expected invalid statement with text " + stmt + ", "
                    + " but instead got this invalid text: " + actualStmt,
                    stmt, actualStmt);
        }
        else {
            fail("Expected DDL batch to contain invalid statement "
                    + "at offset " + offset + " but it was a valid DDL batch.");
        }
    }

    public void testDdlBatchingPositive() {

        // Valid DDL verbs are:
        //   alter
        //   create
        //   drop
        //   export
        //   partition
        // This is unsupported by sqlcmd but is treated like DDL
        // by SQLParser:
        //   import

        // The empty string is a valid batch.
        assertIsValidDdlBatch("");

        // A null reference is treated like an empty string.
        assertIsValidDdlBatch(null);

        assertIsValidDdlBatch("create table t1 (i integer, j integer);");

        assertIsValidDdlBatch("create table t1 (i integer, j integer);"
                + "create index i1 on t1(i);");

        assertIsValidDdlBatch("alter table t1 add limit partition rows 5;");

        assertIsValidDdlBatch("alter table t1 add limit partition rows 5 "
                + "execute delete from t1 where q < 30;");

        assertIsValidDdlBatch("alter table t1 add limit partition rows 5 "
                + "execute delete from t1 where q < 30; "
                + "drop table t2;");

        assertIsValidDdlBatch("create table t3 "
                + "(i integer, j double); "
                + "export table t3;"
                + "partition table t3 on column i;");

        assertIsValidDdlBatch("create procedure p as select * from t3 where q > 30;");

        assertIsValidDdlBatch("create procedure p as select * from t3 where q > 30 and z = ?; "
                        + "partition procedure p on table t3 column z;");

        assertIsValidDdlBatch("import something something; "
                + "create table t (i integer); "
                + "partition procedure p on table t3 column z;");

        // A longer batch.
        assertIsValidDdlBatch(
                "create table t ( \n"
                + "  i integer primary key,"
                + "  limit partition rows 5 -- delete statement enforces this limit\n"
                + "    execute (delete from t where i > 7)\n"
                + ");\n"
                + "partition table t on column i;"
                + "create assumeunique index myidx on t (i);\n"
                + "create procedure selt as \n"
                + "  select * from t where i = ?;\n"
                + "partition procedure selt on table t column i;\n"
                + "alter table t add column vc varchar(255) not null;\n"
                + "drop index myidx;\n"
                + "create table t_ex(\n"
                + "i integer,\n"
                + "j integer\n"
                + ");\n"
                + "export table t_ex;\n"
                + "import from something something;\n");
    }

    public void testDdlBatchingNegative() {

        // just one non-DDL statement
        assertIsInvalidDdlBatchAtOffset("select * from t;", 0, "select * from t");

        // invalid statement at beginning
        assertIsInvalidDdlBatchAtOffset(
                "delete from t;\n"
                + "drop table t;\n"
                + "create table t (id integer);\n",
                0,
                "delete from t");

        // invalid statements at end of batch
        assertIsInvalidDdlBatchAtOffset(
                "create table t (\n"
                + "  vc varchar(32),\n"
                + "  i integer,\n"
                + ");\n"
                + "create index thefileindex on t (vc, i);\n"
                + "insert into t values ('foo', 32);\n"
                + "insert into t values ('bar', 33);\n",
                2,
                "insert into t values ('foo', 32)");

        assertIsInvalidDdlBatchAtOffset(
                "create table t (\n"
                + "  vc varchar(32),\n"
                + "  i integer,\n"
                + ");\n"
                + "update r set q = 5 where z = 9;\n"
                + "create index thefileindex on t (vc, i);\n",
                1,
                "update r set q = 5 where z = 9");

        // cleverly insert a DML statement into an otherwise
        // valid larger batch
        assertIsInvalidDdlBatchAtOffset(
                "create table t ( \n" // 0
                + "  i integer primary key,"
                + "  limit partition rows 5 -- delete statement enforces this limit\n"
                + "    execute (delete from t where i > 7)\n"
                + ");\n"
                + "partition table t on column i;" // 1
                + "create assumeunique index myidx on t (i);\n" // 2
                + "create procedure selt as \n" // 3
                + "  select * from t where i = ?;\n"
                + "truncate table t;\n" // 4    <--- invalid!
                + "partition procedure selt on table t column i;\n"
                + "alter table t add column vc varchar(255) not null;\n"
                + "drop index myidx;\n"
                + "create table t_ex(\n"
                + "i integer,\n"
                + "j integer\n"
                + ");\n"
                + "export table t_ex;\n",
                4,
                "truncate table t");

        // LOAD and REMOVE CLASSES are not DDL.  They are translated to
        // system procedure calls in sqlcmd.
        assertIsInvalidDdlBatchAtOffset("load classes fibonacci.jar;\n"
                + "remove classes trubonacci.jar;\n",
                0,
                "load classes fibonacci.jar");

        assertIsInvalidDdlBatchAtOffset("remove classes trubonacci.jar;\n"
                + "load classes fibonacci.jar;\n",
                0,
                "remove classes trubonacci.jar");

    }

}
