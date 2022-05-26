/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.TestCase;

public class IndexOrderPlayground extends TestCase {
    public void testCompile() throws Exception {
        String simpleSchema =
                "create table table1 (" +
                "column1 bigint not null," +
                "column2 bigint not null," +
                "constraint idx_table1_TREE_pk primary key (column1, column2));\n" +
                "create index idx_table1_hash on table1 (column1);\n" +

                "create table table2 (" +
                "column1 bigint not null," +
                "column2 bigint not null," +
                "constraint idx_table2_TREE_pk primary key (column1, column2));\n" +
                "create index idx_table2_tree on table2 (column1);\n" +

                "create table table3 (" +
                "column1 bigint not null," +
                "column2 bigint not null," +
                "constraint idx_table3_TREE_pk primary key (column1, column2));\n";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("table1", "column1");
        builder.addPartitionInfo("table2", "column1");
        builder.addPartitionInfo("table3", "column1");
        builder.addStmtProcedure("SelectT1", "select * from table1 where column1 = ? and column2 = ?;",
                new ProcedurePartitionData("table1", "column1", "0"));
        builder.addStmtProcedure("SelectT2", "select * from table2 where column1 = ? and column2 = ?;",
                new ProcedurePartitionData("table2", "column1", "0"));
        builder.addStmtProcedure("SelectT3", "select * from table3 where column1 = ? and column2 = ?;",
                new ProcedurePartitionData("table3", "column1", "0"));
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("indexordertest.jar"), 1, 1, 0);
        assertTrue(success);
    }
}
