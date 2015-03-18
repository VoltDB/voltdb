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

package org.voltdb;

import junit.framework.TestCase;

import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.regressionsuites.LocalCluster;

public class IndexOrderPlayground extends TestCase {
    public void testCompile() throws Exception {
        CatalogBuilder cb = new CatalogBuilder(
                "create table table1 (" +
                "column1 bigint not null," +
                "column2 bigint not null," +
                "constraint idx_table1_TREE_pk primary key (column1, column2));\n" +
                "create index idx_table1_hash on table1 (column1);\n" +
                "partition table table1 on column column1;\n" +

                "create table table2 (" +
                "column1 bigint not null," +
                "column2 bigint not null," +
                "constraint idx_table2_TREE_pk primary key (column1, column2));\n" +
                "create index idx_table2_tree on table2 (column1);\n" +
                "partition table table2 on column column1;\n" +

                "create table table3 (" +
                "column1 bigint not null," +
                "column2 bigint not null," +
                "constraint idx_table3_TREE_pk primary key (column1, column2));\n" +
                "partition table table3 on column column1;\n" +
                "")
        .addStmtProcedure("SelectT1",
                "select * from table1 where column1 = ? and column2 = ?;", "table1.column1", 0)
        .addStmtProcedure("SelectT2",
                "select * from table2 where column1 = ? and column2 = ?;", "table1.column1", 0)
        .addStmtProcedure("SelectT3",
                "select * from table3 where column1 = ? and column2 = ?;", "table1.column1", 0)
        ;
        assertNotNull("LocalCluster failed to compile",
                LocalCluster.configure(getClass().getSimpleName(), cb, 1));
    }
}
