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

package org.voltdb.parser;

import junit.framework.TestCase;

public class TestSQLParser extends TestCase {

    public void testBatchBeginsWithDDLKeywordPositive() {

        // alter create drop export partition
        // (and sometimes import?)

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "create table t (i integer);"));

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "alter table t add column j double;"));

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "drop index idx;"));

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "partition table t on column i;"));

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "export table ex_tbl;"));

        // Seems to be considered DDL.  Is that what we want?
        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "import table ex_tbl;"));

        // Now test multiple statements and comments...

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "create table t (i integer);\n"
                + "create index idx on t (i);"));

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "-- Here's some DDL...\n"
                + "create table t (i integer);\n"
                + "create index idx on t (i);"));

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "-- Here's some DDL...\n"
                + "create table t (i integer); -- let's not forget the index...\n"
                + "create index idx on t (i);"));

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "/* here's some DDL: */\n"
                + "create table t (i integer); -- let's not forget the index...\n"
                + "create index idx on t (i);"));

        assertTrue(SQLParser.batchBeginsWithDDLKeyword(
                "// here's some DDL; check it out!\n"
                + "create table t (i integer); -- let's not forget the index...\n"
                + "create index idx on t (i);"));

    }

    public void testBatchBeginsWithDDLKeywordNegative() {

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "insert into t values (47);"));

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "delete from t where i = 9;"));

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "upsert into t values (32);"));

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "update t set i = 70 where i > 69;"));

        // Now some comments

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "-- create table was done earlier...\n"
                + "update t set i = 70 where i > 69;"));

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "// create table was done earlier...\n"
                + "update t set i = 70 where i > 69;"));

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "/* create table was done earlier... */\n"
                + "update t set i = 70 where i > 69;"));

        // Near misses that might appear in a ddl.sql file
        // but that cannot be batched

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "load classes foo-bar.jar"));

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "remove classes foo-bar.jar"));

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "exec SelectAllRowsWithKey 10;"));

        assertFalse(SQLParser.batchBeginsWithDDLKeyword(
                "file \"mysqlcommands.sql\";"));
    }
}
