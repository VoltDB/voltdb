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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSqlInsertSuite extends RegressionSuite {

    private void validateInsertStmt(String insertStmt, long... expectedValues) throws Exception {
        Client client = getClient();

        validateTableOfLongs(client, insertStmt, new long[][] {{1}});
        validateTableOfLongs(client, "select * from p1", new long[][] {expectedValues});
        validateTableOfLongs(client, "delete from p1;", new long[][] {{1}});
    }

    public void testInsert() throws Exception
    {
        Client client = getClient();

        // test with no fields provided (all column values must be provided)
        validateInsertStmt("insert into p1 values (1, 2, 3, 4, 5, 6);",
                1, 2, 3, 4, 5, 6);

        // not enough values
        verifyStmtFails(client, "insert into p1 values (1, 2, 3);", "row column count mismatch");

        // test with all fields specified (in order)
        validateInsertStmt("insert into p1 (ccc, bbb, aaa, zzz, yyy, xxx) values (1, 2, 3, 4, 5, 6);",
                1, 2, 3, 4, 5, 6);

        // test with all fields specified with permuted order
        validateInsertStmt("insert into p1 (xxx, zzz, bbb, ccc, yyy, aaa) values (1, 2, 3, 4, 5, 6);",
                4, 3, 6, 2, 5, 1);

        // test with some fields specified (in order)
        validateInsertStmt("insert into p1 (bbb, aaa, zzz) values (1024, 2048, 4096);",
                10, 1024, 2048, 4096, 14, Long.MIN_VALUE);

        // test with some fields specified with permuted order
        validateInsertStmt("insert into p1 (zzz, bbb, xxx) values (555, 666, 777);",
                10, 666, 12, 555, 14, 777);

        // test with no values provided for NOT NULL columns
        // explicitly set not null field to null.
        verifyStmtFails(client, "insert into p1 (ccc, zzz) values (null, 7);", "CONSTRAINT VIOLATION");

        // try to insert into not null column with no default value
        verifyStmtFails(client, "insert into p1 (ccc) values (32)", "Column ZZZ has no default and is not nullable");


        verifyStmtFails(client, "insert into p1  values (1, 2, 3, 4, 5, 6), (1, 2, 3, 4, 5, 6);",
                "VoltDB does not support multiple rows in the INSERT statement VALUES clause. Use separate INSERT statements.");

        verifyStmtFails(client, "insert into p1 (ccc, zzz) values (1, 2), (3, 4);",
                "VoltDB does not support multiple rows in the INSERT statement VALUES clause. Use separate INSERT statements.");

        // There is an index defined on 1/ccc.
        verifyStmtFails(client, "insert into p1 (ccc, zzz) values (0, 1);",
                "Attempted to divide 1 by 0");
        // Make sure the row is not inserted into the table (ENG-12024).
        validateTableOfScalarLongs(client, "select count(*) from p1 where ccc = 0;", new long[] {0});
    }

    public void testInsertWithExpressionSubquery() throws Exception
    {
        Client client = getClient();
        // Insert a couple of rows into R2
        validateTableOfLongs(client, "insert into r2 values (1, 2, 3, 4, 5, 6);", new long[][] {{1}});
        validateTableOfLongs(client, "insert into r2 values (2, 3, 4, 5, 6, 7);", new long[][] {{1}});
        // Insert a row into R1
        validateTableOfLongs(client, "insert into r1 values (2, 3, 4, 5, 6, 7);", new long[][] {{1}});

        validateTableOfLongs(client, "insert into r1 (ccc, bbb, aaa, zzz, yyy, xxx) " +
                "select ccc, bbb, aaa, zzz, yyy, xxx from r2 " +
                "where not exists (select ccc from r1 rr1 where rr1.ccc = r2.ccc);",
                new long[][] {{1}});

        long[][] expected = new long[][] {{1}, {2}};
        validateTableOfLongs(client, "select ccc from r1 order by ccc", expected);

        // clean-up R1
        validateTableOfLongs(client, "delete from r1;", new long[][] {{2}});

        validateTableOfLongs(client, "insert into r1 (ccc, bbb, aaa, zzz, yyy, xxx) " +
                "select ccc, bbb, aaa, zzz, yyy, xxx from r2 " +
                "where r2.ccc in (select ccc from r2 rr2 where rr2.ccc * 2 = rr2.bbb);",
                new long[][] {{1}});
        expected = new long[][] {{1}};
        validateTableOfLongs(client, "select ccc from r1 order by ccc", expected);

        // clean-up R1
        validateTableOfLongs(client, "delete from r1;", new long[][] {{1}});
        validateTableOfLongs(client, "insert into r1 (ccc, bbb, aaa, zzz, yyy, xxx) " +
                "select (select max(aaa) from r2), 3, 3, 3, 3, 3 from r2;",
                new long[][] {{2}});
        expected = new long[][] {{4}, {4}};
        validateTableOfLongs(client, "select ccc from r1 order by ccc", expected);

        // clean-up R1
        validateTableOfLongs(client, "delete from r1;", new long[][] {{2}});
        validateTableOfLongs(client, "insert into r1 (ccc, bbb, aaa, zzz, yyy, xxx) " +
                "values ((select max(aaa) from r2), 3, 3, 3, 3, 3);",
                new long[][] {{1}});
        expected = new long[][] {{4}};
        validateTableOfLongs(client, "select ccc from r1 order by ccc", expected);

        // clean-up R1
        validateTableOfLongs(client, "delete from r1;", new long[][] {{1}});
        String expectedMsg = "More than one row returned by a scalar/row subquery";
        verifyStmtFails(client, "insert into r1 (ccc, bbb, aaa, zzz, yyy, xxx) " +
                "values ((select ccc from r2), 3, 3, 3, 3, 3);",
                expectedMsg);

    }

    // See also tests for INSERT using DEFAULT NOW columns in TestFunctionsSuite.java

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlInsertSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSqlInsertSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE P1 ( " +
                "ccc bigint default 10 not null, " +
                "bbb bigint default 11, " +
                "aaa bigint default 12, " +
                "zzz bigint not null, " +
                "yyy bigint default 14, " +
                "xxx bigint " + // default null
                ");" +
                "PARTITION TABLE P1 ON COLUMN ccc;" +
                "CREATE INDEX IDX_P1 ON P1(1/ccc);" +
                "" +
                "CREATE TABLE R1 ( " +
                "ccc bigint default 10 not null, " +
                "bbb bigint default 11, " +
                "aaa bigint default 12, " +
                "zzz bigint not null, " +
                "yyy bigint default 14, " +
                "xxx bigint " + // default null
                ");" +
                "" +
                "CREATE TABLE R2 ( " +
                "ccc bigint default 10 not null, " +
                "bbb bigint default 11, " +
                "aaa bigint default 12, " +
                "zzz bigint not null, " +
                "yyy bigint default 14, " +
                "xxx bigint " + // default null
                ");" +
                ""
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        boolean success;

        config = new LocalCluster("sqlinsert-onesite.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("sqlinsert-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
