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

        // test with no fields provided (all column values must be provided)
        validateInsertStmt("insert into p1 values (1, 2, 3, 4, 5, 6);",
                1, 2, 3, 4, 5, 6);

        // not enough values
        verifyStmtFails(getClient(), "insert into p1 values (1, 2, 3);", "row column count mismatch");

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
        verifyStmtFails(getClient(), "insert into p1 (ccc, zzz) values (null, 7);", "CONSTRAINT VIOLATION");

        // try to insert into not null column with no default value
        verifyStmtFails(getClient(), "insert into p1 (ccc) values (32)", "Column ZZZ has no default and is not nullable");
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
