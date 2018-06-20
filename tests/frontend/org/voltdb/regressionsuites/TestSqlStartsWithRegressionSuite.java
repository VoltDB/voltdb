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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.TestStartsWithQueries;
import org.voltdb.TestStartsWithQueries.StartsWithSuite;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.aggregates.Insert;

/**
 * System tests for basic STARTS WITH functionality
 */
public class TestSqlStartsWithRegressionSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    static final int ROWS = 10;

    public void testStartsWith() throws IOException, ProcCallException
    {
        Client client = getClient();
        StartsWithSuite tests = new StartsWithSuite(client);
        tests.doTests(client, true);
        tests.doTests(client, false);
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlStartsWithRegressionSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSqlStartsWithRegressionSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        try {
            project.addLiteralSchema(TestStartsWithQueries.schema);
        } catch (IOException e) {
            e.printStackTrace();
        }
        project.addPartitionInfo("STRINGS", "ID");
        project.addStmtProcedure("Insert", "insert into strings values (?, ?, ?);");
        project.addStmtProcedure("SelectStartsWith", "select * from strings where val starts with ?;");
        project.addStmtProcedure("NotStartsWith", "select * from strings where val not starts with ?;");

        config = new LocalCluster("sqllike-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqllike-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqllike-twosites.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }
}
