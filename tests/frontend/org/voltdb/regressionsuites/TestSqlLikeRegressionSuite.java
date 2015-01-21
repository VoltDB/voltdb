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

import org.voltdb.TestLikeQueries;
import org.voltdb.TestLikeQueries.LikeSuite;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.DeploymentBuilder;

/**
 * System tests for basic LIKE functionality
 */

public class TestSqlLikeRegressionSuite extends RegressionSuite {
    public TestSqlLikeRegressionSuite(String name) {
        super(name);
    }

    public void testLike() throws IOException, ProcCallException
    {
        Client client = getClient();
        LikeSuite tests = new LikeSuite();
        tests.doTests(client, true);
    }

    static public junit.framework.Test suite() {
        return multiClusterSuiteBuilder(TestSqlLikeRegressionSuite.class, TestLikeQueries.schema,
                new DeploymentBuilder(),
                new DeploymentBuilder(2),
                // This config works for TestSqlAggregateSuite, but not here? Don't know why.
                // It seems to get confused in leader election and start down some unwanted RECOVERY code path.
                // Any multi-host config, even single site per host seems to be hanging.
                // Disabling for now.
                //new DeploymentBuilder(2, 3, 1),
                DeploymentBuilder.forHSQLBackend());
    }
}
