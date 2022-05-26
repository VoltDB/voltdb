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

package org.voltdb.plannerv2;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;

public class TestAdHoc extends RegressionSuite {

    public TestAdHoc(String name) {
        super(name);
    }

    public void testCompileAdHoc() throws IOException, ProcCallException {
        Client client = getClient();
//        client.callProcedure("@AdHoc", "select a from t1 where a > some (select c from t2 where c = 1)");
//        client.callProcedure("@AdHoc", "select a from t1 where a > 0;");
//        client.callProcedure("@AdHoc", "select a from t1 where a > ? limit 2 offset ?;", 100, 2);
//        client.callProcedure("@AdHoc", "select b, sum(a) from t1 where b is not null group by b having sum(a) > 10;");
        client.callProcedure("@AdHoc", "select * from t1;");
    }

    static public junit.framework.Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAdHoc.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("create table t1(a int not null, b varchar);");
        project.addLiteralSchema("partition table t1 on column a;");
        project.addLiteralSchema("create table t2(c int, d varchar);");

        LocalCluster config = new LocalCluster("calcite-adhoc.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);
        return builder;
    }
}
