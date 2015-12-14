/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *//* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestWindowingRank extends RegressionSuite {

    public void notestRank() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        client.callProcedure("T1.insert", 30, 7);
        client.callProcedure("T1.insert", 10, 5);
        client.callProcedure("T1.insert", 20, 6);
        client.callProcedure("T1.insert", 40, 8);
        client.callProcedure("T1.insert", 50, 9);


        vt = client.callProcedure("@AdHoc", "select a, rank() over (order by a) from t1 order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 1}, {20, 2}, {30, 3}, {40, 4}, {50, 5}});

        // decending
        vt = client.callProcedure("@AdHoc", "select a, rank() over (order by a desc) from t1 order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 5}, {20, 4}, {30, 3}, {40, 2}, {50, 1}});

        // where clause
        vt = client.callProcedure("@AdHoc", "select a from t1 "
                + "where rank() over (order by a) <= 3 order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10}, {20}, {30}});

        vt = client.callProcedure("@AdHoc", "select a from t1 "
                + "where rank() over (order by a) >= 2 and "
                + "rank() over (order by a) < 4 order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{20}, {30}});

        vt = client.callProcedure("@AdHoc", "select * from t1 "
                + "where rank() over (order by a) = 3 order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{30, 7}});

    }

    public void notestRankPartition() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        client.callProcedure("T1.insert", 30, 7);
        client.callProcedure("T1.insert", 10, 5);
        client.callProcedure("T1.insert", 50, 7);
        client.callProcedure("T1.insert", 40, 8);
        client.callProcedure("T1.insert", 50, 7);
        client.callProcedure("T1.insert", 60, 8);

        vt = client.callProcedure("@AdHoc", "select a, rank() over (partition by b order by a) from t1 order by a;").getResults()[0];
        System.err.println(vt);
        validateTableOfLongs(vt, new long[][]{{10, 1}, {30, 1}, {40, 1}, {50, 2}, {50, 2}, {60, 2}});

        vt = client.callProcedure("@AdHoc", "select a, rank() over (partition by b order by a desc) from t1 order by a;").getResults()[0];
        System.err.println(vt);
        validateTableOfLongs(vt, new long[][]{{10, 1}, {30, 3}, {40, 2}, {50, 1}, {50, 1}, {60, 1}});
    }

    public void testRankIndexScan() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        client.callProcedure("T1.insert", 30, 7);
        client.callProcedure("T1.insert", 10, 5);
        client.callProcedure("T1.insert", 20, 6);
        client.callProcedure("T1.insert", 40, 8);
        client.callProcedure("T1.insert", 50, 9);

//        vt = client.callProcedure("@Explain", "select a from t1 where rank() over (order by a) = 2;").getResults()[0];
//        assertTrue(vt.toString().contains("Rank SCAN"));
//
//        vt = client.callProcedure("@AdHoc", "select a from t1 where rank() over (order by a) = 2;").getResults()[0];
//        validateTableOfScalarLongs(vt, new long[]{20});
//
//        vt = client.callProcedure("@AdHoc", "select a from t1 where rank() over (order by a) = 5;").getResults()[0];
//        validateTableOfScalarLongs(vt, new long[]{50});
//
//        vt = client.callProcedure("@AdHoc", "select a from t1 where rank() over (order by a) = 10;").getResults()[0];
//        validateTableOfScalarLongs(vt, new long[]{});

        vt = client.callProcedure("@Explain", "select a from t1 where rank() over (order by a) = 0.5;").getResults()[0];
        System.err.println(vt);

        vt = client.callProcedure("@AdHoc", "select a from t1 where rank() over (order by a) = 0.5;").getResults()[0];
        System.err.println(vt);
//        validateTableOfScalarLongs(vt, new long[]{20});

    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestWindowingRank(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
                new MultiConfigSuiteBuilder(TestWindowingRank.class);
        boolean success;
        VoltProjectBuilder project = new VoltProjectBuilder();

        final String literalSchema =
                "create table t1 (a integer, b integer);" +
                        "create index idx1 on t1 (a);" +
                        "create index idx2 on t1 (b, a);"
                        ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        //        project.addStmtProcedure("TRIM_ANY", "select id, TRIM(LEADING ? FROM var16) from r1 where id = ?");

        // CONFIG #1: Local Site/Partition running on JNI backend
        config = new LocalCluster("fixedsql-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL -- disabled, the functions being tested are not HSQL compatible
        //        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        //        success = config.compile(project);
        //        assertTrue(success);
        //        builder.addServerConfig(config);

        // no clustering tests for functions

        return builder;
    }
}
