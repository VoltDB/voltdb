/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
 */package org.voltdb.regressionsuites;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestMaterializedViewNonemptyTablesSuite extends RegressionSuite {
    public TestMaterializedViewNonemptyTablesSuite(String name) {
        super(name);
    }

    public void testNothing() throws Exception {
        assertTrue(true);
    }

    public void testUnsafeOperators() throws Exception {
        Client client = getClient();

        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", "create view vv as select a, count(*), max(a+a) from alpha group by a");
        assertEquals(ClientResponse.GRACEFUL_FAILURE, cr.getStatus());
        cr = client.callProcedure("ALPHA.insert", 0, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("BETA.insert", 0, 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        String msg = cr.getAppStatusString();
        System.out.printf("Status: %s\n", msg);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMaterializedViewNonemptyTablesSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestMaterializedViewNonemptyTablesSuite.class.getResource("testmvnonemptytables-ddl.sql"));


        // JNI
        config = new LocalCluster("testMaterializedViewNonemptyTables-onesite.jar", 1, 1, 0,
                BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        // CLUSTER
        config = new LocalCluster("testMateralizedViewNonemptyTables-cluster.jar", 2, 3, 1,
               BackendTarget.NATIVE_EE_JNI);
        boolean t2 = config.compile(project);
        assertTrue(t2);
        builder.addServerConfig(config);

        return builder;
    }
}
