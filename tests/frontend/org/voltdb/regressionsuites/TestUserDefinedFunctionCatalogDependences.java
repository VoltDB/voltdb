/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import junit.framework.Test;

public class TestUserDefinedFunctionCatalogDependences extends RegressionSuite {

    public TestUserDefinedFunctionCatalogDependences(String name) {
        super(name);
    }

    public void testDropFunction() throws Exception {
        Client client = getClient();

        ClientResponse cr;
        String SQL = ""
                + "create table R1 ( BIG BIGINT ); "
                + "create function add2bigint from method org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint; "
                + "-- create index alphidx on R1 ( add2bigint(BIG, BIG) ); "
                + ""
                ;
        cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // /*
        cr = client.callProcedure("@AdHoc", "create procedure proc as select * from R1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // */
        // /*
        try {
            cr = client.callProcedure("@AdHoc", "drop table r1;");
        } catch (Exception ex) {
            ;
        }
        // */
        ///*
        try {
            cr = client.callProcedure("@AdHoc", "drop function add2bigint");
            fail("Should not be able to drop the function add2bigint.");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Cannot define index \"ALPHIDX\".  It depends on the user defined function \"ADD2BIGINT\""));
        }
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // */
    }

    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestUserDefinedFunctionCatalogDependences.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 2 Local Sites/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        LocalCluster config;
        // /*
        config = new LocalCluster("tudfcatdep-onesite.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        assertTrue(config.compile(project));
        // add this config to the set of tests to run
        builder.addServerConfig(config);
        // */
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 3-node k=1 cluster
        /////////////////////////////////////////////////////////////
        /*
        config = new LocalCluster("tudfcatdep-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);
        //*/
        return builder;
    }

}
