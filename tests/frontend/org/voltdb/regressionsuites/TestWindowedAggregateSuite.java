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

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestWindowedAggregateSuite extends RegressionSuite {

    public TestWindowedAggregateSuite(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }

    static private void setupSchema(VoltProjectBuilder project) throws IOException {
        String literalSchema =
                "CREATE TABLE T (\n"
                + "  A INTEGER,"
                + "  B INTEGER,"
                + "  C INTEGER"
                + ");\n"
                ;
        project.addLiteralSchema(literalSchema);
        project.setUseDDLSchema(true);
    }

    public void testRank() throws Exception {
        // Save the guard and restore it after.
        Client client = getClient();

        long expected[][] = new long[][] {
                {  1L,  1L,  101L, 1L },
                {  1L,  1L,  102L, 1L },
                {  1L,  2L,  201L, 3L },
                {  1L,  2L,  202L, 3L },
                {  1L,  3L,  203L, 5L },
                {  2L,  1L, 1101L, 1L },
                {  2L,  1L, 1102L, 1L },
                {  2L,  2L, 1201L, 3L },
                {  2L,  2L, 1202L, 3L },
                {  2L,  3L, 1203L, 5L },
                { 20L,  1L, 2101L, 1L },
                { 20L,  1L, 2102L, 1L },
                { 20L,  2L, 2201L, 3L },
                { 20L,  2L, 2202L, 3L },
                { 20L,  3L, 2203L, 5L },
        };
        long input[][] = expected.clone();
        shuffleArrayOfLongs(input);
        ClientResponse cr;
        VoltTable vt;
        for (long [] row : input) {
            cr = client.callProcedure("T.insert", row[0], row[1], row[2]);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        String sql = "select A, B, C, rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        validateTableOfLongs(vt, expected);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestWindowedAggregateSuite.class);
        boolean success = false;


        try {
            VoltProjectBuilder project = new VoltProjectBuilder();
            config = new LocalCluster("test-windowed-rank.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
        }
        catch (IOException excp) {
            fail();
        }

        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
