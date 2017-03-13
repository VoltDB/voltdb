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
 */

package org.voltdb.planner;

/**
 * ENG-12116 complains about a SEGV in the EE caused by a bad
 * input schema for a send node.  This actually is a bug in the
 * column resolution code which can't be easily fixed.  The root
 * cause is described in the ticket for ENG-12116.  See this for
 * more details.  This test verifies that we get a planning error
 * and not an EE crash.  This is not optimal, but better than it
 * could be.
 *
 * If ENG-12116 is fixed this test will fail, because the
 * query will, in fact, compile.  That is a good thing.  This test
 * will then be irrelevant and should be deleted.
 */
public class TestENG12116  extends PlannerTestCase {

    public void testENG12076() throws Exception {
        failToCompile("SELECT 0 FROM ( " +
                      "  SELECT distinct * FROM P1 AS T1, R1 " +
                      ") T1 LIMIT 2;",
                      "Internal Error: Send Node input schema differs from output schema");
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(true, TestENG12116.class.getResource("testplans-eng12116-ddl.sql"), "testwindowfunctions");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
