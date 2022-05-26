/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

import org.voltdb.types.PlanNodeType;

import junit.framework.AssertionFailedError;

public class TestValidatePlan extends PlannerTestCase {
    public void testAllSome() {
        /*
         * Test someOf.  Try a pass and then a fail.
         */
        validatePlan("SELECT * FROM T3",
                     fragSpec(someOf("Expected SEND",
                                     PlanNodeType.AGGREGATE,
                                     PlanNodeType.COMMONTABLE,
                                     PlanNodeType.SEND),
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                      PlanNodeType.PROJECTION)));
        try {
            validatePlan("SELECT * FROM T3",
                         fragSpec(someOf("Expected SEND",
                                         PlanNodeType.AGGREGATE,
                                         PlanNodeType.COMMONTABLE,
                                         PlanNodeType.HASHAGGREGATE),
                                  new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                          PlanNodeType.PROJECTION)));
            fail("Unexpected success.");
        } catch (AssertionFailedError ex) {
            assertTrue(ex.getMessage().contains("Expected SEND"));
        }
        /*
         * Test allOf.  Try a pass and then a fail.
         *
         * The pass case is kind of goofy, since the matcher is
         * all the same.  But it tests the allOf matcher.
         */
        validatePlan("SELECT * FROM T3",
                     fragSpec(allOf( PlanNodeType.SEND,
                                     PlanNodeType.SEND,
                                     PlanNodeType.SEND),
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                      PlanNodeType.PROJECTION)));
        try {
            validatePlan("SELECT * FROM T3",
                         fragSpec(allOf(PlanNodeType.SEND,
                                        PlanNodeType.AGGREGATE,
                                        PlanNodeType.PROJECTION),
                                  new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                          PlanNodeType.PROJECTION)));
            fail("Unexpected success.");
        } catch (AssertionFailedError ex) {
            assertTrue(ex.getMessage().contains("AGGREGATE"));
        }
        /*
         * Test noneOf.  First a pass and then a fail.
         */
        validatePlan("SELECT * FROM T3",
                     fragSpec(noneOf("Expected None Here",
                                     PlanNodeType.COMMONTABLE,
                                     PlanNodeType.AGGREGATE,
                                     PlanNodeType.PROJECTION),
                              new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                      PlanNodeType.PROJECTION)));
        try {
            validatePlan("SELECT * FROM T3",
                         fragSpec(noneOf("Expected None Here",
                                         PlanNodeType.AGGREGATE,
                                         PlanNodeType.COMMONTABLE,
                                         PlanNodeType.SEND),
                                  new PlanWithInlineNodes(PlanNodeType.SEQSCAN,
                                                          PlanNodeType.PROJECTION)));
            fail("Unexpected success.");
        } catch (AssertionFailedError ex) {
            assertTrue(ex.getMessage().contains("Expected None Here"));
        }
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(true, TestValidatePlan.class.getResource("testplans-validateplans.sql"), "testvalidateplans");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
