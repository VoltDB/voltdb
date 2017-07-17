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
 */package org.voltdb.planner;

import java.nio.ByteBuffer;
import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.Tensor;

public class TestPlansLinearAlgebra extends PlannerTestCase {
    private static final int M_MAGIC = 0x544E5e52;

   @Override
    protected void setUp() throws Exception {
        setupSchema(getClass().getResource("testplans-linear-algebra.sql"),
                    "testcount", false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testTensorSerialization() throws Exception {
        Tensor t = new Tensor(new double[][] {
            { 1.0, 0.0, 0.0 },
            { 0.0, 1.0, 0.0 },
            { 0.0, 0.0, 1.0 },
            { 1.0, 0.0, 0.0 },
            { 0.0, 1.0, 0.0 },
            { 0.0, 0.0, 1.0 },
        });
        assertEquals(t.getNumCols(), 3);
        assertEquals(t.getNumRows(), 6);
        for (int ridx = 0; ridx < t.getNumRows(); ridx += 1) {
            for (int cidx = 0; cidx < t.getNumCols(); cidx += 1) {
                double v = ((ridx % 3) == cidx) ? 1.0 : 0.0;
                assertEquals(t.get(ridx, cidx), v);
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(3 * 4 + 8 * 3 * 6);
        buf.putInt(M_MAGIC);
        buf.putInt(3); // numCols
        buf.putInt(6); // numRows
        for (int ridx = 0; ridx < t.getNumRows(); ridx += 1) {
            for (int cidx = 0; cidx < t.getNumCols(); cidx += 1) {
                buf.putDouble(t.get(ridx, cidx));
            }
        }
        buf.clear();
        Tensor s = new Tensor(buf);
        assertEquals(s.getNumCols(), 3);
        assertEquals(s.getNumRows(), 6);
        for (int ridx = 0; ridx < s.getNumRows(); ridx += 1) {
            for (int cidx = 0; cidx < s.getNumCols(); cidx += 1) {
                double v = ((ridx % 3) == cidx) ? 1.0 : 0.0;
                assertEquals(s.get(ridx, cidx), v);
            }
        }
    }

    public void testTensorCreation() {
        String SQL = "SELECT T_TENSOR_MUL(MODEL, VECTOR) FROM MODELS, INSTANCES;";
        List<AbstractPlanNode> plans = compileToFragments(SQL);
        assertNotNull(plans);

        SQL = "SELECT t_tensor(2, 2, 1.0, 0.0, 0.0, 1.0) from models;";
        plans = compileToFragments(SQL);
        assertNotNull(plans);
    }

}

