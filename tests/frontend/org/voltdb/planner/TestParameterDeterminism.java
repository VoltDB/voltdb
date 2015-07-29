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
package org.voltdb.planner;

public class TestParameterDeterminism extends PlannerTestCase {
    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestParameterDeterminism.class.getResource("testplans-determinism-ddl.sql"),
                    "testparameterdeterminism",
                    planForSinglePartitionFalse);
    }

    public void testIndexDeterminism() throws Exception {
        try {
            compile("INSERT INTO ttree_with_key\n" +
                    "SELECT *\n" +
                    "FROM ttree_with_key\n" +
                    "WHERE b=?\n" +
                    "ORDER BY a, c\n" +
                    "LIMIT 1;" +
                    "");
            assertTrue("Success all around", true);
        } catch (PlanningErrorException ex) {
            assertTrue(String.format("Unexpected planning error: %s", ex.getMessage()),
                       false);
        }
    }
}
