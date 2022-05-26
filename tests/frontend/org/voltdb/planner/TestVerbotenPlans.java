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

package org.voltdb.planner;

import java.io.IOException;

public class TestVerbotenPlans extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestVerbotenPlans.class.getResource("testplans-selfjoins-ddl.sql"),
                "testverboten", planForSinglePartitionFalse);
    }


    public void testTwoPartitionedTableJoin() throws IOException {
        // The important aspect is that one of the tables is NOT being joined on its
        // partition key (always "a" in this schema).
        failToCompile("SELECT p1.a, p1.c, p2.a " +
                "FROM p1, p2 " +
                "WHERE p1.a = p2.c;",
                "This query is not plannable.  The planner cannot guarantee that all rows would be in a single partition.");
    }

    public void testBooleanResultColumn() throws IOException {
        // The important aspect is the column calculated from a boolean function.
        failToCompile("SELECT geo1.poly, geo2.pt, contains(geo1.poly, geo2.pt) " +
                "FROM geo geo1, geo geo2; ",
                "A SELECT clause does not allow a BOOLEAN expression. " +
                        "consider using CASE WHEN to decode the BOOLEAN expression " +
                        "into a value of some other type.");
    }

}
