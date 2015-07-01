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

package org.voltdb.types;

import junit.framework.TestCase;

import org.voltdb.expressions.InComparisonExpression;

public class TestExpressionType extends TestCase {
    public void testGetExpressionClass() {
        // these are the cases that are not covered by other tests
        assertEquals(InComparisonExpression.class,
                     ExpressionType.COMPARE_IN.getExpressionClass());
        assertNull(ExpressionType.INVALID.getExpressionClass());
    }

    public void testGet() {
        // test case insensitivity
        assertEquals(ExpressionType.OPERATOR_PLUS,
                     ExpressionType.get("OPERATOR_PLUS"));
        assertEquals(ExpressionType.OPERATOR_MINUS,
                     ExpressionType.get("Operator_Minus"));
        assertEquals(ExpressionType.OPERATOR_MULTIPLY,
                     ExpressionType.get("operator_multiply"));
        // test (long and) short names
        assertEquals(ExpressionType.COMPARE_IN, ExpressionType.get("IN"));
        assertEquals(ExpressionType.VALUE_TUPLE, ExpressionType.get("Tuple"));
        assertEquals(ExpressionType.AGGREGATE_MIN, ExpressionType.get("min"));
        // test special cases
        assertEquals(ExpressionType.OPERATOR_PLUS, ExpressionType.get("add"));
        assertEquals(ExpressionType.OPERATOR_MINUS,
                     ExpressionType.get("sub"));
        assertEquals(ExpressionType.OPERATOR_MINUS,
                     ExpressionType.get("subtract"));
        // test type not found
        assertEquals(ExpressionType.INVALID, ExpressionType.get("value"));
    }
}
