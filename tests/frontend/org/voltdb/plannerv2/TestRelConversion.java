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

package org.voltdb.plannerv2;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.voltdb.planner.ParsedUnionStmt;
import org.voltdb.plannerv2.converter.RelConverter;
import org.voltdb.types.JoinType;

public class TestRelConversion extends Plannerv2TestCase {

    ConversionTester m_tester = new ConversionTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSimple() {
        m_tester.sql("select i from R2")
                .plan("Root {kind: SELECT, rel: LogicalProject#2, rowType: RecordType(INTEGER I), fields: [<0, I>], collation: []}")
                .pass();
    }

    public void testSetOpTypeConversion() {
        assertEquals(ParsedUnionStmt.UnionType.UNION, RelConverter.convertSetOpType(SqlKind.UNION, false));
        assertEquals(ParsedUnionStmt.UnionType.UNION_ALL, RelConverter.convertSetOpType(SqlKind.UNION, true));
        assertEquals(ParsedUnionStmt.UnionType.EXCEPT, RelConverter.convertSetOpType(SqlKind.EXCEPT, false));
        assertEquals(ParsedUnionStmt.UnionType.EXCEPT_ALL, RelConverter.convertSetOpType(SqlKind.EXCEPT, true));
        assertEquals(ParsedUnionStmt.UnionType.INTERSECT, RelConverter.convertSetOpType(SqlKind.INTERSECT, false));
        assertEquals(ParsedUnionStmt.UnionType.INTERSECT_ALL, RelConverter.convertSetOpType(SqlKind.INTERSECT, true));
        assertEquals(ParsedUnionStmt.UnionType.NOUNION, RelConverter.convertSetOpType(SqlKind.PLUS, false));
    }

    public void testJoinTypeConversion() {
        assertEquals(JoinType.FULL, RelConverter.convertJointType(JoinRelType.FULL));
        assertEquals(JoinType.INNER, RelConverter.convertJointType(JoinRelType.INNER));
        assertEquals(JoinType.LEFT, RelConverter.convertJointType(JoinRelType.LEFT));
        assertEquals(JoinType.RIGHT, RelConverter.convertJointType(JoinRelType.RIGHT));
    }
}
