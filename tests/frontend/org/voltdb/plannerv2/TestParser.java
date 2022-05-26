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

public class TestParser extends Plannerv2TestCase {

    private SqlParserTester m_tester = new SqlParserTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testQuotedCasing() {
        // by default convert quoted identifier to upper case
        m_tester.sql("select \"i\" from R2").pass();
        assertEquals(m_tester.m_parsedNode.toString(),
                "SELECT `I`\n"
                + "FROM `R2`");
        m_tester.sql("select \"I\" from R2").pass();
        assertEquals(m_tester.m_parsedNode.toString(),
                "SELECT `I`\n"
                + "FROM `R2`");
    }

    public void testIdentifierQuoting() {
        // by default double quote is used as identifier delimiter
        m_tester.sql("select \"i\" from R2").pass();
        m_tester.sql("select [i] from R2")
        .exception("Encountered(.*)at line 1, column 8(.*)")
        .pass();
        m_tester.sql("select `i` from R2")
        .exception("Lexical error at line 1, column 8.  Encountered: \"`\"(.*), after : \"\"")
        .pass();
    }

    public void testUnsupportedSelectTop() {
        // Calcite does not support SELECT TOP 2 * from T
        m_tester.sql("select TOP 2 * from R2")
        .exception("(.*)Encountered \"2\" at line 1, column 12(.*)")
        .pass();
    }
}
