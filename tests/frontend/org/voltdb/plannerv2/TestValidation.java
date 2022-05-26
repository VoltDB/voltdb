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

public class TestValidation extends Plannerv2TestCase {

    private ValidationTester m_tester = new ValidationTester();

    @Override protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
    }

    @Override public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testTableColumn() {
        // valid columns
        m_tester.sql("select i from R2").pass();
        m_tester.sql("select pk from R3").pass();;
        // select a not exist column
        m_tester.sql("select ^bad^ from R2").exception("Column 'BAD' not found in any table").pass();
        // select a not exist column from another table
        m_tester.sql("select ^pk^ from R2").exception("Column 'PK' not found in any table").pass();
        // well, this one's error message is a little misleading. But it is the expected behavior for calcite validator
        m_tester.sql("select ^R3^.pk from R2").exception("Table 'R3' not found").pass();
        // select a not exist table.column
        m_tester.sql("select R2.^bad^ from R2").exception("Column 'BAD' not found in table 'R2'").pass();
        // not exist column in where clause
        m_tester.sql("select pk from R3 where ^bad^ < 0").exception("Column 'BAD' not found in any table").pass();
        // not exist table.column in where clause
        m_tester.sql("select pk from R3 where R3.^bad^ < 0").exception("Column 'BAD' not found in table 'R3'").pass();
    }

    public void testTableName() {
        // valid table
        m_tester.sql("select * from R3").pass();
        // not exist table
        m_tester.sql("select * from ^bad^").exception("Object 'BAD' not found").pass();
    }

    public void testCaseInsensitivity() {
        m_tester.sql("select pK from R3").pass();
        m_tester.sql("select * from r3").pass();
    }

    public void testGroupBy() {
        m_tester.sql("select i from R2 group by i").pass();
        m_tester.sql("select i + 1 from R2 group by i").pass();
        m_tester.sql("select i, count(*) from R2 group by i").pass();

        m_tester.sql("select ^i^, count(*) from R2 group by i + 1").exception("Expression 'I' is not being grouped").pass();
        m_tester.sql("select i, ^ti^ from R2 group by i").exception("Expression 'TI' is not being grouped").pass();
    }

    public void testPartitionBy() {
        m_tester.sql("select i, sum(i) over(partition by i + ti order by i) from R2").pass();

        m_tester.sql("select i, sum(i) over(partition by ^i + v^ order by i) from R2")
                .exception("(?s)Cannot apply '\\+' to arguments of type '<INTEGER> \\+ <VARCHAR\\(32\\)>'.*")
                .pass();
    }

    public void testFullUsingJoinWithoutColumnScope() {
        m_tester.sql("select i from R1 FUll JOIN R2 using(i)").pass();
    }

    public void testInnerUsingJoinWithoutColumnScope() {
        m_tester.sql("select i from R1 JOIN R2 using(i)").pass();
    }

    public void testFullOnJoinWithoutColumnScope() {
        m_tester.sql("select ^i^ from R1 FUll JOIN R2 on R1.i = R2.i")
        .exception("Column 'I' is ambiguous")
        .pass();
    }

    public void testAmbiguousLeftUsing() {
        m_tester.sql("select ^I^ from R1 left join R2 using(I)")
        .exception("Column 'I' is ambiguous")
        .pass();
    }

    public void testAmbiguousRightUsing() {
        m_tester.sql("select ^I^ from R1 right join R2 using(I)")
        .exception("Column 'I' is ambiguous")
        .pass();
    }

    public void testAmbiguous3WayUsing() {
        m_tester.sql("select i from R1 full join R2 using(i) full join RI1 using(^i^)")
        .exception("Column name 'I' in USING clause is not unique on one side of join")
        .pass();
    }

    public void testFunctionArg() {
        m_tester.sql("select sum(i) from R2").pass();

        m_tester.sql("select ^sum()^ from R2")
                .exception("Invalid number of arguments to function 'SUM'. Was expecting 1 arguments")
                .pass();
        m_tester.sql("select ^sum(i, ti)^ from R2")
                .exception("Invalid number of arguments to function 'SUM'. Was expecting 1 arguments")
                .pass();
    }

    public void testType() {
        m_tester.sql("select i from R2 where v > 1").pass();

        m_tester.sql("select i from R2 where ^NOT v^")
                .exception("Cannot apply 'NOT' to arguments of type 'NOT<VARCHAR\\(32\\)>'.*")
                .pass();
        m_tester.sql("select ^True or i^ from R2")
                .exception("Cannot apply 'OR' to arguments of type '<BOOLEAN> OR <INTEGER>'.*")
                .pass();
    }

    public void testGroupByAlias() {
        m_tester.sql("select i as foo, count(*) from R2 group by foo").pass();
    }

    public void testBangEqual() {
        m_tester.sql("select i from R1 where i != 3").pass();
    }

    // ENG-15222 allow null to appear as function parameter
    public void testNullAsFunctionParameter() {
        m_tester.sql("select abs(null) from R1").pass();
        m_tester.sql("select power(null, null) from R1").pass();
        m_tester.sql("select power(abs(null), abs(null)) from R1").pass();
        m_tester.sql("select abs(abs(abs(null))) from R1").pass();
    }

    // ENG-15222 allow null to appear as function parameter
    public void testQuestionMarkAsFunctionParameter() {
        m_tester.sql("select abs(?) from R1").pass();
        m_tester.sql("select power(?, ?) from R1").pass();
        m_tester.sql("select power(abs(?), abs(?)) from R1").pass();
        m_tester.sql("select abs(abs(abs(?))) from R1").pass();
    }

    // TODO ENG-15353: allow null to appear in operands
    public void testNullAsOperand() {
        //m_tester.sql("create table foo(i int, primary key (i + 1 + null));").pass();
        //m_tester.sql("create table foo(i int, unique (1 + null));").pass();
    }
}

