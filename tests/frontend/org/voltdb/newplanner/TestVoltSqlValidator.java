/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.newplanner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.sql.SqlNode;
import org.voltdb.catalog.org.voltdb.calciteadaptor.CatalogAdapter;
import org.voltdb.newplanner.rules.PlannerPhase;
import org.voltdb.types.CalcitePlannerType;

public class TestVoltSqlValidator extends VoltSqlValidatorTestCase {

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestVoltSqlValidator.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        setupValidator(CatalogAdapter.schemaPlusFromDatabase(getDatabase()));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testTableColumn() {
        // valid columns
        assertValid("select i from R2");
        assertValid("select pk from R3");
        // select a not exist column
        assertExceptionIsThrown("select ^bad^ from R2", "Column 'BAD' not found in any table");
        // select a not exist column from another table
        assertExceptionIsThrown("select ^pk^ from R2", "Column 'PK' not found in any table");
        // well, this one's error message is a little misleading. But it is the expected behavior for calcite validator
        assertExceptionIsThrown("select ^R3^.pk from R2", "Table 'R3' not found");
        // select a not exist table.column
        assertExceptionIsThrown("select R2.^bad^ from R2", "Column 'BAD' not found in table 'R2'");
        // not exist column in where clause
        assertExceptionIsThrown("select pk from R3 where ^bad^ < 0", "Column 'BAD' not found in any table");
        // not exist table.column in where clause
        assertExceptionIsThrown("select pk from R3 where R3.^bad^ < 0", "Column 'BAD' not found in table 'R3'");
    }

    public void testTableName() {
        // valid table
        assertValid("select * from R3");
        // not exist table
        assertExceptionIsThrown("select * from ^bad^", "Object 'BAD' not found");
    }

    public void testCaseSensitive() {
        assertValid("select pK from R3");
        assertValid("select * from r3");
    }

    public void testGroupBy() {
        assertValid("select i from R2 group by i");
        assertValid("select i + 1 from R2 group by i");
        assertValid("select i, count(*) from R2 group by i");

        assertExceptionIsThrown("select i, ^ti^ from R2 group by i",
                "Expression 'TI' is not being grouped");
    }

    public void testPartitionBy() {
        assertValid("select i, sum(i) over(partition by i + ti order by i) from R2");

        assertExceptionIsThrown("select i, sum(i) over(partition by ^i + v^ order by i) from R2",
                "(?s)Cannot apply '\\+' to arguments of type '<INTEGER> \\+ <VARCHAR\\(32\\)>'.*");
    }

    public void testFunctionArg() {
        assertValid("select sum(i) from R2");

        assertExceptionIsThrown("select ^sum()^ from R2",
                "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments");
        assertExceptionIsThrown("select ^sum(i, ti)^ from R2",
                "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments");
    }

    public void testType() {
        assertValid("select i from R2 where v > 1");

        assertExceptionIsThrown("select i from R2 where ^NOT v^",
                "Cannot apply 'NOT' to arguments of type 'NOT<VARCHAR\\(32\\)>'.*");
        assertExceptionIsThrown("select ^True or i^ from R2",
                "Cannot apply 'OR' to arguments of type '<BOOLEAN> OR <INTEGER>'.*");
    }

    public void testConverter() {
        SqlNode node = parseAndValidate("select i from R2");
        VoltSqlToRelConverter converter = VoltSqlToRelConverter.create(m_validator, CatalogAdapter.schemaPlusFromDatabase(getDatabase()));
        RelRoot root = converter.convertQuery(node, false, true);
        root = root.withRel(converter.decorrelate(node, root.rel));
        assertEquals("Root {kind: SELECT, rel: LogicalProject#1, rowType: RecordType(INTEGER I), fields: [<0, I>], collation: []}",
                root.toString());

        final HepProgram program = HepProgram.builder()
                .addRuleInstance(CalcMergeRule.INSTANCE)
                .addRuleInstance(FilterCalcMergeRule.INSTANCE)
                .addRuleInstance(FilterToCalcRule.INSTANCE)
                .addRuleInstance(ProjectCalcMergeRule.INSTANCE)
                .addRuleInstance(ProjectToCalcRule.INSTANCE)
                .addRuleInstance(ProjectMergeRule.INSTANCE)
                .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
                .build();

        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(root.rel);
        root = root.withRel(planner.findBestExp());
        System.out.println(RelOptUtil.toString(root.rel));
//        RelNode tNode = CalcitePlanner.transform(CalcitePlannerType.HEP, PlannerPhase.CALCITE_LOGICAL,
//                root.rel);
//        System.out.println(RelOptUtil.toString(tNode));
    }

}
