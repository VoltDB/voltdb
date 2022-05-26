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

import org.voltdb.plannerv2.rules.PlannerRules;

public class TestPhysicalIndexSelection extends Plannerv2TestCase {

    private PhysicalConversionRulesTester m_tester = new PhysicalConversionRulesTester();

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-indexselection-ddl.sql"), "testcalcite", false);
        init();
        m_tester.phase(PlannerRules.Phase.PHYSICAL_CONVERSION);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testUniqueWin() {
        // Unique Index RI5_UNIQUE_IND_I wins over the non-unique one RI5_IND_I
        m_tester.sql("SELECT * FROM RI5 where I = 8")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], " +
                        "expr#3=[8], expr#4=[=($t0, $t3)], proj#0..2=[{exprs}], $condition=[$t4], index=[RI5_UNIQUE_IND_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testUniqueWin1() {
        // Unique Index RI5_UNIQUE_IND_I wins over the non-unique one RI5_IND_I_II_III
        m_tester.sql("SELECT * FROM RI5 where I = 5 and II = 8 and III = 9")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], expr#3=[5], expr#4=[=($t0, $t3)], expr#5=[8], expr#6=[=($t1, $t5)], expr#7=[9], expr#8=[=($t2, $t7)], expr#9=[AND($t4, $t6, $t8)], proj#0..2=[{exprs}], $condition=[$t9], index=[RI5_UNIQUE_IND_I_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testUniqueWin2() {
        // Unique Index RI5_UNIQUE_IND_I wins over the non-unique one RI5_IND_I
        m_tester.sql("SELECT * FROM RI5 where I > 8")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], expr#3=[8], " +
                        "expr#4=[>($t0, $t3)], proj#0..2=[{exprs}], $condition=[$t4], index=[RI5_UNIQUE_IND_I_INVALIDGT1_0])\n")
                .pass();
    }

    public void testMultiColumnWin() {
        // Two column Index RI5_IND_II_III wins over the single column RI5_IND_III
        m_tester.sql("SELECT * FROM RI5 where II = 3 and III = 4")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], expr#3=[3], " +
                        "expr#4=[=($t1, $t3)], expr#5=[4], expr#6=[=($t2, $t5)], expr#7=[AND($t4, $t6)], " +
                        "proj#0..2=[{exprs}], $condition=[$t7], index=[RI5_IND_II_III_INVALIDEQ2_2])\n")
                .pass();
    }

    public void testMultiColumnWin1() {
        // Single column RI5_IND_III wins over not-fully covered Two column Index RI5_IND_II_III
        m_tester.sql("SELECT * FROM RI5 where II = 3")
        .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], expr#3=[3], " +
                "expr#4=[=($t1, $t3)], proj#0..2=[{exprs}], $condition=[$t4], index=[RI5_IND_II_INVALIDEQ1_1])\n")
        .pass();
    }

    public void testPartialWin() {
        // Partial RI5_IND_II_PART wins over aregular one RI5_IND_II
        m_tester.sql("SELECT * FROM RI5 where II = 3 and ABS(I) > 8")
                .transform("VoltPhysicalTableIndexScan(table=[[public, RI5]], expr#0..2=[{inputs}], expr#3=[3], " +
                        "expr#4=[=($t1, $t3)], expr#5=[ABS($t0)], expr#6=[8], expr#7=[>($t5, $t6)], expr#8=[AND($t4, $t7)], " +
                        "proj#0..2=[{exprs}], $condition=[$t8], index=[RI5_IND_II_PART_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testEng931Plan() {
        // IDX_1_HASH index is selected because it uses a greater number of key components
        // Note. "HASH" index is backed by the same Binary tree implementation
        m_tester.sql("select a from t where a = ? and b = ? and c = ? and d = ? " +
                "and e >= ? and e <= ?")
                .transform("VoltPhysicalTableIndexScan(table=[[public, T]], expr#0..4=[{inputs}], expr#5=[?0], expr#6=[=($t0, $t5)], expr#7=[?1], expr#8=[=($t1, $t7)], expr#9=[?2], expr#10=[=($t2, $t9)], expr#11=[?3], expr#12=[=($t3, $t11)], expr#13=[?4], expr#14=[>=($t4, $t13)], expr#15=[?5], expr#16=[<=($t4, $t15)], expr#17=[AND($t6, $t8, $t10, $t12, $t14, $t16)], A=[$t0], $condition=[$t17], index=[IDX_1_HASH_INVALIDEQ4_4])\n")
                .pass();
    }

    public void testEng931Plan1() {
        // cover2_TREE on t (a, b) exactly matches the ORDER collation
        m_tester.sql("select a from t order by a, b")
                .transform("VoltPhysicalCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "  VoltPhysicalTableIndexScan(table=[[public, T]], expr#0..4=[{inputs}], proj#0..4=[{exprs}], index=[COVER2_TREE_ASCEQ0_0])\n")
                .pass();
    }

    // This tests recognition of prefix parameters and constants to prefer an index that
    // would use a greater number of key components AND would give the desired ordering.
    // Volt favors IDX_B while Calcite picks VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_LOG
    // Both indexes provide the right ordering but the VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_LOG is
    // a unique one with a lesser cost
    public void testEng2541Plan() {
        m_tester.sql("select * from l where lname=? and b=0 order by id asc limit ?")
                .transform("VoltPhysicalLimit(limit=[?1], pusheddown=[false])\n" +
                        "  VoltPhysicalTableIndexScan(table=[[public, L]], expr#0..3=[{inputs}], " +
                        "expr#4=[?0], expr#5=[=($t1, $t4)], expr#6=[CAST($t3):INTEGER NOT NULL], expr#7=[0], " +
                        "expr#8=[=($t6, $t7)], expr#9=[AND($t5, $t8)], proj#0..3=[{exprs}], $condition=[$t9], " +
                        "index=[VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_LOG_ASCGTE1_1])\n")
                .pass();
    }

    // Volt Planner prefer an index that would use a greater number of key components
    // (DELETED_SINCE_IDX) even though it does not provide the desired ordering.
    // Calcite favors the VOLTDB_AUTOGEN_CONSTRAINT_IDX_ID index that does provide the right
    // ordering allowing the SORT to be dropped
    public void testEng4792PlanWithCompoundEQLTEOrderedByPK() {
        m_tester.sql("select id from a where deleted=? and updated_date <= ? order by id limit ?")
                .transform("VoltPhysicalLimit(limit=[?2], pusheddown=[false])\n" +
                        "  VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[?0], expr#4=[=($t1, $t3)], expr#5=[?1], expr#6=[<=($t2, $t5)], expr#7=[AND($t4, $t6)], ID=[$t0], $condition=[$t7])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, A]], expr#0..2=[{inputs}], proj#0..2=[{exprs}], index=[VOLTDB_AUTOGEN_CONSTRAINT_IDX_ID_ASCEQ0_0])\n")
                .pass();
    }

    //    // @TODO DECODE function is not supported yet
    //    public void testFixedPlanWithExpressionIndexAndAlias() {
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l aliased where b = ? and DECODE(a, null, 0, a) = 0 and id = ?")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l aliased, l where l.b = ? and DECODE(l.a, null, 0, l.a) = 0 and l.id = ? and l.lname = aliased.lname")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where x.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where x.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname")
    //        .transform("")
    //        .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and l.id = ? and l.lname = x.lname")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where x.b = ? and DECODE(l.a, null, 0, l.a) = 0 and x.id = ? and l.lname = x.lname")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and l.id = ? and l.lname = x.lname")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where x.b = ? and DECODE(l.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, l.a) = 0 and l.id = ? and l.lname = x.lname")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where x.b = ? and DECODE(l.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname")
    //        .transform("")
    //                .fail();
    //
    //        // Volt Planner picks DECODE_IDX3
    //        m_tester.sql("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, l.a) = 0 and x.id = ? and l.lname = x.lname")
    //        .transform("")
    //                .fail();
    //
    //    }

    public void testCaseWhenIndex1() {
        m_tester.sql("select * from l where CASE WHEN a > b THEN a ELSE b END > 8")
                .transform("VoltPhysicalTableIndexScan(table=[[public, L]], expr#0..3=[{inputs}], " +
                        "expr#4=[>($t2, $t3)], expr#5=[CASE($t4, $t2, $t3)], expr#6=[8], expr#7=[>($t5, $t6)], " +
                        "proj#0..3=[{exprs}], $condition=[$t7], index=[CASEWHEN_IDX1_INVALIDGT1_0])\n")
                .pass();
    }

    public void testCaseWhenIndex2() {
        m_tester.sql("select * from l WHERE CASE WHEN a < 10 THEN a*5 ELSE a + 5 END > 2")
                .transform("VoltPhysicalTableIndexScan(table=[[public, L]], expr#0..3=[{inputs}], expr#4=[10], " +
                        "expr#5=[<($t2, $t4)], expr#6=[5], expr#7=[*($t2, $t6)], expr#8=[+($t2, $t6)], " +
                        "expr#9=[CASE($t5, $t7, $t8)], expr#10=[2], expr#11=[>($t9, $t10)], proj#0..3=[{exprs}], " +
                        "$condition=[$t11], index=[CASEWHEN_IDX2_INVALIDGT1_0])\n")
                .pass();
    }

    public void testCaseWhenIndex3() {
        // TODO: Volt planner picks PK for deterministic order only (micro optimization)?
        m_tester.sql("select * from l WHERE CASE WHEN a < 10 THEN a*2 ELSE a + 5 END > 2")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[10], expr#5=[<($t2, $t4)], expr#6=[2], " +
                        "expr#7=[*($t2, $t6)], expr#8=[5], expr#9=[+($t2, $t8)], expr#10=[CASE($t5, $t7, $t9)], " +
                        "expr#11=[>($t10, $t6)], proj#0..3=[{exprs}], $condition=[$t11])\n" +
                        "  VoltPhysicalTableSequentialScan(table=[[public, L]], expr#0..3=[{inputs}], proj#0..3=[{exprs}])\n")
                .pass();
    }

    public void testPartialIndexNULLPredicate1() {
        // Volt planner picks Z_FULL_IDX_A
        m_tester.sql("select * from c where a > 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], proj#0..6=[{exprs}], $condition=[$t8], index=[Z_FULL_IDX_A_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexNULLPredicate2() {
        m_tester.sql("select * from c where a > 0 and e is NULL")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], expr#9=[IS NULL($t4)], expr#10=[AND($t8, $t9)], proj#0..6=[{exprs}], " +
                        "$condition=[$t10], index=[PARTIAL_IDX_NULL_E_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexNULLPredicate3() {
        m_tester.sql("select * from c where a > 0 and e is not NULL")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], expr#9=[IS NOT NULL($t4)], expr#10=[AND($t8, $t9)], proj#0..6=[{exprs}], " +
                        "$condition=[$t10], index=[A_PARTIAL_IDX_NOT_NULL_E_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexNULLPredicate4() {
        // Filter e = 0 does satisfy the partial index's predicate E IS NOT NULL
        // but this index predicate does not eliminate the need for the filter
        // and the partial index does not get any additional discount.
        // The cost for the A_PARTIAL_IDX_NOT_NULL_E and the Z_FULL_IDX_A is the same
        // but the number of rows returned by the partial index is less
        m_tester.sql("select * from c where a > 0 and e = 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], expr#9=[0], expr#10=[=($t4, $t9)], expr#11=[AND($t8, $t10)], " +
                        "proj#0..6=[{exprs}], $condition=[$t11], index=[A_PARTIAL_IDX_NOT_NULL_E_INVALIDGT1_0])\n")
                .pass();;
    }

    public void testPartialIndexNULLPredicate5() {
        // Filter e = 0 does satisfy the partial index's predicate E IS NOT NULL
        // but this index predicate does not eliminate the need for the filter
        // and the partial index does not get any additional discount.
        // The cost for the A_PARTIAL_IDX_NOT_NULL_E and the Z_FULL_IDX_A is the same
        // but the number of rows returned by the partial index is less
        m_tester.sql("select * from c where a > 0 and 0 = abs(e + b)")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], expr#9=[0], expr#10=[+($t4, $t1)], expr#11=[ABS($t10)], " +
                        "expr#12=[=($t9, $t11)], expr#13=[AND($t8, $t12)], proj#0..6=[{exprs}], $condition=[$t13], " +
                        "index=[A_PARTIAL_IDX_NOT_NULL_E_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexNULLPredicate6() {
        m_tester.sql("select * from c where a = 0 and e = 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[=($t0, $t7)], expr#9=[=($t4, $t7)], expr#10=[AND($t8, $t9)], proj#0..6=[{exprs}], " +
                        "$condition=[$t10], index=[A_PARTIAL_IDX_NOT_NULL_E_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testPartialIndexArbitraryPredicate1() {
        m_tester.sql("select * from c where f > 0 and (e > 0 or d < 5)")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t5, $t7)], expr#9=[>($t4, $t7)], expr#10=[5], expr#11=[<($t3, $t10)], " +
                        "expr#12=[OR($t9, $t11)], expr#13=[AND($t8, $t12)], proj#0..6=[{exprs}], $condition=[$t13], " +
                        "index=[PARTIAL_IDX_OR_EXPR_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexArbitraryPredicate2() {
        // ENG-15719
        // CREATE INDEX partial_idx_8 ON c (b) WHERE abs(a) > 0; PARTIAL_IDX_8
        m_tester.sql("SELECT COUNT(b) FROM c WHERE abs(a) > 0")
                .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT()], coordinator=[false], type=[serial])\n" +
                        "  VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], " +
                        "expr#7=[ABS($t0)], expr#8=[0], expr#9=[>($t7, $t8)], B=[$t1], $condition=[$t9], index=[PARTIAL_IDX_8_INVALIDGTE0_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateExactMatch1() {
        // CREATE INDEX partial_idx_or_expr ON c (a) where e > 0 or d < 5; -- expression trees differ Z_FULL_IDX_A
        // NOTE: Calcite picks A_PARTIAL_IDX_NOT_NULL_D_E on calcite_* branch;
        // but picks A_PARTIAL_IDX_NOT_NULL_E on master branch.
        m_tester.sql("select * from c where a > 0 and e > 0 or d < 5")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], expr#9=[>($t4, $t7)], expr#10=[AND($t8, $t9)], expr#11=[5], " +
                        "expr#12=[<($t3, $t11)], expr#13=[OR($t10, $t12)], proj#0..6=[{exprs}], $condition=[$t13], " +
                        "index=[A_PARTIAL_IDX_NOT_NULL_D_E_INVALIDGTE0_0])\n")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], expr#9=[>($t4, $t7)], expr#10=[AND($t8, $t9)], expr#11=[5], " +
                        "expr#12=[<($t3, $t11)], expr#13=[OR($t10, $t12)], proj#0..6=[{exprs}], $condition=[$t13], " +
                        "index=[A_PARTIAL_IDX_NOT_NULL_E_INVALIDGTE0_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateExactMatch2() {
        // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1; PARTIAL_IDX_1
        m_tester.sql("select * from c where abs(b) = 1 and abs(e) > 1")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], " +
                        "expr#7=[ABS($t1)], expr#8=[1], expr#9=[=($t7, $t8)], expr#10=[ABS($t4)], expr#11=[1], " +
                        "expr#12=[>($t10, $t11)], expr#13=[AND($t9, $t12)], proj#0..6=[{exprs}], $condition=[$t13], " +
                        "index=[PARTIAL_IDX_1_INVALIDEQ1_1])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateExactMatch3() {
        // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1; PARTIAL_IDX_1
        m_tester.sql("select * from c where abs(b) > 1 and 1 < abs(e)")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], " +
                        "expr#7=[ABS($t1)], expr#8=[1], expr#9=[>($t7, $t8)], expr#10=[ABS($t4)], expr#11=[<($t8, $t10)], " +
                        "expr#12=[AND($t9, $t11)], proj#0..6=[{exprs}], $condition=[$t12], index=[PARTIAL_IDX_1_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateExactMatch4() {
        // CREATE INDEX partial_idx_2 ON c (b) where d > 0 and d < 5;
        // CREATE INDEX partial_idx_3 ON c (b) where d > 0; is also a match
        // but has higher cost because of the extra post filter. PARTIAL_IDX_2
        m_tester.sql("select * from c where b > 0 and d > 0 and d < 5")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t1, $t7)], expr#9=[>($t3, $t7)], expr#10=[5], expr#11=[<($t3, $t10)], " +
                        "expr#12=[AND($t8, $t9, $t11)], proj#0..6=[{exprs}], $condition=[$t12], index=[PARTIAL_IDX_2_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateExactMatch5() {
        // CREATE INDEX partial_idx_2 ON c (b) where d > 0 and d < 5; PARTIAL_IDX_2
        m_tester.sql("select * from c where b > 0 and d < 5 and 0 < d")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t1, $t7)], expr#9=[5], expr#10=[<($t3, $t9)], expr#11=[<($t7, $t3)], " +
                        "expr#12=[AND($t8, $t10, $t11)], proj#0..6=[{exprs}], $condition=[$t12], index=[PARTIAL_IDX_2_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateExactMatch6() {
        // CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f; PARTIAL_IDX_4
        // Is it necessarily a poorer plan? Since even if partial_idx_4's condition is f > 0, it still picks Z_FULL_IDX_A.
        m_tester.sql("select * from c where a > 0 and b > 0 and f > 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], expr#8=[>($t0, $t7)], expr#9=[>($t1, $t7)], expr#10=[>($t5, $t7)], expr#11=[AND($t8, $t9, $t10)], proj#0..6=[{exprs}], $condition=[$t11], index=[PARTIAL_IDX_4_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateExactMatch7() {
        // CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f; PARTIAL_IDX_4
        m_tester.sql("select * from c where a > 0 and b > 0 and 0 < f")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], expr#8=[>($t0, $t7)], expr#9=[>($t1, $t7)], expr#10=[<($t7, $t5)], expr#11=[AND($t8, $t9, $t10)], proj#0..6=[{exprs}], $condition=[$t11], index=[PARTIAL_IDX_4_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateExactMatch8() {
        // CREATE INDEX partial_idx_5 ON c (b) where d > f; PARTIAL_IDX_5
        m_tester.sql("select * from c where b > 0 and d > f")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t1, $t7)], expr#9=[>($t3, $t5)], expr#10=[AND($t8, $t9)], proj#0..6=[{exprs}], " +
                        "$condition=[$t10], index=[PARTIAL_IDX_5_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateExactMatch9() {
        // CREATE INDEX partial_idx_5 ON c (b) where d > f; PARTIAL_IDX_5
        m_tester.sql("select * from c where b > 0 and f < d")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t1, $t7)], expr#9=[<($t5, $t3)], expr#10=[AND($t8, $t9)], proj#0..6=[{exprs}], " +
                        "$condition=[$t10], index=[PARTIAL_IDX_5_INVALIDGT1_0])\n")
                .pass();
    }

//  // @TODO Geo Indexes are not supported yet
//    public void testENG15616PenalizeGeoIndex() {
//        m_tester.sql("SELECT R.VCHAR_INLINE_MAX FROM R WHERE NOT R.TINY = R.TINY")
//        .transform("")
//        .pass();
//    }
//
//    public void testGeoIndex1() {
//        // Volt planner picks POLYPOINTSPOLY
//        m_tester.sql(
//                "select polys.point " +
//                "from polypoints polys " +
//                "where contains(polys.poly, ?)")
//        .transform("")
//        .pass();
//    }
//
//    public void testGeoIndex2() {
//        // Volt planner picks POLYPOINTSPOLY
//        m_tester.sql(
//                "select polys.poly, points.point " +
//                        "from polypoints polys, polypoints points " +
//                        "where contains(polys.poly, points.point)")
//        .transform("")
//        .pass();
//    }
//
//    public void testGeoIndex3() {
//        // Volt planner picks POLYPOINTSPOLY
//        m_tester.sql(
//                "select polys.point " +
//                "from polypoints polys " +
//                "where contains(polys.poly, ?)")
//        .transform("")
//        .pass();
//    }

    // This tests recognition of a complex expression value
    // -- an addition -- used as an indexable join key's search key value.
    // Some time ago, this would throw a casting error in the planner.
    public void testEng3850ComplexIndexablePlan() {
        m_tester.sql("select id from a, t where a.id < (t.a + ?)")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], ID=[$t1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[<($1, +($0, ?0))], joinType=[inner], " +
                        "innerIndex=[VOLTDB_AUTOGEN_CONSTRAINT_IDX_ID])\n" +
                        "    VoltPhysicalCalc(expr#0..4=[{inputs}], A=[$t0])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, T]], expr#0..4=[{inputs}], proj#0..4=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], ID=[$t0])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, A]], expr#0..2=[{inputs}], " +
                        "proj#0..2=[{exprs}], index=[VOLTDB_AUTOGEN_CONSTRAINT_IDX_ID_INVALIDLT1_0])\n")
                .pass();
    }

    public void testParameterizedQueryPartialIndex1() {
        // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
        m_tester.sql("select * from c where a > 0 and e = ?")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], expr#9=[?0], expr#10=[=($t4, $t9)], expr#11=[AND($t8, $t10)], " +
                        "proj#0..6=[{exprs}], $condition=[$t11], index=[A_PARTIAL_IDX_NOT_NULL_E_INVALIDGT1_0])\n")
                .pass();
    }

    public void testParameterizedQueryPartialIndex2() {
        // CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f; - not selected because of the parameter
        m_tester.sql("select * from c where a > 0 and b > 0 and ? < f")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], expr#9=[>($t1, $t7)], expr#10=[?0], expr#11=[<($t10, $t5)], " +
                        "expr#12=[AND($t8, $t9, $t11)], proj#0..6=[{exprs}], $condition=[$t12], index=[Z_FULL_IDX_A_INVALIDGT1_0])\n")
                .pass();
    }

    public void testParameterizedQueryPartialIndex3() {
        // Volt gives the same estimates to the Z_FULL_IDX_A and A_PARTIAL_IDX_NOT_NULL_E
        // (Z_FULL_IDX_A is added as a micro-optimization) and simply picks the first one
        // Calcite favors A_PARTIAL_IDX_NOT_NULL_E because it's the only index
        // available (no micro-optimization phase)
        // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1; not selected because of the parameter
        m_tester.sql("select * from c where abs(b) = 1 and abs(e) > ?")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[ABS($t1)], expr#8=[1], expr#9=[=($t7, $t8)], expr#10=[ABS($t4)], expr#11=[?0], expr#12=[>($t10, $t11)], expr#13=[AND($t9, $t12)], proj#0..6=[{exprs}], $condition=[$t13], index=[A_PARTIAL_IDX_NOT_NULL_E_INVALIDGTE0_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateNonExactMatch1() {
        // TODO: At the moment an index filter must exactly match a
        // query filter expression (or sub-expression) to be selected

        // Volt gives the same estimates to the Z_FULL_IDX_A and A_PARTIAL_IDX_NOT_NULL_E
        // (Z_FULL_IDX_A is added as a micro-optimization) and simply picks the first one
        // Calcite favors A_PARTIAL_IDX_NOT_NULL_E because it's the only index
        // available (no micro-optimization phase)
        // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1; Not exact match
        m_tester.sql("select * from c where abs(b) > 1 and 2 <  abs(e)")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[ABS($t1)], expr#8=[1], expr#9=[>($t7, $t8)], expr#10=[2], expr#11=[ABS($t4)], expr#12=[<($t10, $t11)], expr#13=[AND($t9, $t12)], proj#0..6=[{exprs}], $condition=[$t13], index=[A_PARTIAL_IDX_NOT_NULL_E_INVALIDGTE0_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateNonExactMatch2() {
        // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1; Not exact match
        m_tester.sql("select * from c where abs(b) > 1 and 1 <  abs(e)")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[ABS($t1)], expr#8=[1], expr#9=[>($t7, $t8)], expr#10=[ABS($t4)], expr#11=[<($t8, $t10)], expr#12=[AND($t9, $t11)], proj#0..6=[{exprs}], $condition=[$t12], index=[PARTIAL_IDX_1_INVALIDGT1_0])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateNonExactMatch3() {
        // Volt - Z_FULL_IDX_A (micro-optimization, Calcite - Sequential scan
        // CREATE INDEX partial_idx_3 ON c (b) where d > 0; Not exact match
        m_tester.sql("select * from c where b > 0 and d > 3")
                .transform("VoltPhysicalCalc(expr#0..6=[{inputs}], expr#7=[0], expr#8=[>($t1, $t7)], expr#9=[3], " +
                        "expr#10=[>($t3, $t9)], expr#11=[AND($t8, $t10)], proj#0..6=[{exprs}], $condition=[$t11])\n" +
                        "  VoltPhysicalTableSequentialScan(table=[[public, C]], expr#0..6=[{inputs}], proj#0..6=[{exprs}])\n")
                .pass();
    }

    public void testPartialIndexComparisonPredicateNonExactMatch4() {
        // Volt - Z_FULL_IDX_A (micro-optimization, Calcite - Sequential scan
        // CREATE INDEX partial_idx_5 ON c (b) where d > f;  Not exact match
        m_tester.sql("select * from c where b > 0 and f + 1 < d")
                .transform("VoltPhysicalCalc(expr#0..6=[{inputs}], expr#7=[0], expr#8=[>($t1, $t7)], expr#9=[1], " +
                        "expr#10=[+($t5, $t9)], expr#11=[<($t10, $t3)], expr#12=[AND($t8, $t11)], proj#0..6=[{exprs}], " +
                        "$condition=[$t12])\n" +
                        "  VoltPhysicalTableSequentialScan(table=[[public, C]], expr#0..6=[{inputs}], proj#0..6=[{exprs}])\n")
                .pass();
    }

    public void testPartialIndexPredicateOnly1() {
        // Partial index can be used solely to eliminate a post-filter
        // even when the indexed columns are irrelevant

        // CREATE INDEX partial_idx_3 ON c (b)  where d > 0;
        m_tester.sql("select * from c where d > 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t3, $t7)], proj#0..6=[{exprs}], $condition=[$t8], index=[PARTIAL_IDX_3_INVALIDGTE0_0])\n")
                .pass();
    }

    public void testPartialIndexPredicateOnly2() {
        // CREATE UNIQUE INDEX z_full_idx_a ON c (a); takes precedence over the partial_idx_3
        // because indexed column (A) is part of the WHERE expressions
        m_tester.sql("select * from c where d > 0 and a < 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t3, $t7)], expr#9=[<($t0, $t7)], expr#10=[AND($t8, $t9)], proj#0..6=[{exprs}], " +
                        "$condition=[$t10], index=[Z_FULL_IDX_A_INVALIDLT1_0])\n")
                .pass();
    }

    public void testPartialIndexPredicateOnly3() {
        // CREATE INDEX partial_idx_3 ON c (b)  where d > 0;
        m_tester.sql("select c.d from a join c on a.id = c.e and d > 0")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], D=[$t1])\n" +
                        "  VoltPhysicalNestLoopIndexJoin(condition=[AND(=($0, $2), $3)], joinType=[inner], innerIndex=[PARTIAL_IDX_3])\n" +
                        "    VoltPhysicalCalc(expr#0..2=[{inputs}], ID=[$t0])\n" +
                        "      VoltPhysicalTableSequentialScan(table=[[public, A]], expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n" +
                        "    VoltPhysicalCalc(expr#0..6=[{inputs}], expr#7=[0], expr#8=[>($t3, $t7)], D=[$t3], E=[$t4], $f7=[$t8])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], proj#0..6=[{exprs}], index=[PARTIAL_IDX_3_INVALIDGTE0_0])\n")
                .pass();
    }

    public void testSkipNullPartialIndex1() {
        //CREATE INDEX partial_idx_7 ON c (g) where g is not null;
        // skipNull predicate is redundant and eliminated
        m_tester.sql("select count(*) from c where g > 0")
                .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT()], coordinator=[false], type=[serial])\n" +
                        "  VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t6, $t7)], $f0=[$t7], $condition=[$t8], index=[PARTIAL_IDX_7_INVALIDGT1_0])\n")
                .pass();
    }

    public void testSkipNullPartialIndex2() {
        //CREATE INDEX partial_idx_7 ON c (g) where g is not null;
        // skipNull predicate is redundant and eliminated
        m_tester.sql("select e from c where g > 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t6, $t7)], E=[$t4], $condition=[$t8], index=[PARTIAL_IDX_7_INVALIDGT1_0])\n")
                .pass();
    }

    public void testSkipNullPartialIndex3() {
        //CREATE INDEX partial_idx_6 ON c (g) where g < 0;
        // skipNull predicate is redundant and eliminated
        m_tester.sql("select count(*) from c where g < 0")
                .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT()], coordinator=[false], type=[serial])\n" +
                        "  VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[<($t6, $t7)], $f0=[$t7], $condition=[$t8], index=[PARTIAL_IDX_6_INVALIDLT1_0])\n")
                .pass();
    }

    public void testSkipNullPartialIndex4() {
        //CREATE INDEX partial_idx_6 ON c (g) where g < 0;
        // skipNull predicate is redundant and eliminated
        m_tester.sql("select g from c where g < 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[<($t6, $t7)], G=[$t6], $condition=[$t8], index=[PARTIAL_IDX_6_INVALIDLT1_0])\n")
                .pass();
    }

    public void testSkipNullPartialIndex5() {
        // CREATE UNIQUE INDEX z_full_idx_a ON c (a);
        // skipNull is required - full index
        m_tester.sql("select count(*) from c where a > 0")
                .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT()], coordinator=[false], type=[serial])\n" +
                        "  VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], $f0=[$t7], $condition=[$t8], index=[Z_FULL_IDX_A_INVALIDGT1_0])\n")
                .pass();
    }

    public void testSkipNullPartialIndex6() {
        // CREATE UNIQUE INDEX z_full_idx_a ON c (a);
        // skipNull is required - full index
        m_tester.sql("select e from c where a > 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t0, $t7)], E=[$t4], $condition=[$t8], index=[Z_FULL_IDX_A_INVALIDGT1_0])\n")
                .pass();
    }

    public void testSkipNullPartialIndex7() {
        // CREATE INDEX partial_idx_3 ON c (b) where d > 0;
        // skipNull is required - index predicate is not NULL-rejecting for column B
        m_tester.sql("select count(*) from c where b > 0 and d > 0")
                .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT()], coordinator=[false], type=[serial])\n" +
                        "  VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], expr#7=[0], " +
                        "expr#8=[>($t1, $t7)], expr#9=[>($t3, $t7)], expr#10=[AND($t8, $t9)], $f0=[$t7], $condition=[$t10], " +
                        "index=[PARTIAL_IDX_3_INVALIDGT1_0])\n")
                .pass();
    }

    public void testSkipNullPartialIndex8() {
        // CREATE INDEX partial_idx_3 ON c (b) where d > 0;
        // skipNull is required - index predicate is not NULL-rejecting for column B
        m_tester.sql("select b from c where b > 0 and d > 0")
                .transform("VoltPhysicalTableIndexScan(table=[[public, C]], expr#0..6=[{inputs}], " +
                        "expr#7=[0], expr#8=[>($t1, $t7)], expr#9=[>($t3, $t7)], expr#10=[AND($t8, $t9)], B=[$t1], " +
                        "$condition=[$t10], index=[PARTIAL_IDX_3_INVALIDGT1_0])\n")
                .pass();
    }

}
