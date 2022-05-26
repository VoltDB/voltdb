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

/**
 * Test if we can correctly throw an exception if the query is determined to be multi-partitioned.
 * We have this test because we only have SP query support in our first release of calcite planner.
 * And it will go away when we can handle MP query in calcite.
 */
public class TestMPQueryFallbackRules extends Plannerv2TestCase {

    private MPFallbackTester m_tester = new MPFallbackTester();

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        m_tester.phase(PlannerRules.Phase.MP_FALLBACK);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    // when we only deal with replicated table, we will always have a SP query.
    public void testReplicated() {
        m_tester.sql("select * from R2").pass();

        m_tester.sql("select i, si from R1").pass();

        m_tester.sql("select i, si from R1 where si = 9").pass();
    }

    public void testPartitionedWithFilter() {
        // equal condition on partition key
        m_tester.sql("select * from P1 where i = 1")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        // equal condition involves SQL functions
        m_tester.sql("SELECT P4.j FROM P4, P5 WHERE P5.i = P4.i AND P4.i = LOWER('foO') || 'bar'")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], J=[$t1])\n" +
                    "  VoltLogicalJoin(condition=[=($2, $0)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=['foO'], expr#3=[LOWER($t2)], expr#4=['bar'], expr#5=[||($t3, $t4)], expr#6=[CAST($t5):VARCHAR(8) CHARACTER SET \"UTF-8\" COLLATE \"UTF-8$en_US$primary\" NOT NULL], expr#7=[=($t0, $t6)], proj#0..1=[{exprs}], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P4]])\n" +
                    "    VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t1])\n" +
                    "      VoltLogicalTableScan(table=[[public, P5]])\n")
        .pass();

        // equal condition on partition key with ORs
        m_tester.sql("select si, v from P1 where 7=si or i=2")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[7], expr#7=[CAST($t1):INTEGER], expr#8=[=($t6, $t7)], expr#9=[2], expr#10=[=($t0, $t9)], expr#11=[OR($t8, $t10)], SI=[$t1], V=[$t5], $condition=[$t11])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        // equal condition on partition key with ORs and ANDs
        m_tester.sql("select si, v from P1 where 7>si or (i=2 and ti<3)")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[7], expr#7=[>($t6, $t1)], expr#8=[2], expr#9=[=($t0, $t8)], expr#10=[3], expr#11=[<($t2, $t10)], expr#12=[AND($t9, $t11)], expr#13=[OR($t7, $t12)], SI=[$t1], V=[$t5], $condition=[$t13])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
        m_tester.sql("select si, v from P1 where 7>si and (i=2 and ti<3)")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[7], expr#7=[>($t6, $t1)], expr#8=[2], expr#9=[=($t0, $t8)], expr#10=[3], expr#11=[<($t2, $t10)], expr#12=[AND($t7, $t9, $t11)], SI=[$t1], V=[$t5], $condition=[$t12])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        // equal condition with some expression that always TRUE
        m_tester.sql("select si, v from P1 where (7=si and i=2) and 1=1")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[7], expr#7=[CAST($t1):INTEGER], expr#8=[=($t6, $t7)], expr#9=[2], expr#10=[=($t0, $t9)], expr#11=[AND($t8, $t10)], SI=[$t1], V=[$t5], $condition=[$t11])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        // equal condition with some expression that always FALSE
        m_tester.sql("select si, v from P1 where (7=si and i=2) and 1=2")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[7], expr#7=[CAST($t1):INTEGER], expr#8=[=($t6, $t7)], expr#9=[2], expr#10=[=($t0, $t9)], expr#11=[1], expr#12=[=($t11, $t9)], expr#13=[AND($t8, $t10, $t12)], SI=[$t1], V=[$t5], $condition=[$t13])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testPartitionedWithNotFilter() {
        // when comes to NOT operator, we need to decide if the complement of its
        // operand is single-partitioned
        m_tester.sql("select * from P1 where NOT (i <> 15 OR si = 16)")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[15], expr#7=[=($t0, $t6)], expr#8=[CAST($t1):INTEGER], expr#9=[16], expr#10=[<>($t8, $t9)], expr#11=[AND($t7, $t10)], proj#0..5=[{exprs}], $condition=[$t11])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        m_tester.sql("select * from P1 where NOT si <> 15")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[15], expr#8=[=($t6, $t7)], proj#0..5=[{exprs}], $condition=[$t8])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testJoinReplicatedTables() {
        m_tester.sql("select R1.i, R2.v from R1, R2 " +
                "where R2.si = R1.i and R2.v = 'foo'").pass();

        m_tester.sql("select R1.i, R2.v from R1 inner join R2 " +
                "on R2.si = R1.i where R2.v = 'foo'").pass();

        m_tester.sql("select R2.si, R1.i from R1 inner join " +
                "R2 on R2.i = R1.si where R2.v = 'foo' and R1.si > 4 and R1.ti > R2.i").pass();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si where R1.I + R2.ti = 5").pass();
    }

    public void testOuterJoinPartitionedTable1() {
        m_tester.sql("select P1.i, P2.v FROM P1 full join P2 " +
                "on P1.si = P2.i AND P1.i = 34 and P2.i = 45")
        .fail();

        m_tester.sql("select P1.i, P2.v FROM P1 left join P2 " +
                "on P1.si = P2.i")
        .fail();

        m_tester.sql("select P1.i, P2.v FROM P1 right join P2 " +
                "on P1.si = P2.i")
        .fail();
    }

    public void testOuterJoinPartitionedTable2() {
        m_tester.sql("select p1.i from r1 left join p1 on p1.si = r1.i and p1.i = 8")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I0=[$t1])\n" +
                    "  VoltLogicalJoin(condition=[=($2, $0)], joinType=[left])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[8], expr#8=[=($t0, $t7)], I=[$t0], SI0=[$t6], $condition=[$t8])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testOuterJoinPartitionedTable3() {
        m_tester.sql("select p1.i from r1 full join p1 on p1.si = r1.i and p1.i = 8")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I0=[$t1])\n" +
                    "  VoltLogicalJoin(condition=[AND(=($2, $0), =($1, 8))], joinType=[full])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], I=[$t0], SI0=[$t6])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testOuterJoinPartitionedTable4() {
        m_tester.sql("select p1.i from p1 right join r1 on p1.si = r1.i and p1.i = 8")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($1, $2)], joinType=[right])\n" +
                    "    VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[8], expr#8=[=($t0, $t7)], I=[$t0], SI0=[$t6], $condition=[$t8])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n")
        .pass();
    }

    public void testOuterJoinPartitionedTable5() {
        m_tester.sql("select p1.i from p1 full join r1 on p1.si = r1.i and p1.i = 8")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[AND(=($1, $2), =($0, 8))], joinType=[full])\n" +
                    "    VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], I=[$t0], SI0=[$t6])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n")
        .pass();
    }

    public void testOuterJoinPartitionedTable6() {
        m_tester.sql("select p1.i from p1 full join p2 on p1.i = p2.i and p1.i = 8")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[AND(=($0, $1), =($0, 8))], joinType=[full])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();
    }

    public void testOuterJoinPartitionedTable7() {
        m_tester.sql("select p1.i from p1 left join p2 on p1.i = p2.i and p1.i = 8")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[AND(=($0, $1), =($0, 8))], joinType=[left])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();
    }

    public void testOuterJoinPartitionedTable8() {
        m_tester.sql("select p1.i from p1 right join p2 on p1.i = p2.i and p1.i = 8")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[right])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[8], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();
    }

    public void testOuterJoinPartitionedTableWithSort() {
        m_tester.sql("select p1.i from r1 full join p1 on p1.si = r1.i order by p1.i")
        .transform("VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                    "  VoltLogicalCalc(expr#0..2=[{inputs}], I0=[$t1])\n" +
                    "    VoltLogicalJoin(condition=[=($2, $0)], joinType=[full])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "      VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "        VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], I=[$t0], SI0=[$t6])\n" +
                    "          VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testJoinPartitionedTable() {
        // Two partitioned table joined that results in SP
        m_tester.sql("select P1.i, P2.v FROM P1, P2 " +
                "WHERE P1.i = P2.i AND P2.i = 34")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0], V=[$t2])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[34], expr#7=[=($t0, $t6)], I=[$t0], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select P1.i, P2.v FROM P1 INNER JOIN P2 " +
                "ON P1.i = P2.i AND P2.i = 34")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0], V=[$t2])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[34], expr#7=[=($t0, $t6)], I=[$t0], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select P1.i, P2.v FROM P1 INNER JOIN P2 " +
                "USING(i) WHERE P2.i = 34")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0], V=[$t2])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[34], expr#7=[=($t0, $t6)], I=[$t0], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        // Two partitioned table joined that results in MP (or sometimes un-plannable query):
        m_tester.sql("select P1.i, P2.v from P1, P2 " +
                "where P2.si = P1.i and P2.v = 'foo'").fail();

        m_tester.sql("select P1.i, P2.v from P1 inner join P2 " +
                "ON P2.si = P1.i WHERE P2.v = 'foo'").fail();

        // when join a partitioned table with a replicated table,
        // if the filtered result on the partitioned table is not SP, then the query is MP
        m_tester.sql("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.i > 3")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], V=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=(CAST($2):INTEGER, $0)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[>($t0, $t6)], proj#0..1=[{exprs}], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and (P2.i > 3 or P2.i =1)")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], V0=[$t2])\n" +
                    "  VoltLogicalJoin(condition=[=($3, $0)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[3], expr#8=[>($t0, $t7)], expr#9=[1], expr#10=[=($t0, $t9)], expr#11=[OR($t8, $t10)], I=[$t0], V=[$t5], SI0=[$t6], $condition=[$t11])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        // when join a partitioned table with a replicated table,
        // if the filtered result on the partitioned table is SP, then the query is SP
        m_tester.sql("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.i = 3")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], V=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=(CAST($2):INTEGER, $0)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[3], expr#7=[=($t0, $t6)], proj#0..1=[{exprs}], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i where P2.i =3")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], V=[$t2])\n" +
                    "  VoltLogicalJoin(condition=[=($3, $0)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[3], expr#8=[=($t0, $t7)], I=[$t0], V=[$t5], SI0=[$t6], $condition=[$t8])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i where P2.i =3 and P2.v = 'bar'")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], V=[$t2])\n" +
                    "  VoltLogicalJoin(condition=[=($3, $0)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[3], expr#8=[=($t0, $t7)], expr#9=['bar'], expr#10=[=($t5, $t9)], expr#11=[AND($t8, $t10)], I=[$t0], V=[$t5], SI0=[$t6], $condition=[$t11])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        // when join a partitioned table with a replicated table,
        // if the join condition can filter the partitioned table in SP, then the query is SP
        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and P2.i =3")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], V0=[$t2])\n" +
                    "  VoltLogicalJoin(condition=[=($3, $0)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[3], expr#8=[=($t0, $t7)], I=[$t0], V=[$t5], SI0=[$t6], $condition=[$t8])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and P2.i =3 where P2.v = 'bar'")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], V0=[$t2])\n" +
                    "  VoltLogicalJoin(condition=[=($3, $0)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[3], expr#8=[=($t0, $t7)], expr#9=['bar'], expr#10=[=($t5, $t9)], expr#11=[AND($t8, $t10)], I=[$t0], V=[$t5], SI0=[$t6], $condition=[$t11])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();
    }

    public void testThreeWayJoinWithoutFilter() {
        // three-way join on replicated tables, SP
        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si inner join " +
                "R3 on R2.i = R3.ii")
        .transform("VoltLogicalCalc(expr#0..4=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($2, $4)], joinType=[inner])\n" +
                    "    VoltLogicalJoin(condition=[=($1, $3)], joinType=[inner])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, R2]])\n" +
                    "    VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, R3]])\n")
        .pass();

        // all partitioned, MP
        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.si inner join " +
                "P3 on P2.i = P3.i").fail();

        // one of them partitioned, MP
        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.si inner join " +
                "R3 on R2.i = R3.ii")
        .transform("VoltLogicalCalc(expr#0..4=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($2, $4)], joinType=[inner])\n" +
                    "    VoltLogicalJoin(condition=[=($1, $3)], joinType=[inner])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, R2]])\n" +
                    "    VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, R3]])\n")
        .pass();

        m_tester.sql("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.si inner join " +
                "R3 on P2.i = R3.ii")
        .transform("VoltLogicalCalc(expr#0..4=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($2, $4)], joinType=[inner])\n" +
                    "    VoltLogicalJoin(condition=[=($1, $3)], joinType=[inner])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, P2]])\n" +
                    "    VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, R3]])\n")
        .pass();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si inner join " +
                "P3 on R2.i = P3.i")
        .transform("VoltLogicalCalc(expr#0..4=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($2, $4)], joinType=[inner])\n" +
                    "    VoltLogicalJoin(condition=[=($1, $3)], joinType=[inner])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, R2]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P3]])\n")
        .pass();

        // two of them partitioned, MP
        m_tester.sql("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.si inner join " +
                "P3 on P2.i = P3.i")
        .transform("VoltLogicalCalc(expr#0..4=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($2, $4)], joinType=[inner])\n" +
                    "    VoltLogicalJoin(condition=[=($1, $3)], joinType=[inner])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, P2]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P3]])\n")
        .pass();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.si inner join " +
                "P3 on R2.i = P3.i")
        .fail();

        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.si inner join " +
                "R3 on P2.i = R3.ii")
        .fail();

        // this is tricky. Note `P1.si = R2.i` will produce a Calc with CAST.
        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.i = R3.ii")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($1, $2)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0], I0=[$t2])\n" +
                    "      VoltLogicalJoin(condition=[=($1, $2)], joinType=[inner])\n" +
                    "        VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], I=[$t0], SI0=[$t6])\n" +
                    "          VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "        VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "          VoltLogicalTableScan(table=[[public, R2]])\n" +
                    "    VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, R3]])\n")
        .pass();
    }

    public void testNLNotDistinct() {
      m_tester.sql("SELECT P1.I FROM P1 INNER JOIN P2 ON P1.I IS NOT DISTINCT FROM P2.I")
      .transform("VoltLogicalCalc(expr#0..1=[{inputs}], I=[$t0])\n" +
                  "  VoltLogicalJoin(condition=[CASE(IS NULL($0), IS NULL($1), IS NULL($1), IS NULL($0), =($0, $1))], joinType=[inner])\n" +
                  "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                  "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                  "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                  "      VoltLogicalTableScan(table=[[public, P2]])\n")
      .pass();
  }

    public void testMultiWayJoinWithFilter() {
        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo'").pass();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where P1.si > 4 and R3.vc <> 'foo'").pass();

        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo'").fail();

        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo' and P2.i = 5").fail();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "P3 on R2.v = P3.v where R1.si > 4 and P3.si = 6").pass();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i AND P1.i = 4 inner join " +
                "R3 on R2.v = R3.vc WHERE R3.vc <> 'foo'").pass();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2 ON P1.si = R2.i AND P1.i = 4 inner join " +
                "P2 ON R2.i = P2.v WHERE P2.i = 4 AND P2.v <> 'foo'").pass();

        m_tester.sql("select P1.si, P2.si, P3.si FROM P1, P2, P3 WHERE " +
                "P1.i = 0 AND P2.i = 0 and P3.i = 0").pass();

        m_tester.sql("select P1.si, P2.si, P3.si FROM P1, P2, P3 WHERE " +
                "P1.i = 0 AND P2.i = 0 and P3.i = 1").fail();

        m_tester.sql("select P1.si, R1.i, P2.si, P3.si FROM P1, R1, P2, P3 WHERE " +
                "P1.i = 0 AND P2.i = 0 and P3.i = 0").pass();

        m_tester.sql("select P1.si, P2.si, P3.si FROM P1, P2, P3 WHERE " +
                "P1.i = 0 AND P2.i = 0 OR P3.i = 0").fail();

        m_tester.sql("select P1.si, P2.si FROM P1 full join P2 on " +
                "P2.i = 0 ").fail();

        m_tester.sql("select P1.si, P2.si FROM P1, P2 WHERE P2.i = 0 ").fail();

        m_tester.sql("select P1.si, P2.si FROM P2, P1 WHERE P2.i = 0 ").fail();

        m_tester.sql("select P1.si, P2.si, P3.si FROM P1, P2, P3 WHERE P2.i = 0 AND P3.i = 0").fail();


        m_tester.sql("select P1.si, P2.si, P3.si FROM P1, P2, P3 WHERE " +
                "P1.i = 0 AND P2.i = 0 and P3.i = 1").fail();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2 ON P1.si = R2.i AND P1.i = 4 inner join " +
                "P2 ON R2.i = P2.v WHERE P2.i = 4 OR P2.v <> 'foo'").fail();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo'").pass();

        m_tester.sql("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo' and P2.i = 5").pass();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "P3 on R2.v = P3.v where R1.si > 4 and P3.i = 6").pass();
    }

    public void testSubqueriesJoin() {
        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from R2 where f = 30.3) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], V=[$t1], V0=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], expr#8=[3], expr#9=[=($t0, $t8)], expr#10=[AND($t7, $t9)], I=[$t0], V=[$t5], $condition=[$t10])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[30.3], expr#7=[=($t4, $t6)], I=[$t0], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n")
        .pass();

        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from P1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where f = 30.3) as t2 "
                + "on t1.i = t2.si")
        .fail();

        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from P1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where f = 30.3) as t2 "
                + "on t1.i = t2.i")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], V=[$t1], V0=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], I=[$t0], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[30.3], expr#7=[=($t4, $t6)], I=[$t0], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where f = 30.3) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], V=[$t1], V0=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], expr#8=[3], expr#9=[=($t0, $t8)], expr#10=[AND($t7, $t9)], I=[$t0], V=[$t5], $condition=[$t10])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[30.3], expr#7=[=($t4, $t6)], I=[$t0], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where i = 303) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], V=[$t1], V0=[$t3])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], expr#8=[3], expr#9=[=($t0, $t8)], expr#10=[AND($t7, $t9)], I=[$t0], V=[$t5], $condition=[$t10])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[303], expr#7=[=($t0, $t6)], I=[$t0], V=[$t5], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select t1.v, t2.v from "
                + "  (select si, v from R1 where v = 'foo') t1, "
                + "  (select si, v from P2 where i = 303) t2")
        .transform("VoltLogicalJoin(condition=[true], joinType=[inner])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], V=[$t5], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[303], expr#7=[=($t0, $t6)], V=[$t5], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select t1.v, t2.v from "
                + "  (select si, v from P1 where i = 303) t1, "
                + "  (select si, v from P2 where i = 303) t2")
        .transform("VoltLogicalJoin(condition=[true], joinType=[inner])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[303], expr#7=[=($t0, $t6)], V=[$t5], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[303], expr#7=[=($t0, $t6)], V=[$t5], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select t1.v, t2.v from "
                + "  (select si, v from P1 where i = 300) t1, "
                + "  (select si, v from P2 where i = 303) t2").fail();

        // Nice example where Calcite takes advantage of the literal FALSE join condition
        // and makes the join trivial returning nothing
        m_tester.sql("select t1.v, t2.v from "
                + "  (select si, v from P1 where i = 300) t1, "
                + "  (select si, v from P2 where i = 303) t2 where FALSE")
        .transform("VoltLogicalJoin(condition=[true], joinType=[inner])\n" +
                    "  VoltLogicalValues(tuples=[[]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[303], expr#7=[=($t0, $t6)], V=[$t5], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select t1.v, t2.v from "
                + "  (select i, v from R1 where v = 'foo') t1 inner join "
                + "  (select v, i from P2 where i = 303) t2 "
                + " on t1.i = t2.i where t1.i = 4")
        .transform("VoltLogicalCalc(expr#0..3=[{inputs}], V=[$t1], V0=[$t2])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $3)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=['foo'], expr#7=[=($t5, $t6)], expr#8=[4], expr#9=[=($t0, $t8)], expr#10=[AND($t7, $t9)], I=[$t0], V=[$t5], $condition=[$t10])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[303], expr#7=[=($t0, $t6)], V=[$t5], I=[$t0], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        m_tester.sql("select RI1.bi from RI1, (select I from P2 where I = 5 order by I) P22 where RI1.i = P22.I")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], BI=[$t1])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], BI=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, RI1]])\n" +
                    "    VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "        VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        // I think Volt plan is better - it drops useless ORDER BY from subquery
        //  RETURN RESULTS TO STORED PROCEDURE
        //    RECEIVE FROM ALL PARTITIONS
        //      SEND PARTITION RESULTS TO COORDINATOR
        //        NESTLOOP INDEX INNER JOIN
        //          inline INDEX SCAN of "RI1" using its primary key index uniquely match (I = column#0)
        //          SEQUENTIAL SCAN of "P2 (P22)"
        m_tester.sql("select RI1.bi from RI1, (select SI from P2 order by I) P22 where RI1.i = P22.SI")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], BI=[$t1])\n" +
                    "  VoltLogicalJoin(condition=[=($0, CAST($2):INTEGER)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], BI=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, RI1]])\n" +
                    "    VoltLogicalCalc(expr#0..1=[{inputs}], SI=[$t0])\n" +
                    "      VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                    "        VoltLogicalExchange(distribution=[hash[1]])\n" +
                    "          VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], I=[$t0])\n" +
                    "            VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        // Same here. ORDER BY is useless
        m_tester.sql("select RI1.bi from RI1, (select I from P2 order by I) P22 where RI1.i = P22.I")
        .transform("VoltLogicalCalc(expr#0..2=[{inputs}], BI=[$t1])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $2)], joinType=[inner])\n" +
                    "    VoltLogicalCalc(expr#0..3=[{inputs}], I=[$t0], BI=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, RI1]])\n" +
                    "    VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                    "      VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "        VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "          VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        // More than one Exchanges
        m_tester.sql("select P1.bi from P1, (select SI from P2 order by I) P22 where P1.i = P22.SI")
        .fail();

        // Taken from TestInsertIntoSelectSuite#testSelectListConstants
        m_tester.sql("select count(*) FROM target_p INNER JOIN (SELECT 9 bi, vc, ii, ti FROM source_p1) ins_sq ON "
                + "target_p.bi = ins_sq.bi").fail();
    }

    public void testIn() {
        // NOTE: This is a good example that Calcite rewrites to table join operation.
        m_tester.sql("select i from R1 where i in (select si from P1)")
        .transform("VoltLogicalCalc(expr#0..6=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $6)], joinType=[inner])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalAggregate(group=[{0}])\n" +
                    "      VoltLogicalExchange(distribution=[hash])\n" +
                    "        VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                    "          VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        // The re-written JOIN would require more than one Exchanges
        m_tester.sql("select i from P2 where i in (select si from P1)")
        .fail();

        // The re-written JOIN is fine because IN clause is SP
        m_tester.sql("select i from P2 where i in (select si from P1 where i = 5)")
        .transform("VoltLogicalCalc(expr#0..6=[{inputs}], I=[$t0])\n" +
                    "  VoltLogicalJoin(condition=[=($0, $6)], joinType=[inner])\n" +
                    "    VoltLogicalTableScan(table=[[public, P2]])\n" +
                    "    VoltLogicalAggregate(group=[{0}])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[5], expr#7=[=($t0, $t6)], SI=[$t1], $condition=[$t7])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        // calcite will use equal to rewrite IN
        m_tester.sql("select * from P1 where i in (16)")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[16], expr#7=[=($t0, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        m_tester.sql("select * from P1 where i in (1,2,3,4,5,6,7,8,9,10)")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], expr#8=[2], expr#9=[=($t0, $t8)], expr#10=[3], expr#11=[=($t0, $t10)], expr#12=[4], expr#13=[=($t0, $t12)], expr#14=[5], expr#15=[=($t0, $t14)], expr#16=[6], expr#17=[=($t0, $t16)], expr#18=[7], expr#19=[=($t0, $t18)], expr#20=[8], expr#21=[=($t0, $t20)], expr#22=[9], expr#23=[=($t0, $t22)], expr#24=[10], expr#25=[=($t0, $t24)], expr#26=[OR($t7, $t9, $t11, $t13, $t15, $t17, $t19, $t21, $t23, $t25)], proj#0..5=[{exprs}], $condition=[$t26])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        m_tester.sql("select * from P1 where i Not in (1, 2)")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], expr#8=[2], expr#9=[=($t0, $t8)], expr#10=[OR($t7, $t9)], expr#11=[NOT($t10)], proj#0..5=[{exprs}], $condition=[$t11])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        m_tester.sql("select si from P1 where i in (1,2) and i in (1,3)")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], expr#8=[2], expr#9=[=($t0, $t8)], expr#10=[OR($t7, $t9)], expr#11=[3], expr#12=[=($t0, $t11)], expr#13=[OR($t7, $t12)], expr#14=[AND($t10, $t13)], SI=[$t1], $condition=[$t14])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        m_tester.sql("select si from P1 where i in (1,2) or i not in (1,3)")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], expr#8=[2], expr#9=[=($t0, $t8)], expr#10=[<>($t0, $t6)], expr#11=[3], expr#12=[<>($t0, $t11)], expr#13=[AND($t10, $t12)], expr#14=[OR($t7, $t9, $t13)], SI=[$t1], $condition=[$t14])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        // calcite will use Join to rewrite IN (sub query)
        m_tester.sql("select si from P1 where si in (select i from R1)")
        .transform("VoltLogicalCalc(expr#0..6=[{inputs}], SI=[$t1])\n" +
                    "  VoltLogicalJoin(condition=[=($1, $6)], joinType=[inner])\n" +
                    "    VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalAggregate(group=[{0}])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "        VoltLogicalTableScan(table=[[public, R1]])\n")
        .pass();
    }

    public void testPartitionKeyEqualToTableColumn() {
        m_tester.sql("select * from P1 where i = si")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[=($t0, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        m_tester.sql("select * from P1 where NOT i <> si")
        .transform("VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[CAST($t1):INTEGER], expr#7=[=($t0, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n" +
                    "  VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testSetOp() {
        // All tables are replicated. Pass
        m_tester.sql("select I from R1 union (select I from R2 except select II from R3)")
        .transform("VoltLogicalUnion(all=[false])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "  VoltLogicalMinus(all=[false])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n" +
                    "    VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, R3]])\n")
        .pass();

        // Only one table is partitioned but it has an equality filter based on its partitioning column. Pass
        m_tester.sql("select I from R1 union (select I from P1 where I = 1 except select II from R3)")
        .transform("VoltLogicalUnion(all=[false])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "  VoltLogicalMinus(all=[false])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, R3]])\n")
        .pass();

        // Only one table is partitioned but it has an equality filter based on its partitioning column. Pass
        m_tester.sql("select I from R1 union select I from P1 where I = 1 union select II from R3")
        .transform("VoltLogicalUnion(all=[false])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "  VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                    "    VoltLogicalTableScan(table=[[public, R3]])\n")
        .pass();

        // Two partitioned tables, both have equality filters based on their partitioning columns with
        // equal partitioning values. Positions of partitioning columns do not match. Pass
        m_tester.sql("select I, SI from P1  where I = 1 union " +
                     "select I, SI from R1 union " +
                     "select SI, I from P2   where I = 1")
        .transform("VoltLogicalUnion(all=[false])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], proj#0..1=[{exprs}], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], SI=[$t1], I=[$t0], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        // SP Join of two partitioned table and another partitioned table with
        // compatible partitioning values
        m_tester.sql("select P1.I, P2.SI from P1, P2  where P1.I = P2.I and P1.I = 1 union " +
                "select SI, I from P3 where P3.I = 1")
        .transform("VoltLogicalUnion(all=[false])\n" +
                    "  VoltLogicalCalc(expr#0..2=[{inputs}], I=[$t0], SI=[$t2])\n" +
                    "    VoltLogicalJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                    "        VoltLogicalTableScan(table=[[public, P2]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], SI=[$t1], I=[$t0], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P3]])\n")
        .pass();

        // Two partitioned tables, both have equality filters based on their partitioning columns.
        // Two SetOps. Pass.
        m_tester.sql("select I from P1  where I = 1 except " +
                     "(select I from R1 intersect select I from P2  where I = 1)")
        .transform("VoltLogicalMinus(all=[false])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "  VoltLogicalIntersect(all=[false])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

        // Only one table is partitioned without an equality filter based on its partitioning column. Fail
        m_tester.sql("select I from R1 union (select I from P1 except select II from R3)")
        .transform("VoltLogicalUnion(all=[false])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "  VoltLogicalMinus(all=[false])\n" +
                    "    VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "    VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                    "      VoltLogicalTableScan(table=[[public, R3]])\n")
        .pass();

        // Same as above but with added ORDER BY
        m_tester.sql("select I from R1 union (select I from P1 except select II from R3) order by 1")
        .transform("VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                    "  VoltLogicalUnion(all=[false])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "    VoltLogicalMinus(all=[false])\n" +
                    "      VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "        VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "          VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "      VoltLogicalCalc(expr#0..2=[{inputs}], II=[$t2])\n" +
                    "        VoltLogicalTableScan(table=[[public, R3]])\n")
        .pass();

        // Two partitioned tables, one has an equality filter based on its partitioning column.
        // Only one SetOp with three children. Fail
        m_tester.sql("select I from P1  where I = 1 union " +
                     "select I from R1 union " +
                     "select I from P2").fail();

        // Two partitioned tables, one has an equality filter based on its partitioning column.
        // Two SetOps. Fail.
        m_tester.sql("select I from P1  where I = 1 except " +
                     "(select I from R1 intersect select I from P2)").fail();

        // Two partitioned tables, both have equality filters based on their partitioning columns with
        // non-equal partitioning values. Fail
        m_tester.sql("select I from P1  where I = 1 union " +
                     "select I from R1 union " +
                     "select I from P2   where I = 2").fail();

        // Two partitioned tables, both have equality filters based on their partitioning columns with
        // non-equal partitioning values.
        // Two SetOps. Fail.
        m_tester.sql("select I from P1  where I = 1 except " +
                     "(select I from R1 intersect select I from P2  where I = 2)").fail();

        // SP Join of two partitioned table and another partitioned table with
        // incompatible partitioning values
        m_tester.sql("select P1.I, P2.SI from P1, P2  where P1.I = P2.I and P1.I = 1 union " +
                "select SI, I from P3 where P3.I = 2").fail();

        // SetOP of partitioned and replicated tables. Pass
        m_tester.sql("select P1.I from P1 union select I from R1")
        .transform("VoltLogicalUnion(all=[false])\n" +
                    "  VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n")
        .pass();

        // SetOP of partitioned and replicated tables. Pass
        m_tester.sql("select R1.I from R1 union (select I from R2 except select I from P1)")
        .transform("VoltLogicalUnion(all=[false])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "  VoltLogicalMinus(all=[false])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, R2]])\n" +
                    "    VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();

        // SetOP of partitioned and replicated tables. Pass
        m_tester.sql("select P1.I from P1 where I = 8 union select I from R1 " +
                "union select P2.I from P2 where I = 8")
        .transform("VoltLogicalUnion(all=[false])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[8], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P1]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "    VoltLogicalTableScan(table=[[public, R1]])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[8], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P2]])\n")
        .pass();

    }


    public void testPartitionedWithSortAndLimit() {
        m_tester.sql("select P1.I from P1 order by 1 limit 10")
        .transform("VoltLogicalLimit(limit=[10])\n" +
                    "  VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                    "    VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testPartitionedWithSortAndLimit1() {
        m_tester.sql("select P1.I from P1 where I = 10 order by 1 limit 10")
        .transform("VoltLogicalLimit(limit=[10])\n" +
                    "  VoltLogicalSort(sort0=[$0], dir0=[ASC])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[10], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testPartitionedWithAggregate1() {
        m_tester.sql("select max(P1.I) from P1")
        .transform("VoltLogicalAggregate(group=[{}], EXPR$0=[MAX($0)])\n" +
                    "  VoltLogicalExchange(distribution=[hash[0]])\n" +
                    "    VoltLogicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                    "      VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testPartitionedWithAggregate2() {
        m_tester.sql("select max(P1.I), P1.SI from P1 group by P1.SI having max(P1.I) > 0")
        .transform("VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[>($t1, $t2)], EXPR$0=[$t1], SI=[$t0], $condition=[$t3])\n" +
                    "  VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                    "    VoltLogicalExchange(distribution=[hash[1]])\n" +
                    "      VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], I=[$t0])\n" +
                    "        VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testPartitionedWithAggregate3() {
        m_tester.sql("select max(P1.I), P1.SI from P1 group by P1.SI having max(P1.I) > 0 order by P1.SI")
        .transform("VoltLogicalSort(sort0=[$1], dir0=[ASC])\n" +
                    "  VoltLogicalCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[>($t1, $t2)], EXPR$0=[$t1], SI=[$t0], $condition=[$t3])\n" +
                    "    VoltLogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                    "      VoltLogicalExchange(distribution=[hash[1]])\n" +
                    "        VoltLogicalCalc(expr#0..5=[{inputs}], SI=[$t1], I=[$t0])\n" +
                    "          VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

    public void testPartitionedWithAggregate4() {
        m_tester.sql("select max(P1.I) from P1 where P1.I = 0")
        .transform("VoltLogicalAggregate(group=[{}], EXPR$0=[MAX($0)])\n" +
                    "  VoltLogicalCalc(expr#0..5=[{inputs}], expr#6=[0], expr#7=[=($t0, $t6)], I=[$t0], $condition=[$t7])\n" +
                    "    VoltLogicalTableScan(table=[[public, P1]])\n")
        .pass();
    }

}
