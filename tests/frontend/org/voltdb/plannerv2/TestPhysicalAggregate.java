/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

public class TestPhysicalAggregate extends Plannerv2TestCase {

    private PhysicalConversionRulesTester m_tester = new PhysicalConversionRulesTester();

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestValidation.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init();
        m_tester.phase(PlannerRules.Phase.PHYSICAL_CONVERSION);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    // Single GROUB BY column I  matches index (I)
    public void testSerailAggreagteNoGroupBy() {
        m_tester.sql("SELECT avg(R1.si) FROM R1 ")
                .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[AVG($0)], pusheddown=[false], type=[serial])\n" +
                        "  VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                        "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
                .pass();
    }

    // Single GROUB BY column I  matches index (I)
    public void testSerailAggreagteWithSeqScan1() {
        m_tester.sql("SELECT max(RI1.si), i FROM RI1 group by I")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1], I=[$t0])\n" +
                        "  VoltPhysicalSerialAggregate(group=[{0}], EXPR$0=[MAX($1)], pusheddown=[false], type=[serial])\n" +
                        "    VoltPhysicalCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n" +
                        "      VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], proj#0..3=[{exprs}], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDEQ0_0])\n"
                )
                .pass();
    }

    // HASH aggregate with IndexScan(I) wins over Serial Aggregate with IndexScan(BI, SI)??
    public void testSerailAggreagteWithSeqScan2() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where I > 0 group by BI, SI")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], EXPR$0=[$t2])\n" +
                        "  VoltPhysicalHashAggregate(group=[{0, 1}], EXPR$0=[MAX($1)], pusheddown=[false], type=[hash])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t0, $t4)], BI=[$t2], SI=[$t1], $condition=[$t5], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // Single GROUB BY column I  matches index (I)
    public void testSerailAggreagteWithIndexScan1() {
        m_tester.sql("SELECT max(RI1.si), i FROM RI1 where I > 0 group by I")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1], I=[$t0])\n" +
                        "  VoltPhysicalSerialAggregate(group=[{0}], EXPR$0=[MAX($1)], pusheddown=[false], type=[serial])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t0, $t4)], proj#0..1=[{exprs}], $condition=[$t5], index=[VOLTDB_AUTOGEN_IDX_PK_RI1_I_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // GROUB BY columns (SI, BI) match index (BI, SI)
    public void testSerailAggreagteWithIndexScan2() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where BI > 0 and SI > 0 group by SI, BI")
                .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], EXPR$0=[$t2])\n" +
                        "  VoltPhysicalSerialAggregate(group=[{0, 1}], EXPR$0=[MAX($0)], pusheddown=[false], type=[serial])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t2, $t4)], expr#6=[>($t1, $t4)], expr#7=[AND($t5, $t6)], SI=[$t1], BI=[$t2], $condition=[$t7], index=[RI1_IND2_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // GROUB BY column (SI) does not match index (BI, SI) -
    // The first index column BI is not part of GROUP BY columns
    public void testHashAggreagteWithIndexScan3() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where BI > 0 and SI > 0 group by SI")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "  VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($0)], pusheddown=[false], type=[hash])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t2, $t4)], expr#6=[>($t1, $t4)], expr#7=[AND($t5, $t6)], SI=[$t1], $condition=[$t7], index=[RI1_IND2_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // GROUB BY columns (BI) matches index (BI, SI) -
    public void testSerialAggreagteWithIndexScan4() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where BI > 0 and SI > 0 group by BI")
                .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1])\n" +
                        "  VoltPhysicalSerialAggregate(group=[{0}], EXPR$0=[MAX($1)], pusheddown=[false], type=[serial])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t2, $t4)], expr#6=[>($t1, $t4)], expr#7=[AND($t5, $t6)], BI=[$t2], SI=[$t1], $condition=[$t7], index=[RI1_IND2_INVALIDGT1_0])\n"
                )
                .pass();
    }

    // GROUB BY columns (SI, BI, I) does not match index (BI, SI) -
    // Column I is not part of the (BI, SI) index
    public void testHashAggreagteWithIndexScan5() {
        m_tester.sql("SELECT max(RI1.si) FROM RI1 where BI > 0 and SI > 0 group by SI, BI, I")
                .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], EXPR$0=[$t3])\n" +
                        "  VoltPhysicalHashAggregate(group=[{0, 1, 2}], EXPR$0=[MAX($0)], pusheddown=[false], type=[hash])\n" +
                        "    VoltPhysicalTableIndexScan(table=[[public, RI1]], expr#0..3=[{inputs}], expr#4=[0], expr#5=[>($t2, $t4)], expr#6=[>($t1, $t4)], expr#7=[AND($t5, $t6)], SI=[$t1], BI=[$t2], I=[$t0], $condition=[$t7], index=[RI1_IND2_INVALIDGT1_0])\n"
                )
                .pass();
    }

    public void testDistributedAvgAggregate1() {
        m_tester.sql("SELECT avg(P1.i) FROM P1")
        .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], expr#2=[/($t0, $t1)], expr#3=[CAST($t2):INTEGER], EXPR$0=[$t3])\n" +
                "  VoltPhysicalSerialAggregate(group=[{}], agg#0=[SUM($0)], agg#1=[$SUM0FROMCOUNTY($1)], pusheddown=[true], type=[serial])\n" +
                "    VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "      VoltPhysicalSerialAggregate(group=[{}], agg#0=[SUM($0)], agg#1=[COUNT()], pusheddown=[false], type=[serial])\n" +
                "        VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                "          VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedAvgAggregate2() {
        m_tester.sql("SELECT avg(P1.si) FROM P1")
        .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], expr#2=[/($t0, $t1)], expr#3=[CAST($t2):SMALLINT], EXPR$0=[$t3])\n" +
                "  VoltPhysicalSerialAggregate(group=[{}], agg#0=[SUM($0)], agg#1=[$SUM0FROMCOUNTY($1)], pusheddown=[true], type=[serial])\n" +
                "    VoltPhysicalExchange(distribution=[single], childDistribution=[hash])\n" +
                "      VoltPhysicalSerialAggregate(group=[{}], agg#0=[SUM($0)], agg#1=[COUNT($0)], pusheddown=[false], type=[serial])\n" +
                "        VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                "          VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedAvgAggregate3() {
        m_tester.sql("SELECT avg(P1.bi) FROM P1")
        .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], expr#2=[/($t0, $t1)], expr#3=[CAST($t2):BIGINT], EXPR$0=[$t3])\n" +
                "  VoltPhysicalSerialAggregate(group=[{}], agg#0=[SUM($0)], agg#1=[$SUM0FROMCOUNTY($1)], pusheddown=[true], type=[serial])\n" +
                "    VoltPhysicalExchange(distribution=[single], childDistribution=[hash])\n" +
                "      VoltPhysicalSerialAggregate(group=[{}], agg#0=[SUM($0)], agg#1=[COUNT($0)], pusheddown=[false], type=[serial])\n" +
                "        VoltPhysicalCalc(expr#0..5=[{inputs}], BI=[$t3])\n" +
                "          VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedAvgAggregate4() {
        m_tester.sql("SELECT avg(P1.si) FROM P1 WHERE P1.I = 9")
        .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], expr#2=[/($t0, $t1)], expr#3=[CAST($t2):SMALLINT], EXPR$0=[$t3])\n" +
                "  VoltPhysicalSerialAggregate(group=[{}], agg#0=[SUM($0)], agg#1=[COUNT($0)], pusheddown=[false], type=[serial])\n" +
                "    VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[9], expr#7=[=($t0, $t6)], SI=[$t1], $condition=[$t7])\n" +
                "      VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testReplicatedAvgAggregate() {
        m_tester.sql("SELECT avg(R1.si) FROM R1 ")
        .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[AVG($0)], pusheddown=[false], type=[serial])\n" +
                "  VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                "    VoltPhysicalTableSequentialScan(table=[[public, R1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedTwoAggregate1() {
        m_tester.sql("SELECT i, sum(P1.bi), COUNT(P1.si), bi FROM P1 group by i, bi")
        .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], I=[$t0], EXPR$1=[$t2], EXPR$2=[$t3], BI=[$t1])\n" +
                "  VoltPhysicalHashAggregate(group=[{0, 1}], EXPR$1=[SUM($1)], agg#1=[$SUM0FROMCOUNTY($3)], pusheddown=[true], type=[hash])\n" +
                "    VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "      VoltPhysicalHashAggregate(group=[{0, 1}], EXPR$1=[SUM($1)], EXPR$2=[COUNT($2)], pusheddown=[false], type=[hash])\n" +
                "        VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0], BI=[$t3], SI=[$t1])\n" +
                "          VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedSumAggregate1() {
        m_tester.sql("select sum(P1.si) from P1")
        .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[SUM($0)], pusheddown=[true], type=[serial])\n" +
                "  VoltPhysicalExchange(distribution=[single], childDistribution=[hash])\n" +
                "    VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[SUM($0)], pusheddown=[false], type=[serial])\n" +
                "      VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedSumDistinctAggregate1() {
        m_tester.sql("SELECT sum(distinct(P1.bi)) FROM P1")
        .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[SUM(DISTINCT $0)], pusheddown=[true], type=[serial])\n" +
                "  VoltPhysicalExchange(distribution=[single], childDistribution=[hash])\n" +
                "    VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[SUM(DISTINCT $0)], pusheddown=[false], type=[serial])\n" +
                "      VoltPhysicalCalc(expr#0..5=[{inputs}], BI=[$t3])\n" +
                "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n")
        .pass();
    }

    public void testDistributedCountAggregate1() {
        m_tester.sql("SELECT count(P1.v) FROM P1")
        .transform("VoltPhysicalSerialAggregate(group=[{}], agg#0=[$SUM0FROMCOUNTY($0)], pusheddown=[true], type=[serial])\n" +
                "  VoltPhysicalExchange(distribution=[single], childDistribution=[hash])\n" +
                "    VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT($0)], pusheddown=[false], type=[serial])\n" +
                "      VoltPhysicalCalc(expr#0..5=[{inputs}], V=[$t5])\n" +
                "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedCountAggregate2() {
        m_tester.sql("SELECT count(P1.v) FROM P1 where I = 9")
        .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT($0)], pusheddown=[false], type=[serial])\n" +
                "  VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[9], expr#7=[=($t0, $t6)], V=[$t5], $condition=[$t7])\n" +
                "    VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedCountAggregate3() {
        m_tester.sql("select count(*) from P1")
        .transform("VoltPhysicalSerialAggregate(group=[{}], agg#0=[$SUM0FROMCOUNTY($0)], pusheddown=[true], type=[serial])\n" +
                "  VoltPhysicalExchange(distribution=[single], childDistribution=[hash])\n" +
                "    VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT()], pusheddown=[false], type=[serial])\n" +
                "      VoltPhysicalCalc(expr#0..5=[{inputs}], expr#6=[0], $f0=[$t6])\n" +
                "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedCountAggregate4() {
        m_tester.sql("select count(P1.I) from P1")
        .transform("VoltPhysicalSerialAggregate(group=[{}], agg#0=[$SUM0FROMCOUNTY($0)], pusheddown=[true], type=[serial])\n" +
                "  VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "    VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[COUNT()], pusheddown=[false], type=[serial])\n" +
                "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testPartitionedWithMaxAggregate() {
        m_tester.sql("select max(P1.I) from P1")
        .transform("VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[MAX($0)], pusheddown=[true], type=[serial])\n" +
                "  VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "    VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[MAX($0)], pusheddown=[false], type=[serial])\n" +
                "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    // Volt omits a coordinator aggregate in this case because P1.I is a partition column
    public void testPartitionedWithDistinctAggregate1() {
        m_tester.sql("select distinct(P1.I) from P1")
        .transform("VoltPhysicalHashAggregate(group=[{0}], pusheddown=[true], type=[hash])\n" +
                "  VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "    VoltPhysicalHashAggregate(group=[{0}], pusheddown=[false], type=[hash])\n" +
                "      VoltPhysicalCalc(expr#0..5=[{inputs}], I=[$t0])\n" +
                "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    // Here, the coordinator aggregate is required - DISTINCT over a non-partition column
    public void testPartitionedWithDistinctAggregate2() {
        m_tester.sql("select distinct(P1.SI) from P1")
        .transform("VoltPhysicalHashAggregate(group=[{0}], pusheddown=[true], type=[hash])\n" +
                "  VoltPhysicalExchange(distribution=[single], childDistribution=[hash])\n" +
                "    VoltPhysicalHashAggregate(group=[{0}], pusheddown=[false], type=[hash])\n" +
                "      VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                "        VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedMaxAggregateWithGroupBy1() {
        m_tester.sql("SELECT max(P1.si), i  FROM P1 GROUP BY i")
        .transform("VoltPhysicalCalc(expr#0..1=[{inputs}], EXPR$0=[$t1], I=[$t0])\n" +
                "  VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($1)], pusheddown=[true], type=[hash])\n" +
                "    VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "      VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($1)], pusheddown=[false], type=[hash])\n" +
                "        VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                "          VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedAvgAggregateWithGroupBy2() {
        m_tester.sql("SELECT avg(P1.si), i  FROM P1 GROUP BY i")
        .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[/($t1, $t2)], expr#4=[CAST($t3):SMALLINT], EXPR$0=[$t4], I=[$t0])\n" +
                "  VoltPhysicalHashAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[$SUM0FROMCOUNTY($2)], pusheddown=[true], type=[hash])\n" +
                "    VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "      VoltPhysicalHashAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[COUNT($1)], pusheddown=[false], type=[hash])\n" +
                "        VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                "          VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedAvgAggregateWithHaving1() {
        m_tester.sql("SELECT max(P1.si), i  FROM P1 GROUP BY i HAVING min(P1.si) > 9")
        .transform("VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[9], expr#4=[>($t2, $t3)], EXPR$0=[$t1], I=[$t0], $condition=[$t4], pusheddown=[true])\n" +
                "  VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($1)], agg#1=[MIN($1)], pusheddown=[true], type=[hash])\n" +
                "    VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "      VoltPhysicalCalc(expr#0..2=[{inputs}], expr#3=[9], expr#4=[>($t2, $t3)], proj#0..2=[{exprs}], $condition=[$t4])\n" +
                "        VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($1)], agg#1=[MIN($1)], pusheddown=[false], type=[hash])\n" +
                "          VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                "            VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedAvgAggregateWithHaving2() {
        m_tester.sql("SELECT max(P1.si), i  FROM P1 GROUP BY i HAVING avg(P1.si) > 9")
        .transform("VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[/($t2, $t3)], expr#5=[CAST($t4):SMALLINT], expr#6=[9], expr#7=[>($t5, $t6)], EXPR$0=[$t1], I=[$t0], $condition=[$t7], pusheddown=[true])\n" +
                "  VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($1)], agg#1=[SUM($1)], agg#2=[$SUM0FROMCOUNTY($3)], pusheddown=[true], type=[hash])\n" +
                "    VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "      VoltPhysicalCalc(expr#0..3=[{inputs}], expr#4=[/($t2, $t3)], expr#5=[CAST($t4):SMALLINT], expr#6=[9], expr#7=[>($t5, $t6)], proj#0..3=[{exprs}], $condition=[$t7])\n" +
                "        VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($1)], agg#1=[SUM($1)], agg#2=[COUNT($1)], pusheddown=[false], type=[hash])\n" +
                "          VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                "            VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedAvgAggregateWithLimit1() {
        m_tester.sql("SELECT max(P1.si)  FROM P1 limit 4")
        .transform("VoltPhysicalLimit(limit=[4], pusheddown=[true])\n" +
                "  VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[MAX($0)], pusheddown=[true], type=[serial])\n" +
                "    VoltPhysicalExchange(distribution=[single], childDistribution=[hash])\n" +
                "      VoltPhysicalLimit(limit=[4], pusheddown=[false])\n" +
                "        VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[MAX($0)], pusheddown=[false], type=[serial])\n" +
                "          VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                "            VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    public void testDistributedAvgAggregateWithLimit2() {
        m_tester.sql("SELECT max(P1.si), i FROM P1 GROUP BY i HAVING max(si) > 9 limit 4")
        .transform("VoltPhysicalLimit(limit=[4], pusheddown=[true])\n" +
                "  VoltPhysicalCalc(expr#0..1=[{inputs}], expr#2=[9], expr#3=[>($t1, $t2)], EXPR$0=[$t1], I=[$t0], $condition=[$t3], pusheddown=[true])\n" +
                "    VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($1)], pusheddown=[true], type=[hash])\n" +
                "      VoltPhysicalExchange(distribution=[single], childDistribution=[hash[0]])\n" +
                "        VoltPhysicalLimit(limit=[4], pusheddown=[false])\n" +
                "          VoltPhysicalCalc(expr#0..1=[{inputs}], expr#2=[9], expr#3=[>($t1, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n" +
                "            VoltPhysicalHashAggregate(group=[{0}], EXPR$0=[MAX($1)], pusheddown=[false], type=[hash])\n" +
                "              VoltPhysicalCalc(expr#0..5=[{inputs}], proj#0..1=[{exprs}])\n" +
                "                VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

    // SORT is not pushed down - aggregated output has to be sorted.
    // Can VoltPhysicalSerialAggregate guarantee the right order?
    public void testDistributedAvgAggregateWithSortAndLimit1() {
        m_tester.sql("SELECT max(P1.si)  FROM P1  order by 1 limit 4")
        .transform("VoltPhysicalLimit(limit=[4], pusheddown=[false])\n" +
                "  VoltPhysicalSort(sort0=[$0], dir0=[ASC], pusheddown=[false])\n" +
                "    VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[MAX($0)], pusheddown=[true], type=[serial])\n" +
                "      VoltPhysicalExchange(distribution=[single], childDistribution=[hash])\n" +
                "        VoltPhysicalSerialAggregate(group=[{}], EXPR$0=[MAX($0)], pusheddown=[false], type=[serial])\n" +
                "          VoltPhysicalCalc(expr#0..5=[{inputs}], SI=[$t1])\n" +
                "            VoltPhysicalTableSequentialScan(table=[[public, P1]], expr#0..5=[{inputs}], proj#0..5=[{exprs}])\n"
                )
        .pass();
    }

}