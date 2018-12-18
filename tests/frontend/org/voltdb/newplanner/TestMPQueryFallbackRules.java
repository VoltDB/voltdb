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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.voltdb.calciteadapter.CatalogAdapter;
import org.voltdb.calciteadapter.rel.logical.VoltDBLRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.newplanner.rules.PlannerPhase;
import org.voltdb.newplanner.util.VoltDBRelUtil;
import org.voltdb.types.CalcitePlannerType;

public class TestMPQueryFallbackRules extends VoltConverterTestCase {
    @Override
    protected void setUp() throws Exception {
        setupSchema(TestVoltSqlValidator.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        init(CatalogAdapter.schemaPlusFromDatabase(getDatabase()));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void assertNotFallback(String sql) {
        RelRoot root = parseValidateAndConvert(sql);

        // apply logical rules
        RelTraitSet logicalTraits = root.rel.getTraitSet().replace(VoltDBLRel.VOLTDB_LOGICAL);
        RelNode nodeAfterLogicalRules = CalcitePlanner.transform(CalcitePlannerType.VOLCANO, PlannerPhase.LOGICAL,
                root.rel, logicalTraits);

        System.out.println(RelOptUtil.toString(nodeAfterLogicalRules));

        nodeAfterLogicalRules = CalcitePlanner.transform(CalcitePlannerType.HEP_BOTTOM_UP, PlannerPhase.MP_FALLBACK,
                nodeAfterLogicalRules);

        // Add RelDistribution trait definition to the planner to make Calcite aware of the new trait.
        nodeAfterLogicalRules.getCluster().getPlanner().addRelTraitDef(RelDistributionTraitDef.INSTANCE);

        // Add RelDistributions.ANY trait to the rel tree.
        nodeAfterLogicalRules = VoltDBRelUtil.addTraitRecurcively(nodeAfterLogicalRules, RelDistributions.SINGLETON);

        // Prepare the set of RelTraits required of the root node at the termination of the physical conversion phase.
        RelTraitSet physicalTraits = nodeAfterLogicalRules.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL).replace(RelDistributions.SINGLETON);

        // apply physical conversion rules.
        RelNode nodeAfterPhysicalRules = CalcitePlanner.transform(CalcitePlannerType.VOLCANO,
                PlannerPhase.PHYSICAL_CONVERSION, nodeAfterLogicalRules, physicalTraits);
    }

    private void assertFallback(String sql) {
        try {
            assertNotFallback(sql);
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            assertTrue(e.getMessage().startsWith("Error while applying rule") ||
                    e.getMessage().equals("MP query not supported in Calcite planner."));
            // we got the exception, we are good.
            return;
        }
        fail("Expected fallback.");
    }

    // when we only deal with replicated table, we will always have a SP query.
    public void testReplicated() {
        assertNotFallback("select * from R2");

        assertNotFallback("select i, si from R1");

        assertNotFallback("select i, si from R1 where si = 9");
    }

    // Partitioned with no filter, always a MP query
    public void testPartitionedWithoutFilter() {
        assertFallback("select * from P1");

        assertFallback("select i from P1");
    }

    public void testPartitionedWithFilter() {
        // equal condition on partition key
        assertNotFallback("select * from P1 where i = 1");

        assertNotFallback("select * from P1 where 1 = i");

        // other conditions on partition key
        assertFallback("select * from P1 where i > 10");
        assertFallback("select * from P1 where i <> 10");

        // equal condition on partition key with ANDs
        assertNotFallback("select si, v from P1 where 7=si and i=2");
        assertNotFallback("select si, v from P1 where 7>si and i=2 and ti<3");

        // equal condition on partition key with ORs
        assertFallback("select si, v from P1 where 7=si or i=2");
        assertFallback("select si, v from P1 where 7=si or i=2 or ti=3");

        // equal condition on partition key with ORs and ANDs
        assertFallback("select si, v from P1 where 7>si or (i=2 and ti<3)");
        assertNotFallback("select si, v from P1 where 7>si and (i=2 and ti<3)");
        assertNotFallback("select si, v from P1 where (7>si or ti=2) and i=2");
        assertFallback("select si, v from P1 where (7>si or ti=2) or i=2");

        // equal condition with some expression that always TURE
        assertNotFallback("select si, v from P1 where (7=si and i=2) and 1=1");
        assertFallback("select si, v from P1 where (7=si and i=2) or 1=1");

        // equal condition with some expression that always FALSE
        assertNotFallback("select si, v from P1 where (7=si and i=2) and 1=2");
        // TODO: we should pass the commented test below if the planner is clever enough
//        assertNotFallback("select si, v from P1 where (7=si and i=2) or 1=2");
    }

    public void testJoin() {
        assertNotFallback("select R1.i, R2.v from R1, R2 " +
                "where R2.si = R1.i and R2.v = 'foo'");

        assertNotFallback("select R1.i, R2.v from R1 inner join R2 " +
                "on R2.si = R1.i where R2.v = 'foo'");

        assertNotFallback("select R2.si, R1.i from R1 inner join " +
                "R2 on R2.i = R1.si where R2.v = 'foo' and R1.si > 4 and R1.ti > R2.i");

        assertNotFallback("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si where R1.I + R2.ti = 5");
    }

    public void testJoinPartitionedTable() {
        // when join 2 partitioned table, assume it is always MP
        assertFallback("select P1.i, P2.v from P1, P2 " +
                "where P2.si = P1.i and P2.v = 'foo'");

        assertFallback("select P1.i, P2.v from P1, P2 " +
                "where P2.si = P1.i and P2.i = 34");

        assertFallback("select P1.i, P2.v from P1 inner join P2 " +
                "on P2.si = P1.i where P2.v = 'foo'");

        // when join a partitioned table with a replicated table,
        // if the filtered result on the partitioned table is not SP, then the query is MP
        assertFallback("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.v = 'foo'");

        assertFallback("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.i > 3");

        assertFallback("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and (P2.i > 3 or P2.i =1)");

        // when join a partitioned table with a replicated table,
        // if the filtered result on the partitioned table is SP, then the query is SP
        assertNotFallback("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.i = 3");

        assertNotFallback("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i where P2.i =3");

        assertNotFallback("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i where P2.i =3 and P2.v = 'bar'");

        // when join a partitioned table with a replicated table,
        // if the join condition can filter the partitioned table in SP, then the query is SP
        assertNotFallback("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and P2.i =3");

        assertNotFallback("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and P2.i =3 where P2.v = 'bar'");
    }

    public void testThreeWayJoinWithoutFilter() {
        // three-way join on replicated tables, SP
        assertNotFallback("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si inner join " +
                "R3 on R2.i = R3.ii");

        // all partitioned, MP
        assertFallback("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.si inner join " +
                "P3 on P2.i = P3.i");

        // one of them partitioned, MP
        assertFallback("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.si inner join " +
                "R3 on R2.i = R3.ii");

        assertFallback("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.si inner join " +
                "R3 on P2.i = R3.ii");

        assertFallback("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si inner join " +
                "P3 on R2.i = P3.i");

        // two of them partitioned, MP
        assertFallback("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.si inner join " +
                "P3 on P2.i = P3.i");

        assertFallback("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.si inner join " +
                "P3 on R2.i = P3.i");

        assertFallback("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.si inner join " +
                "R3 on P2.i = R3.ii");

        // this is tricky. Note `P1.si = R2.i` will produce a Calc with CAST.
        assertFallback("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.i = R3.ii");
    }

    public void testThreeWayJoinWithFilter() {
        assertNotFallback("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo'");

        assertFallback("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where P1.si > 4 and R3.vc <> 'foo'");

        assertFallback("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo'");

        assertFallback("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo' and P2.i = 5");

        assertFallback("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "P3 on R2.v = P3.v where R1.si > 4 and P3.si = 6");

        assertNotFallback("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo'");

        assertNotFallback("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo' and P2.i = 5");

        assertNotFallback("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "P3 on R2.v = P3.v where R1.si > 4 and P3.i = 6");
    }

    public void testSubqueriesJoin() {
        assertNotFallback("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from R2 where f = 30.3) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3");

        assertFallback("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where f = 30.3) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3");

        assertNotFallback("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where i = 303) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3");
    }
}
