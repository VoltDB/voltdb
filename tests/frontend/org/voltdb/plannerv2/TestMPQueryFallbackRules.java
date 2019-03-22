/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

    // Partitioned with no filter, always a MP query
    public void testPartitionedWithoutFilter() {
        m_tester.sql("select * from P1").fail();

        m_tester.sql("select i from P1").fail();
    }

    public void testPartitionedWithFilter() {
        // equal condition on partition key
        m_tester.sql("select * from P1 where i = 1").pass();

        m_tester.sql("select * from P1 where 1 = i").pass();

        // other conditions on partition key
        m_tester.sql("select * from P1 where i > 10").fail();
        m_tester.sql("select * from P1 where i <> 10").fail();

        // equal condition on partition key with ANDs
        m_tester.sql("select si, v from P1 where 7=si and i=2").pass();
        m_tester.sql("select si, v from P1 where 7>si and i=2 and ti<3").pass();

        m_tester.sql("SELECT si + 1 FROM P1 WHERE 7 = i").pass();
        m_tester.sql("SELECT max(v) FROM P1 WHERE 7 = i").pass();

        // equal condition involves SQL functions
        m_tester.sql("SELECT P4.j FROM P4, P5 WHERE P5.i = P4.i AND P4.i = LOWER('foO') || 'bar'").pass();

        // equal condition on partition key with ORs
        m_tester.sql("select si, v from P1 where 7=si or i=2").fail();

        // equal condition on partition key with ORs and ANDs
        m_tester.sql("select si, v from P1 where 7>si or (i=2 and ti<3)").fail();
        m_tester.sql("select si, v from P1 where 7>si and (i=2 and ti<3)").pass();
        m_tester.sql("select si, v from P1 where (7>si or ti=2) and i=2").pass();
        m_tester.sql("select si, v from P1 where (7>si or ti=2) or i=2").fail();

        // equal condition with some expression that always TRUE
        m_tester.sql("select si, v from P1 where (7=si and i=2) and 1=1").pass();
        m_tester.sql("select si, v from P1 where (7=si and i=2) and true").pass();
        m_tester.sql("select si, v from P1 where (7=si and i=2) or 1=1").fail();

        // equal condition with some expression that always FALSE
        m_tester.sql("select si, v from P1 where (7=si and i=2) and 1=2").pass();

        m_tester.sql("select si, v from P1 where (7=si and i=2) or 1=2").pass();
    }

    public void testPartitionedWithNotFilter() {
        // when comes to NOT operator, we need to decide if the complement of its
        // operand is single-partitioned
        m_tester.sql("select * from P1 where NOT i <> 15").pass();

        m_tester.sql("select * from P1 where NOT (NOT i = 1)").pass();

        m_tester.sql("select * from P1 where NOT (i <> 15 OR si = 16)").pass();

        m_tester.sql("select * from P1 where NOT ( NOT (i = 15 AND si = 16))").pass();

        m_tester.sql("select * from P1 where NOT si <> 15").fail();

        m_tester.sql("select * from P1 where NOT (i <> 15 AND si = 16)").fail();

        m_tester.sql("select * from P1 where NOT ( NOT (i = 15 OR si = 16))").fail();
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

    public void testJoinPartitionedTable() {
        // Two partitioned table joined that results in SP
        m_tester.sql("select P1.i, P2.v FROM P1, P2 " +
                "WHERE P1.i = P2.i AND P2.i = 34").pass();

        m_tester.sql("select P1.i, P2.v FROM P1 INNER JOIN P2 " +
                "ON P1.i = P2.i AND P2.i = 34").pass();

        m_tester.sql("select P1.i, P2.v FROM P1 INNER JOIN P2 " +
                "ON P1.i = P2.i WHERE P2.i = 34").pass();

        m_tester.sql("select P1.i, P2.v FROM P1 INNER JOIN P2 " +
                "USING(i) WHERE P2.i = 34").pass();

        m_tester.sql("select P1.i, P2.v from P1 inner join P2 " +
                "ON P2.i = P1.i AND P1.i = 34 AND 34 = P2.i").pass();

        m_tester.sql("select P1.i, P2.v from P1 inner join P2 " +
                "ON P2.i = P1.i AND P1.i = 34 AND P2.i = 43").pass();

        m_tester.sql("select P1.i, P2.v FROM P1 INNER JOIN P2 " +
                "ON P1.i = P2.i AND P2.v = 'foo' WHERE 0 = P1.i").pass();

        m_tester.sql("select P1.i, P2.v FROM P1 INNER JOIN P2 " +
                "ON P1.i = P2.i AND P1.i = P1.si AND P1.si = 34").pass();

        // Two partitioned table joined that results in MP (or sometimes un-plannable query):
        m_tester.sql("select P1.i, P2.v from P1, P2 " +
                "where P2.si = P1.i and P2.v = 'foo'").fail();

        m_tester.sql("select P1.i, P2.v from P1, P2 " +
                "where P2.si = P1.i and P2.i = 34").fail();

        m_tester.sql("select P1.i, P2.v from P1 inner join P2 " +
                "ON P2.si = P1.i WHERE P2.v = 'foo'").fail();

        m_tester.sql("select P1.i, P2.v from P1 inner join P2 " +
                "ON P2.si = P1.si WHERE P2.v = 'foo' AND P1.i = 34").fail();

        m_tester.sql("select P1.i, P2.v from P1 inner join P2 " +
                "ON P2.i = P1.i AND P1.i = 34 OR P2.i = 34").fail();

        // when join a partitioned table with a replicated table,
        // if the filtered result on the partitioned table is not SP, then the query is MP
        m_tester.sql("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.v = 'foo'").fail();

        m_tester.sql("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.i > 3").fail();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and (P2.i > 3 or P2.i =1)").fail();

        // when join a partitioned table with a replicated table,
        // if the filtered result on the partitioned table is SP, then the query is SP
        m_tester.sql("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.i = 3").pass();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i where P2.i =3").pass();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i where P2.i =3 and P2.v = 'bar'").pass();

        // when join a partitioned table with a replicated table,
        // if the join condition can filter the partitioned table in SP, then the query is SP
        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and P2.i =3").pass();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and P2.i =3 where P2.v = 'bar'").pass();
    }

    public void testThreeWayJoinWithoutFilter() {
        // three-way join on replicated tables, SP
        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si inner join " +
                "R3 on R2.i = R3.ii").pass();

        // all partitioned, MP
        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.si inner join " +
                "P3 on P2.i = P3.i").fail();

        // one of them partitioned, MP
        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.si inner join " +
                "R3 on R2.i = R3.ii").fail();

        m_tester.sql("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.si inner join " +
                "R3 on P2.i = R3.ii").fail();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si inner join " +
                "P3 on R2.i = P3.i").fail();

        // two of them partitioned, MP
        m_tester.sql("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.si inner join " +
                "P3 on P2.i = P3.i").fail();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.si inner join " +
                "P3 on R2.i = P3.i").fail();

        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.si inner join " +
                "R3 on P2.i = R3.ii").fail();

        // this is tricky. Note `P1.si = R2.i` will produce a Calc with CAST.
        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.i = R3.ii").fail();
    }

    public void testMultiWayJoinWithFilter() {
        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo'").pass();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where P1.si > 4 and R3.vc <> 'foo'").fail();

        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo'").fail();

        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo' and P2.i = 5").fail();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "P3 on R2.v = P3.v where R1.si > 4 and P3.si = 6").fail();

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

        m_tester.sql("select P1.si, P2.si, P3.si FROM P1, P2, P3 WHERE " +
                "P2.i = 0 AND P3.i = 0").fail();


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
                + "where t1.i = 3").pass();

        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where f = 30.3) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3").fail();

        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where i = 303) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3").pass();

        m_tester.sql("select t1.v, t2.v from "
                + "  (select si, v from R1 where v = 'foo') t1, "
                + "  (select si, v from P2 where i = 303) t2").pass();

        m_tester.sql("select t1.v, t2.v from "
                + "  (select si, v from P1 where i = 300) t1, "
                + "  (select si, v from P2 where i = 303) t2").fail();

        m_tester.sql("select t1.v, t2.v from "
                + "  (select i, v from R1 where v = 'foo') t1 inner join "
                + "  (select v, i from P2 where i = 303) t2 "
                + " on t1.i = t2.i where t1.i = 4").pass();

        m_tester.sql("select RI1.bi from RI1, (select I from P2 order by I) P22 where RI1.i = P22.I").fail();

        m_tester.sql("select RI1.bi from RI1, (select I from P2 where I = 5 order by I) P22 where RI1.i = P22.I").pass();

        // Taken from TestInsertIntoSelectSuite#testSelectListConstants
        m_tester.sql("select count(*) FROM target_p INNER JOIN (SELECT 9 bi, vc, ii, ti FROM source_p1) ins_sq ON "
                + "target_p.bi = ins_sq.bi").fail();
    }

    public void testIn() {
        // NOTE: This is a good example that Calcite rewrites to table join operation.
        m_tester.sql("select i from R1 where i in (select si from P1)").fail();

        // calcite will use equal to rewrite IN
        m_tester.sql("select * from P1 where i in (16)").pass();

        m_tester.sql("select * from P1 where i in (16, 16)").pass();

        m_tester.sql("select * from P1 where i in (1,2,3,4,5,6,7,8,9,10)").fail();

        m_tester.sql("select * from P1 where i Not in (1, 2)").fail();

        m_tester.sql("select si from P1 where i in (1,2) and i in (1,3)").fail();

        m_tester.sql("select si from P1 where i in (1,2) or i not in (1,3)").fail();
        // calcite will use Join to rewrite IN (sub query)
        m_tester.sql("select si from P1 where si in (select i from R1)").fail();

        m_tester.sql("select i from R1 where i in (select si from P1)").fail();
    }

    public void testPartitionKeyEqualToTableColumn() {
        m_tester.sql("select * from P1 where i = si").fail();

        m_tester.sql("select * from P1 where NOT i <> si").fail();
    }

    public void testSetOp() {
        // All tables are replicated. Pass
        m_tester.sql("select I from R1 union (select I from R2 except select II from R3)").pass();

        // Only one table is partitioned but it has an equality filter based on its partitioning column. Pass
        m_tester.sql("select I from R1 union (select I from P1 where I = 1 except select II from R3)").pass();

        // Only one table is partitioned but it has an equality filter based on its partitioning column. Pass
        m_tester.sql("select I from R1 union select I from P1 where I = 1 union select II from R3").pass();

        // Two partitioned tables, both have equality filters based on their partitioning columns with
        // equal partitioning values. Positions of partitioning columns do not match. Pass
        m_tester.sql("select I, SI from P1  where I = 1 union " +
                     "select I, SI from R1 union " +
                     "select SI, I from P2   where I = 1").pass();

        // SP Join of two partitioned table and another partitioned table with
        // compatible partitioning values
        m_tester.sql("select P1.I, P2.SI from P1, P2  where P1.I = P2.I and P1.I = 1 union " +
                "select SI, I from P3 where P3.I = 1").pass();

        // Two partitioned tables, both have equality filters based on their partitioning columns.
        // Two SetOps. Fail.
        m_tester.sql("select I from P1  where I = 1 except " +
                     "(select I from R1 intersect select I from P2  where I = 1)").pass();

        // Only one table is partitioned without an equality filter based on its partitioning column. Fail
        m_tester.sql("select I from R1 union (select I from P1 except select II from R3)").fail();

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

        // Two partitioned tables, both have equality filters based on their partitioning columns.
        // Two SetOps. Fail.
        m_tester.sql("select I from P1  where I = 1 except " +
                     "(select I from R1 intersect select I from P2  where I = 2)").fail();

        // SP Join of two partitioned table and another partitioned table with
        // incompatible partitioning values
        m_tester.sql("select P1.I, P2.SI from P1, P2  where P1.I = P2.I and P1.I = 1 union " +
                "select SI, I from P3 where P3.I = 2").fail();

    }

}
