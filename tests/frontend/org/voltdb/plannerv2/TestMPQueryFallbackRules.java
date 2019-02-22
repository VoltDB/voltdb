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
        m_tester.sql("select * from R2").test();

        m_tester.sql("select i, si from R1").test();

        m_tester.sql("select i, si from R1 where si = 9").test();
    }

    // Partitioned with no filter, always a MP query
    public void testPartitionedWithoutFilter() {
        m_tester.sql("select * from P1").testFail();

        m_tester.sql("select i from P1").testFail();
    }

    public void testPartitionedWithFilter() {
        // equal condition on partition key
        m_tester.sql("select * from P1 where i = 1").test();

        m_tester.sql("select * from P1 where 1 = i").test();

        // other conditions on partition key
        m_tester.sql("select * from P1 where i > 10").testFail();
        m_tester.sql("select * from P1 where i <> 10").testFail();

        // equal condition on partition key with ANDs
        m_tester.sql("select si, v from P1 where 7=si and i=2").test();
        m_tester.sql("select si, v from P1 where 7>si and i=2 and ti<3").test();

        m_tester.sql("SELECT si + 1 FROM P1 WHERE 7 = i").test();
        m_tester.sql("SELECT max(v) FROM P1 WHERE 7 = i").test();

        // equal condition on partition key with ORs
        m_tester.sql("select si, v from P1 where 7=si or i=2").testFail();

        // equal condition on partition key with ORs and ANDs
        m_tester.sql("select si, v from P1 where 7>si or (i=2 and ti<3)").testFail();
        m_tester.sql("select si, v from P1 where 7>si and (i=2 and ti<3)").test();
        m_tester.sql("select si, v from P1 where (7>si or ti=2) and i=2").test();
        m_tester.sql("select si, v from P1 where (7>si or ti=2) or i=2").testFail();

        // equal condition with some expression that always TURE
        m_tester.sql("select si, v from P1 where (7=si and i=2) and 1=1").test();
        m_tester.sql("select si, v from P1 where (7=si and i=2) and true").test();
        m_tester.sql("select si, v from P1 where (7=si and i=2) or 1=1").testFail();

        // equal condition with some expression that always FALSE
        m_tester.sql("select si, v from P1 where (7=si and i=2) and 1=2").test();

        m_tester.sql("select si, v from P1 where (7=si and i=2) or 1=2").test();
    }

    public void testPartitionedWithNotFilter() {
        // when comes to NOT operator, we need to decide if the complement of its
        // operand is single-partitioned
        m_tester.sql("select * from P1 where NOT i <> 15").test();

        m_tester.sql("select * from P1 where NOT (NOT i = 1)").test();

        m_tester.sql("select * from P1 where NOT (i <> 15 OR si = 16)").test();

        m_tester.sql("select * from P1 where NOT ( NOT (i = 15 AND si = 16))").test();

        m_tester.sql("select * from P1 where NOT si <> 15").testFail();

        m_tester.sql("select * from P1 where NOT (i <> 15 AND si = 16)").testFail();

        m_tester.sql("select * from P1 where NOT ( NOT (i = 15 OR si = 16))").testFail();
    }

    public void testJoin() {
        m_tester.sql("select R1.i, R2.v from R1, R2 " +
                "where R2.si = R1.i and R2.v = 'foo'").test();

        m_tester.sql("select R1.i, R2.v from R1 inner join R2 " +
                "on R2.si = R1.i where R2.v = 'foo'").test();

        m_tester.sql("select R2.si, R1.i from R1 inner join " +
                "R2 on R2.i = R1.si where R2.v = 'foo' and R1.si > 4 and R1.ti > R2.i").test();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si where R1.I + R2.ti = 5").test();
    }

    public void testJoinPartitionedTable() {
        // when join 2 partitioned table, assume it is always MP
        m_tester.sql("select P1.i, P2.v from P1, P2 " +
                "where P2.si = P1.i and P2.v = 'foo'").testFail();

        m_tester.sql("select P1.i, P2.v from P1, P2 " +
                "where P2.si = P1.i and P2.i = 34").testFail();

        m_tester.sql("select P1.i, P2.v from P1 inner join P2 " +
                "on P2.si = P1.i where P2.v = 'foo'").testFail();

        // when join a partitioned table with a replicated table,
        // if the filtered result on the partitioned table is not SP, then the query is MP
        m_tester.sql("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.v = 'foo'").testFail();

        m_tester.sql("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.i > 3").testFail();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and (P2.i > 3 or P2.i =1)").testFail();

        // when join a partitioned table with a replicated table,
        // if the filtered result on the partitioned table is SP, then the query is SP
        m_tester.sql("select R1.i, P2.v from R1, P2 " +
                "where P2.si = R1.i and P2.i = 3").test();

        /*m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i where P2.i =3").test();*/

        /*m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i where P2.i =3 and P2.v = 'bar'").test();*/

        // when join a partitioned table with a replicated table,
        // if the join condition can filter the partitioned table in SP, then the query is SP
        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and P2.i =3").test();

        m_tester.sql("select R1.i, P2.v from R1 inner join P2 " +
                "on P2.si = R1.i and P2.i =3 where P2.v = 'bar'").test();
    }

    public void testThreeWayJoinWithoutFilter() {
        // three-way join on replicated tables, SP
        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si inner join " +
                "R3 on R2.i = R3.ii").test();

        // all partitioned, MP
        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.si inner join " +
                "P3 on P2.i = P3.i").testFail();

        // one of them partitioned, MP
        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.si inner join " +
                "R3 on R2.i = R3.ii").testFail();

        m_tester.sql("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.si inner join " +
                "R3 on P2.i = R3.ii").testFail();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.si inner join " +
                "P3 on R2.i = P3.i").testFail();

        // two of them partitioned, MP
        m_tester.sql("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.si inner join " +
                "P3 on P2.i = P3.i").testFail();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.si inner join " +
                "P3 on R2.i = P3.i").testFail();

        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.si inner join " +
                "R3 on P2.i = R3.ii").testFail();

        // this is tricky. Note `P1.si = R2.i` will produce a Calc with CAST.
        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.i = R3.ii").testFail();
    }

    public void testThreeWayJoinWithFilter() {
        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo'").test();

        m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where P1.si > 4 and R3.vc <> 'foo'").testFail();

        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo'").testFail();

        m_tester.sql("select P1.i from P1 inner join " +
                "P2  on P1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo' and P2.i = 5").testFail();

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "P3 on R2.v = P3.v where R1.si > 4 and P3.si = 6").testFail();

        /*m_tester.sql("select P1.i from P1 inner join " +
                "R2  on P1.si = R2.i inner join " +
                "R3 on R2.v = R3.vc where P1.i = 4 and R3.vc <> 'foo'").test();*/

        /*m_tester.sql("select R1.i from R1 inner join " +
                "P2  on R1.si = P2.i inner join " +
                "R3 on P2.v = R3.vc where R1.si > 4 and R3.vc <> 'foo' and P2.i = 5").test();*/

        m_tester.sql("select R1.i from R1 inner join " +
                "R2  on R1.si = R2.i inner join " +
                "P3 on R2.v = P3.v where R1.si > 4 and P3.i = 6").test();
    }

    public void testSubqueriesJoin() {
        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from R2 where f = 30.3) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3").test();

        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where f = 30.3) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3").testFail();

        m_tester.sql("select t1.v, t2.v "
                + "from "
                + "  (select * from R1 where v = 'foo') as t1 "
                + "  inner join "
                + "  (select * from P2 where i = 303) as t2 "
                + "on t1.i = t2.i "
                + "where t1.i = 3").test();

        m_tester.sql("select RI1.bi from RI1, (select I from P2 order by I) P22 where RI1.i = P22.I").testFail();

        m_tester.sql("select RI1.bi from RI1, (select I from P2 where I = 5 order by I) P22 where RI1.i = P22.I").test();
    }

    public void testIn() {
        // calcite will use equal to rewrite IN
        m_tester.sql("select * from P1 where i in (16)").test();

        m_tester.sql("select * from P1 where i in (16, 16)").test();

        m_tester.sql("select * from P1 where i in (1,2,3,4,5,6,7,8,9,10)").testFail();

        m_tester.sql("select * from P1 where i Not in (1, 2)").testFail();

        m_tester.sql("select si from P1 where i in (1,2) and i in (1,3)").testFail();

        m_tester.sql("select si from P1 where i in (1,2) or i not in (1,3)").testFail();
        // calcite will use Join to rewrite IN (sub query)
        m_tester.sql("select si from P1 where si in (select i from R1)").testFail();

        m_tester.sql("select i from R1 where i in (select si from P1)").testFail();
    }

    public void testPartitionKeyEqualToTableColumn() {
        m_tester.sql("select * from P1 where i = si").testFail();

        m_tester.sql("select * from P1 where NOT i <> si").testFail();
    }
}
