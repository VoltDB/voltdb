/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter;

import org.apache.calcite.schema.SchemaPlus;

public class TestCalciteJoin extends TestCalciteBase {

    SchemaPlus m_schemaPlus;

    @Override
    public void setUp() throws Exception {
        setupSchema(TestCalciteJoin.class.getResource("testcalcite-scan-ddl.sql"),
                "testcalcitescan", false);
        m_schemaPlus = schemaPlusFromDDL();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testDistributedJoin() throws Exception {
        String sql;
        sql = "select * from P1 join P2 on P1.I = P2.I join P3 on P2.I = P3.I";
        comparePlans(sql, true);
    }

    public void testReplicatedJoin() throws Exception {
    // should be R2-R1 because of an additional filer on R2 with everything else equal
        String sql;
        sql = "select R1.i from R1 inner join " +
              "R2  on R2.i = R1.i where R2.v = 'foo';";

        comparePlans(sql, true);
    }

    public void testReplicatedSubqueriesJoin() throws Exception {
        String sql;
        sql = "select t1.v, t2.v "
              + "from "
              + "  (select * from R1 where v = 'foo') as t1 "
              + "  inner join "
              + "  (select * from R2 where f = 30.3) as t2 "
              + "on t1.i = t2.i "
              + "where t1.i = 3;";

        comparePlans(sql, true);
    }

}
