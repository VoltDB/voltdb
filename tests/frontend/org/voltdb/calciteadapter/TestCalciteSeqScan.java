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

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.SchemaPlus;
import org.voltdb.types.PlannerType;

public class TestCalciteSeqScan extends TestCalciteBase {

    SchemaPlus m_schemaPlus;

    @Override
    public void setUp() throws Exception {
        setupSchema(TestCalciteSeqScan.class.getResource("testcalcite-scan-ddl.sql"),
                "testcalcitescan", false);
        m_schemaPlus = schemaPlusFromDDL();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSeqScan() throws Exception {
        String sql;
        sql = "select * from R1";
        comparePlans(sql);
    }

    public void testSeqScanWithProjection() throws Exception {
        String sql;
        sql = "select i, si from R1";
        comparePlans(sql);
    }

    public void testSeqScanWithProjectionExpr() throws Exception {
        String sql;
        sql = "select i * 5 from R1";
        Map<String, String> ignores = new HashMap<>();
        ignores.put("EXPR$0", "C1");
        comparePlans(sql, ignores);
    }

    public void testSeqScanWithFilter() throws Exception {
        String sql;
        sql = "select i from R1 where i = 5";
        comparePlans(sql);
    }

    public void testSeqScanWithFilterParam() throws Exception {
        String sql;
        sql = "select i from R1 where i = ? and v = ?";
        comparePlans(sql);
    }

    public void testSeqScanWithStringFilter() throws Exception {
        String sql;
        sql = "select i from R1 where v = 'FOO1'";
        comparePlans(sql);
    }

    public void testSeqScanWithFilterWithTypeConversion() throws Exception {
        String sql;
        sql = "select i from R1 where si = 5";
        // calcite adds a CAST expression to cast SMALLINT to INT
        Map<String, String> ignores = new HashMap<>();
        ignores.put("\"TYPE\":7,\"VALUE_TYPE\":5,\"LEFT\":{\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1}",
                "\"TYPE\":32,\"VALUE_TYPE\":4,\"COLUMN_IDX\":1");

        comparePlans(sql, ignores);
    }

    //    public void testSeqScanPartitioned() throws Exception {
//        String sql;
//        sql = "select * from P1";
//        comparePlans(sql);
//    }
//
//    public void testSeqScanWithProjectionPartitioned() throws Exception {
//        String sql;
//        sql = "select i from P1";
//        comparePlans(sql);
//    }
//
//    public void testSeqScanWithProjectionPartitioned1() throws Exception {
//        String sql;
//        sql = "select i * 5 from P1";
//        comparePlans(sql);
//    }
//
//    public void testSeqScanWithFilterPartitioned() throws Exception {
//        String sql;
//        sql = "select i from P1 where si = 5";
//        comparePlans(sql);
//    }

}
