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
        comparePlans(sql, true);
    }

    public void testSeqScanWithProjection() throws Exception {
        String sql;
        sql = "select i from R1";
        comparePlans(sql, true);
    }

    public void testSeqScanWithProjection1() throws Exception {
        String sql;
        sql = "select i * 5 from R1";
        comparePlans(sql, true);
    }

    public void testSeqScanWithFilter() throws Exception {
        String sql;
        sql = "select i from R1 where si = 5";
        comparePlans(sql, true);
    }

    public void testSeqScanPartitioned() throws Exception {
        String sql;
        sql = "select * from P1";
        comparePlans(sql, false);
    }

    public void testSeqScanWithProjectionPartitioned() throws Exception {
        String sql;
        sql = "select i from P1";
        comparePlans(sql, false);
    }

    public void testSeqScanWithProjectionPartitioned1() throws Exception {
        String sql;
        sql = "select i * 5 from P1";
        comparePlans(sql, false);
    }

    public void testSeqScanWithFilterPartitioned() throws Exception {
        String sql;
        sql = "select i from P1 where si = 5";
        comparePlans(sql, false);
    }

}
