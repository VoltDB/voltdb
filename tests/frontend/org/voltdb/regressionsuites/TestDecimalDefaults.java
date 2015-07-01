/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.math.BigDecimal;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestDecimalDefaults extends RegressionSuite
{
    public TestDecimalDefaults(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE T(" +
                                 "A1 INTEGER NOT NULL, " +
                                 "A2 DECIMAL, " +
                                 "A3 DECIMAL DEFAULT 0, " +
                                 "A4 DECIMAL DEFAULT 999, " +
                                 "A5 DECIMAL DEFAULT 9.99E2, " +
                                 "A6 DECIMAL DEFAULT 1.012345678901, " +
                                 "PRIMARY KEY(A1));"
                                 );
        builder.addPartitionInfo("T", "A1");
        builder.addStmtProcedure("Insert", "INSERT INTO T(A1) VALUES(?);", "T.A1: 0");
        builder.addStmtProcedure("Select", "SELECT * FROM T WHERE A1 = ?;", "T.A1: 0");
        return builder;
    }

    public void testDecimalDefaults() throws Exception {
        final Client client = getClient();

        client.callProcedure("Insert", 13);
        VoltTable results = client.callProcedure("Select", 13).getResults()[0];
        results.advanceRow();
        BigDecimal answer = (BigDecimal)results.get("A3", VoltType.DECIMAL);
        assertEquals(0, answer.compareTo(new BigDecimal(0)));
        answer = (BigDecimal)results.get("A4", VoltType.DECIMAL);
        assertEquals(0, answer.compareTo(new BigDecimal(999)));
        answer = (BigDecimal)results.get("A5", VoltType.DECIMAL);
        assertEquals(0, answer.compareTo(new BigDecimal(999)));
        // ENG-1098
        answer = (BigDecimal)results.get("A6", VoltType.DECIMAL);
        assertEquals(0, answer.compareTo(new BigDecimal("1.012345678901")));
    }

    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestDecimalDefaults.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        boolean success;
        LocalCluster config = new LocalCluster("decimal-default.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
