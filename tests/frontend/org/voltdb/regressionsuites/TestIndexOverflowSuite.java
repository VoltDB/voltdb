/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestIndexOverflowSuite extends RegressionSuite {

    void callWithExpectedRowCount(Client client, String procName, int rowCount, Object... params) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = client.callProcedure(procName, params);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];
        assertEquals(rowCount, result.getRowCount());
    }

    public void testAll() throws IOException, ProcCallException {
        Client client = getClient();

        // insert one row
        client.callProcedure("P1.insert", 1, 1, 1, 1);

        callWithExpectedRowCount(client, "BasicEQParam", 1, 1);
        callWithExpectedRowCount(client, "BasicEQParam", 0, 6000000000L);
        callWithExpectedRowCount(client, "BasicEQParam", 0, -6000000000L);

        callWithExpectedRowCount(client, "BasicGTParam", 0, 1);
        callWithExpectedRowCount(client, "BasicGTParam", 0, 6000000000L);
        callWithExpectedRowCount(client, "BasicGTParam", 1, -6000000000L);

        callWithExpectedRowCount(client, "BasicLTParam", 0, 1);
        callWithExpectedRowCount(client, "BasicLTParam", 1, 6000000000L);
        callWithExpectedRowCount(client, "BasicLTParam", 0, -6000000000L);

        callWithExpectedRowCount(client, "BasicUnderflowEQ", 0);
        callWithExpectedRowCount(client, "BasicUnderflowGT", 1);
        callWithExpectedRowCount(client, "BasicUnderflowLT", 0);

        callWithExpectedRowCount(client, "BasicOverflowEQ", 0);
        callWithExpectedRowCount(client, "BasicOverflowGT", 0);
        callWithExpectedRowCount(client, "BasicOverflowLT", 1);

        callWithExpectedRowCount(client, "TwoColUnderflowEQ", 0, 1);
        callWithExpectedRowCount(client, "TwoColUnderflowGT", 1, 1);
        callWithExpectedRowCount(client, "TwoColUnderflowLT", 0, 1);

        callWithExpectedRowCount(client, "TwoColOverflowEQ", 0, 1);
        callWithExpectedRowCount(client, "TwoColOverflowGT", 0, 1);
        callWithExpectedRowCount(client, "TwoColOverflowLT", 1, 1);

        client.callProcedure("R1.insert", 1, 1, 1, 1);

        callWithExpectedRowCount(client, "Join", 1);
        callWithExpectedRowCount(client, "JoinReverse", 1);

        callWithExpectedRowCount(client, "JoinWithOrderOverflow", 0);
        callWithExpectedRowCount(client, "JoinWithOrderUnderflow", 1);

        // add some bigger rows
        client.callProcedure("P1.insert", 200, 2, 1, 1);
        client.callProcedure("R1.insert", 200, 2, 1, 1);

        // retry with expected same result
        callWithExpectedRowCount(client, "Join", 1);
        callWithExpectedRowCount(client, "JoinReverse", 1);
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestIndexOverflowSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestIndexOverflowSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestIndexOverflowSuite.class.getResource("indexoverflowsuite-ddl.sql"));
        project.addPartitionInfo("P1", "ID");
        project.addStmtProcedure("BasicEQParam",           "select * from P1 where ID = ?");
        project.addStmtProcedure("BasicGTParam",           "select * from P1 where ID > ?");
        project.addStmtProcedure("BasicLTParam",           "select * from P1 where ID < ?");

        project.addStmtProcedure("BasicUnderflowEQ",       "select * from P1 where ID = -6000000000");
        project.addStmtProcedure("BasicUnderflowGT",       "select * from P1 where ID > -6000000000");
        project.addStmtProcedure("BasicUnderflowLT",       "select * from P1 where ID < -6000000000");

        project.addStmtProcedure("BasicOverflowEQ",        "select * from P1 where ID = 6000000000");
        project.addStmtProcedure("BasicOverflowGT",        "select * from P1 where ID > 6000000000");
        project.addStmtProcedure("BasicOverflowLT",        "select * from P1 where ID < 6000000000");

        project.addStmtProcedure("TwoColUnderflowEQ",      "select * from P1 where ID = ? and TINY = -200");
        project.addStmtProcedure("TwoColUnderflowGT",      "select * from P1 where ID = ? and TINY > -200");
        project.addStmtProcedure("TwoColUnderflowLT",      "select * from P1 where ID = ? and TINY < -200");

        project.addStmtProcedure("TwoColOverflowEQ",       "select * from P1 where ID = ? and TINY = 200");
        project.addStmtProcedure("TwoColOverflowGT",       "select * from P1 where ID = ? and TINY > 200");
        project.addStmtProcedure("TwoColOverflowLT",       "select * from P1 where ID = ? and TINY < 200");

        project.addStmtProcedure("Join",                   "select * from P1, R1 where P1.ID = R1.TINY");
        project.addStmtProcedure("JoinReverse",            "select * from P1, R1 where P1.TINY = R1.ID");
        project.addStmtProcedure("JoinWithOrderOverflow",  "select * from P1, R1 where P1.ID = R1.ID and P1.TINY >= 200");
        project.addStmtProcedure("JoinWithOrderUnderflow", "select * from P1, R1 where P1.ID = R1.ID and P1.TINY >= -200");

        boolean success;

        // JNI
        config = new LocalCluster("testindexes-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
