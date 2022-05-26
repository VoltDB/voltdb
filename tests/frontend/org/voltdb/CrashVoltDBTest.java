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

package org.voltdb;

import org.junit.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrashVoltDBProc;

public class CrashVoltDBTest extends JUnit4LocalClusterTest {

    @Test
    public void testSimple() throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addProcedure(CrashVoltDBProc.class);
        /*boolean success = builder.compile(Configuration.getPathToCatalogForTest("crash.jar"), 1, 1, 0, "localhost");
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("crash.xml"));*/

        LocalCluster cluster = new LocalCluster("crash.jar",
                2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.setHasLocalServer(true);
        boolean success = cluster.compile(builder);
        assert (success);
        cluster.startUp(true);

        final String listener = cluster.getListenerAddresses().get(0);
        final Client client = ClientFactory.createClient();
        //client.createConnection(listener);
        client.createConnection(listener);

        try {
            client.callProcedure("CrashVoltDBProc");
        }
        catch (Exception e) {

        }

        Thread.sleep(10000);

        client.close();
        cluster.shutDown();
    }

}
