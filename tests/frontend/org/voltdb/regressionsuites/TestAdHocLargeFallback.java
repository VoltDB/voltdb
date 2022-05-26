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

package org.voltdb.regressionsuites;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestAdHocLargeFallback extends JUnit4LocalClusterTest {

    @Test
    public void testAdHocLargeFallbackLogMessage() throws Exception {
        if (LocalCluster.isMemcheckDefined()) {
            // don't run this test under valgrind, as it needs IPC support.
            return;
        }
        System.out.println("testAdHocLargeFallbackLogMessage");
        String testSchema = "create table t (i integer not null, "
                + "inl_vc00 varchar(63 bytes), "
                + "inl_vc01 varchar(63 bytes), "
                + "longval varchar(500000));";
        String[] queries = {
                "insert into t values (1, '2', '3', '4');",
                "insert into t values (5, '6', '7', '8');",
                "delete from t where i = 1;",
                "update t set i = 0;",
                "select count(*) over (partition by i) from t;",
                "truncate table t;"
        };
        String logMessage = "6 queries planned through @AdHocLarge were converted to normal @AdHoc plans.";
        boolean adHocLarge = false;
        // First try normal @AdHoc, which will not generate any log message,
        // then try @AdHocLarge, which will generate the message.
        do {
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(testSchema);
            builder.setUseDDLSchema(true);
            LocalCluster cluster = new LocalCluster("adhoclarge-fallback.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
            cluster.setHasLocalServer(true);
            cluster.compile(builder);
            cluster.startUp();
            Client client = ClientFactory.createClient();
            client.createConnection(cluster.getListenerAddress(0));
            for (String query : queries) {
                assertEquals(ClientResponse.SUCCESS,
                        client.callProcedure(adHocLarge ? "@AdHocLarge" : "@AdHoc", query).getStatus());
            }
            client.close();
            cluster.shutDown();
            if (adHocLarge) {
                cluster.verifyLogMessage(logMessage);
            }
            else {
                // Regardless the number, any log message like "xx queries planned through..." is considered as failure.
                cluster.verifyLogMessageNotExist(logMessage.substring(2));
            }

            adHocLarge = ! adHocLarge;
        } while (adHocLarge);
    }
}
