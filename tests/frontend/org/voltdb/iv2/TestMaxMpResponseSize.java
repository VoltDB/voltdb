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

package org.voltdb.iv2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

public class TestMaxMpResponseSize extends JUnit4LocalClusterTest {
    @Test
    public void calculateMpMaxTotalResponse() {
        long systemMemory = Runtime.getRuntime().maxMemory();
        long defaultMax = (long) (systemMemory * 0.65D);
        assertEquals(defaultMax, MpTransactionState.calculateMpMaxTotalResponse());

        // Test static size
        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, Long.toString(512L * 1024 * 1024));
        assertEquals(512L * 1024 * 1024, MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, Long.toString(5L * 1024 * 1024 * 1024));
        assertEquals(5L * 1024 * 1024 * 1024, MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, Long.toString(100));
        assertEquals(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_MIN, MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, Long.toString(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, Long.toString(-5L * 1024 * 1024 * 1024));
        assertEquals(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_MIN, MpTransactionState.calculateMpMaxTotalResponse());

        // Test percentage of heap
        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, "30%");
        assertEquals((long) (systemMemory * 0.30D), MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, "130%");
        assertEquals((long) (systemMemory * 1.30D), MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, "9999999999999999999%");
        assertEquals(Long.MAX_VALUE, MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, "-20%");
        assertEquals(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_MIN, MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, "0.5%");
        assertEquals(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_MIN, MpTransactionState.calculateMpMaxTotalResponse());

        // Test not numbers should always return default
        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, "7897a456");
        assertEquals(defaultMax, MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, "7897a456%");
        assertEquals(defaultMax, MpTransactionState.calculateMpMaxTotalResponse());

        System.setProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY, "7897.456");
        assertEquals(defaultMax, MpTransactionState.calculateMpMaxTotalResponse());
    }

    @Test
    public void largeMpResponsesFail() throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(true);
        LocalCluster cluster = new LocalCluster("mp_max_transaction.jar", 12, 1, 0, BackendTarget.NATIVE_EE_JNI);
        Client client = null;
        try {
            builder.addLiteralSchema("CREATE TABLE my_table (key INT NOT NULL, value VARCHAR(4096) NOT NULL);"
                    + "PARTITION TABLE my_table ON COLUMN key;"
                    + "CREATE PROCEDURE Insert PARTITION ON TABLE my_table COLUMN key AS INSERT INTO my_table (key, value) VALUES (?, ?);");
            cluster.setHasLocalServer(false);
            cluster.overrideAnyRequestForValgrind();
            cluster.compile(builder);
            cluster.startCluster();

            ClientConfig config = new ClientConfig();
            config.setTopologyChangeAware(true);
            client = cluster.createClient(config);

            AtomicInteger failures = new AtomicInteger();
            // Load cluster with data
            for (int i = 0; i < 4096; ++i) {
                client.callProcedure(r -> {
                    if (r.getStatus() != ClientResponse.SUCCESS) {
                        failures.getAndIncrement();
                    }
                }, "Insert", i, RandomStringUtils.random(4096));
            }
            client.drain();

            assertEquals("Some inserts failed", 0, failures.get());

            // Should succeed with default mp max response size
            client.callProcedure("@AdHoc", "SELECT COUNT(DISTINCT value) FROM my_table");

            client.close();
            client = null;

            cluster.shutdownSave(cluster.createAdminClient(new ClientConfig()));
            cluster.waitForNodesToShutdown();

            // Reduce max mp max response size to minimum
            cluster.setJavaProperty(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_KEY,
                    Long.toString(MpTransactionState.MP_MAX_TOTAL_RESP_SIZE_MIN));

            cluster.startUp(false);
            client = cluster.createClient(config);

            try {
                client.callProcedure("@AdHoc", "SELECT COUNT(DISTINCT value) FROM my_table");
                fail("Should not have been able to perform transaction");
            } catch (ProcCallException e) {}
        } finally {
            if (client != null) {
                client.close();
            }
            cluster.shutDown();
        }
    }
}
