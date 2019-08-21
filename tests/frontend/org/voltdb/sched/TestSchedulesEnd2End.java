/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

package org.voltdb.sched;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Test;
import org.voltdb.LocalClustersTestBase;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public class TestSchedulesEnd2End extends LocalClustersTestBase {
    @Override
    public void setUp() throws Exception {
        super.setUp();

        configureClustersAndClients(Collections.singletonList(new ClusterConfiguration(4, 3, 1)), 2, 2);
    }

    @After
    public void cleanUpSchedules() {
        try {
            Client client = getClient(0);
            VoltTable table = client.callProcedure("@SystemCatalog", "SCHEDULES").getResults()[0];
            StringBuilder sb = new StringBuilder();
            while (table.advanceRow()) {
                sb.append("DROP SCHEDULE ").append(table.getString("SCHEDULE_NAME")).append(';');
            }
            if (sb.length() != 0) {
                client.callProcedure("@AdHoc", sb.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * Test schedules running on the system.
     *
     * Create 3 schedules which insert a summary row into a table at different intervals
     */
    @Test
    public void systemSchedules() throws Exception {
        Client client = getClient(0);

        String summaryTable = getTableName(3, TableType.REPLICATED);
        client.callProcedure("@AdHoc", "CREATE TABLE " + summaryTable
                + " (date TIMESTAMP NOT NULL, id INT NOT NULL, cnt BIGINT NOT NULL, key_sum DECIMAL NOT NULL, value_sum DECIMAL NOT NULL);");

        AtomicReference<Exception> error = new AtomicReference<>();
        Thread producer = new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {
                        insertRandomRows(client, 0, TableType.REPLICATED, 5);
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    error.set(e);
                }
            };
        };
        producer.start();

        String schedule1 = getMethodName() + "_1";
        String schedule2 = getMethodName() + "_2";
        String schedule3 = getMethodName() + "_3";

        String summaryFormat = "CREATE SCHEDULE %s %s ON ERROR IGNORE AS '@AdHoc', 'INSERT INTO " + summaryTable
                + " SELECT NOW, %d, COUNT(*), SUM(CAST(key as DECIMAL)), SUM(CAST(value AS DECIMAL)) FROM "
                + getTableName(0, TableType.REPLICATED) + ";';";

        client.callProcedure("@AdHoc", String.format(summaryFormat, schedule1, "DELAY PT0.05S", 1));
        client.callProcedure("@AdHoc", String.format(summaryFormat, schedule2, "CRON * * * * * *", 2));
        client.callProcedure("@AdHoc", String.format(summaryFormat, schedule3, "EVERY 75 MILLISECONDS", 3));

        // Give everything some time to run
        Thread.sleep(1000);
        producer.interrupt();
        producer.join();

        VoltTable table = getScheduleStats(client);
        assertEquals(3, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }

        client.callProcedure("@AdHoc",
                "ALTER SCHEDULE " + schedule1 + " DISABLE; ALTER SCHEDULE " + schedule2 + " DISABLE; ALTER SCHEDULE "
                        + schedule3 + " DISABLE;");

        Thread.sleep(5);

        table = getScheduleStats(client);
        assertEquals(3, table.getRowCount());
        while (table.advanceRow()) {
            String scheduleName = table.getString("NAME");
            int id = -1;
            if (schedule1.equalsIgnoreCase(scheduleName)) {
                id = 1;
            } else if (schedule2.equalsIgnoreCase(scheduleName)) {
                id = 2;
            } else if (schedule3.equalsIgnoreCase(scheduleName)) {
                id = 3;
            } else {
                fail("Unknown schedule " + scheduleName);
            }

            assertEquals("DISABLED", table.getString("STATE"));

            long summaryCount = client
                    .callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + summaryTable + " WHERE id = " + id + ";")
                    .getResults()[0].asScalarLong();
            long procedureInvocations = table.getLong("PROCEDURE_INVOCATIONS");
            long successfulProcedureInvocations = procedureInvocations - table.getLong("PROCEDURE_FAILURES");

            /*
             * There can be one extra invocation since stats are done when the result comes back and a schedule can be
             * stopped after procedure is called but before the result comes back
             */
            assertTrue(
                    "Summary table has " + summaryCount + " rows. Invocation count is " + successfulProcedureInvocations
                            + " for " + scheduleName,
                    summaryCount == successfulProcedureInvocations
                            || summaryCount == successfulProcedureInvocations + 1);

            long schedulerInvocations = table.getLong("SCHEDULER_INVOCATIONS");
            assertTrue(
                    schedulerInvocations == procedureInvocations || schedulerInvocations == procedureInvocations + 1);
        }
    }

    /*
     * Test a schedule which runs on the partitions
     *
     * Create a simple schedule which prunes each partition down to 10 entries
     *
     * Test that partition failover works correctly
     */
    @Test
    public void partitionsSchedules() throws Exception {
        Client client = getClient(0);

        String tableName = getTableName(0, TableType.PARTITIONED);

        String procName = getMethodName() + "_prune";
        client.callProcedure("@AdHoc",
                "CREATE PROCEDURE " + procName + " PARTITION ON TABLE " + tableName
                        + " COLUMN key PARAMETER 0 AS DELETE FROM "
                        + tableName + " WHERE CAST(? AS INTEGER) IS NOT NULL ORDER BY key OFFSET 10");

        AtomicReference<Exception> error = new AtomicReference<>();
        Thread producer = new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {
                        insertRandomRows(client, 0, TableType.PARTITIONED, 10);
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    error.set(e);
                }
            };
        };
        producer.start();

        String schedule = getMethodName();
        client.callProcedure("@AdHoc",
                "CREATE SCHEDULE " + schedule + " RUN ON PARTITIONS DELAY PT0.01S AS '" + procName
                        + "'");

        // Give everything some time to run
        Thread.sleep(1000);
        producer.interrupt();
        producer.join();

        VoltTable table = getScheduleStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }

        Thread.sleep(15);

        client.callProcedure("@AdHoc", "ALTER SCHEDULE " + schedule + " DISABLE;");

        Thread.sleep(5);

        table = getScheduleStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("DISABLED", table.getString("STATE"));
            assertTrue("Scheduler invocation count is lower than expected: " + table.getLong("SCHEDULER_INVOCATIONS"),
                    table.getLong("SCHEDULER_INVOCATIONS") >= 50);
            assertTrue("Procedure invocation count is lower than expected: " + table.getLong("PROCEDURE_INVOCATIONS"),
                    table.getLong("PROCEDURE_INVOCATIONS") >= 50);
        }

        assertEquals(60,
                client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + tableName).getResults()[0].asScalarLong());

        // Test that partition schedules fail over
        getCluster(0).killSingleHost(m_random.nextInt(3));

        table = getScheduleStats(client);
        assertEquals(4, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("DISABLED", table.getString("STATE"));
        }

        client.callProcedure("@AdHoc", "ALTER SCHEDULE " + schedule + " ENABLE;");

        table = getScheduleStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }
    }

    private static VoltTable getScheduleStats(Client client)
            throws NoConnectionsException, IOException, ProcCallException {
        return client.callProcedure("@Statistics", "SCHEDULES", 0).getResults()[0];
    }
}
