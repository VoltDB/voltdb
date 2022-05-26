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
package org.voltdb.regressionsuites.statistics;

import static org.awaitility.Awaitility.await;
import static org.voltdb.ClockSkewCollectorAgent.CLOCK_SKEW;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import junit.framework.Test;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.regressionsuites.StatisticsTestSuiteBase;
import org.voltdb.task.TaskStatsSource;
import org.voltdb.tasks.clockskew.ClockSkewStats.Skew;

public class TestStatisticsSuiteClockSkewStats extends StatisticsTestSuiteBase {
    public TestStatisticsSuiteClockSkewStats(String name) {
        super(name);

        Awaitility.setDefaultPollInterval(Durations.ONE_SECOND);
        Awaitility.setDefaultTimeout(30L, TimeUnit.SECONDS);
    }

    public void testClockSkewStatistics() throws Exception {
        Client client  = getFullyConnectedClient();

        ColumnInfo[] expectedSchema = new ColumnInfo[6];
        expectedSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedSchema[3] = new ColumnInfo(Skew.SKEW_TIME.name(), Skew.SKEW_TIME.m_type);
        expectedSchema[4] = new ColumnInfo(Skew.REMOTE_HOST_ID.name(), Skew.REMOTE_HOST_ID.m_type);
        expectedSchema[5] = new ColumnInfo(Skew.REMOTE_HOST_NAME.name(), Skew.REMOTE_HOST_NAME.m_type);
        VoltTable expectedTable = new VoltTable(expectedSchema);

        await("for any task to kicks in")
                .until(
                        () -> client.callProcedure("@Statistics", "SYSTEM_TASK", 0).getResults(),
                        this::statsCollectionHasBeenScheduled);

        // when
        VoltTable[] results = await("to gather some clock skew statistics")
                .until(
                        () -> client.callProcedure("@Statistics", CLOCK_SKEW, 0).getResults(),
                        this::hasAtLeastOneClockSkewStatistics);

        // then
        VoltTable table = results[0];
        validateSchema(table, expectedTable);
        // NOTE we collect 2 clock skew replies for each node in 3 node cluster
        assertEquals(2 * HOSTS, table.getRowCount());

        Set<String> expectedPairs = createSet("01", "02", "10", "12", "20", "21");
        for (int i = 0; i < 2 * HOSTS; i++) {
            table.advanceRow();
            int fromHostId = (int) table.get("HOST_ID", VoltType.INTEGER);
            int toHostId = (int) table.get(Skew.REMOTE_HOST_ID.name(), Skew.REMOTE_HOST_ID.m_type);
            assertTrue(fromHostId >= 0);
            assertTrue(toHostId >= 0);
            assertNotNull(table.getString("HOSTNAME"));
            assertNotNull(table.getString(Skew.REMOTE_HOST_NAME.name()));
            assertTrue(((int) table.get(Skew.SKEW_TIME.name(), Skew.SKEW_TIME.m_type)) >= 0);

            String pair = fromHostId + "" + toHostId;
            assertTrue(expectedPairs.contains(pair));
            expectedPairs.remove(pair);
        }

        assertTrue(expectedPairs.isEmpty());
    }

    private Set<String> createSet(String... items) {
        return new HashSet<>(Arrays.asList(items));
    }

    private boolean hasAtLeastOneClockSkewStatistics(VoltTable[] table) {
        return table.length == 1 && table[0].getRowCount() > 0;
    }

    private boolean statsCollectionHasBeenScheduled(VoltTable[] table) {
        boolean hasData = table.length == 1 && table[0].getRowCount() > 0;
        if (hasData) {
            VoltTable tbl = table[0];
            tbl.advanceRow();
            return tbl.getString(TaskStatsSource.Task.TASK_NAME.name()).equals("_SYSTEM_ClockSkewCollector")
                    && tbl.getString(TaskStatsSource.Task.STATE.name()).equals("RUNNING");
        } else {
            return false;
        }
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        Map<String, String> envs = ImmutableMap.of("CLOCK_SKEW_SCHEDULER_INTERVAL", "PT1M");
        return StatisticsTestSuiteBase
                .suite(TestStatisticsSuiteClockSkewStats.class, envs);
    }
}
