/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.tasks.clockskew;

import com.google_voltpatches.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

import java.time.Clock;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.voltdb.ClockSkewCollectorAgent.HOST_ID;
import static org.voltdb.ClockSkewCollectorAgent.NODE_CURRENT_TIME;

public class TestClockSkewStats {

    private final VoltLogger logger = Mockito.mock(VoltLogger.class);
    private final Clock clock = Mockito.mock(Clock.class);
    private final VoltDBInterface voltDb = Mockito.mock(VoltDBInterface.class);
    private final HostMessenger hostMessenger = Mockito.mock(HostMessenger.class);

    private ClockSkewStats collector;

    @Before
    public void setUp() throws Exception {
        when(voltDb.getMyHostId()).thenReturn(0);
        when(voltDb.getHostMessenger()).thenReturn(hostMessenger);
        when(hostMessenger.getHostname()).thenReturn("localhost");
        when(hostMessenger.getHostId()).thenReturn(0);

        collector = new ClockSkewStats(clock, voltDb, logger);
    }

    @Test
    public void shouldCollectSkewsFromRemote() {
        int localTime = 1000;
        int expectedSkewForHostOne = 100;
        int expectedSkewForHostTwo = 40;
        ClientResponse responseOne = createTimeReplayForHost(1, localTime - expectedSkewForHostOne);
        ClientResponse responseTwo = createTimeReplayForHost(2, localTime - expectedSkewForHostTwo);

        when(clock.millis()).thenReturn((long) localTime);

        // when
        collector.reportCompletion(null, null, responseOne);
        collector.reportCompletion(null, null, responseTwo);

        // then
        Map<Integer, Long> expectedStats = ImmutableMap.of(
                1, (long) expectedSkewForHostOne,
                2, (long) expectedSkewForHostTwo);
        assertEquals(expectedStats, collector.cachedSkews);

        // and when
        collector.clear();

        responseTwo = createTimeReplayForHost(2, localTime - expectedSkewForHostTwo);
        collector.reportCompletion(null, null, responseTwo);

        // then
        expectedStats = ImmutableMap.of(
                2, (long) expectedSkewForHostTwo);
        assertEquals(expectedStats, collector.cachedSkews);
    }

    @Test
    public void shouldSkipOnEmptyResult() {
        ClientResponse response = Mockito.mock(ClientResponse.class, "Empty response");

        when(clock.millis()).thenReturn(0L);
        when(response.getResults()).thenReturn(new VoltTable[0]);

        assertEquals(ImmutableMap.of(), collector.cachedSkews);

        // when
        collector.reportCompletion(null, null, response);

        // when
        collector.reportCompletion(null, null, null);

        // then
        assertEquals(ImmutableMap.of(), collector.cachedSkews);
    }

    @Test
    public void shouldCollectSkewsFromAggregatedResponse() {
        int localTime = 1000;
        int expectedSkewForHostOne = 100;
        int expectedSkewForHostTwo = 40;
        ClientResponse response = createTimeReplayForHost(
                1, localTime - expectedSkewForHostOne,
                2, localTime - expectedSkewForHostTwo);

        when(clock.millis()).thenReturn((long) localTime);

        // when
        collector.reportCompletion(null, null, response);

        // then
        Map<Integer, Long> expectedStats = ImmutableMap.of(
                1, (long) expectedSkewForHostOne,
                2, (long) expectedSkewForHostTwo);
        assertEquals(expectedStats, collector.cachedSkews);
    }

    @Test
    public void shouldAlertOnBigSkews() {
        ClientResponse response = createTimeReplayForHost(
                1, 100,
                2, 200,
                4, 199,
                3, 211,
                5, 99);

        when(clock.millis()).thenReturn(0L);

        // when
        collector.reportCompletion(null, null, response);

        // then
        verify(logger).info("Clock skew between node 0 and 1 is 100ms. Clock skew is collected on a best-effort basis - a large value may include anomalies, such as JVM GC or network latency.");
        verify(logger).info("Clock skew between node 0 and 4 is 199ms. Clock skew is collected on a best-effort basis - a large value may include anomalies, such as JVM GC or network latency.");
        verify(logger).info("Clock skew between node 0 and 2 is 200ms. Clock skew is collected on a best-effort basis - a large value may include anomalies, such as JVM GC or network latency.");
        verify(logger).info("Clock skew between node 0 and 3 is 211ms. Clock skew is collected on a best-effort basis - a large value may include anomalies, such as JVM GC or network latency.");
    }

    @Test
    public void shouldFilterResponseFromSelf() {
        int localTime = 1000;
        ClientResponse responseOne = createTimeReplayForHost(0, localTime);

        when(clock.millis()).thenReturn((long)localTime);

        // when
        collector.reportCompletion(null, null, responseOne);

        // then
        Map<Integer, Long> expectedStats = ImmutableMap.of();
        assertEquals(expectedStats, collector.cachedSkews);
    }

    @Test
    public void shouldCreateStatisticResponse() {
        ClientResponse response = createTimeReplayForHost(
                1, 10,
                2, 200,
                3, 30);

        when(clock.millis()).thenReturn(0L);
        collector.reportCompletion(null, null, response);

        when(hostMessenger.getHostDisplayNameForHostId(1)).thenReturn("pk-voltdb-cluster-1");
        when(hostMessenger.getHostDisplayNameForHostId(2)).thenReturn("pk-voltdb-cluster-2");
        when(hostMessenger.getHostDisplayNameForHostId(3)).thenReturn("pk-voltdb-cluster-3");

        // when
        Object[][] statsRows = collector.getStatsRows(true, 0L);

        // then
        assertThat(statsRows)
                .isNotNull()
                .isEqualTo(new Object[] {
                        new Object[] {0L, 0, "localhost", 10L, 1, "pk-voltdb-cluster-1"},
                        new Object[] {0L, 0, "localhost", 200L, 2, "pk-voltdb-cluster-2"},
                        new Object[] {0L, 0, "localhost", 30L, 3, "pk-voltdb-cluster-3"}
                });
    }

    private ClientResponse createTimeReplayForHost(int hostId, int time, int... pairs) {
        ClientResponse response = Mockito.mock(ClientResponse.class, "Response from " + hostId);
        VoltTable row = new VoltTable(
                new VoltTable.ColumnInfo(NODE_CURRENT_TIME, VoltType.BIGINT),
                new VoltTable.ColumnInfo(HOST_ID, VoltType.INTEGER));
        row.addRow(time, hostId);
        if (pairs.length > 0) {
            if (pairs.length % 2 != 0) {
                throw new IllegalArgumentException("each id needs to have its time pair, got " + Arrays.toString(pairs));
            }
            for (int i = 0; i < pairs.length; i += 2) {
                int remoteHostId = pairs[i];
                long remoteTime = pairs[i + 1];

                row.addRow(remoteTime, remoteHostId);
            }
        }
        when(response.getResults()).thenReturn(new VoltTable[]{row});
        return response;
    }
}
