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
package org.voltdb.stats;

import org.junit.Before;
import org.junit.Test;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.StatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltTable;

import java.util.ArrayList;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.voltdb.StatsSource.StatsCommon.*;
import static org.voltdb.stats.LimitsStats.Column.*;

public class TestLimitsStats {

    private static final int HOST_ID = 42;
    private static final String HOST_NAME = "answer";

    @Before
    public void setUp() {
        HostMessenger hostMessenger = mock(HostMessenger.class);
        when(hostMessenger.getHostId()).thenReturn(HOST_ID);
        when(hostMessenger.getHostname()).thenReturn(HOST_NAME);

        VoltDBInterface voltDBInterface = mock(VoltDBInterface.class);
        when(voltDBInterface.getHostMessenger()).thenReturn(hostMessenger);

        VoltDB.replaceVoltDBInstanceForTest(voltDBInterface);
    }

    @Test
    public void shouldReturnSingleRow() {
        // Given
        LimitsStats stats = new LimitsStats(
                mock(FileDescriptorsTracker.class),
                mock(ClientConnectionsTracker.class)
        );

        // When
        Iterator<Object> actual = stats.getStatsRowKeyIterator(false);

        // Then
        assertThat(actual).hasNext();
        assertThat(actual.next()).isEqualTo(false);
        assertThat(actual).isExhausted();
    }

    @Test
    public void shouldPopulateSchemaWithGenericAndSpecificColumns() {
        // Given
        LimitsStats stats = new LimitsStats(
                mock(FileDescriptorsTracker.class),
                mock(ClientConnectionsTracker.class)
        );

        ArrayList<VoltTable.ColumnInfo> columns = newArrayList();

        // When
        stats.populateColumnSchema(columns);

        // Then
        assertThat(columns).containsExactly(
                new VoltTable.ColumnInfo(TIMESTAMP.name(), TIMESTAMP.m_type),
                new VoltTable.ColumnInfo(StatsSource.StatsCommon.HOST_ID.name(),
                                         StatsSource.StatsCommon.HOST_ID.m_type),
                new VoltTable.ColumnInfo(HOSTNAME.name(), HOSTNAME.m_type),
                new VoltTable.ColumnInfo(FILE_DESCRIPTORS_LIMIT.name(), FILE_DESCRIPTORS_LIMIT.getType()),
                new VoltTable.ColumnInfo(FILE_DESCRIPTORS_OPEN.name(), FILE_DESCRIPTORS_OPEN.getType()),
                new VoltTable.ColumnInfo(CLIENT_CONNECTIONS_LIMIT.name(), CLIENT_CONNECTIONS_LIMIT.getType()),
                new VoltTable.ColumnInfo(CLIENT_CONNECTIONS_OPEN.name(), CLIENT_CONNECTIONS_OPEN.getType()),
                new VoltTable.ColumnInfo(ACCEPTED_CONNECTIONS.name(), ACCEPTED_CONNECTIONS.getType()),
                new VoltTable.ColumnInfo(DROPPED_CONNECTIONS.name(), DROPPED_CONNECTIONS.getType())
        );
    }

    @Test
    public void shouldReturnSingleRowConsistentWithInterval() {
        // Given
        LimitsStats stats = new LimitsStats(
                mock(FileDescriptorsTracker.class),
                mock(ClientConnectionsTracker.class)
        );

        // When
        Iterator<Object> actualFalse = stats.getStatsRowKeyIterator(false);
        Iterator<Object> actualTrue = stats.getStatsRowKeyIterator(true);

        // Then
        assertThat(actualFalse)
                .toIterable()
                .containsExactly(false);

        assertThat(actualTrue)
                .toIterable()
                .containsExactly(true);
    }

    @Test
    public void shouldPopulateTableWithTotalStats() {
        // Given
        LimitsStats stats = new LimitsStatsTester().shouldReturn(
                442,
                20,
                142,
                2,
                112233,
                456
        );

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + getValues().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When
        int position = stats.updateStatsRow(false, actualValues);

        // Then
        assertThat(position).isEqualTo(expectedColumnsCount);
        assertThat(actualValues).containsSequence(
                42, // hostMessenger.getHostId()
                "answer", // hostMessenger.getHostname()
                442, // getOpenFileDescriptorLimit()
                20, // getOpenFileDescriptorCount()
                142, // getMaxNumberOfAllowedConnections()
                2, // getConnectionsCount()
                112233, // getAcceptedConnectionsCount
                456 // getDroppedConnectionsCount
        );
    }

    @Test
    public void shouldPopulateTableWithIntervalStatsForFirstTime() {
        // Given
        LimitsStats stats = new LimitsStatsTester().shouldReturn(
                442,
                20,
                142,
                2,
                112233,
                456
        );

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + getValues().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When
        int position = stats.updateStatsRow(true, actualValues);

        // Then
        assertThat(position).isEqualTo(expectedColumnsCount);
        assertThat(actualValues).containsSequence(
                42, // hostMessenger.getHostId()
                "answer", // hostMessenger.getHostname()
                442, // getOpenFileDescriptorLimit()
                20, // getOpenFileDescriptorCount()
                142, // getMaxNumberOfAllowedConnections()
                2, // getConnectionsCount()
                112233, // getAcceptedConnectionsCount
                456 // getDroppedConnectionsCount
        );
    }

    @Test
    public void shouldPopulateTableWithIntervalStatsManyTimesWithNoChangeBetween() {
        // Given
        LimitsStats stats = new LimitsStatsTester().shouldReturn(
                442,
                20,
                142,
                2,
                112233,
                456
        );

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + getValues().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When
        stats.updateStatsRow(true, actualValues);
        int position = stats.updateStatsRow(true, actualValues);

        // Then
        assertThat(position).isEqualTo(expectedColumnsCount);
        assertThat(actualValues).containsSequence(
                42, // hostMessenger.getHostId()
                "answer", // hostMessenger.getHostname()
                442, // getOpenFileDescriptorLimit()
                20, // getOpenFileDescriptorCount()
                142, // getMaxNumberOfAllowedConnections()
                2, // getConnectionsCount()
                0, // getAcceptedConnectionsCount
                0 // getDroppedConnectionsCount
        );
    }

    @Test
    public void shouldPopulateTableWithIntervalStatsManyTimes() {
        // Given
        LimitsStatsTester limitsStatsTester = new LimitsStatsTester();
        LimitsStats stats = limitsStatsTester.shouldReturn(
                442,
                20,
                142,
                2,
                112233,
                456
        );

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + getValues().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When
        stats.updateStatsRow(true, actualValues);
        limitsStatsTester.shouldReturn(
                442,
                24,
                142,
                10,
                223321,
                532
        );

        int position = stats.updateStatsRow(true, actualValues);

        // Then
        assertThat(position).isEqualTo(expectedColumnsCount);
        assertThat(actualValues).containsSequence(
                42, // hostMessenger.getHostId()
                "answer", // hostMessenger.getHostname()
                442, // getOpenFileDescriptorLimit()
                24, // getOpenFileDescriptorCount()
                142, // getMaxNumberOfAllowedConnections()
                10, // getConnectionsCount()
                111088, // getAcceptedConnectionsCount
                76 // getDroppedConnectionsCount
        );
    }

    @Test
    public void shouldPopulateTableWithIntervalStatsManyTimes2() {
        // Given
        LimitsStatsTester limitsStatsTester = new LimitsStatsTester();
        LimitsStats stats = limitsStatsTester.shouldReturn(
                442,
                20,
                142,
                2,
                112233,
                456
        );

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + getValues().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When
        stats.updateStatsRow(true, actualValues);
        limitsStatsTester.shouldReturn(
                442,
                24,
                142,
                10,
                223321,
                532
        );

        stats.updateStatsRow(true, actualValues);
        int position = stats.updateStatsRow(false, actualValues);

        // Then
        assertThat(position).isEqualTo(expectedColumnsCount);
        assertThat(actualValues).containsSequence(
                42, // hostMessenger.getHostId()
                "answer", // hostMessenger.getHostname()
                442, // getOpenFileDescriptorLimit()
                24, // getOpenFileDescriptorCount()
                142, // getMaxNumberOfAllowedConnections()
                10, // getConnectionsCount()
                223321, // getAcceptedConnectionsCount
                532 // getDroppedConnectionsCount
        );
    }
}

class LimitsStatsTester {

    FileDescriptorsTracker fileDescriptorsTracker = mock(FileDescriptorsTracker.class);
    ClientConnectionsTracker clientConnectionsTracker = mock(ClientConnectionsTracker.class);

    LimitsStats limitsStats = new LimitsStats(
            fileDescriptorsTracker,
            clientConnectionsTracker
    );

    public LimitsStats shouldReturn(
            int openFileDescriptorLimit,
            int openFileDescriptorCount,
            int maxNumberOfAllowedConnections,
            int connectionsCount,
            int acceptedConnectionsCount,
            int droppedConnectionsCount
    ) {
        when(fileDescriptorsTracker.getOpenFileDescriptorLimit()).thenReturn(openFileDescriptorLimit);
        when(fileDescriptorsTracker.getOpenFileDescriptorCount()).thenReturn(openFileDescriptorCount);
        when(clientConnectionsTracker.getMaxNumberOfAllowedConnections()).thenReturn(maxNumberOfAllowedConnections);
        when(clientConnectionsTracker.getConnectionsCount()).thenReturn(connectionsCount);
        when(clientConnectionsTracker.getAcceptedConnectionsCount()).thenReturn(acceptedConnectionsCount);
        when(clientConnectionsTracker.getDroppedConnectionsCount()).thenReturn(droppedConnectionsCount);

        return limitsStats;
    }
}
