/* This file is part of VoltDB.
 * Copyright (C) 2022 VoltDB Inc.
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

public class LimitsStatsTest {

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
        assertThat(actual.next()).isEqualTo(stats);
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
                new VoltTable.ColumnInfo(HOST_ID.name(), HOST_ID.m_type),
                new VoltTable.ColumnInfo(HOSTNAME.name(), HOSTNAME.m_type),
                new VoltTable.ColumnInfo(FILE_DESCRIPTORS_LIMIT.name(), FILE_DESCRIPTORS_LIMIT.getType()),
                new VoltTable.ColumnInfo(FILE_DESCRIPTORS_OPEN.name(), FILE_DESCRIPTORS_OPEN.getType()),
                new VoltTable.ColumnInfo(CLIENT_CONNECTIONS_LIMIT.name(), CLIENT_CONNECTIONS_LIMIT.getType()),
                new VoltTable.ColumnInfo(CLIENT_CONNECTIONS_OPEN.name(), CLIENT_CONNECTIONS_OPEN.getType())
        );
    }

    @Test
    public void shouldPopulateTable() {
        // Given
        HostMessenger hostMessenger = mock(HostMessenger.class);
        when(hostMessenger.getHostId()).thenReturn(42);
        when(hostMessenger.getHostname()).thenReturn("answer");

        VoltDBInterface voltDBInterface = mock(VoltDBInterface.class);
        when(voltDBInterface.getHostMessenger()).thenReturn(hostMessenger);

        VoltDB.replaceVoltDBInstanceForTest(voltDBInterface);

        FileDescriptorsTracker fileDescriptorsTracker = mock(FileDescriptorsTracker.class);
        ClientConnectionsTracker clientConnectionsTracker = mock(ClientConnectionsTracker.class);

        when(fileDescriptorsTracker.getOpenFileDescriptorLimit()).thenReturn(442);
        when(fileDescriptorsTracker.getOpenFileDescriptorCount()).thenReturn(20);
        when(clientConnectionsTracker.getMaxNumberOfAllowedConnections()).thenReturn(142);
        when(clientConnectionsTracker.getConnectionsCount()).thenReturn(2);

        LimitsStats stats = new LimitsStats(
                fileDescriptorsTracker,
                clientConnectionsTracker
        );

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + getValues().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When
        int position = stats.updateStatsRow(stats, actualValues);

        // Then
        assertThat(position).isEqualTo(expectedColumnsCount);
        assertThat(actualValues).containsSequence(
                42, // hostMessenger.getHostId()
                "answer", // hostMessenger.getHostname()
                442, // getOpenFileDescriptorLimit()
                20, // getOpenFileDescriptorCount()
                142, // getMaxNumberOfAllowedConnections()
                2 // getConnectionsCount()
        );
    }
}
