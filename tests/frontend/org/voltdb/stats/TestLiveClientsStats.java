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

import com.google.common.collect.ImmutableMap;
import org.assertj.core.data.MapEntry;
import org.junit.Test;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.Pair;
import org.voltdb.*;

import java.util.ArrayList;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.voltdb.StatsSource.StatsCommon.*;
import static org.voltdb.stats.LiveClientsStats.LiveClients.*;

public class TestLiveClientsStats {

    private static final MapEntry<Long, Pair<String, long[]>> ENTRY_1 = MapEntry.entry(0L, Pair.of("host1", new long[]{1, 2, 3, 4}));
    private static final MapEntry<Long, Pair<String, long[]>> ENTRY_2 = MapEntry.entry(1L, Pair.of("host2", new long[]{5, 6, 7, 8}));
    private static final MapEntry<Long, Pair<String, long[]>> ENTRY_3 = MapEntry.entry(2L, Pair.of("host3", new long[]{9, 10, 11, 12}));

    @Test
    public void shouldReturnSingleRow() {
        // Given
        ClientInterface clientInterface = mock(ClientInterface.class);
        when(clientInterface.getLiveClientStats()).thenReturn(
                ImmutableMap.ofEntries(
                        ENTRY_1,
                        ENTRY_2,
                        ENTRY_3
                )
        );

        VoltDBInterface voltDBInterface = mock(VoltDBInterface.class);
        when(voltDBInterface.getClientInterface()).thenReturn(clientInterface);
        VoltDB.replaceVoltDBInstanceForTest(voltDBInterface);

        LiveClientsStats stats = new LiveClientsStats();

        // When
        Iterator<Object> actual = stats.getStatsRowKeyIterator(false);

        // Then
        assertThat(actual)
                .toIterable()
                .hasSize(3);
    }

    @Test
    public void shouldPopulateSchemaWithGenericAndSpecificColumns() {
        // Given
        LiveClientsStats stats = new LiveClientsStats();

        ArrayList<VoltTable.ColumnInfo> columns = newArrayList();

        // When
        stats.populateColumnSchema(columns);

        // Then
        assertThat(columns).containsExactly(
                new VoltTable.ColumnInfo(TIMESTAMP.name(), TIMESTAMP.m_type),
                new VoltTable.ColumnInfo(HOST_ID.name(), HOST_ID.m_type),
                new VoltTable.ColumnInfo(HOSTNAME.name(), HOSTNAME.m_type),
                new VoltTable.ColumnInfo(CONNECTION_ID.name(), CONNECTION_ID.getType()),
                new VoltTable.ColumnInfo(CLIENT_HOSTNAME.name(), CLIENT_HOSTNAME.getType()),
                new VoltTable.ColumnInfo(ADMIN.name(), ADMIN.getType()),
                new VoltTable.ColumnInfo(OUTSTANDING_REQUEST_BYTES.name(), OUTSTANDING_REQUEST_BYTES.getType()),
                new VoltTable.ColumnInfo(OUTSTANDING_RESPONSE_MESSAGES.name(), OUTSTANDING_RESPONSE_MESSAGES.getType()),
                new VoltTable.ColumnInfo(OUTSTANDING_TRANSACTIONS.name(), OUTSTANDING_TRANSACTIONS.getType())
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

        LiveClientsStats stats = new LiveClientsStats();

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + LiveClientsStats.LiveClients.getValues().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When
        int position = stats.updateStatsRow(ENTRY_1, actualValues);

        // Then
        assertThat(position).isEqualTo(expectedColumnsCount);
        assertThat(actualValues)
                .hasSize(9)
                .containsSequence(
                        42, // hostMessenger.getHostId()
                        "answer", // hostMessenger.getHostId()
                        0L, // CONNECTION_ID (ENTRY_1.getKey())
                        "host1", // CLIENT_HOSTNAME (ENTRY_1.getValue().getFirst())
                        1L, // ADMIN (ENTRY_1.getValue().getSecond()[0])
                        2L, // OUTSTANDING_REQUEST_BYTES (ENTRY_1.getValue().getSecond()[1])
                        3L, // OUTSTANDING_RESPONSE_MESSAGES (ENTRY_1.getValue().getSecond()[2])
                        4L // OUTSTANDING_TRANSACTIONS (ENTRY_1.getValue().getSecond()[3])
                );
    }
}
