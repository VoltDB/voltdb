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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;
import static org.voltdb.StatsSource.StatsCommon.HOSTNAME;
import static org.voltdb.StatsSource.StatsCommon.HOST_ID;
import static org.voltdb.StatsSource.StatsCommon.TIMESTAMP;
import static org.voltdb.stats.GcStats.GC.NEWGEN_AVG_GC_TIME;
import static org.voltdb.stats.GcStats.GC.NEWGEN_GC_COUNT;
import static org.voltdb.stats.GcStats.GC.OLDGEN_AVG_GC_TIME;
import static org.voltdb.stats.GcStats.GC.OLDGEN_GC_COUNT;

import java.util.ArrayList;
import java.util.Iterator;
import org.junit.Test;
import org.voltdb.StatsSource;
import org.voltdb.VoltTable;

public class TestGcStats {

    @Test
    public void shouldPopulateSchemaWithGenericAndSpecificColumns() {
        // Given
        GcStats stats = new GcStats();

        ArrayList<VoltTable.ColumnInfo> columns = newArrayList();

        // When
        stats.populateColumnSchema(columns);

        // Then
        assertThat(columns).containsExactly(
                new VoltTable.ColumnInfo(TIMESTAMP.name(), TIMESTAMP.m_type),
                new VoltTable.ColumnInfo(HOST_ID.name(), HOST_ID.m_type),
                new VoltTable.ColumnInfo(HOSTNAME.name(), HOSTNAME.m_type),
                new VoltTable.ColumnInfo(NEWGEN_GC_COUNT.name(), NEWGEN_GC_COUNT.getType()),
                new VoltTable.ColumnInfo(NEWGEN_AVG_GC_TIME.name(), NEWGEN_AVG_GC_TIME.getType()),
                new VoltTable.ColumnInfo(OLDGEN_GC_COUNT.name(), OLDGEN_GC_COUNT.getType()),
                new VoltTable.ColumnInfo(OLDGEN_AVG_GC_TIME.name(), OLDGEN_AVG_GC_TIME.getType())
        );
    }

    @Test
    public void shouldReturnSingleRowConsistentWithInterval() {
        // Given
        GcStats stats = new GcStats();

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
        GcStats stats = new GcStats();

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + GcStats.GC.values().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When
        stats.gcInspectorReport(true, 1, 34);
        stats.gcInspectorReport(true, 2, 123);
        stats.gcInspectorReport(true, 1, 11);
        stats.gcInspectorReport(false, 1, 2002);
        stats.gcInspectorReport(false, 1, 4008);

        int position = stats.updateStatsRow(false, actualValues);

        // Then
        assertThat(position).isEqualTo(expectedColumnsCount);
        assertThat(actualValues).containsSequence(
                4, // NEWGEN_GC_COUNT
                42, // NEWGEN_AVG_GC_TIME
                2, // OLDGEN_GC_COUNT
                3005 // OLDGEN_AVG_GC_TIME
        );
    }

    @Test
    public void shouldPopulateTableWithLatestStatsForFirstTime() {
        // Given
        GcStats stats = new GcStats();

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + GcStats.GC.values().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When
        stats.gcInspectorReport(true, 1, 34);
        stats.gcInspectorReport(true, 2, 123);
        stats.gcInspectorReport(true, 1, 11);
        stats.gcInspectorReport(false, 1, 2002);
        stats.gcInspectorReport(false, 1, 4008);

        int position = stats.updateStatsRow(true, actualValues);

        // Then
        assertThat(position).isEqualTo(expectedColumnsCount);
        assertThat(actualValues).containsSequence(
                4, // NEWGEN_GC_COUNT
                42, // NEWGEN_AVG_GC_TIME
                2, // OLDGEN_GC_COUNT
                3005 // OLDGEN_AVG_GC_TIME
        );
    }

    @Test
    public void shouldPopulateTableWithLatestStatsMultipleTimes() {
        // Given
        GcStats stats = new GcStats();

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + GcStats.GC.values().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When & Then
        stats.gcInspectorReport(true, 1, 34);
        stats.gcInspectorReport(false, 1, 4008);
        stats.updateStatsRow(true, actualValues);

        assertThat(actualValues).containsSequence(
                1, // NEWGEN_GC_COUNT
                34, // NEWGEN_AVG_GC_TIME
                1, // OLDGEN_GC_COUNT
                4008 // OLDGEN_AVG_GC_TIME
        );

        stats.gcInspectorReport(true, 2, 123);
        stats.gcInspectorReport(true, 1, 11);
        stats.gcInspectorReport(false, 1, 2002);
        stats.updateStatsRow(true, actualValues);

        assertThat(actualValues).containsSequence(
                3, // NEWGEN_GC_COUNT
                44, // NEWGEN_AVG_GC_TIME
                1, // OLDGEN_GC_COUNT
                2002 // OLDGEN_AVG_GC_TIME
        );
    }

    @Test
    public void shouldPopulateTableWithLatestStatsThenReportCorrectTotals() {
        // Given
        GcStats stats = new GcStats();

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + GcStats.GC.values().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When & Then
        stats.gcInspectorReport(true, 1, 34);
        stats.gcInspectorReport(false, 1, 4008);
        stats.updateStatsRow(true, actualValues);

        stats.gcInspectorReport(true, 2, 123);
        stats.gcInspectorReport(true, 1, 11);
        stats.gcInspectorReport(false, 1, 2002);
        stats.updateStatsRow(true, actualValues);

        stats.gcInspectorReport(true, 1, 42);
        stats.gcInspectorReport(false, 1, 120);
        stats.updateStatsRow(false, actualValues);

        assertThat(actualValues).containsSequence(
                5, // NEWGEN_GC_COUNT
                42, // NEWGEN_AVG_GC_TIME
                3, // OLDGEN_GC_COUNT
                2043 // OLDGEN_AVG_GC_TIME
        );
    }

    @Test
    public void shouldReportZerosIfNothingHappened() {
        // Given
        GcStats stats = new GcStats();

        int expectedColumnsCount = StatsSource.StatsCommon.values().length + GcStats.GC.values().length;
        Object[] actualValues = new Object[expectedColumnsCount];

        // When & Then
        stats.gcInspectorReport(true, 1, 34);
        stats.gcInspectorReport(false, 1, 4008);
        stats.updateStatsRow(true, actualValues);
        stats.updateStatsRow(true, actualValues);

        assertThat(actualValues).containsSequence(
                0, // NEWGEN_GC_COUNT
                0, // NEWGEN_AVG_GC_TIME
                0, // OLDGEN_GC_COUNT
                0 // OLDGEN_AVG_GC_TIME
        );
    }
}
