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
package org.voltdb.stats.procedure;

import static org.assertj.core.api.Assertions.entry;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Suppliers;
import com.google_voltpatches.common.collect.ImmutableMap;
import org.junit.Test;
import org.voltdb.TableShorthand;
import org.voltdb.MockRow;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.assertions.VoltTablesAssertion;

public class TestProcedureDetailAggregator_AggregateProcedureOutputStats {

    VoltTable inputSchemaTemplate = TableShorthand.tableFromShorthand(
            "PROCEDUREDETAIL(" +
            "TIMESTAMP:BIGINT," +
            VoltSystemProcedure.CNAME_HOST_ID + ":INTEGER," +
            "HOSTNAME:STRING," +
            VoltSystemProcedure.CNAME_SITE_ID + ":INTEGER," +
            "PARTITION_ID:INTEGER," +
            "PROCEDURE:STRING," +
            "STATEMENT:STRING," +
            "INVOCATIONS:BIGINT," +
            "TIMED_INVOCATIONS:BIGINT," +
            "MIN_EXECUTION_TIME:BIGINT," +
            "MAX_EXECUTION_TIME:BIGINT," +
            "AVG_EXECUTION_TIME:BIGINT," +
            "MIN_RESULT_SIZE:INTEGER," +
            "MAX_RESULT_SIZE:INTEGER," +
            "AVG_RESULT_SIZE:INTEGER," +
            "MIN_PARAMETER_SET_SIZE:INTEGER," +
            "MAX_PARAMETER_SET_SIZE:INTEGER," +
            "AVG_PARAMETER_SET_SIZE:INTEGER," +
            "ABORTS:BIGINT," +
            "FAILURES:BIGINT," +
            "TRANSACTIONAL:TINYINT" +
            ")"
    );

    VoltTable outputSchemaTemplate = TableShorthand.tableFromShorthand(
            "(" +
            "TIMESTAMP:BIGINT," +
            "PROCEDURE:STRING," +
            "WEIGHTED_PERC:BIGINT," +
            "INVOCATIONS:BIGINT," +
            "MIN_RESULT_SIZE:BIGINT," +
            "MAX_RESULT_SIZE:BIGINT," +
            "AVG_RESULT_SIZE:BIGINT," +
            "TOTAL_RESULT_SIZE_MB:BIGINT" +
            ")"
    );

    MockRow baseInputRowWithStatement = MockRow.of(
            inputSchemaTemplate,
            42L,
            1,
            "db2-volt-node-1",
            2,
            34,
            "org.voltdb.stats.procedure.ProcedureDetailResultTableTest",
            "<ALL>",
            2345L,
            100L,
            TimeUnit.MILLISECONDS.toNanos(22),
            TimeUnit.MILLISECONDS.toNanos(123),
            TimeUnit.MILLISECONDS.toNanos(42),
            32, // MIN_RESULT_SIZE
            123452, // MAX_RESULT_SIZE
            433, // AVG_RESULT_SIZE
            12,
            15,
            14,
            3421L,
            5L,
            (byte) 1
    );

    MockRow baseOutput = MockRow.of(
            outputSchemaTemplate,
            42L, // TIMESTAMP
            "org.voltdb.stats.procedure.ProcedureDetailResultTableTest",
            50L, // WEIGHTED_PERC
            2345L, // INVOCATIONS
            32L, // MIN_RESULT_SIZE
            123452L, // MAX_RESULT_SIZE
            433L, // AVG_RESULT_SIZE
            0L // TOTAL_RESULT_SIZE_MB
    );

    @Test
    public void shouldAcceptEmptyTable() {
        // Given
        ProcedureDetailAggregator aggregator = new ProcedureDetailAggregator(
                new ReadOnlyProcedureInformation(
                        Suppliers.ofInstance(Collections.emptyMap())
                )
        );
        // When
        VoltTable[] voltTables = aggregator.aggregateProcedureOutputStats(new VoltTable[]{inputSchemaTemplate});

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .isEmpty();
    }

    @Test
    public void shouldFilterOutNonAggregatesNonTransactionalAndReturnEmptyTable() {
        // Given
        ProcedureDetailAggregator aggregator = new ProcedureDetailAggregator(
                new ReadOnlyProcedureInformation(
                        Suppliers.ofInstance(Collections.emptyMap())
                )
        );

        MockRow row1 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "Roger,roger")
        );

        MockRow row2 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "PerlinNoise"),
                entry("STATEMENT", "Tron")
        );

        MockRow row3 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "AAArdvark"),
                entry("STATEMENT", "ACME")
        );

        MockRow row4 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("TRANSACTIONAL", (byte) 0)
        );

        VoltTable voltTable = inputSchemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);
        row4.insertInto(voltTable);

        // When
        VoltTable[] voltTables = aggregator.aggregateProcedureOutputStats(new VoltTable[]{voltTable});

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .isEmpty();
    }

    @Test
    public void shouldFilterOutNonAggregates() {
        // Given
        ProcedureDetailAggregator aggregator = new ProcedureDetailAggregator(
                new ReadOnlyProcedureInformation(
                        Suppliers.ofInstance(Collections.emptyMap())
                )
        );

        MockRow row1 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>")
        );

        MockRow row2 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "PerlinNoise"),
                entry("STATEMENT", "Tron")
        );

        MockRow row3 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "AAArdvark"),
                entry("STATEMENT", "<ALL>")
        );

        MockRow expectedRow1 = MockRow.with(
                baseOutput,
                entry("PROCEDURE", "AAArdvark")
        );

        MockRow expectedRow2 = MockRow.with(
                baseOutput,
                entry("PROCEDURE", "Order66")
        );

        VoltTable voltTable = inputSchemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);

        // When
        VoltTable[] voltTables = aggregator.aggregateProcedureOutputStats(new VoltTable[]{voltTable});

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .containsExactly(expectedRow1, expectedRow2);
    }

    @Test
    public void shouldSortByParametersSize() {
        // Given
        ProcedureDetailAggregator aggregator = new ProcedureDetailAggregator(
                new ReadOnlyProcedureInformation(
                        Suppliers.ofInstance(Collections.emptyMap())
                )
        );

        MockRow row1 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("AVG_RESULT_SIZE", 10),
                entry("INVOCATIONS", 1000)
        );

        MockRow row2 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "WillLandLast"),
                entry("STATEMENT", "<ALL>"),
                entry("AVG_RESULT_SIZE", 2),
                entry("INVOCATIONS", 1)
        );

        MockRow row3 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "AAArdvark"),
                entry("STATEMENT", "<ALL>"),
                entry("AVG_RESULT_SIZE", 456),
                entry("INVOCATIONS", 1000)
        );

        MockRow expectedRow1 = MockRow.with(
                baseOutput,
                entry("PROCEDURE", "AAArdvark"),
                entry("WEIGHTED_PERC", 98L),
                entry("AVG_RESULT_SIZE", 456L),
                entry("INVOCATIONS", 1000L)
        );

        MockRow expectedRow2 = MockRow.with(
                baseOutput,
                entry("PROCEDURE", "Order66"),
                entry("WEIGHTED_PERC", 2L),
                entry("AVG_RESULT_SIZE", 10L),
                entry("INVOCATIONS", 1000L)
        );

        MockRow expectedRow3 = MockRow.with(
                baseOutput,
                entry("PROCEDURE", "WillLandLast"),
                entry("WEIGHTED_PERC", 0L),
                entry("AVG_RESULT_SIZE", 2L),
                entry("INVOCATIONS", 1L)
        );

        VoltTable voltTable = inputSchemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);

        // When
        VoltTable[] voltTables = aggregator.aggregateProcedureOutputStats(new VoltTable[]{voltTable});

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .containsExactly(expectedRow1, expectedRow2, expectedRow3);
    }

    @Test
    public void shouldDeduplicateNonReadOnlyProcedures() {
        // Given
        ProcedureDetailAggregator aggregator = new ProcedureDetailAggregator(
                new ReadOnlyProcedureInformation(
                        Suppliers.ofInstance(
                                ImmutableMap.of(
                                        "Order66", false
                                )
                        )
                )
        );

        MockRow row1 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("PARTITION_ID", 33),
                entry("AVG_RESULT_SIZE", 2),
                entry("INVOCATIONS", 1000)
        );

        MockRow row2 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("PARTITION_ID", 33),
                entry("AVG_RESULT_SIZE", 4),
                entry("INVOCATIONS", 100)
        );

        MockRow row3 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("PARTITION_ID", 12),
                entry("AVG_RESULT_SIZE", 4),
                entry("INVOCATIONS", 1)
        );

        MockRow expectedRow1 = MockRow.with(
                baseOutput,
                entry("PROCEDURE", "Order66"),
                entry("WEIGHTED_PERC", 100L),
                entry("AVG_RESULT_SIZE", 2L),
                entry("INVOCATIONS", 1001L)
        );

        VoltTable voltTable = inputSchemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);

        // When
        VoltTable[] voltTables = aggregator.aggregateProcedureOutputStats(new VoltTable[]{voltTable});

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .containsExactly(expectedRow1);
    }

    @Test
    public void shouldNotDeduplicateReadOnlyProcedures() {
        // Given
        ProcedureDetailAggregator aggregator = new ProcedureDetailAggregator(
                new ReadOnlyProcedureInformation(
                        Suppliers.ofInstance(
                                ImmutableMap.of(
                                        "Order66", true
                                )
                        )
                )
        );

        MockRow row1 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("PARTITION_ID", 33),
                entry("AVG_RESULT_SIZE", 2),
                entry("INVOCATIONS", 1000)
        );

        MockRow row2 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("PARTITION_ID", 33),
                entry("AVG_RESULT_SIZE", 4),
                entry("INVOCATIONS", 100)
        );

        MockRow row3 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("PARTITION_ID", 12),
                entry("AVG_RESULT_SIZE", 4),
                entry("INVOCATIONS", 1)
        );

        MockRow expectedRow1 = MockRow.with(
                baseOutput,
                entry("PROCEDURE", "Order66"),
                entry("WEIGHTED_PERC", 100L),
                entry("AVG_RESULT_SIZE", 2L),
                entry("INVOCATIONS", 1101L)
        );

        VoltTable voltTable = inputSchemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);

        // When
        VoltTable[] voltTables = aggregator.aggregateProcedureOutputStats(new VoltTable[]{voltTable});

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .containsExactly(expectedRow1);
    }

    @Test
    public void shouldAssumeMissingProcedureIsNotReadOnly() {
        // Given
        ProcedureDetailAggregator aggregator = new ProcedureDetailAggregator(
                new ReadOnlyProcedureInformation(
                        Suppliers.ofInstance(Collections.emptyMap())
                )
        );

        MockRow row1 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("PARTITION_ID", 33),
                entry("AVG_RESULT_SIZE", 2),
                entry("INVOCATIONS", 1000)
        );

        MockRow row2 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("PARTITION_ID", 33),
                entry("AVG_RESULT_SIZE", 4),
                entry("INVOCATIONS", 100)
        );

        MockRow row3 = MockRow.with(
                baseInputRowWithStatement,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "<ALL>"),
                entry("PARTITION_ID", 12),
                entry("AVG_RESULT_SIZE", 4),
                entry("INVOCATIONS", 1)
        );

        MockRow expectedRow1 = MockRow.with(
                baseOutput,
                entry("PROCEDURE", "Order66"),
                entry("WEIGHTED_PERC", 100L),
                entry("AVG_RESULT_SIZE", 2L),
                entry("INVOCATIONS", 1001L)
        );

        VoltTable voltTable = inputSchemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);

        // When
        VoltTable[] voltTables = aggregator.aggregateProcedureOutputStats(new VoltTable[]{voltTable});

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .containsExactly(expectedRow1);
    }
}
