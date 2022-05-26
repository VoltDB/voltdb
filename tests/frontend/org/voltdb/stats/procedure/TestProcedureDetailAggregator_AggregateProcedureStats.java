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

import com.google.common.base.Suppliers;
import org.junit.Test;
import org.voltdb.TableShorthand;
import org.voltdb.MockRow;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.assertions.VoltTablesAssertion;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.entry;

public class TestProcedureDetailAggregator_AggregateProcedureStats {

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
            "TRANSACTIONAL:TINYINT," +
            "COMPOUND:TINYINT" +
            ")"
    );

    VoltTable outputSchemaTemplate = TableShorthand.tableFromShorthand(
            "PROCEDUREDETAIL(" +
            "TIMESTAMP:BIGINT," +
            VoltSystemProcedure.CNAME_HOST_ID + ":INTEGER," +
            "HOSTNAME:STRING," +
            VoltSystemProcedure.CNAME_SITE_ID + ":INTEGER," +
            "PARTITION_ID:INTEGER," +
            "PROCEDURE:STRING," +
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
            "TRANSACTIONAL:TINYINT," +
            "COMPOUND:TINYINT" +
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
            32,
            123452,
            433,
            12,
            15,
            14,
            3421L,
            5L,
            (byte) 0,
            (byte) 1
    );

    MockRow baseOutput = MockRow.of(
            outputSchemaTemplate,
            42L,
            1,
            "db2-volt-node-1",
            2,
            34,
            "org.voltdb.stats.procedure.ProcedureDetailResultTableTest",
            2345L,
            100L,
            TimeUnit.MILLISECONDS.toNanos(22),
            TimeUnit.MILLISECONDS.toNanos(123),
            TimeUnit.MILLISECONDS.toNanos(42),
            32,
            123452,
            433,
            12,
            15,
            14,
            3421L,
            5L,
            (byte) 0,
            (byte) 1
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
        VoltTable[] voltTables = aggregator.aggregateProcedureStats(new VoltTable[]{inputSchemaTemplate});

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .isEmpty();
    }

    @Test
    public void shouldFilterOutNonAggregatesAndReturnEmptyTable() {
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

        VoltTable voltTable = inputSchemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);

        // When
        VoltTable[] voltTables = aggregator.aggregateProcedureStats(new VoltTable[]{voltTable});

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
                entry("PROCEDURE", "Order66")
        );

        MockRow expectedRow2 = MockRow.with(
                baseOutput,
                entry("PROCEDURE", "AAArdvark")
        );

        VoltTable voltTable = inputSchemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);

        // When
        VoltTable[] voltTables = aggregator.aggregateProcedureStats(new VoltTable[]{voltTable});

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .containsExactly(expectedRow1, expectedRow2);
    }
}
