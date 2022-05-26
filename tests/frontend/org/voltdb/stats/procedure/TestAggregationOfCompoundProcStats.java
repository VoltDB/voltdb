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
import org.voltdb.VoltTable;
import org.voltdb.assertions.VoltTablesAssertion;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.entry;

public class TestAggregationOfCompoundProcStats {

    // The basic input to CompoundProcSummaryStatisticsTable is a
    // VoltTableRow in the form produced by ProcedureStatsCollector.

    VoltTable inputSchemaTemplate = TableShorthand.tableFromShorthand(
            "PROCEDUREDETAIL(" +
            "TIMESTAMP:BIGINT," +
            "HOST_ID:INTEGER," +
            "HOSTNAME:STRING," +
            "SITE_ID:INTEGER," +                   // n/a
            "PARTITION_ID:INTEGER," +              // n/a
            "PROCEDURE:STRING," +
            "STATEMENT:STRING," +                  // n/a
            "INVOCATIONS:BIGINT," +
            "TIMED_INVOCATIONS:BIGINT," +          // don't care
            "MIN_EXECUTION_TIME:BIGINT," +
            "MAX_EXECUTION_TIME:BIGINT," +
            "AVG_EXECUTION_TIME:BIGINT," +
            "MIN_RESULT_SIZE:INTEGER," +           // don't care
            "MAX_RESULT_SIZE:INTEGER," +           // don't care
            "AVG_RESULT_SIZE:INTEGER," +           // don't care
            "MIN_PARAMETER_SET_SIZE:INTEGER," +    // don't care
            "MAX_PARAMETER_SET_SIZE:INTEGER," +    // don't care
            "AVG_PARAMETER_SET_SIZE:INTEGER," +    // don't care
            "ABORTS:BIGINT," +
            "FAILURES:BIGINT," +
            "TRANSACTIONAL:TINYINT," +             // 0 for non-transactional
            "COMPOUND:TINYINT" +                   // 1 for compound
            ")"
    );

    // Output is only rows with COMPOUND = 1, subset of
    // columns, no rollup of any data

    VoltTable outputSchemaTemplate = TableShorthand.tableFromShorthand(
            "PROCEDUREDETAIL(" +
            "TIMESTAMP:BIGINT," +
            "HOST_ID:INTEGER," +
            "HOSTNAME:STRING," +
            "PROCEDURE:STRING," +
            "INVOCATIONS:BIGINT," +
            "MIN_ELAPSED:BIGINT," +
            "MAX_ELAPSED:BIGINT," +
            "AVG_ELAPSED:BIGINT," +
            "ABORTS:BIGINT," +
            "FAILURES:BIGINT" +
            ")"
      );

    MockRow compoundProcRow1 = MockRow.of(
            inputSchemaTemplate,
            11111L, // timestamp
            1, // host id
            "volt-node-1", // hostname
            2,
            34,
            "org.voltdb.stats.procedure.CompoundProcTestCase", // procedure
            "<ALL>",
            2345L, // invocations
            100L,
            TimeUnit.MILLISECONDS.toNanos(22), // min time
            TimeUnit.MILLISECONDS.toNanos(123), // max time
            TimeUnit.MILLISECONDS.toNanos(42), // avg time
            32,
            123452,
            433,
            12,
            15,
            14,
            3421L, // aborts
            567L, // failures
            (byte) 0,
            (byte) 1 // compound
    );

    MockRow expectedOutput1 = MockRow.of(
            outputSchemaTemplate,
            11111L,
            1,
            "volt-node-1",
            "org.voltdb.stats.procedure.CompoundProcTestCase",
            2345L,
            TimeUnit.MILLISECONDS.toNanos(22),
            TimeUnit.MILLISECONDS.toNanos(123),
            TimeUnit.MILLISECONDS.toNanos(42),
            3421L,
            567L
     );

    MockRow compoundProcRow2 = MockRow.of(
            inputSchemaTemplate,
            22222L, // timestamp
            2, // host id
            "volt-node-2", // hostname
            2,
            34,
            "org.voltdb.stats.procedure.CompoundProcTestCase", // procedure
            "<ALL>",
            2345L, // invocations
            100L,
            TimeUnit.MILLISECONDS.toNanos(22), // min time
            TimeUnit.MILLISECONDS.toNanos(123), // max time
            TimeUnit.MILLISECONDS.toNanos(42), // avg time
            32,
            123452,
            433,
            12,
            15,
            14,
            3421L, // aborts
            567L, // failures
            (byte) 0,
            (byte) 1 // compound
    );

    MockRow expectedOutput2 = MockRow.of(
            outputSchemaTemplate,
            22222L,
            2,
            "volt-node-2",
            "org.voltdb.stats.procedure.CompoundProcTestCase",
            2345L,
            TimeUnit.MILLISECONDS.toNanos(22),
            TimeUnit.MILLISECONDS.toNanos(123),
            TimeUnit.MILLISECONDS.toNanos(42),
            3421L,
            567L
    );

    MockRow transactionRow = MockRow.of(
            inputSchemaTemplate,
            42L, // timestamp
            2, // host id
            "volt-node-2", // hostname
            2,
            34,
            "org.voltdb.stats.procedure.PlainOldProcedure", // procedure
            "<ALL>",
            2345L, // invocations
            100L,
            TimeUnit.MILLISECONDS.toNanos(22), // min time
            TimeUnit.MILLISECONDS.toNanos(123), // max time
            TimeUnit.MILLISECONDS.toNanos(42), // avg time
            32,
            123452,
            433,
            12,
            15,
            14,
            3421L, // aborts
            567L, // failures
            (byte) 1, // transactional
            (byte) 0
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
        VoltTable[] voltTables = aggregator.aggregateCompoundProcByHost(new VoltTable[] { inputSchemaTemplate });

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .isEmpty();
    }

    @Test
    public void shouldProcessCompoundProcsOnly() {
        // Given
        ProcedureDetailAggregator aggregator = new ProcedureDetailAggregator(
                new ReadOnlyProcedureInformation(
                        Suppliers.ofInstance(Collections.emptyMap())
                )
        );

        VoltTable voltTable = inputSchemaTemplate.clone(0);
        compoundProcRow1.insertInto(voltTable); // will be accepted
        compoundProcRow2.insertInto(voltTable); // will be accepted
        transactionRow.insertInto(voltTable); // will be rejected

        // When
        VoltTable[] voltTables = aggregator.aggregateCompoundProcByHost(new VoltTable[] { voltTable });

        // Then
        VoltTablesAssertion.assertThat(voltTables)
                           .onlyResponse()
                           .containsExactly(expectedOutput1, expectedOutput2);
    }
 }
