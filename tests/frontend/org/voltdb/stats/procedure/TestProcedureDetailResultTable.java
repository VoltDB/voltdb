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

import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.voltdb.TableShorthand;
import org.voltdb.MockRow;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.assertions.VoltTablesAssertion;

//  procedure_name, statement_name, host_id, site_id, partition_id, timestamp (descending);
public class TestProcedureDetailResultTable {
    VoltTable schemaTemplate = TableShorthand.tableFromShorthand(
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
            "FAILURES:BIGINT" +
            ")"
    );

    MockRow baseRow = MockRow.of(
            schemaTemplate,
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
            5L
    );

    @Test
    public void shouldAcceptEmptyTable() {
        // Given
        VoltTable voltTable = schemaTemplate.clone(0);

        // When
        ProcedureDetailResultTable table = new ProcedureDetailResultTable(voltTable);
        VoltTable[] sortedResultTable = table.getSortedResultTable();

        // Then
        VoltTablesAssertion.assertThat(sortedResultTable)
                           .onlyResponse()
                           .isEmpty();
    }

    @Test
    public void shouldAcceptSingleRow() {
        // Given
        VoltTable voltTable = schemaTemplate.clone(0);
        baseRow.insertInto(voltTable);

        // When
        ProcedureDetailResultTable table = new ProcedureDetailResultTable(voltTable);
        VoltTable[] sortedResultTable = table.getSortedResultTable();

        // Then
        VoltTablesAssertion.assertThat(sortedResultTable)
                           .onlyResponse()
                           .containsExactly(baseRow);
    }

    @Test
    public void shouldSortByProcedureAndStatement() {
        // Given
        MockRow row1 = MockRow.with(
                baseRow,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "Roger,roger")
        );

        MockRow row2 = MockRow.with(
                baseRow,
                entry("PROCEDURE", "PerlinNoise"),
                entry("STATEMENT", "<ALL>")
        );

        MockRow row3 = MockRow.with(
                baseRow,
                entry("PROCEDURE", "AAArdvark"),
                entry("STATEMENT", "<ALL>")
        );

        MockRow row4 = MockRow.with(
                baseRow,
                entry("PROCEDURE", "Order66"),
                entry("STATEMENT", "Execute")
        );

        VoltTable voltTable = schemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);
        row4.insertInto(voltTable);

        // When
        ProcedureDetailResultTable table = new ProcedureDetailResultTable(voltTable);
        VoltTable[] sortedResultTable = table.getSortedResultTable();

        // Then
        VoltTablesAssertion.assertThat(sortedResultTable)
                           .onlyResponse()
                           .containsExactly(row3, row4, row1, row2);
    }

    @Test
    public void shouldSortByHostIdAndSiteId() {
        // Given
        MockRow row1 = MockRow.with(
                baseRow,
                entry(VoltSystemProcedure.CNAME_HOST_ID, 0),
                entry(VoltSystemProcedure.CNAME_SITE_ID, 1)
        );

        MockRow row2 = MockRow.with(
                baseRow,
                entry(VoltSystemProcedure.CNAME_HOST_ID, 1),
                entry(VoltSystemProcedure.CNAME_SITE_ID, 2)
        );

        MockRow row3 = MockRow.with(
                baseRow,
                entry(VoltSystemProcedure.CNAME_HOST_ID, 1),
                entry(VoltSystemProcedure.CNAME_SITE_ID, 1)
        );

        MockRow row4 = MockRow.with(
                baseRow,
                entry(VoltSystemProcedure.CNAME_HOST_ID, 2),
                entry(VoltSystemProcedure.CNAME_SITE_ID, 2)
        );

        VoltTable voltTable = schemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);
        row4.insertInto(voltTable);

        // When
        ProcedureDetailResultTable table = new ProcedureDetailResultTable(voltTable);
        VoltTable[] sortedResultTable = table.getSortedResultTable();

        // Then
        VoltTablesAssertion.assertThat(sortedResultTable)
                           .onlyResponse()
                           .containsExactly(row1, row3, row2, row4);
    }

    @Test
    public void shouldSortByPartitionIdAndTimestamp() {
        // Given
        MockRow row1 = MockRow.with(
                baseRow,
                entry("PARTITION_ID", 1),
                entry("TIMESTAMP", 4642352345L)
        );

        MockRow row2 = MockRow.with(
                baseRow,
                entry("PARTITION_ID", 1),
                entry("TIMESTAMP", 8567L)
        );

        MockRow row3 = MockRow.with(
                baseRow,
                entry("PARTITION_ID", 0),
                entry("TIMESTAMP", 11L)
        );

        MockRow row4 = MockRow.with(
                baseRow,
                entry("PARTITION_ID", 0),
                entry("TIMESTAMP", 3563457643574L)
        );

        VoltTable voltTable = schemaTemplate.clone(0);
        row1.insertInto(voltTable);
        row2.insertInto(voltTable);
        row3.insertInto(voltTable);
        row4.insertInto(voltTable);

        // When
        ProcedureDetailResultTable table = new ProcedureDetailResultTable(voltTable);
        VoltTable[] sortedResultTable = table.getSortedResultTable();

        // Then
        VoltTablesAssertion.assertThat(sortedResultTable)
                           .onlyResponse()
                           .containsExactly(row4, row3, row1, row2);
    }
}
