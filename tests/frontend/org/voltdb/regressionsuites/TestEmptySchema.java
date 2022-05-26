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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.awaitility.Awaitility;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.Test;

public class TestEmptySchema extends RegressionSuite
{
    public TestEmptySchema(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("");
        return builder;
    }

    public void testEmptySchema() throws Exception {
        final Client client = getClient();

        // Even running should be an improvement (ENG-4645), but do something just to be sure
        // Also, check to be sure we get a full schema for the table and index stats
        ColumnInfo[] expectedTableSchema = new ColumnInfo[15];
        expectedTableSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedTableSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedTableSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedTableSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedTableSchema[4] = new ColumnInfo("PARTITION_ID", VoltType.BIGINT);
        expectedTableSchema[5] = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        expectedTableSchema[6] = new ColumnInfo("TABLE_TYPE", VoltType.STRING);
        expectedTableSchema[7] = new ColumnInfo("TUPLE_COUNT", VoltType.BIGINT);
        expectedTableSchema[8] = new ColumnInfo("TUPLE_ALLOCATED_MEMORY", VoltType.BIGINT);
        expectedTableSchema[9] = new ColumnInfo("TUPLE_DATA_MEMORY", VoltType.BIGINT);
        expectedTableSchema[10] = new ColumnInfo("STRING_DATA_MEMORY", VoltType.BIGINT);
        expectedTableSchema[11] = new ColumnInfo("TUPLE_LIMIT", VoltType.INTEGER);
        expectedTableSchema[12] = new ColumnInfo("PERCENT_FULL", VoltType.INTEGER);
        expectedTableSchema[13] = new ColumnInfo("DR", VoltType.STRING);
        expectedTableSchema[14] = new ColumnInfo("EXPORT", VoltType.STRING);
        VoltTable expectedTable = new VoltTable(expectedTableSchema);

        VoltTable[] results = Awaitility
                .await("for the IPC backend to actually be running")
                .until(
                        () -> client.callProcedure("@Statistics", "TABLE", 0).getResults(),
                        voltTables -> voltTables.length > 0);

        VoltTable result = results[0];
        System.out.println("TABLE RESULTS: " + result);
        assertEquals(0, result.getRowCount());
        assertEquals(expectedTableSchema.length, result.getColumnCount());
        validateSchema(result, expectedTable);

        ColumnInfo[] expectedIndexSchema = new ColumnInfo[12];
        expectedIndexSchema[0] = new ColumnInfo("TIMESTAMP", VoltType.BIGINT);
        expectedIndexSchema[1] = new ColumnInfo("HOST_ID", VoltType.INTEGER);
        expectedIndexSchema[2] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        expectedIndexSchema[3] = new ColumnInfo("SITE_ID", VoltType.INTEGER);
        expectedIndexSchema[4] = new ColumnInfo("PARTITION_ID", VoltType.BIGINT);
        expectedIndexSchema[5] = new ColumnInfo("INDEX_NAME", VoltType.STRING);
        expectedIndexSchema[6] = new ColumnInfo("TABLE_NAME", VoltType.STRING);
        expectedIndexSchema[7] = new ColumnInfo("INDEX_TYPE", VoltType.STRING);
        expectedIndexSchema[8] = new ColumnInfo("IS_UNIQUE", VoltType.TINYINT);
        expectedIndexSchema[9] = new ColumnInfo("IS_COUNTABLE", VoltType.TINYINT);
        expectedIndexSchema[10] = new ColumnInfo("ENTRY_COUNT", VoltType.BIGINT);
        expectedIndexSchema[11] = new ColumnInfo("MEMORY_ESTIMATE", VoltType.BIGINT);
        expectedTable = new VoltTable(expectedIndexSchema);

        results = client.callProcedure("@Statistics", "INDEX", 0).getResults();
        result = results[0];
        System.out.println("INDEX RESULTS: " + result);
        assertEquals(0, result.getRowCount());
        assertEquals(expectedIndexSchema.length, result.getColumnCount());
        validateSchema(result, expectedTable);
    }

    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestEmptySchema.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        boolean success;
        LocalCluster config = new LocalCluster("decimal-default.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
