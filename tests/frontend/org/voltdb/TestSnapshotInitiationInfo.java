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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Database;
import org.voltdb.sysprocs.saverestore.SnapshotRequestConfig;
import org.voltdb.sysprocs.saverestore.SystemTable;

import com.google_voltpatches.common.collect.Sets;

public class TestSnapshotInitiationInfo {
    private static Database database;

    // Most of the previously possible error conditions are tested in
    // TestSnapshotDaemon.  Could get moved here at some point

    @Test
    public void testInvalidService() throws JSONException
    {
        SnapshotInitiationInfo dut;
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("service").value("pwnme");
        stringer.endObject();
        Object[] params = new Object[1];
        params[0] = stringer.toString();
        try {
            dut = new SnapshotInitiationInfo(params);
        }
        catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Unknown snapshot save service"));
        }
    }

    @Test
    public void testJSONWithTruncation() throws Exception
    {
        // Need to mock VoltDB.instance().getCommandLog()
        VoltDBInterface mockVolt = mock(VoltDBInterface.class);
        CommandLog cl = mock(CommandLog.class);
        when(cl.isEnabled()).thenReturn(false);
        when(mockVolt.getCommandLog()).thenReturn(cl);
        VoltDB.replaceVoltDBInstanceForTest(mockVolt);

        // Start off w/ command log disabled
        SnapshotInitiationInfo dut;
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("service").value("log_truncation");
        stringer.endObject();
        Object[] params = new Object[1];
        params[0] = stringer.toString();
        try {
            dut = new SnapshotInitiationInfo(params);
        }
        catch (Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("command logging is not present or enabled"));
        }

        // now turn it on and try again
        when(cl.isEnabled()).thenReturn(true);
        dut = new SnapshotInitiationInfo(params);
        assertTrue(dut.isTruncationRequest());
    }

    @Test
    public void testTableFilter() throws JSONException
    {
        // Inclusion
        tableFilterAndCheck(new String[]{"district"},           null, null);
        tableFilterAndCheck(new String[]{"district", "orders"}, null, null);

        // Exclusion
        tableFilterAndCheck(null, new String[]{"district"}, null);
        tableFilterAndCheck(null, new String[]{"district", "orders"}, null);

        // Mixed
        tableFilterAndCheck(new String[]{"district", "orders"}, new String[]{"district"}, null);

        // Include all tables
        tableFilterAndCheck(null, null, null);

        // Include no tables
        tableFilterAndCheck(new String[0], null, null);

        // Include invalid tables
        tableFilterAndCheck(new String[]{"nope"}, null, new String[]{"nope"});
        tableFilterAndCheck(new String[]{"district", "nope", "nothingHere"}, null, new String[]{"nope", "nothingHere"});
        tableFilterAndCheck(new String[]{"nope"}, new String[]{"district"}, new String[]{"nope"});

        // Exclude invalid tables
        tableFilterAndCheck(null, new String[]{"nope"}, new String[]{"nope"});
        tableFilterAndCheck(null, new String[]{"district", "nope", "nothingHere"}, new String[]{"nope", "nothingHere"});
        tableFilterAndCheck(new String[]{"district"}, new String[]{"nope"}, new String[]{"nope"});
    }

    // Note that currently, error validation is only supported for the first of
    // include/exclude to contain an invalid table name
    private static void tableFilterAndCheck(String[] include, String[] exclude, String[] invalid) throws JSONException
    {
        int expectedTableCount = 10 + SystemTable.values().length; // TPC-C has 10 tables in total

        JSONStringer js = new JSONStringer();
        js.object();
        if (include != null) {
            js.key("tables").array();
            expectedTableCount = include.length;
            for (String name : include) {
                js.value(name);
            }
            js.endArray();
        }
        if (exclude != null) {
            js.key("skiptables").array();
            expectedTableCount -= exclude.length;
            for (String name : exclude) {
                js.value(name);
            }
            js.endArray();
        }
        js.endObject();

        final SnapshotRequestConfig config;
        try {
            config = new SnapshotRequestConfig(new JSONObject(js.toString()), database);
        } catch (IllegalArgumentException e) {
            assertNotNull(invalid);
            String msg = e.getMessage().toLowerCase();
            for (String t : invalid) {
                assertTrue(msg.contains(t.toLowerCase()));
            }
            return;
        }
        assertNull(invalid);
        assertEquals(expectedTableCount, config.tables.size());

        final Set<String> allTables = new HashSet<>();
        for (SnapshotTableInfo t : config.tables) {
            allTables.add(t.getName().toLowerCase());
        }

        if (include != null) {
            if (include.length > 0) {
                Set<String> includeTables = Sets.newHashSet(include);
                if (exclude != null) {
                    includeTables.removeAll(Sets.newHashSet(exclude));
                }
                assertTrue(allTables.containsAll(includeTables));
            } else {
                assertEquals(Sets.newHashSet(include), allTables);
            }
        }
        if (exclude != null) {
            assertTrue(Collections.disjoint(allTables, Arrays.asList(exclude)));
        }
    }

    @BeforeClass
    public static void setup() throws IOException
    {
        database = TPCCProjectBuilder.getTPCCSchemaCatalog()
        .getClusters().get("cluster")
        .getDatabases().get("database");
    }
}
