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

package org.voltdb.rejoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Arrays;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Database;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.StreamSnapshotRequestConfig;

import com.google_voltpatches.common.collect.HashMultimap;
import com.google_voltpatches.common.collect.Multimap;
import com.google_voltpatches.common.collect.Sets;

public class TestStreamSnapshotRequestConfig {
    private static Database database;

    @BeforeClass
    public static void setupBeforeClass() throws IOException
    {
        database = TPCCProjectBuilder.getTPCCSchemaCatalog()
                                     .getClusters().get("cluster")
                                     .getDatabases().get("database");
    }

    @Test
    public void testRejoinConfigRoundtrip() throws JSONException
    {
        Multimap<Long, Long> pairs = HashMultimap.create();
        pairs.put(1l, 3l);
        pairs.put(1l, 4l);
        pairs.put(2l, 3l);
        StreamSnapshotRequestConfig.Stream stream = new StreamSnapshotRequestConfig.Stream(pairs, 3L);

        JSONStringer stringer = new JSONStringer();
        stringer.object();
        new StreamSnapshotRequestConfig(SnapshotUtil.getTablesToSave(database),
                                        Arrays.asList(stream),
                                        false).toJSONString(stringer);
        stringer.endObject();

        StreamSnapshotRequestConfig config = new StreamSnapshotRequestConfig(new JSONObject(stringer.toString()),
                                                                             database);
        assertEquals(1, config.streams.size());
        assertFalse(config.shouldTruncate);
        assertEquals(Sets.newHashSet(3l, 4l), Sets.newHashSet(config.streams.get(0).streamPairs.get(1l)));
        assertEquals(Sets.newHashSet(3l), Sets.newHashSet(config.streams.get(0).streamPairs.get(2l)));
    }

    @Test
    public void testJoinConfigRoundtrip() throws JSONException
    {
        Multimap<Long, Long> pairs = HashMultimap.create();
        pairs.put(1l, 3l);
        pairs.put(1l, 4l);
        pairs.put(2l, 3l);
        StreamSnapshotRequestConfig.Stream stream = new StreamSnapshotRequestConfig.Stream(pairs, 3L);

        JSONStringer stringer = new JSONStringer();
        stringer.object();
        new StreamSnapshotRequestConfig(SnapshotUtil.getTablesToSave(database), 6,
                                        Arrays.asList(stream),
                false).toJSONString(stringer);
        stringer.endObject();

        StreamSnapshotRequestConfig config = new StreamSnapshotRequestConfig(new JSONObject(stringer.toString()),
                                                                             database);
        assertEquals(1, config.streams.size());
        assertFalse(config.shouldTruncate);
        assertEquals(Sets.newHashSet(3l, 4l), Sets.newHashSet(config.streams.get(0).streamPairs.get(1l)));
        assertEquals(Sets.newHashSet(3l), Sets.newHashSet(config.streams.get(0).streamPairs.get(2l)));
        assertEquals(6, config.newPartitionCount.intValue());
    }
}
