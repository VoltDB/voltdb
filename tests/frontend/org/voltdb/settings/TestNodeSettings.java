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

package org.voltdb.settings;

import com.google_voltpatches.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;
import junit.framework.TestCase;

public class TestNodeSettings extends TestCase {

    @Test
    public void testAllPaths() {

        Map<String, String> mapFull = ImmutableMap.<String, String>builder()
            .put(NodeSettings.LOCAL_SITES_COUNT_KEY, "8")
            .put(NodeSettings.LOCAL_ACTIVE_SITES_COUNT_KEY, "8")
            .put(NodeSettings.CL_SNAPSHOT_PATH_KEY, "command_log_snapshot")
            .put(NodeSettings.CL_PATH_KEY, "command_log")
            .put(NodeSettings.SNAPSHOT_PATH_KEY, "snapshots")
            .put(NodeSettings.VOLTDBROOT_PATH_KEY, "/tmp/test/voltdbroot")
            .put(NodeSettings.EXPORT_CURSOR_PATH_KEY, "export_cursor")
            .put(NodeSettings.EXPORT_OVERFLOW_PATH_KEY, "export_overflow")
            .put(NodeSettings.TOPICS_DATA_PATH_KEY, "topics_data")
            .put(NodeSettings.DR_OVERFLOW_PATH_KEY, "dr_overflow")
            .put(NodeSettings.LARGE_QUERY_SWAP_PATH_KEY, "large_query_swap")
            .build();


        try {

            NodeSettings ns1 = NodeSettings.create(mapFull);
            assertNotNull(ns1);

        } catch (Exception e) {
            fail("Exception while creating NodeSettings with all properties provided in create(): " + e.getMessage());
        }
    }

    @Test
    public void testMinimalPaths() {

        Map<String, String> mapMinimal = ImmutableMap.<String, String>builder()
            .put(NodeSettings.LOCAL_SITES_COUNT_KEY, "8")
            .put(NodeSettings.LOCAL_ACTIVE_SITES_COUNT_KEY, "8")
            .put(NodeSettings.VOLTDBROOT_PATH_KEY, "/tmp/test/voltdbroot")
            .build();

        try {

            NodeSettings ns1 = NodeSettings.create(mapMinimal);

        } catch (Exception e) {
            String msg = e.getMessage();
            assertTrue(msg, msg.contains("Missing property") && msg.contains("in path.properties"));
        }
    }

}
