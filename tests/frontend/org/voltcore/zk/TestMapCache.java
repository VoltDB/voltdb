/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package org.voltcore.zk;

import java.util.Map;

import org.json_voltpatches.JSONObject;

import static org.junit.Assert.assertEquals;

import org.apache.zookeeper_voltpatches.ZooDefs.Ids;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google_voltpatches.common.collect.ImmutableMap;

public class TestMapCache extends ZKTestBase {

    private final int NUM_AGREEMENT_SITES = 8;

    public static class TestCallback extends MapCache.Callback
    {
        ImmutableMap<String, JSONObject> m_cache = null;

        public void run(ImmutableMap<String, JSONObject> cache)
        {
            m_cache = cache;
        }
    }

    @Before
    public void setUp() throws Exception
    {
        setUpZK(NUM_AGREEMENT_SITES);
    }

    @After
    public void tearDown() throws Exception
    {
        tearDownZK();
    }

    void configure(String root, ZooKeeper zk) throws Exception
    {
        JSONObject aa = new JSONObject("{key:aaval}");
        JSONObject bb = new JSONObject("{key:bbval}");
        JSONObject cc = new JSONObject("{key:ccval}");
        zk.create(root, new byte[]{}, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(root + "/aa", aa.toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(root + "/bb", bb.toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(root + "/cc", cc.toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void testInitialCache() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache01", zk);

        MapCache dut = new  MapCache(zk, "/cache01");
        dut.start(true);
        Map<String, JSONObject> cache = dut.pointInTimeCache();

        assertEquals("3 items cached.", 3, cache.size());
        assertEquals("aaval", cache.get("/cache01/aa").get("key"));
        assertEquals("bbval", cache.get("/cache01/bb").get("key"));
        assertEquals("ccval", cache.get("/cache01/cc").get("key"));

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testInitialCacheWithCallback() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache01", zk);

        TestCallback cb = new TestCallback();
        MapCache dut = new  MapCache(zk, "/cache01", cb);
        dut.start(true);

        assertEquals("3 items cached.", 3, cb.m_cache.size());
        assertEquals("aaval", cb.m_cache.get("/cache01/aa").get("key"));
        assertEquals("bbval", cb.m_cache.get("/cache01/bb").get("key"));
        assertEquals("ccval", cb.m_cache.get("/cache01/cc").get("key"));

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testModifyChild() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache03", zk);

        MapCache dut = new  MapCache(zk, "/cache03");
        dut.start(true);
        Map<String, JSONObject> cache = dut.pointInTimeCache();

        assertEquals("3 items cached.", 3, cache.size());
        assertEquals("aaval", cache.get("/cache03/aa").get("key"));

        JSONObject aa = new JSONObject("{key:aaval2}");
        zk.setData("/cache03/aa", aa.toString().getBytes(), -1);
        while(true) {
            cache = dut.pointInTimeCache();
            if (cache.get("/cache03/aa").get("key").equals("aaval2")) {
                break;
            }
        }
        assertEquals("3 items cached.", 3, cache.size());
        assertEquals("aaval2", cache.get("/cache03/aa").get("key"));
        assertEquals("bbval", cache.get("/cache03/bb").get("key"));
        assertEquals("ccval", cache.get("/cache03/cc").get("key"));

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testModifyChildWithCallback() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache03", zk);

        TestCallback cb = new TestCallback();
        MapCache dut = new  MapCache(zk, "/cache03", cb);
        dut.start(true);
        Map<String, JSONObject> cache = cb.m_cache;

        assertEquals("3 items cached.", 3, cache.size());
        assertEquals("aaval", cache.get("/cache03/aa").get("key"));

        JSONObject aa = new JSONObject("{key:aaval2}");
        zk.setData("/cache03/aa", aa.toString().getBytes(), -1);
        while(true) {
            cache = cb.m_cache;
            if (cache.get("/cache03/aa").get("key").equals("aaval2")) {
                break;
            }
        }
        assertEquals("3 items cached.", 3, cache.size());
        assertEquals("aaval2", cache.get("/cache03/aa").get("key"));
        assertEquals("bbval", cache.get("/cache03/bb").get("key"));
        assertEquals("ccval", cache.get("/cache03/cc").get("key"));

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testDeleteChild() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache02", zk);

        MapCache dut = new MapCache(zk, "/cache02");
        dut.start(true);
        Map<String, JSONObject> cache = dut.pointInTimeCache();
        assertEquals("3 items cached.", 3, cache.size());

        zk.delete("/cache02/bb", -1);
        while(true) {
            cache = dut.pointInTimeCache();
            if (cache.size() == 3) {
                Thread.sleep(1);
            }
            else {
                break;
            }
        }
        assertEquals("Item removed", 2, cache.size());
        assertEquals(null, cache.get("/cache02/bb"));
        assertEquals("aaval", cache.get("/cache02/aa").get("key"));
        assertEquals("ccval", cache.get("/cache02/cc").get("key"));

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testDeleteChildWithCallback() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache02", zk);

        TestCallback cb = new TestCallback();
        MapCache dut = new MapCache(zk, "/cache02", cb);
        dut.start(true);
        Map<String, JSONObject> cache = cb.m_cache;
        assertEquals("3 items cached.", 3, cache.size());

        zk.delete("/cache02/bb", -1);
        while(true) {
            cache = cb.m_cache;
            if (cache.size() == 3) {
                Thread.sleep(1);
            }
            else {
                break;
            }
        }
        assertEquals("Item removed", 2, cache.size());
        assertEquals(null, cache.get("/cache02/bb"));
        assertEquals("aaval", cache.get("/cache02/aa").get("key"));
        assertEquals("ccval", cache.get("/cache02/cc").get("key"));

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testAddChildWithPut() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache04", zk);

        MapCache dut = new MapCache(zk, "/cache04");
        dut.start(true);
        Map<String, JSONObject> cache = dut.pointInTimeCache();

        JSONObject dd = new JSONObject("{key:ddval}");
        dut.put("dd", dd);

        while(true) {
            cache = dut.pointInTimeCache();
            if (cache.size() == 3) {
                Thread.sleep(1);
            }
            else {
                break;
            }
        }
        assertEquals("Item added", 4, cache.size());
        assertEquals("aaval", cache.get("/cache04/aa").get("key"));
        assertEquals("bbval", cache.get("/cache04/bb").get("key"));
        assertEquals("ccval", cache.get("/cache04/cc").get("key"));
        assertEquals("ddval", cache.get("/cache04/dd").get("key"));

        // modify the new child and make sure it has a watch set.
        JSONObject dd2 = new JSONObject("{key:ddval2}");
        dut.put("dd", dd2);
        while(true) {
            cache = dut.pointInTimeCache();
            if (cache.get("/cache04/dd").get("key").equals("ddval2")) {
                break;
            }
        }
        assertEquals("Items accounted for.", 4, cache.size());
        assertEquals("ddval2", cache.get("/cache04/dd").get("key"));

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testAddChildWithPutWithCallback() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache04", zk);

        TestCallback cb = new TestCallback();
        MapCache dut = new MapCache(zk, "/cache04", cb);
        dut.start(true);
        Map<String, JSONObject> cache = cb.m_cache;

        JSONObject dd = new JSONObject("{key:ddval}");
        dut.put("dd", dd);

        while(true) {
            cache = cb.m_cache;
            if (cache.size() == 3) {
                Thread.sleep(1);
            }
            else {
                break;
            }
        }
        assertEquals("Item added", 4, cache.size());
        assertEquals("aaval", cache.get("/cache04/aa").get("key"));
        assertEquals("bbval", cache.get("/cache04/bb").get("key"));
        assertEquals("ccval", cache.get("/cache04/cc").get("key"));
        assertEquals("ddval", cache.get("/cache04/dd").get("key"));

        // modify the new child and make sure it has a watch set.
        JSONObject dd2 = new JSONObject("{key:ddval2}");
        dut.put("dd", dd2);
        while(true) {
            cache = cb.m_cache;
            if (cache.get("/cache04/dd").get("key").equals("ddval2")) {
                break;
            }
        }
        assertEquals("Items accounted for.", 4, cache.size());
        assertEquals("ddval2", cache.get("/cache04/dd").get("key"));

        dut.shutdown();
        zk.close();
    }
}
