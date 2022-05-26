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
package org.voltdb.iv2;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.zk.ZKTestBase;
import org.voltdb.iv2.LeaderCache.LeaderCallBackInfo;

import com.google_voltpatches.common.collect.ImmutableMap;

public class TestLeaderCache extends ZKTestBase {

    private final int NUM_AGREEMENT_SITES = 8;
    private final String LAST_HOST_PREFIX = Long.toString(Long.MAX_VALUE) + "/";

    public static class TestCallback extends LeaderCache.Callback
    {
        volatile ImmutableMap<Integer, LeaderCallBackInfo> m_cache = null;

        @Override
        public void run(ImmutableMap<Integer, LeaderCallBackInfo> cache)
        {
            HashMap<Integer, LeaderCallBackInfo> cacheCopy = new HashMap<Integer, LeaderCallBackInfo>();
            for (Entry<Integer, LeaderCallBackInfo> e : cache.entrySet()) {
                cacheCopy.put(e.getKey(), e.getValue());
            }
            m_cache = ImmutableMap.copyOf(cacheCopy);
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
        Long aa = 12345678L;
        Long bb = 87654321L;
        Long cc = 11223344L;
        zk.create(root, new byte[]{}, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(root + "/0", (LAST_HOST_PREFIX + aa.toString()).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(root + "/1", (LAST_HOST_PREFIX + bb.toString()).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(root + "/2", (LAST_HOST_PREFIX + cc.toString()).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void testInitialCache() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache01", zk);

        LeaderCache dut = new  LeaderCache(zk, "", "/cache01");
        dut.start(true);
        Map<Integer, Long> cache = dut.pointInTimeCache();

        assertEquals("3 items cached.", 3, cache.size());
        assertEquals(12345678L, dut.get(0).longValue());
        assertEquals(87654321L, dut.get(1).longValue());
        assertEquals(11223344L, dut.get(2).longValue());

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testInitialCacheWithCallback() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache01", zk);

        TestCallback cb = new TestCallback();
        LeaderCache dut = new  LeaderCache(zk, "", "/cache01", cb);
        dut.start(true);

        assertEquals("3 items cached.", 3, cb.m_cache.size());
        assertEquals(12345678, cb.m_cache.get(0).m_HSId.longValue());
        assertEquals(87654321, cb.m_cache.get(1).m_HSId.longValue());
        assertEquals(11223344, cb.m_cache.get(2).m_HSId.longValue());

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testModifyChild() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache03", zk);

        LeaderCache dut = new  LeaderCache(zk, "", "/cache03");
        dut.start(true);
        Map<Integer, Long> cache = dut.pointInTimeCache();

        assertEquals("3 items cached.", 3, cache.size());
        assertEquals(12345678, dut.get(0).longValue());

        zk.setData("/cache03/0", (LAST_HOST_PREFIX + Long.toString(23456789)).getBytes(), -1);
        while(true) {
            if (dut.get(0) == 23456789) {
                break;
            }
        }
        assertEquals("3 items cached.", 3, cache.size());
        assertEquals(23456789L, dut.get(0).longValue());
        assertEquals(87654321L, dut.get(1).longValue());
        assertEquals(11223344L, dut.get(2).longValue());

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testModifyChildWithCallback() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache03", zk);

        TestCallback cb = new TestCallback();
        LeaderCache dut = new  LeaderCache(zk, "", "/cache03", cb);
        dut.start(true);
        Map<Integer, LeaderCallBackInfo> cache = cb.m_cache;

        assertEquals("3 items cached.", 3, cache.size());
        assertEquals(12345678, cache.get(0).m_HSId.longValue());

        dut.put(0, 23456789);
        while(true) {
            cache = cb.m_cache;
            if (cache.get(0).m_HSId == 23456789) {
                break;
            }
        }
        cache = cb.m_cache;
        assertEquals("3 items cached.", 3, cache.size());
        assertEquals(23456789, cache.get(0).m_HSId.longValue());
        assertEquals(87654321, cache.get(1).m_HSId.longValue());
        assertEquals(11223344, cache.get(2).m_HSId.longValue());

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testDeleteChild() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache02", zk);

        LeaderCache dut = new LeaderCache(zk, "", "/cache02");
        dut.start(true);
        Map<Integer, Long> cache = dut.pointInTimeCache();
        assertEquals("3 items cached.", 3, cache.size());

        zk.delete("/cache02/1", -1);
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
        assertEquals(null, cache.get(1));
        assertEquals(12345678, cache.get(0).longValue());
        assertEquals(11223344, cache.get(2).longValue());

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testDeleteChildWithCallback() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache02", zk);

        TestCallback cb = new TestCallback();
        LeaderCache dut = new LeaderCache(zk, "", "/cache02", cb);
        dut.start(true);
        Map<Integer, LeaderCallBackInfo> cache = cb.m_cache;
        assertEquals("3 items cached.", 3, cache.size());

        zk.delete("/cache02/1", -1);
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
        assertEquals(null, cache.get(1));
        assertEquals(12345678, cache.get(0).m_HSId.longValue());
        assertEquals(11223344, cache.get(2).m_HSId.longValue());

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testAddChildWithPut() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache04", zk);

        LeaderCache dut = new LeaderCache(zk, "", "/cache04");
        dut.start(true);
        Map<Integer, Long> cache = dut.pointInTimeCache();

        dut.put(3, 88776655);

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
        assertEquals(12345678, cache.get(0).longValue());
        assertEquals(87654321, cache.get(1).longValue());
        assertEquals(11223344, cache.get(2).longValue());
        assertEquals(88776655, cache.get(3).longValue());

        // modify the new child and make sure it has a watch set.
        dut.put(3, 99887766);
        while(true) {
            cache = dut.pointInTimeCache();
            if (cache.get(3) == 99887766) {
                break;
            }
        }
        assertEquals("Items accounted for.", 4, cache.size());
        assertEquals(99887766L, cache.get(3).longValue());

        dut.shutdown();
        zk.close();
    }

    @Test
    public void testAddChildWithPutWithCallback() throws Exception
    {
        ZooKeeper zk = getClient(0);
        configure("/cache04", zk);

        TestCallback cb = new TestCallback();
        LeaderCache dut = new LeaderCache(zk, "", "/cache04", cb);
        dut.start(true);
        Map<Integer, LeaderCallBackInfo> cache = cb.m_cache;

        dut.put(3, 88776655);

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
        assertEquals(12345678, cache.get(0).m_HSId.longValue());
        assertEquals(87654321, cache.get(1).m_HSId.longValue());
        assertEquals(11223344, cache.get(2).m_HSId.longValue());
        assertEquals(88776655, cache.get(3).m_HSId.longValue());

        // modify the new child and make sure it has a watch set.
        dut.put(3, 99887766);
        while(true) {
            cache = cb.m_cache;
            if (cache.get(3).m_HSId == 99887766) {
                break;
            }
        }
        assertEquals("Items accounted for.", 4, cache.size());
        assertEquals(99887766, cache.get(3).m_HSId.longValue());

        dut.shutdown();
        zk.close();
    }
}
