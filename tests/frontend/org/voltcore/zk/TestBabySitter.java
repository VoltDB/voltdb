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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Semaphore;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google_voltpatches.common.collect.Lists;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.voltcore.utils.Pair;
import org.voltdb.VoltZK;

public class TestBabySitter extends ZKTestBase {

    private final int NUM_AGREEMENT_SITES = 3;

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

    @Test
    public void testBabySitter() throws Exception {

        System.out.println("\t\t ***** Starting BabySitter testcase.");

        final Semaphore sem = new Semaphore(0);
        BabySitter.Callback cb = new BabySitter.Callback() {
            @Override
            public void run(List<String> children) {
                sem.release(1);
            }
        };

        ZooKeeper zk = getClient(0);
        zk.create("/babysitterroot", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/babysitterroot/c1", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        Pair<BabySitter, List<String>> pair = BabySitter.blockingFactory(zk, "/babysitterroot", cb);
        BabySitter bs = pair.getFirst();
        sem.acquire();
        assertTrue(bs.lastSeenChildren().size() == 1);
        assertTrue(pair.getSecond().size() == 1);

        zk.create("/babysitterroot/c2", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        sem.acquire();
        assertTrue(bs.lastSeenChildren().size() == 2);

        zk.delete("/babysitterroot/" + bs.lastSeenChildren().get(0), -1);
        sem.acquire();
        assertTrue(bs.lastSeenChildren().size() == 1);

        bs.shutdown();
    }

    @Test
    public void testChildrenOrdering() throws Exception {
        final Semaphore sem = new Semaphore(0);
        final AtomicReference<List<String>> latestChildren = new AtomicReference<List<String>>();
        BabySitter.Callback cb = new BabySitter.Callback() {
            @Override
            public void run(List<String> children) {
                latestChildren.set(children);
                sem.release();
            }
        };

        ZooKeeper zk = getClient(0);
        zk.create("/babysitterroot", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        final String node6 = zk.create("/babysitterroot/6_", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        zk.create("/babysitterroot/7_", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        zk.create("/babysitterroot/10_", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        Pair<BabySitter, List<String>> pair = BabySitter.blockingFactory(zk, "/babysitterroot", cb);
        BabySitter bs = pair.getFirst();

        sem.acquire();
        assertEquals(Lists.newArrayList(6l, 7l, 10l), VoltZK.childrenToReplicaHSIds(latestChildren.get()));

        zk.delete(node6, -1);

        sem.acquire();
        assertEquals(Lists.newArrayList(7l, 10l), VoltZK.childrenToReplicaHSIds(latestChildren.get()));

        bs.shutdown();
    }
}

