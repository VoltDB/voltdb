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

package org.voltdb.importer;

import static com.google_voltpatches.common.base.Predicates.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.zk.ZKTestBase;

import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;

public class TestChannelDistributer extends ZKTestBase {

    private final static String ZERO = "zero";
    private final static String UNO  = "uno";
    private final static String DUE  = "due";
    private final static String YO   = "yo";

    Map<String, ZooKeeper> zks;
    Map<String, ChannelDistributer> distributers;
    BlockingDeque<ImporterChannelAssignment> queue;

    public class Collector implements ChannelChangeCallback {
        @Override
        public void onChange(ImporterChannelAssignment assignment) {
            queue.offer(assignment);
        }
        @Override
        public void onClusterStateChange(VersionedOperationMode mode) {
        }
    }

    static Set<URI> generateURIs(int count) {
        ImmutableSet.Builder<URI> sbldr = ImmutableSet.builder();
        for (int i=0; i < count; ++i) {
            sbldr.add(URI.create(String.format("x-import://yo/no%04d", i)));
        }
        return sbldr.build();
    }

    static Set<ChannelSpec> asSpecs(Set<URI> uris) {
        return FluentIterable.from(uris).transform(ChannelSpec.fromUri(YO)).toSet();
    }

    Set<URI> getRemoved(int expected) throws Exception {
        int received = 0;
        ImmutableSet.Builder<URI> sbldr = ImmutableSet.builder();
        ImporterChannelAssignment assignment = null;
        while (received < expected && (assignment=queue.poll(200,TimeUnit.MILLISECONDS)) != null) {
            received += assignment.getRemoved().size();
            sbldr.addAll(assignment.getRemoved());
        }
        assertEquals("failed to poll the expected number of removed", expected, received);
        assertTrue(queue.isEmpty());
        return sbldr.build();
    }

    Set<URI> getAdded(int expected) throws Exception {
        int received = 0;
        ImmutableSet.Builder<URI> sbldr = ImmutableSet.builder();
        ImporterChannelAssignment assignment = null;
        while (received < expected && (assignment=queue.poll(200, TimeUnit.MILLISECONDS)) != null) {
            received += assignment.getAdded().size();
            sbldr.addAll(assignment.getAdded());
        }
        assertEquals("failed to poll the expected number of removed", expected, received);
        assertTrue(queue.isEmpty());
        return sbldr.build();
    }

    @Before
    public void setup() throws Exception {
        setUpZK(3);
        queue = new LinkedBlockingDeque<>();
        zks = ImmutableMap.<String, ZooKeeper>builder()
                .put(ZERO, getClient(0))
                .put(UNO,  getClient(1))
                .put(DUE,  getClient(2))
                .build();
        distributers = ImmutableMap.<String, ChannelDistributer>builder()
                .put(ZERO, new ChannelDistributer(zks.get(ZERO), ZERO))
                .put(UNO,  new ChannelDistributer(zks.get(UNO), UNO))
                .put(DUE,  new ChannelDistributer(zks.get(DUE), DUE))
                .build();
        for (ChannelDistributer cd: distributers.values()) {
            cd.registerCallback(YO, new Collector());
        }
    }

    @Test
    public void testRegistration() throws Exception {
        Set<URI> uris = generateURIs(9);
        Set<URI> expected = uris;
        // add nine
        distributers.get(UNO).registerChannels(YO, uris);
        Set<URI> actual = getAdded(9);

        assertEquals(expected, actual);

        Set<URI> pruned = generateURIs(6);
        expected = Sets.difference(uris, pruned);
        // remove 3
        distributers.get(DUE).registerChannels(YO, pruned);
        actual = getRemoved(3);

        assertEquals(expected, actual);
        // register the same
        distributers.get(ZERO).registerChannels(YO, pruned);
        assertNull(queue.poll(200, TimeUnit.MILLISECONDS));

        uris = generateURIs(8);
        expected = Sets.difference(uris, pruned);
        // add two
        distributers.get(UNO).registerChannels(YO, uris);
        actual = getAdded(2);

        assertEquals(expected, actual);

        expected = uris;
        // remove all
        distributers.get(UNO).registerChannels(YO, ImmutableSet.<URI>of());
        actual = getRemoved(8);

        assertEquals(expected, actual);

        int leaderCount = 0;
        for (ChannelDistributer distributer: distributers.values()) {
            if (distributer.m_isLeader) {
                ++leaderCount;
            }
        }
        assertEquals(1, leaderCount);
    }

    @Test
    public void testHostFailure() throws Exception {
        Set<URI> uris = generateURIs(9);
        Set<URI> expected = uris;
        // add nine
        distributers.get(UNO).registerChannels(YO, uris);
        Set<URI> actual = getAdded(9);

        assertEquals(expected, actual);

        // let's wait for the mesh to settle
        int attempts = 4;
        boolean settled = false;
        while (!settled && --attempts >=0) {
            Thread.sleep(50);
            settled = true;
            int stamp = distributers.get(ZERO).m_specs.getStamp();
            for (ChannelDistributer distributer: distributers.values()) {
                settled = settled && stamp == distributer.m_specs.getStamp();
            }
        }
        assertTrue(settled);

        Set<ChannelSpec> inZERO = Maps.filterValues(
                distributers.get(DUE).m_specs.getReference(),
                equalTo(ZERO))
                .navigableKeySet();
        assertTrue(inZERO.size() > 0);

        zks.get(ZERO).close();

        actual = getAdded(inZERO.size());
        assertEquals(inZERO, asSpecs(actual));
    }

    @After
    public void tearDown() throws Exception {
        for (ChannelDistributer distributer: distributers.values()) {
            distributer.shutdown();
        }
        tearDownZK();
    }
}
