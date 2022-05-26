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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;

import com.google_voltpatches.common.collect.RangeSet;
import com.google_voltpatches.common.collect.TreeRangeSet;

public class TestDRConsumerDrIdTracker {

    private DRSiteDrIdTracker tracker;

    @Before
    public void setUp() throws IOException {
        tracker = DRConsumerDrIdTracker.createSiteTracker(0L, -1L, 0L, 0L, 0);
    }

    @After
    public void tearDown() throws InterruptedException {
    }

    @Test
    public void testAppend() throws Exception {
        // Append single to range
        tracker.append(5L, 8L, 0L, 0L);
        tracker.append(9L, 9L, 0L, 0L);

        // Append single to single
        tracker.append(11L, 11L, 0L, 0L);
        tracker.append(12L, 12L, 0L, 0L);

        // Append range to single
        tracker.append(14L, 14L, 0L, 0L);
        tracker.append(15L, 20L, 0L, 0L);

        // Append range to range
        tracker.append(25L, 30L, 0L, 0L);
        tracker.append(31L, 40L, 0L, 0L);

        // try appending an overlapping range
        boolean failed = false;
        try {
            tracker.append(40L, 45L, 0L, 0L);
        }
        catch (AssertionError e) {
            failed = true;
        }
        assertTrue(failed);

        // try appending an overlapping value
        failed = false;
        try {
            tracker.append(7L, 7L, 0L, 0L);
        }
        catch (AssertionError e) {
            failed = true;
        }
        assertTrue(failed);

        assertTrue(tracker.size() == 5);
        tracker.truncate(9L);
        assertTrue(tracker.size() == 4 && tracker.getSafePointDrId() == 9L);
        tracker.truncate(11L);
        assertTrue(tracker.size() == 3 && tracker.getSafePointDrId() == 12L);
        tracker.truncate(20L);
        assertTrue(tracker.size() == 2 && tracker.getSafePointDrId() == 20L);
        tracker.truncate(20L);
        assertTrue(tracker.size() == 2 && tracker.getSafePointDrId() == 20L);
        tracker.truncate(25L);
        assertTrue(tracker.size() == 1 && tracker.getSafePointDrId() == 40L);
        tracker.truncate(25L);
        assertTrue(tracker.size() == 1 && tracker.getSafePointDrId() == 40L);
        tracker.truncate(39L);
        assertTrue(tracker.size() == 1 && tracker.getSafePointDrId() == 40L);
        tracker.truncate(40L);
        assertTrue(tracker.size() == 1 && tracker.getSafePointDrId() == 40L);
        tracker.truncate(40L);
        assertTrue(tracker.size() == 1 && tracker.getSafePointDrId() == 40L);
        tracker.truncate(41L);
        assertTrue(tracker.size() == 1 && tracker.getSafePointDrId() == 41L);
    }

    @Test
    public void testMergeTrackers() throws Exception {
        RangeSet<Long> expectedMap = TreeRangeSet.create();
        tracker.append(8L, 9L, 0L, 0L);
        tracker.append(11L, 12L, 0L, 0L);

        tracker.append(20L, 25L, 0L, 0L);
        tracker.append(30L, 35L, 0L, 0L);

        tracker.append(40L, 40L, 0L, 0L);

        tracker.append(50L, 60L, 0L, 0L);

        tracker.append(70L, 80L, 0L, 0L);

        tracker.append(90L, 90L, 0L, 0L);

        DRConsumerDrIdTracker tracker2 = DRConsumerDrIdTracker.createBufferReceiverTracker(5L, 0L, 0L, 0);
        // This should insert a new entry before the beginning
        tracker2.append(6L, 6L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(6L, 6L));

        // This should combine tracker's first and second entries (singletons)
        tracker2.append(10L, 10L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(8L, 12L));

        // This should combine tracker's third and fourth entries (ranges)
        tracker2.append(26L, 29L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(20L, 35L));

        // This should extend the new second entry
        tracker2.append(36L, 37L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(20L, 37L));

        // This should prepend the entry starting at 40
        tracker2.append(39L, 39L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(39L, 40L));

        // This should prepend the entry starting at 50
        tracker2.append(48L, 49L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(48L, 60L));

        // This should create a new entry
        tracker2.append(62L, 66L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(62L, 66L));

        // This should append the entry now starting at 70
        tracker2.append(81L, 81L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(70L, 81L));

        // This should append the entry now starting at 90
        tracker2.append(91L, 95L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(90L, 95L));

        // Add a new entry at the end
        tracker2.append(98L, 99L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(98L, 99L));

        tracker.mergeTracker(tracker2);

        assertTrue(tracker.getSafePointDrId() == 6L);
        assertTrue(tracker.getDrIdRanges().equals(expectedMap));
    }

    @Test
    public void testMergeTrackersWithOverlaps() throws Exception {
        RangeSet<Long> expectedMap = TreeRangeSet.create();
        tracker.append(8L, 9L, 0L, 0L);
        tracker.append(11L, 12L, 0L, 0L);
        tracker.append(20L, 25L, 0L, 0L);
        tracker.append(32L, 35L, 0L, 0L);
        tracker.append(40L, 40L, 0L, 0L);
        tracker.append(50L, 60L, 0L, 0L);

        DRConsumerDrIdTracker tracker2 = DRConsumerDrIdTracker.createBufferReceiverTracker(6L, 0L, 0L, 0);
        // overlaps with the beginning of the first entry
        tracker2.append(7L, 8L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(8L, 9L));

        // overlaps with the end of the second entry
        tracker2.append(12L, 14L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(11L, 14L));

        // completely covers the third entry
        tracker2.append(19L, 30L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(19L, 30L));

        // covers multiple ranges at the end
        tracker2.append(36L, 70L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(32L, 70L));

        tracker.mergeTracker(tracker2);

        assertTrue(tracker.getSafePointDrId() == 9L);
        assertEquals(expectedMap, tracker.getDrIdRanges());
    }

    @Test
    public void testAppendToEmptyTracker() {
        RangeSet<Long> expectedMap = TreeRangeSet.create();
        DRConsumerDrIdTracker tracker2 = DRConsumerDrIdTracker.createBufferReceiverTracker(5L, 0L, 0L, 0);
        expectedMap.add(DRConsumerDrIdTracker.range(5L, 5L));
        tracker2.append(11L, 11L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(11L, 11L));
        tracker2.append(13L, 15L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(13L, 15L));

        tracker.mergeTracker(tracker2);
        // should not have modified neighbor tracker
        assertEquals(3, tracker2.size());
        // modification to the neighbor tracker should not affect our tracker after the append
        tracker2.getDrIdRanges().clear();
        assertTrue(tracker.getDrIdRanges().equals(expectedMap));
    }

    @Test
    public void testAppendNeighborTracker() throws Exception {
        RangeSet<Long> expectedMap = TreeRangeSet.create();
        tracker.append(6L, 10L, 0L, 0L);
        DRConsumerDrIdTracker tracker2 = DRConsumerDrIdTracker.createBufferReceiverTracker(2L, 0L, 0L, 0);
        expectedMap.add(DRConsumerDrIdTracker.range(2L, 2L));
        tracker2.append(11L, 11L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(6L, 11L));
        tracker2.append(13L, 15L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(13L, 15L));

        tracker.mergeTracker(tracker2);
        // should not have modified neighbor tracker
        assertEquals(3, tracker2.size());
        // modification to the neighbor tracker should not affect our tracker after the append
        tracker2.getDrIdRanges().clear();
        assertEquals(expectedMap, tracker.getDrIdRanges());
    }

    @Test
    public void testAppendSparseNeighborTracker() throws Exception {
        RangeSet<Long> expectedMap = TreeRangeSet.create();
        tracker.append(6L, 10L, 0L, 0L);
        tracker.append(15L, 20L, 0L, 0L);
        DRConsumerDrIdTracker tracker2 = DRConsumerDrIdTracker.createBufferReceiverTracker(20L, 0L, 0L, 0);
        expectedMap.add(DRConsumerDrIdTracker.range(20L, 20L));
        tracker2.append(22L, 30L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(22L, 30L));
        tracker2.append(35L, 40L, 0L, 0L);
        expectedMap.add(DRConsumerDrIdTracker.range(35L, 40L));

        tracker.mergeTracker(tracker2);
        assertEquals(expectedMap, tracker.getDrIdRanges());
    }

    @Test
    public void testSerialization() throws Exception {
        tracker.append(5L, 10L, 0L, 0L);
        tracker.append(15L, 20L, 0L, 0L);
        tracker.append(25L, 30L, 100L, 200L);

        byte[] flattened = new byte[tracker.getSerializedSize()];
        tracker.serialize(flattened);
        DRConsumerDrIdTracker tracker2 = new DRConsumerDrIdTracker(flattened);

        assertTrue(tracker.getSafePointDrId() == tracker2.getSafePointDrId());
        assertTrue(tracker.getLastSpUniqueId() == tracker2.getLastSpUniqueId());
        assertTrue(tracker.getLastMpUniqueId() == tracker2.getLastMpUniqueId());
        assertTrue(tracker.getDrIdRanges().equals(tracker2.getDrIdRanges()));
    }

    @Test
    public void testJsonSerialization() throws Exception {
        tracker.append(5L, 5L, 0L, 0L);
        tracker.append(15L, 20L, 0L, 0L);
        DRSiteDrIdTracker tracker2 = DRConsumerDrIdTracker.createSiteTracker(0L, 17L, 0L, 0L, 0);
        tracker2.append(20L, 25L, 0L, 0L);
        tracker2.append(28L, 28L, 0L, 0L);
        Map<Integer, DRSiteDrIdTracker> perProducerPartitionTrackers = new HashMap<Integer, DRSiteDrIdTracker>();
        perProducerPartitionTrackers.put(0, tracker);
        perProducerPartitionTrackers.put(1, tracker2);
        Map<Integer, Map<Integer, DRSiteDrIdTracker>> perSiteTrackers = new HashMap<Integer, Map<Integer, DRSiteDrIdTracker>>();
        // Insert trackers from cluster 20
        perSiteTrackers.put(20, perProducerPartitionTrackers);
        JSONObject trackersInJSON = ExtensibleSnapshotDigestData.serializeSiteConsumerDrIdTrackersToJSON(perSiteTrackers);
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key("5");  // ConsumerPartitionId
        stringer.value(trackersInJSON);
        stringer.endObject();

        String output = stringer.toString();
        JSONObject allsiteInfo = new JSONObject(output);
        JSONObject siteInfo = allsiteInfo.getJSONObject("5");

        final Map<Integer, Map<Integer, DRSiteDrIdTracker>> siteTrackers = ExtensibleSnapshotDigestData.buildConsumerSiteDrIdTrackersFromJSON(siteInfo, false);
        DRConsumerDrIdTracker tracker3 = siteTrackers.get(20).get(0);
        DRConsumerDrIdTracker tracker4 = siteTrackers.get(20).get(1);
        assertTrue(tracker.getSafePointDrId() == tracker3.getSafePointDrId());
        assertTrue(tracker.getLastSpUniqueId() == tracker3.getLastSpUniqueId());
        assertTrue(tracker.getLastMpUniqueId() == tracker3.getLastMpUniqueId());
        assertTrue(tracker.getDrIdRanges().equals(tracker3.getDrIdRanges()));
        assertTrue(tracker2.getSafePointDrId() == tracker4.getSafePointDrId());
        assertTrue(tracker2.getLastSpUniqueId() == tracker4.getLastSpUniqueId());
        assertTrue(tracker2.getLastMpUniqueId() == tracker4.getLastMpUniqueId());
        assertTrue(tracker2.getDrIdRanges().equals(tracker4.getDrIdRanges()));
    }

    @Test
    public void testContains() {
        tracker.append(5L, 10L, 0L, 0L);
        tracker.append(15L, 20L, 0L, 0L);
        tracker.append(22L, 30L, 0L, 0L);
        tracker.append(35L, 40L, 0L, 0L);
        tracker.truncate(6L);

        assertTrue(tracker.contains(2L, 2L));
        assertTrue(tracker.contains(4L, 10L));
        assertTrue(tracker.contains(10L, 10L));
        assertFalse(tracker.contains(14L, 33L));
        assertTrue(tracker.contains(16L, 19L));
        assertFalse(tracker.contains(21L, 21L));
        assertTrue(tracker.contains(25L, 25L));
        assertTrue(tracker.contains(30L, 30L));
        assertTrue(tracker.contains(40L, 40L));
        assertFalse(tracker.contains(38L, 45L));
        assertFalse(tracker.contains(41L, 45L));
        assertFalse(tracker.contains(45L, 45L));
    }

    @Test
    public void testFirstLastDrId() {
        tracker.append(5L, 10L, 0L, 0L);
        tracker.append(15L, 20L, 0L, 0L);
        tracker.append(25L, 30L, 0L, 0L);
        tracker.truncate(5L);

        assertEquals(5L, tracker.getFirstDrId());
        assertEquals(30L, tracker.getLastDrId());
    }
}
