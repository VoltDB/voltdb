/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDRConsumerDrIdTracker {

    private DRConsumerDrIdTracker tracker;

    @Before
    public void setUp() throws IOException {
        tracker = new DRConsumerDrIdTracker(-1L, 0L, 0L);
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

        assertTrue(tracker.size() == 4);
        tracker.truncate(9L);
        assertTrue(tracker.size() == 3 && tracker.getLastAckedDrId() == 9L);
        tracker.truncate(11L);
        assertTrue(tracker.size() == 3 && tracker.getLastAckedDrId() == 11L);
        tracker.truncate(20L);
        assertTrue(tracker.size() == 1 && tracker.getLastAckedDrId() == 20L);
        tracker.truncate(20L);
        assertTrue(tracker.size() == 1 && tracker.getLastAckedDrId() == 20L);
        tracker.truncate(25L);
        assertTrue(tracker.size() == 1 && tracker.getLastAckedDrId() == 25L);
        tracker.truncate(25L);
        assertTrue(tracker.size() == 1 && tracker.getLastAckedDrId() == 25L);
        tracker.truncate(39L);
        assertTrue(tracker.size() == 1 && tracker.getLastAckedDrId() == 39L);
        tracker.truncate(40L);
        assertTrue(tracker.size() == 0 && tracker.getLastAckedDrId() == 40L);
        tracker.truncate(40L);
        assertTrue(tracker.size() == 0 && tracker.getLastAckedDrId() == 40L);
        tracker.truncate(41L);
        assertTrue(tracker.size() == 0 && tracker.getLastAckedDrId() == 41L);
    }

    @Test
    public void testMergeTrackers() throws Exception {
        TreeMap<Long, Long> expectedMap = new TreeMap<>();
        tracker.append(8L, 9L, 0L, 0L);
        tracker.append(11L, 12L, 0L, 0L);

        tracker.append(20L, 25L, 0L, 0L);
        tracker.append(30L, 35L, 0L, 0L);

        tracker.append(40L, 40L, 0L, 0L);

        tracker.append(50L, 60L, 0L, 0L);

        tracker.append(70L, 80L, 0L, 0L);

        tracker.append(90L, 90L, 0L, 0L);

        DRConsumerDrIdTracker tracker2 = new DRConsumerDrIdTracker(5L, 0L, 0L);
        // This should insert a new entry before the beginning
        tracker2.append(6L, 6L, 0L, 0L);
        expectedMap.put(6L, 6L);

        // This should combine tracker's first and second entries (singletons)
        tracker2.append(10L, 10L, 0L, 0L);
        expectedMap.put(8L, 12L);

        // This should combine tracker's third and fourth entries (ranges)
        tracker2.append(26L, 29L, 0L, 0L);
        expectedMap.put(20L, 35L);

        // This should extend the new second entry
        tracker2.append(36L, 37L, 0L, 0L);
        expectedMap.put(20L, 37L);

        // This should prepend the entry starting at 40
        tracker2.append(39L, 39L, 0L, 0L);
        expectedMap.put(39L, 40L);

        // This should prepend the entry starting at 50
        tracker2.append(48L, 49L, 0L, 0L);
        expectedMap.put(48L, 60L);

        // This should create a new entry
        tracker2.append(62L, 66L, 0L, 0L);
        expectedMap.put(62L, 66L);

        // This should append the entry now starting at 70
        tracker2.append(81L, 81L, 0L, 0L);
        expectedMap.put(70L, 81L);

        // This should append the entry now starting at 90
        tracker2.append(91L, 95L, 0L, 0L);
        expectedMap.put(90L, 95L);

        // Add a new entry at the end
        tracker2.append(98L, 99L, 0L, 0L);
        expectedMap.put(98L, 99L);

        tracker.mergeTracker(tracker2);

        assertTrue(tracker.getLastAckedDrId() == 5L);
        assertTrue(tracker.getDrIdRanges().equals(expectedMap));
    }

    @Test
    public void testAppendToEmptyTracker() {
        TreeMap<Long, Long> expectedMap = new TreeMap<>();
        DRConsumerDrIdTracker tracker2 = new DRConsumerDrIdTracker(5L, 0L, 0L);
        tracker2.append(11L, 11L, 0L, 0L);
        expectedMap.put(11L, 11L);
        tracker2.append(13L, 15L, 0L, 0L);
        expectedMap.put(13L, 15L);

        tracker.appendTracker(tracker2);
        // should not have modified neighbor tracker
        assertEquals(2, tracker2.getDrIdRanges().size());
        // modification to the neighbor tracker should not affect our tracker after the append
        tracker2.getDrIdRanges().clear();
        assertTrue(tracker.getDrIdRanges().equals(expectedMap));
    }


    @Test
    public void testAppendNeighborTracker() throws Exception {
        TreeMap<Long, Long> expectedMap = new TreeMap<Long, Long>();
        tracker.append(6L, 10L, 0L, 0L);
        DRConsumerDrIdTracker tracker2 = new DRConsumerDrIdTracker(2L, 0L, 0L);
        tracker2.append(11L, 11L, 0L, 0L);
        expectedMap.put(6L, 11L);
        tracker2.append(13L, 15L, 0L, 0L);
        expectedMap.put(13L, 15L);

        tracker.appendTracker(tracker2);
        // should not have modified neighbor tracker
        assertEquals(2, tracker2.getDrIdRanges().size());
        // modification to the neighbor tracker should not affect our tracker after the append
        tracker2.getDrIdRanges().clear();
        assertEquals(expectedMap, tracker.getDrIdRanges());
    }

    @Test
    public void testAppendSparseNeighborTracker() throws Exception {
        TreeMap<Long, Long> expectedMap = new TreeMap<Long, Long>();
        tracker.append(6L, 10L, 0L, 0L);
        expectedMap.put(6L, 10L);
        tracker.append(15L, 20L, 0L, 0L);
        expectedMap.put(15L, 20L);
        DRConsumerDrIdTracker tracker2 = new DRConsumerDrIdTracker(2L, 0L, 0L);
        tracker2.append(22L, 30L, 0L, 0L);
        expectedMap.put(22L, 30L);
        tracker2.append(35L, 40L, 0L, 0L);
        expectedMap.put(35L, 40L);

        tracker.appendTracker(tracker2);
        assertEquals(expectedMap, tracker.getDrIdRanges());
    }

    @Test
    public void testAppendOverlappingNeighborTracker() throws Exception {
        boolean failed;
        tracker.append(5L, 10L, 0L, 0L);
        DRConsumerDrIdTracker tracker2 = new DRConsumerDrIdTracker(5L, 0L, 0L);
        tracker2.append(10L, 15L, 0L, 0L);

        failed = false;
        try {
            tracker.appendTracker(tracker2);
        }
        catch (AssertionError e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void testAppendAfterNeighborTracker() throws Exception {
        boolean failed;
        tracker.append(5L, 10L, 0L, 0L);
        DRConsumerDrIdTracker tracker2 = new DRConsumerDrIdTracker(0L, 0L, 0L);
        tracker2.append(1L, 4L, 0L, 0L);

        failed = false;
        try {
            tracker.appendTracker(tracker2);
        }
        catch (AssertionError e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void testSerialization() throws Exception {
        tracker.append(5L, 10L, 0L, 0L);
        tracker.append(15L, 20L, 0L, 0L);
        tracker.append(25L, 30L, 100L, 200L);

        byte[] flattened = new byte[tracker.getSerializedSize()];
        tracker.serialize(flattened);
        DRConsumerDrIdTracker tracker2 = new DRConsumerDrIdTracker(flattened);

        assertTrue(tracker.getLastAckedDrId() == tracker2.getLastAckedDrId());
        assertTrue(tracker.getLastSpUniqueId() == tracker2.getLastSpUniqueId());
        assertTrue(tracker.getLastMpUniqueId() == tracker2.getLastMpUniqueId());
        assertTrue(tracker.getDrIdRanges().equals(tracker2.getDrIdRanges()));
    }

    @Test
    public void testJsonSerialization() throws Exception {
        tracker.append(5L, 5L, 0L, 0L);
        tracker.append(15L, 20L, 0L, 0L);
        DRConsumerDrIdTracker tracker2 = new DRConsumerDrIdTracker(17L, 0L, 0L);
        tracker2.append(20L, 25L, 0L, 0L);
        tracker2.append(28L, 28L, 0L, 0L);
        Map<Integer, DRConsumerDrIdTracker> perProducerPartitionTrackers = new HashMap<Integer, DRConsumerDrIdTracker>();
        perProducerPartitionTrackers.put(0, tracker);
        perProducerPartitionTrackers.put(1, tracker2);
        Map<Integer, Map<Integer, DRConsumerDrIdTracker>> perSiteTrackers = new HashMap<Integer, Map<Integer, DRConsumerDrIdTracker>>();
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

        final Map<Integer, Map<Integer, DRConsumerDrIdTracker>> siteTrackers = ExtensibleSnapshotDigestData.buildConsumerSiteDrIdTrackersFromJSON(siteInfo);
        DRConsumerDrIdTracker tracker3 = siteTrackers.get(20).get(0);
        DRConsumerDrIdTracker tracker4 = siteTrackers.get(20).get(1);
        assertTrue(tracker.getLastAckedDrId() == tracker3.getLastAckedDrId());
        assertTrue(tracker.getLastSpUniqueId() == tracker3.getLastSpUniqueId());
        assertTrue(tracker.getLastMpUniqueId() == tracker3.getLastMpUniqueId());
        assertTrue(tracker.getDrIdRanges().equals(tracker3.getDrIdRanges()));
        assertTrue(tracker2.getLastAckedDrId() == tracker4.getLastAckedDrId());
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

        assertTrue(tracker.contains(4L) == null);
        Map.Entry<Long, Long> entry1 = tracker.contains(5L);
        assertTrue(entry1.getKey() == 5L && entry1.getValue() == 10L);
        Map.Entry<Long, Long> entry2 = tracker.contains(10L);
        assertTrue(entry2.getKey() == 5L && entry2.getValue() == 10L);
        assertTrue(tracker.contains(21L) == null);
        Map.Entry<Long, Long> entry3 = tracker.contains(25L);
        assertTrue(entry3.getKey() == 22L && entry3.getValue() == 30L);
        Map.Entry<Long, Long> entry4 = tracker.contains(30L);
        assertTrue(entry4.getKey() == 22L && entry4.getValue() == 30L);
        Map.Entry<Long, Long> entry5 = tracker.contains(40L);
        assertTrue(entry5.getKey() == 35L && entry5.getValue() == 40L);
        assertTrue(tracker.contains(45L) == null);
    }
}
