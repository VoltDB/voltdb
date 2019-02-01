/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

public class ExtensibleSnapshotDigestData {
    //JSON property names for export values stored in the snapshot json
    public static final String EXPORT_SEQUENCE_NUMBER_ARR = "exportSequenceNumbers";
    public static final String EXPORT_TABLE_NAME = "exportTableName";
    public static final String SEQUENCE_NUM_PER_PARTITION = "sequenceNumberPerPartition";
    public static final String PARTITION = "partition";
    public static final String EXPORT_SEQUENCE_NUMBER = "exportSequenceNumber";
    public static final String EXPORT_USO = "exportUso";

    public static final String DISABLED_EXTERNAL_STREAMS = "disabledExternalStreams";

    /**
     * This field is the same values as m_exportSequenceNumbers once they have been extracted
     * in SnapshotSaveAPI.createSetup and then passed back in to SSS.initiateSnapshots. The only
     * odd thing is that setting up a snapshot can fail in which case values will have been populated into
     * m_exportSequenceNumbers and kept until the next snapshot is started in which case they are repopulated.
     * Decoupling them seems like a good idea in case snapshot code is every re-organized.
     */
    private final Map<String, Map<Integer, Pair<Long, Long>>> m_exportSequenceNumbers;

    /**
     * Same as m_exportSequenceNumbersToLogOnCompletion, but for m_drTupleStreamInfo
     */
    private final Map<Integer, TupleStreamStateInfo> m_drTupleStreamInfo;

    /**
     * Set of partitions where external streams are disabled.
     */
    private Set<Integer> m_disabledExternalStreams;

    /**
     * Used to pass the last seen unique ids from remote datacenters into the snapshot
     * termination path so it can publish it to ZK where it is extracted by rejoining
     * nodes
     */
    private final Map<Integer, JSONObject> m_drMixedClusterSizeConsumerState;

    /**
     * Value that denotes that this snapshot is one created with shutdown --save. 0
     * being no, and other values yes
     */
    private long m_terminus;

    private final JSONObject m_elasticOperationMetadata;

    public ExtensibleSnapshotDigestData(
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            Map<Integer, TupleStreamStateInfo> drTupleStreamInfo,
            Map<Integer, JSONObject> drMixedClusterSizeConsumerState,
            JSONObject elasticOperationMetadata, final JSONObject jsData) {
        m_exportSequenceNumbers = exportSequenceNumbers;
        m_drTupleStreamInfo = drTupleStreamInfo;
        m_drMixedClusterSizeConsumerState = drMixedClusterSizeConsumerState;
        m_terminus = jsData != null ? jsData.optLong(SnapshotUtil.JSON_TERMINUS, 0L) : 0L;
        m_elasticOperationMetadata = elasticOperationMetadata;
    }

    void setDisabledExternalStreams(Set<Integer> disabledStreams) {
        m_disabledExternalStreams = disabledStreams;
    }

    private void writeExportSequencesToSnapshot(JSONStringer stringer) throws JSONException {
            stringer.key(EXPORT_SEQUENCE_NUMBER_ARR).array();
            for (Map.Entry<String, Map<Integer, Pair<Long, Long>>> entry : m_exportSequenceNumbers.entrySet()) {
                stringer.object();

                stringer.keySymbolValuePair(EXPORT_TABLE_NAME, entry.getKey());
                stringer.key(SEQUENCE_NUM_PER_PARTITION).array();
                for (Map.Entry<Integer, Pair<Long,Long>> sequenceNumber : entry.getValue().entrySet()) {
                    stringer.object();
                    stringer.keySymbolValuePair(PARTITION, sequenceNumber.getKey());
                    stringer.keySymbolValuePair(EXPORT_USO, sequenceNumber.getValue().getFirst());
                    stringer.keySymbolValuePair(EXPORT_SEQUENCE_NUMBER, sequenceNumber.getValue().getSecond());
                    stringer.endObject();
                }
                stringer.endArray();

                stringer.endObject();
            }
            stringer.endArray();
    }

    /*
     * When recording snapshot completion in ZooKeeper we also record export
     * sequence numbers as JSON. Need to merge our sequence numbers with
     * existing numbers since multiple replicas will submit the sequence number
     */
    private void mergeExportSequenceNumbersToZK(JSONObject jsonObj, VoltLogger log) throws JSONException {
        JSONObject tableSequenceMap;
        if (jsonObj.has(EXPORT_SEQUENCE_NUMBER_ARR)) {
            tableSequenceMap = jsonObj.getJSONObject(EXPORT_SEQUENCE_NUMBER_ARR);
        } else {
            tableSequenceMap = new JSONObject();
            jsonObj.put(EXPORT_SEQUENCE_NUMBER_ARR, tableSequenceMap);
        }

        for (Map.Entry<String, Map<Integer, Pair<Long, Long>>> tableEntry : m_exportSequenceNumbers.entrySet()) {
            JSONObject sequenceNumbers;
            final String tableName = tableEntry.getKey();
            if (tableSequenceMap.has(tableName)) {
                sequenceNumbers = tableSequenceMap.getJSONObject(tableName);
            } else {
                sequenceNumbers = new JSONObject();
                tableSequenceMap.put(tableName, sequenceNumbers);
            }

            for (Map.Entry<Integer, Pair<Long, Long>> partitionEntry : tableEntry.getValue().entrySet()) {
                final Integer partitionId = partitionEntry.getKey();
                final String partitionIdString = partitionId.toString();
                final Long ackOffset = partitionEntry.getValue().getFirst();
                final Long partitionSequenceNumber = partitionEntry.getValue().getSecond();

                /*
                 * Check that the sequence number is the same everywhere and log if it isn't.
                 * Not going to crash because we are worried about poison pill transactions.
                 */
                if (sequenceNumbers.has(partitionIdString)) {
                    JSONObject existingEntry = sequenceNumbers.getJSONObject(partitionIdString);
                    Long existingSequenceNumber = existingEntry.getLong("sequenceNumber");
                    if (!existingSequenceNumber.equals(partitionSequenceNumber)) {
                        log.debug("Found a mismatch in export sequence numbers of export table " + tableName +
                                " while recording snapshot metadata for partition " + partitionId +
                                ". This is expected only on replicated, write-to-file export streams (remote node reported " +
                                existingSequenceNumber + " and the local node reported " + partitionSequenceNumber + ")");
                    }
                    existingEntry.put(partitionIdString, Math.max(existingSequenceNumber, partitionSequenceNumber));

                    Long existingAckOffset = existingEntry.getLong("ackOffset");
                    existingEntry.put("ackOffset", Math.max(ackOffset, existingAckOffset));
                } else {
                    JSONObject newObj = new JSONObject();
                    newObj.put("sequenceNumber", partitionSequenceNumber);
                    newObj.put("ackOffset", ackOffset);
                    sequenceNumbers.put(partitionIdString, newObj);
                }
            }
        }
    }

    private void mergeExternalStreamStatesToZK(JSONObject jsonObj, VoltLogger log) throws JSONException {
        JSONArray jsonPartitions;
        Set<Integer> disabledStreamsInJson = new HashSet<>();
        if (jsonObj.has(DISABLED_EXTERNAL_STREAMS)) {
            jsonPartitions = jsonObj.getJSONArray(DISABLED_EXTERNAL_STREAMS);
            for (int i=0; i<jsonPartitions.length(); i++) {
                disabledStreamsInJson.add(jsonPartitions.getInt(i));
            }
        } else {
            jsonPartitions = new JSONArray();
            jsonObj.put(DISABLED_EXTERNAL_STREAMS, jsonPartitions);
        }

        for (Integer partition : m_disabledExternalStreams) {
            if (!disabledStreamsInJson.contains(partition)) {
                jsonPartitions.put(partition);
            }
        }
    }

    private void mergeTerminusToZK(JSONObject jsonObj) throws JSONException {
        long jsTerminus = jsonObj.optLong(SnapshotUtil.JSON_TERMINUS, 0L);
        m_terminus = Math.max(jsTerminus, m_terminus);
        jsonObj.put(SnapshotUtil.JSON_TERMINUS, m_terminus);
    }

    private void writeDRTupleStreamInfoToSnapshot(JSONStringer stringer) throws JSONException {
        stringer.key("drTupleStreamStateInfo");
        stringer.object();
        for (Map.Entry<Integer, TupleStreamStateInfo> e : m_drTupleStreamInfo.entrySet()) {
            stringer.key(e.getKey().toString());
            stringer.object();
            if (e.getKey() != MpInitiator.MP_INIT_PID) {
                stringer.keySymbolValuePair("sequenceNumber", e.getValue().partitionInfo.drId);
                stringer.keySymbolValuePair("spUniqueId", e.getValue().partitionInfo.spUniqueId);
                stringer.keySymbolValuePair("mpUniqueId", e.getValue().partitionInfo.mpUniqueId);
            } else {
                stringer.keySymbolValuePair("sequenceNumber", e.getValue().replicatedInfo.drId);
                stringer.keySymbolValuePair("spUniqueId", e.getValue().replicatedInfo.spUniqueId);
                stringer.keySymbolValuePair("mpUniqueId", e.getValue().replicatedInfo.mpUniqueId);
            }
            stringer.endObject();
        }
        stringer.endObject();
    }

    private void mergeDRTupleStreamInfoToZK(JSONObject jsonObj, VoltLogger log) throws JSONException {
        JSONObject stateInfoMap;
        // clusterCreateTime should be same across the cluster
        long clusterCreateTime = VoltDB.instance().getClusterCreateTime();
        assert (!jsonObj.has("clusterCreateTime") || (clusterCreateTime == jsonObj.getLong("clusterCreateTime")));
        jsonObj.put("clusterCreateTime", clusterCreateTime);
        if (jsonObj.has("drTupleStreamStateInfo")) {
            stateInfoMap = jsonObj.getJSONObject("drTupleStreamStateInfo");
        } else {
            stateInfoMap = new JSONObject();
            jsonObj.put("drTupleStreamStateInfo", stateInfoMap);
        }

        for (Map.Entry<Integer, TupleStreamStateInfo> e : m_drTupleStreamInfo.entrySet()) {
            final String partitionId = e.getKey().toString();
            DRLogSegmentId partitionStateInfo;
            if (e.getKey() != MpInitiator.MP_INIT_PID) {
                partitionStateInfo = e.getValue().partitionInfo;
            } else {
                partitionStateInfo = e.getValue().replicatedInfo;
            }
            JSONObject existingStateInfo = stateInfoMap.optJSONObject(partitionId);
            boolean addEntry = false;
            if (existingStateInfo == null) {
                addEntry = true;
            }
            else if (partitionStateInfo.drId != existingStateInfo.getLong("sequenceNumber")) {
                if (partitionStateInfo.drId > existingStateInfo.getLong("sequenceNumber")) {
                    addEntry = true;
                }
                log.debug("Found a mismatch in dr sequence numbers for partition " + partitionId +
                        " the DRId should be the same at all replicas, but one node had " +
                        DRLogSegmentId.getDebugStringFromDRId(existingStateInfo.getLong("sequenceNumber")) +
                        " and the local node reported " + DRLogSegmentId.getDebugStringFromDRId(partitionStateInfo.drId));
            }

            if (addEntry) {
                JSONObject stateInfo = new JSONObject();
                stateInfo.put("sequenceNumber", partitionStateInfo.drId);
                stateInfo.put("spUniqueId", partitionStateInfo.spUniqueId);
                stateInfo.put("mpUniqueId", partitionStateInfo.mpUniqueId);
                stateInfo.put("drVersion", e.getValue().drVersion);
                stateInfoMap.put(partitionId, stateInfo);
            }
        }
    }

    static public JSONObject serializeSiteConsumerDrIdTrackersToJSON(Map<Integer, Map<Integer, DRSiteDrIdTracker>> drMixedClusterSizeConsumerState)
            throws JSONException {
        JSONObject clusters = new JSONObject();
        if (drMixedClusterSizeConsumerState == null) {
            return clusters;
        }
        for (Map.Entry<Integer, Map<Integer, DRSiteDrIdTracker>> e : drMixedClusterSizeConsumerState.entrySet()) {
            // The key is the remote Data Center's partitionId. HeteroTopology implies a different partition count
            // from the local cluster's partition count (which is not tracked here)
            JSONObject partitions = new JSONObject();
            for (Map.Entry<Integer, DRSiteDrIdTracker> e2 : e.getValue().entrySet()) {
                partitions.put(e2.getKey().toString(), e2.getValue().toJSON());
            }
            clusters.put(e.getKey().toString(), partitions);
        }
        return clusters;
    }

    static public Map<Integer, Map<Integer, DRSiteDrIdTracker>> buildConsumerSiteDrIdTrackersFromJSON(JSONObject siteTrackers, boolean resetLastReceiedLogIds) throws JSONException {
        Map<Integer, Map<Integer, DRSiteDrIdTracker>> perSiteTrackers = new HashMap<Integer, Map<Integer, DRSiteDrIdTracker>>();
        Iterator<String> clusterKeys = siteTrackers.keys();
        while (clusterKeys.hasNext()) {
            Map<Integer, DRSiteDrIdTracker> perProducerPartitionTrackers = new HashMap<Integer, DRSiteDrIdTracker>();
            String clusterIdStr = clusterKeys.next();
            int clusterId = Integer.valueOf(clusterIdStr);
            JSONObject producerPartitionInfo = siteTrackers.getJSONObject(clusterIdStr);
            Iterator<String> producerPartitionKeys = producerPartitionInfo.keys();
            while (producerPartitionKeys.hasNext()) {
                String producerPartitionIdStr = producerPartitionKeys.next();
                int producerPartitionId = Integer.valueOf(producerPartitionIdStr);
                DRSiteDrIdTracker producerPartitionTracker = new DRSiteDrIdTracker(producerPartitionInfo.getJSONObject(producerPartitionIdStr), resetLastReceiedLogIds);
                perProducerPartitionTrackers.put(producerPartitionId, producerPartitionTracker);
            }
            perSiteTrackers.put(clusterId, perProducerPartitionTrackers);
        }
        return perSiteTrackers;
    }

    /*
     * When recording snapshot completion we also record DR remote DC unique ids
     * as JSON. Need to merge our unique ids with existing numbers
     * since multiple replicas will submit the unique ids
     */
    private void mergeConsumerDrIdTrackerToZK(JSONObject jsonObj) throws JSONException {
        //DR ids/unique ids for remote partitions indexed by remote datacenter id,
        //each DC has a full partition set
        JSONObject dcIdMap;
        if (jsonObj.has("drMixedClusterSizeConsumerState")) {
            dcIdMap = jsonObj.getJSONObject("drMixedClusterSizeConsumerState");
        } else {
            dcIdMap = new JSONObject();
            jsonObj.put("drMixedClusterSizeConsumerState", dcIdMap);
        }

        for (Map.Entry<Integer, JSONObject> dcEntry : m_drMixedClusterSizeConsumerState.entrySet()) {
            //Last seen ids for a specific data center
            final String consumerPartitionString = dcEntry.getKey().toString();
            if (!dcIdMap.has(consumerPartitionString)) {
                dcIdMap.put(consumerPartitionString, dcEntry.getValue());
            }
        }
    }

    private void writeDRStateToSnapshot(JSONStringer stringer) throws JSONException {
        long clusterCreateTime = VoltDB.instance().getClusterCreateTime();
        stringer.keySymbolValuePair("clusterCreateTime", clusterCreateTime);

        Iterator<Entry<Integer, TupleStreamStateInfo>> iter = m_drTupleStreamInfo.entrySet().iterator();
        if (iter.hasNext()) {
            stringer.keySymbolValuePair("drVersion", iter.next().getValue().drVersion);
        }
        writeDRTupleStreamInfoToSnapshot(stringer);
        stringer.key("drMixedClusterSizeConsumerState");
        stringer.object();
        for (Entry<Integer, JSONObject> e : m_drMixedClusterSizeConsumerState.entrySet()) {
            stringer.key(e.getKey().toString()); // Consumer partitionId
            stringer.value(e.getValue()); // Trackers from that site
        }
        stringer.endObject();
    }

    public void writeToSnapshotDigest(JSONStringer stringer) throws IOException {
        try {
            writeExportSequencesToSnapshot(stringer);
            writeExternalStreamStates(stringer);
            writeDRStateToSnapshot(stringer);
            stringer.key(SnapshotUtil.JSON_ELASTIC_OPERATION).value(m_elasticOperationMetadata);
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    /**
     * Writes external streams state for partitions into snapshot digest.
     */
    private void writeExternalStreamStates(JSONStringer stringer) throws JSONException {
            stringer.key(DISABLED_EXTERNAL_STREAMS).array();
            for (int partition : m_disabledExternalStreams) {
                stringer.value(partition);
            }
            stringer.endArray();
    }

    public void mergeToZooKeeper(JSONObject jsonObj, VoltLogger log) throws JSONException {
        mergeExportSequenceNumbersToZK(jsonObj, log);
        mergeExternalStreamStatesToZK(jsonObj, log);
        mergeDRTupleStreamInfoToZK(jsonObj, log);
        mergeConsumerDrIdTrackerToZK(jsonObj);
        mergeTerminusToZK(jsonObj);
        jsonObj.put(SnapshotUtil.JSON_ELASTIC_OPERATION, m_elasticOperationMetadata);
    }

    public long getTerminus() {
        return m_terminus;
    }
}
