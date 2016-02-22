/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.iv2.MpInitiator;

public class ExtensibleSnapshotDigestData {
    /**
     * This field is the same values as m_exportSequenceNumbers once they have been extracted
     * in SnapshotSaveAPI.createSetup and then passed back in to SSS.initiateSnapshots. The only
     * odd thing is that setting up a snapshot can fail in which case values will have been populated into
     * m_exportSequenceNumbers and kept until the next snapshot is started in which case they are repopulated.
     * Decoupling them seems like a good idea in case snapshot code is every re-organized.
     */
    private Map<String, Map<Integer, Pair<Long, Long>>> m_exportSequenceNumbers;

    /**
     * Same as m_exportSequenceNumbersToLogOnCompletion, but for m_drTupleStreamInfo
     */
    private Map<Integer, TupleStreamStateInfo> m_drTupleStreamInfo;

    /**
     * Used to pass the last seen unique ids from remote datacenters into the snapshot
     * termination path so it can publish it to ZK where it is extracted by rejoining
     * nodes
     */
    private Map<Integer, Map<Integer, DRLogSegmentId>> m_remoteDCLastIds;

    public ExtensibleSnapshotDigestData(Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            Map<Integer, TupleStreamStateInfo> drTupleStreamInfo,
            Map<Integer, Map<Integer, DRLogSegmentId>> remoteDCLastIds) {
        m_exportSequenceNumbers = exportSequenceNumbers;
        m_drTupleStreamInfo = drTupleStreamInfo;
        m_remoteDCLastIds = remoteDCLastIds;
    }

    private void writeExportSequenceNumbersToSnapshot(JSONStringer stringer) throws IOException {
        try {
            stringer.key("exportSequenceNumbers").array();
            for (Map.Entry<String, Map<Integer, Pair<Long, Long>>> entry : m_exportSequenceNumbers.entrySet()) {
                stringer.object();

                stringer.key("exportTableName").value(entry.getKey());

                stringer.key("sequenceNumberPerPartition").array();
                for (Map.Entry<Integer, Pair<Long,Long>> sequenceNumber : entry.getValue().entrySet()) {
                    stringer.object();
                    stringer.key("partition").value(sequenceNumber.getKey());
                    //First value is the ack offset which matters for pauseless rejoin, but not persistence
                    stringer.key("exportSequenceNumber").value(sequenceNumber.getValue().getSecond());
                    stringer.endObject();
                }
                stringer.endArray();

                stringer.endObject();
            }
            stringer.endArray();
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    /*
     * When recording snapshot completion in ZooKeeper we also record export
     * sequence numbers as JSON. Need to merge our sequence numbers with
     * existing numbers since multiple replicas will submit the sequence number
     */
    private void mergeExportSequenceNumbersToZK(JSONObject jsonObj, VoltLogger log) throws JSONException {
        JSONObject tableSequenceMap;
        if (jsonObj.has("exportSequenceNumbers")) {
            tableSequenceMap = jsonObj.getJSONObject("exportSequenceNumbers");
        } else {
            tableSequenceMap = new JSONObject();
            jsonObj.put("exportSequenceNumbers", tableSequenceMap);
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
                        log.error("Found a mismatch in export sequence numbers while recording snapshot metadata " +
                                " for partition " + partitionId +
                                " the sequence number should be the same at all replicas, but one had " +
                                existingSequenceNumber
                                + " and another had " + partitionSequenceNumber);
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

    private void writeDRTupleStreamInfoToSnapshot(JSONStringer stringer) throws IOException {
        try {
            stringer.key("drTupleStreamStateInfo");
            stringer.object();
            for (Map.Entry<Integer, TupleStreamStateInfo> e : m_drTupleStreamInfo.entrySet()) {
                stringer.key(e.getKey().toString());
                stringer.object();
                if (e.getKey() != MpInitiator.MP_INIT_PID) {
                    stringer.key("sequenceNumber").value(e.getValue().partitionInfo.drId);
                    stringer.key("spUniqueId").value(e.getValue().partitionInfo.spUniqueId);
                    stringer.key("mpUniqueId").value(e.getValue().partitionInfo.mpUniqueId);
                } else {
                    stringer.key("sequenceNumber").value(e.getValue().replicatedInfo.drId);
                    stringer.key("spUniqueId").value(e.getValue().replicatedInfo.spUniqueId);
                    stringer.key("mpUniqueId").value(e.getValue().replicatedInfo.mpUniqueId);
                }
                stringer.endObject();
            }
            stringer.endObject();
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    private void mergeDRTupleStreamInfoToZK(JSONObject jsonObj) throws JSONException {
        JSONObject stateInfoMap;
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
            if (existingStateInfo == null || partitionStateInfo.drId > existingStateInfo.getLong("sequenceNumber")) {
                JSONObject stateInfo = new JSONObject();
                stateInfo.put("sequenceNumber", partitionStateInfo.drId);
                stateInfo.put("spUniqueId", partitionStateInfo.spUniqueId);
                stateInfo.put("mpUniqueId", partitionStateInfo.mpUniqueId);
                stateInfo.put("drVersion", e.getValue().drVersion);
                stateInfoMap.put(partitionId, stateInfo);
            }
        }
    }

    private void writeRemoteDRLastIdsToSnapshot(JSONStringer stringer) throws IOException {
        try {
            stringer.key("remoteDCLastIds");
            stringer.object();
            for (Map.Entry<Integer, Map<Integer, DRLogSegmentId>> e : m_remoteDCLastIds.entrySet()) {
                stringer.key(e.getKey().toString());
                stringer.object();
                for (Map.Entry<Integer, DRLogSegmentId> e2 : e.getValue().entrySet()) {
                    stringer.key(e2.getKey().toString());
                    stringer.object();
                    stringer.key("drId").value(e2.getValue().drId);
                    stringer.key("spUniqueId").value(e2.getValue().spUniqueId);
                    stringer.key("mpUniqueId").value(e2.getValue().mpUniqueId);
                    stringer.endObject();
                }
                stringer.endObject();
            }
            stringer.endObject();
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    /*
     * When recording snapshot completion we also record DR remote DC unique ids
     * as JSON. Need to merge our unique ids with existing numbers
     * since multiple replicas will submit the unique ids
     */
    private void mergeRemoteDRLastIdsToZK(JSONObject jsonObj) throws JSONException {
        //DR ids/unique ids for remote partitions indexed by remote datacenter id,
        //each DC has a full partition set
        JSONObject dcIdMap;
        if (jsonObj.has("remoteDCLastIds")) {
            dcIdMap = jsonObj.getJSONObject("remoteDCLastIds");
        } else {
            dcIdMap = new JSONObject();
            jsonObj.put("remoteDCLastIds", dcIdMap);
        }

        for (Map.Entry<Integer, Map<Integer, DRLogSegmentId>> dcEntry : m_remoteDCLastIds.entrySet()) {
            //Last seen ids for a specific data center
            JSONObject lastSeenIds;
            final String dcKeyString = dcEntry.getKey().toString();
            if (dcIdMap.has(dcKeyString)) {
                lastSeenIds = dcIdMap.getJSONObject(dcKeyString);
            } else {
                lastSeenIds = new JSONObject();
                dcIdMap.put(dcKeyString, lastSeenIds);
            }

            for (Map.Entry<Integer, DRLogSegmentId> partitionEntry : dcEntry.getValue().entrySet()) {
                final String partitionIdString = partitionEntry.getKey().toString();
                final Long lastSeenDRIdLong = partitionEntry.getValue().drId;
                final Long lastSeenSpUniqueIdLong = partitionEntry.getValue().spUniqueId;
                final Long lastSeenMpUniqueIdLong = partitionEntry.getValue().mpUniqueId;
                long existingDRId = Long.MIN_VALUE;
                if (lastSeenIds.has(partitionIdString)) {
                    existingDRId = lastSeenIds.getJSONObject(partitionIdString).getLong("drId");
                }
                if (lastSeenDRIdLong > existingDRId) {
                    JSONObject ids = new JSONObject();
                    ids.put("drId", lastSeenDRIdLong);
                    ids.put("spUniqueId", lastSeenSpUniqueIdLong);
                    ids.put("mpUniqueId", lastSeenMpUniqueIdLong);
                    lastSeenIds.put(partitionIdString, ids);
                }
            }
        }
    }

    private void writeDRVersionToSnapshot(JSONStringer stringer) throws IOException {
        try {
            Iterator<Entry<Integer, TupleStreamStateInfo>> iter = m_drTupleStreamInfo.entrySet().iterator();
            if (iter.hasNext()) {
                stringer.key("drVersion").value(iter.next().getValue().drVersion);
            }
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    public void writeToSnapshotDigest(JSONStringer stringer) throws IOException {
        writeExportSequenceNumbersToSnapshot(stringer);
        writeDRVersionToSnapshot(stringer);
        writeDRTupleStreamInfoToSnapshot(stringer);
        writeRemoteDRLastIdsToSnapshot(stringer);
    }

    public void mergeToZooKeeper(JSONObject jsonObj, VoltLogger log) throws JSONException {
        mergeExportSequenceNumbersToZK(jsonObj, log);
        mergeDRTupleStreamInfoToZK(jsonObj);
        mergeRemoteDRLastIdsToZK(jsonObj);
    }
}
