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

package org.voltdb.dr2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.Pair;
import org.voltdb.DRConsumerDrIdTracker;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.SystemProcedureExecutionContext;

public class DRIDTrackerHelper {

    /**
     * Serialize the cluster trackers into JSON.
     *
     * @param lastConsumerUniqueIds UniqueIDs recorded on each consumer partition.
     * @param allProducerTrackers   All producer cluster trackers retrieved from each consumer partition.
     * @return A JSON string containing all information in the parameters.
     * @throws JSONException
     */
    public static String jsonifyClusterTrackers(Pair<Long, Long> lastConsumerUniqueIds,
                                                Map<Integer, Map<Integer, DRSiteDrIdTracker>> allProducerTrackers)
    throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.keySymbolValuePair("lastConsumerSpUniqueId", lastConsumerUniqueIds.getFirst());
        stringer.keySymbolValuePair("lastConsumerMpUniqueId", lastConsumerUniqueIds.getSecond());
        stringer.key("trackers").object();
        if (allProducerTrackers != null) {
            for (Map.Entry<Integer, Map<Integer, DRSiteDrIdTracker>> clusterTrackers : allProducerTrackers.entrySet()) {
                stringer.key(Integer.toString(clusterTrackers.getKey())).object();
                for (Map.Entry<Integer, DRSiteDrIdTracker> e : clusterTrackers.getValue().entrySet()) {
                    stringer.key(e.getKey().toString());
                    stringer.value(e.getValue().toJSON());
                }
                stringer.endObject();
            }
        }
        stringer.endObject();
        stringer.endObject();
        return stringer.toString();
    }

    /**
     * Deserialize the trackers retrieved from each consumer partitions.
     *
     * @param jsonData Tracker data retrieved from each consumer partition.
     * @param partitionsMissingTracker
     * @return A map of producer cluster ID to tracker for each producer
     * partition. If no tracker information is found, the map will be empty.
     * @throws JSONException
     */
    public static Map<Integer, Map<Integer, DRSiteDrIdTracker>> dejsonifyClusterTrackers(final String jsonData, boolean resetLastReceivedLogIds)
    throws JSONException
    {
        Map<Integer, Map<Integer, DRSiteDrIdTracker>> producerTrackers = new HashMap<>();

        JSONObject clusterData = new JSONObject(jsonData);
        final JSONObject trackers = clusterData.getJSONObject("trackers");
        Iterator<String> clusterIdKeys = trackers.keys();
        while (clusterIdKeys.hasNext()) {
            final String clusterIdStr = clusterIdKeys.next();
            final int clusterId = Integer.parseInt(clusterIdStr);
            final JSONObject trackerData = trackers.getJSONObject(clusterIdStr);
            Iterator<String> srcPidKeys = trackerData.keys();
            while (srcPidKeys.hasNext()) {
                final String srcPidStr = srcPidKeys.next();
                final int srcPid = Integer.valueOf(srcPidStr);
                final JSONObject ids = trackerData.getJSONObject(srcPidStr);
                final DRSiteDrIdTracker tracker = new DRSiteDrIdTracker(ids, resetLastReceivedLogIds);

                Map<Integer, DRSiteDrIdTracker> clusterTrackers = producerTrackers.computeIfAbsent(clusterId, k -> new HashMap<>());
                clusterTrackers.put(srcPid, tracker);
            }
        }

        return producerTrackers;
    }

    /**
     * Merge trackers in the additional map into the base map.
     * @param base The base map to merge trackers into.
     * @param add  The additional trackers to merge.
     */
    public static void mergeTrackers(Map<Integer, Map<Integer, DRSiteDrIdTracker>> base,
                                     Map<Integer, Map<Integer, DRSiteDrIdTracker>> add)
    {
        for (Map.Entry<Integer, Map<Integer, DRSiteDrIdTracker>> clusterEntry : add.entrySet()) {
            final Map<Integer, DRSiteDrIdTracker> baseClusterEntry = base.get(clusterEntry.getKey());
            if (baseClusterEntry == null) {
                base.put(clusterEntry.getKey(), clusterEntry.getValue());
            } else {
                for (Map.Entry<Integer, DRSiteDrIdTracker> partitionEntry : clusterEntry.getValue().entrySet()) {
                    final DRConsumerDrIdTracker basePartitionTracker = baseClusterEntry.get(partitionEntry.getKey());
                    if (basePartitionTracker == null) {
                        baseClusterEntry.put(partitionEntry.getKey(), partitionEntry.getValue());
                    } else {
                        basePartitionTracker.mergeTracker(partitionEntry.getValue());
                    }
                }
            }
        }
    }

    public static byte[] clusterTrackersToBytes(Map<Integer, Map<Integer, DRSiteDrIdTracker>> trackers) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(trackers);
        oos.flush();
        byte trackerBytes[] = baos.toByteArray();
        oos.close();
        return trackerBytes;
    }

    public static Map<Integer, Map<Integer, DRSiteDrIdTracker>> bytesToClusterTrackers(byte[] trackerBytes) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(trackerBytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (Map<Integer, Map<Integer, DRSiteDrIdTracker>>)ois.readObject();
    }

    public static void setDRIDTrackerFromBytes(SystemProcedureExecutionContext context, byte[] trackerBytes) throws IOException, ClassNotFoundException
    {
        Map<Integer, Map<Integer, DRSiteDrIdTracker>> clusterToPartitionMap = bytesToClusterTrackers(trackerBytes);
        context.recoverWithDrAppliedTrackers(clusterToPartitionMap);
    }
}
