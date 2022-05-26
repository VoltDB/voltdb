/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltcore.utils.Pair;
import org.voltdb.DRConsumerDrIdTracker;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.iv2.MpInitiator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DRIDTrackerHelper {
    /**
     * @param clusterId    ID of the cluster which is part of the filter
     * @param partitionIds {@link Collection} of partition IDs in the filter. May be {@code null}
     * @return size of this filter when serialized
     */
    public static int serializeTrackerFilterSize(int clusterId, Collection<Integer> partitionIds) {
        return Byte.BYTES + Character.BYTES + (partitionIds == null ? 0 : partitionIds.size() * Integer.BYTES);
    }

    /**
     * @param buffer       where the filter should be serialized
     * @param clusterId    ID of the cluster which is part of the filter
     * @param partitionIds {@link Collection} of partition IDs in the filter. May be {@code null}
     */
    public static void serializeTrackerFilter(ByteBuffer buffer, int clusterId, Collection<Integer> partitionIds) {
        buffer.put((byte) clusterId);
        if (partitionIds == null) {
            buffer.putChar((char) -1);
        } else {
            buffer.putChar((char) partitionIds.size());
            partitionIds.forEach(buffer::putInt);
        }
    }

    /**
     * Serialize lastConsumerUniqueIds and trackers using {@code filter} to select desired trackers
     *
     * @param lastConsumerUniqueIds last mp and sp unique IDs which were consumed
     * @param allProducerTrackers   All of the trackers
     * @param filter                to apply to {@code allProducerTrackers}
     * @return serialized trackers
     */
    public static ByteBuf serializeClustersTrackers(Pair<Long, Long> lastConsumerUniqueIds,
            Map<Integer, Map<Integer, DRSiteDrIdTracker>> allProducerTrackers, ByteBuffer filter) {
        int clusterId = -1;
        Set<Integer> producerPartitionIds = null;
        if (filter != null && filter.hasRemaining()) {
            clusterId = filter.get();
            int partitionCount = filter.getChar();
            if (partitionCount >= 0) {
                producerPartitionIds = new HashSet<>();
                for (int i = 0; i < partitionCount; ++i) {
                    producerPartitionIds.add(filter.getInt());
                }
            }
        }

        ByteBuf data = Unpooled.buffer();
        data.writeLong(lastConsumerUniqueIds.getFirst()).writeLong(lastConsumerUniqueIds.getSecond());

        if (allProducerTrackers == null) {
            data.writeByte(0);
        } else {
            if (clusterId < 0) {
                data.writeByte(allProducerTrackers.size());
                for (Map.Entry<Integer, Map<Integer, DRSiteDrIdTracker>> entry : allProducerTrackers.entrySet()) {
                    serializeClusterTrackers(data, entry.getKey(), entry.getValue(), producerPartitionIds);
                }
            } else {
                Map<Integer, DRSiteDrIdTracker> clusterTrackers = allProducerTrackers.get(clusterId);
                if (clusterTrackers == null) {
                    data.writeByte(0);
                } else {
                    data.writeByte(1);
                    serializeClusterTrackers(data, clusterId, clusterTrackers, producerPartitionIds);
                }
            }
        }

        return data;
    }

    private static void serializeClusterTrackers(ByteBuf data, int clusterId,
            Map<Integer, DRSiteDrIdTracker> clusterTrackers, Set<Integer> producerPartitionIds) {
        data.writeByte(clusterId);
        data.writeChar(clusterTrackers.size() - (clusterTrackers.containsKey(MpInitiator.MP_INIT_PID) ? 1 : 0));
        if (producerPartitionIds == null) {
            data.writeChar(clusterTrackers.size());
            for (Map.Entry<Integer, DRSiteDrIdTracker> e : clusterTrackers.entrySet()) {
                serializePartitionTracker(data, e.getKey(), e.getValue());
            }
        } else {
            int countPosition = data.writerIndex();
            data.writerIndex(countPosition + Character.BYTES);
            char count = 0;
            for (Integer producerPartitionId : producerPartitionIds) {
                DRSiteDrIdTracker tracker = clusterTrackers.get(producerPartitionId);
                if (tracker != null) {
                    serializePartitionTracker(data, producerPartitionId, tracker);
                    ++count;
                }
            }
            data.setChar(countPosition, count);
        }
    }

    private static void serializePartitionTracker(ByteBuf data, Integer producerPartitionId,
            DRSiteDrIdTracker tracker) {
        data.writeInt(producerPartitionId);
        tracker.serialize(data);
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

    public static void setDRIDTrackerFromBytes(SystemProcedureExecutionContext context, int clusterId, byte[] trackerBytes) throws IOException, ClassNotFoundException
    {
        Map<Integer, Map<Integer, DRSiteDrIdTracker>> clusterToPartitionMap = bytesToClusterTrackers(trackerBytes);
        context.recoverDrState(clusterId, clusterToPartitionMap, null);
    }
}
