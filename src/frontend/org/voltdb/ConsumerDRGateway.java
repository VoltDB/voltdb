/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.voltcore.utils.Pair;


// Interface through which the outside world can interact with the consumer side
// of DR. Currently, there's not much to do here, since the subsystem is
// largely self-contained
public interface ConsumerDRGateway extends Promotable {

    public abstract void updateCatalog(CatalogContext catalog);

    public abstract boolean isActive();

    public abstract void initialize(boolean resumeReplication);

    public abstract void shutdown(boolean blocking) throws InterruptedException;

    public abstract void notifyOfLastSeenSegmentId(int partitionId, long maxDRId, long maxUniqueId);

    public abstract void notifyOfLastAppliedSegmentId(int partitionId, long endDRId, long endUniqueId);

    /**
     * Should only be called before initialization. Populate all previously seen
     * [drId, uniqueId] pairs from remote clusters
     * @param lastAppliedIds A map of clusterIds to maps of partitionIds to [drId, uniqueId] pairs.
     * The outer map is assumed to be of size 1
     */
    public abstract void populateLastAppliedSegmentIds(Map<Integer, Map<Integer, Pair<Long, Long>>> lastAppliedIds);

    public abstract void assertSequencing(int partitionId, long drId);

    public abstract Map<Integer, Map<Integer, Pair<Long, Long>>> getLastReceivedBinaryLogIds();

    public static class DummyConsumerDRGateway implements ConsumerDRGateway {
        Map<Integer, Map<Integer, Pair<Long, Long>>> ids = new HashMap<Integer, Map<Integer, Pair<Long, Long>>>();

        @Override
        public void initialize(boolean resumeReplication) {}

        @Override
        public void acceptPromotion() throws InterruptedException,
                ExecutionException, KeeperException {}

        @Override
        public void updateCatalog(CatalogContext catalog) {}

        @Override
        public boolean isActive() { return false; }

        @Override
        public void shutdown(boolean blocking) {}

        @Override
        public void notifyOfLastSeenSegmentId(int partitionId, long maxDRId, long maxUniqueId) {}

        @Override
        public void notifyOfLastAppliedSegmentId(int partitionId, long endDRId, long endUniqueId) {
            int dataCenter = (int)(endDRId >> 55);
            if (!ids.containsKey(dataCenter)) {
                ids.put(dataCenter, new HashMap<Integer, Pair<Long, Long>>());
            }
            ids.get(dataCenter).put(partitionId, Pair.of(endDRId, endUniqueId));
        };

        @Override
        public void assertSequencing(int partitionId, long drId) {}

        @Override
        public Map<Integer, Map<Integer, Pair<Long, Long>>> getLastReceivedBinaryLogIds() { return ids; }

        @Override
        public void populateLastAppliedSegmentIds(Map<Integer, Map<Integer, Pair<Long, Long>>> lastAppliedIds) {
            ids.clear();
            ids.putAll(lastAppliedIds);
        }
    }
}
