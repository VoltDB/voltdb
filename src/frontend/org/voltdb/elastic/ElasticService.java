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

package org.voltdb.elastic;

import java.util.Collection;
import java.util.List;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONObject;
import org.voltdb.CatalogContext;

import com.google_voltpatches.common.collect.ImmutableList;

public interface ElasticService {
    /**
     * Start the elastic service so that it can begin or resume elastic operations
     *
     * @throws KeeperException      If there was an error with the leader election in {@link ZooKeeper}
     * @throws InterruptedException If this thread was interrupted
     */
    void start() throws KeeperException, InterruptedException;
    void shutdown();
    void updateConfig(CatalogContext context);

    /**
     * Return any metadata required to resume a running elastic operation after a cluster recovery.
     *
     * @return {@link JSONObject} with metadata required to resume the currently running elastic operation. May return
     *         {@code null}
     */
    JSONObject getResumeMetadata();

    /**
     * Notify the ElasticService that one or more host failed
     *
     * @param failedHostIds {@link Collection} of host IDs which failed
     */
    void hostsFailed(Collection<Integer> failedHostIds);

    /**
     * During some elastic operations partitions might exist which are not on the hash ring but they cannot be removed
     * from the system. This method is a way to test if any partitions can be removed from the system.
     *
     * @return {@code true} if partitions can be removed from the system
     */
    boolean canRemovePartitions();

    /**
     * Add a new {@link BalancePartitionsListener} that will be called during balance partitions transactions
     *
     * @param listener to be added
     */
    void addBalancePartitionsListener(BalancePartitionsListener listener);


    /**
     * Return a list of prefixes for the snapshots created by elastic operations.
     * <p>
     * Must match how snapshot nonces are built in {@code SnapshotUtil.makeSnapshotNonce(String, long)}
     *
     * @return  list of elastic snapshot none prefixes
     */
    static public List<String> getSnapshotPrefixes() {
        return ImmutableList.of(
                ElasticOperation.JOIN + "_" + getSnapshotHsid(),
                ElasticOperation.REMOVE + "_" + getSnapshotHsid());
    }

    /**
     * Return the hsid to use for building the elastic snapshot nonces
     */
    static public int getSnapshotHsid() {
        return 0;
    }
}
