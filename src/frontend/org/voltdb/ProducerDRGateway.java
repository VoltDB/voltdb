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

import org.voltdb.pmsg.DRAgent.ClusterInfo;

import java.util.List;
import java.util.Map;

public interface ProducerDRGateway {

    public interface DRProducerResponseHandler {
        public void notifyOfResponse(boolean success, boolean shouldRetry, String failureCause);
    }

    /*
     * Ensure that all enabled DR Producer Hosts have agreed on the PBD file name
     */
    public abstract void blockOnDRStateConvergence();

    /**
     * Start listening on the ports
     */
    public abstract void initialize(boolean drProducerEnabled, int listenPort, String portInterface);

    /**
     * @return true if bindPorts has been called.
     */
    public abstract boolean isStarted();

    /**
     * Queues up a task to move all the InvocationBuffers to the PersistentBinaryDeque
     * @param nofsync do not force the sync to disk (when True)
     * @return the FutureTask indicating completion
     */
    public abstract void forceAllBuffersToDisk(boolean nofsync);

    public abstract boolean isActive();
    public abstract void setActive(boolean active);

    public abstract void start();
    public abstract void shutdown() throws InterruptedException;

    public abstract void updateCatalog(final CatalogContext catalog, final int listenPort);

    public abstract int getDRClusterId();

    public void truncateDRLogsForRestore(Map<Integer, Long> sequenceNumbers);

    /**
     * Clear all queued DR buffers for a master, useful when the replica goes away
     */
    public void deactivateDRProducer();
    public void activateDRProducer();

    /**
     * Blocks until snaphot is generated for the specified cluster id.
     * If the snapshot has already been generated, this will return immediately
     *
     * @param forClusterId the cluster for which producer should generate snapshot.
     *        This is used and has a meaningful value only in MULTICLUSTER_PROTOCOL_VERSION
     *        or higher.
     */
    public void blockOnSyncSnapshotGeneration(byte forClusterId);

    /**
     * Sets the DR protocol version with EE. This will also generate a <code>DR_STREAM_START</code>
     * event for all partitions, if <code>genStreamStart</code> flag is true.
     *
     * @param drVersion the DR protocol version that must be set with EE.
     * @param genStreamStart <code>DR_STREAM_START</code> event will be generated for all partitions
     * if this is true.
     *
     * @return Returns true if the operation was successful. False otherwise.
     */
    public boolean setDRProtocolVersion(int drVersion, boolean genStreamStart);

    /**
     * Use this to set up cursors in DR binary logs for clusters. This will initiate the process.
     * When the process is complete, the passed in handler will be notified of the status.
     *
     * @param requestedCursors the clusters for which cursors must be started
     * @param handler callback to notify the status of the operation
     */
    public void startCursor(final List<ClusterInfo> requestedCursors, final DRProducerResponseHandler handler);
}
