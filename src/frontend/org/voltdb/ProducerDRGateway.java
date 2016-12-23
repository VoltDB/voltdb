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
import java.util.Map;

public interface ProducerDRGateway {

    /**
     * Start the main thread and the state machine, wait until all nodes converge on the initial state.
     * @throws IOException
     */
    public void startAndWaitForGlobalAgreement() throws IOException;

    /**
     * Truncate the DR log using the snapshot restore truncation point cached
     * earlier. This is called on recover before the command log replay starts
     * to drop all binary logs generated after the snapshot. Command log replay
     * will recreate those binary logs.
     */
    public void truncateDRLog();

    /**
     * Start listening on the ports
     */
    public abstract void startListening(boolean drProducerEnabled, int listenPort, String portInterface) throws IOException;

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

    public void cacheSnapshotRestoreTruncationPoint(Map<Integer, Long> sequenceNumbers);

    /**
     * Clear all queued DR buffers for a master, useful when the replica goes away
     */
    public void deactivateDRProducer();
    public void activateDRProducer();

    public void blockOnSyncSnapshotGeneration();
}
