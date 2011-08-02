/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

public interface CommandLogReinitiator {
    public interface Callback {
        public void onReplayCompletion();
    }

    /**
     * Set the snapshot transaction ID that got restored
     * @param txnId
     */
    public void setSnapshotTxnId(long txnId);

    public void setCallback(Callback callback);

    /**
     * Start replaying the log. Two threads will be started, one for reading the
     * log and transforming them into task messages, the other one for reading
     * them off the queue and reinitiating them.
     */
    public void replay();

    /**
     * Whether or not we have started replaying local command log.
     *
     * @return true if it's replaying or it has finished, false if we are still
     *         waiting for replay plan
     */
    public boolean started();

    /**
     * Joins the two threads
     * @throws InterruptedException
     */
    public void join() throws InterruptedException;

    /**
     * Whether or not there were SPIs replayed in the cluster. This will return
     * true even if there were SPIs replayed by other nodes.
     *
     * @return true if there were at least one SPI replayed
     */
    public boolean hasReplayed();

    /**
     * Get the maximum transaction ID among the last seen transactions across
     * all initiators in the previous segment.
     *
     * @return null if the log is empty
     */
    public Long getMaxLastSeenTxn();

    /**
     * Returns all command log segments to the pool and closes the reader. This
     * discards the command log.
     */
    public void returnAllSegments();
}