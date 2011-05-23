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

import java.util.Set;

public interface CommandLogReinitiator {
    /**
     * Start replaying the log. Two threads will be started, one for reading the
     * log and transforming them into task messages, the other one for reading
     * them off the queue and reinitiating them.
     */
    public void replay();

    /**
     * Joins the two threads
     * @throws InterruptedException
     */
    public void join() throws InterruptedException;

    /**
     * Set the partitions to skip when replaying the SPIs.
     *
     * @param partitions
     *            The IDs of the partitions to skip
     */
    public void skipPartitions(Set<Integer> partitions);

    /**
     * Whether or not to skip multi-partition transaction invocations
     * @param val true to skip
     */
    public void skipMultiPartitionTxns(boolean val);
}