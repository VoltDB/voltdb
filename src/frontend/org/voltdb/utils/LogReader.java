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

package org.voltdb.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.voltdb.LogEntry;

public interface LogReader {
    public interface LogIterator extends Iterator<LogEntry> {}

    /**
     * Get the minimum transaction ID among the last seen transactions across
     * all initiators in the previous segment.
     *
     * @return
     */
    public long getMinLastSeenTxn();

    /**
     * Get the total number of partitions that were in the database.
     *
     * @return The partition count
     */
    public int getPartitionCount();

    /**
     * Get the site ID -> partition ID mapping of all alive sites. Can be null
     * if the log is empty
     *
     * @return
     */
    public Map<Integer, Integer> getPartitionMap();

    /**
     * Get the initiator IDs. Can be null if the log is empty
     * @return
     */
    public int[] getInitiatorIds();

    /**
     * Get the set of failed sites. Can be null if the log is empty
     * @return
     */
    public Set<Integer> getFailedSites();

    /**
     * Get the set of failed transaction IDs. Can be null if the log is empty
     * @return
     */
    public Set<Long> getFailedTxns();

    /**
     * Whether or not the log is empty
     * @return
     */
    public boolean isEmpty();

    /**
     * Get the iterator that iterates through the filtered log
     *
     * @return The iterator
     * @throws IOException if an error occurred
     */
    public LogIterator iterator() throws IOException;

    public void close() throws IOException;
}