/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.network;

/**
 * An interface for a stream to report how many bytes it has queued or dequed. Supply a negative number of bytes
 * if necessary
 */
public interface QueueMonitor {
    /**
     * Indicates whether the write stream should signal backpressure
     * @param bytes
     * @return True if the write stream should signal backpressure and false otherwise.
     */
    public boolean queue(int bytes);
}
