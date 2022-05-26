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
package org.voltdb.utils;

import java.io.IOException;

import org.voltcore.utils.DBBPool.BBContainer;

/**
 * Writes to a BinaryDeque are always at the tail by default. This is an interface that allows writing
 * in the middle, to fill missing entries in the BinaryDeque.
 * <p> Only one gap writer may be active at a time in the BinaryDeque.
 */
public interface BinaryDequeGapWriter<M> {

    /**
     * Sets the extra header information to be used for subsequent gap offers.
     * This must be set before any offers. A new segment will be started when
     * the extra header information changes.
     *
     * @param extraHeader extra header information for subsequent offers
     * @throws if an IO error occurs closing currently open segment
     */
    public void updateGapHeader(M extraHeader) throws IOException;

    /**
     * Method to write data at the appropriate location in the BinaryDeque.
     * The offer implementation should find the exact location where the data should
     * be inserted and insert it there. The id range being offered must not already
     * exist in the BinaryDeque.
     * <p>
     * {@code data} will be guaranteed to be discarded before this method returns
     *
     * @param data the bytes to be written
     * @param startId starting id of the data block being offered
     * @param endId ending id of the data block being offered
     * @param timestamp of this data
     * @return the number of bytes written
     * @throws IOException on any IO error writing the data
     * @throws IllegalArguementException if the id range being offered intersects with data in the BinaryDeque
     */
    public int offer(BBContainer data, long startId, long endId, long timestamp) throws IOException;

    /**
     * Close the writer and release any resources used only by this.
     * @throws IOException if an IO error occurs releasing resources
     */
    public void close() throws IOException;
}
