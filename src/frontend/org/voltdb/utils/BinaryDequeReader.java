/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.nio.ByteBuffer;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;

/**
 * Reader interface used to read entries from the deque. Multiple readers may be active at the same time,
 * each of them maintaining their own read location within the deque.
 */
public interface BinaryDequeReader<M> {
    /**
     * Read and return the object at the current read position of this reader.
     * The entry will be removed once all active readers have read the entry.
     * @param ocf
     * @return BBContainer with the bytes read. Null if there is nothing left to read.
     * @throws IOException
     */
    public BBContainer poll(OutputContainerFactory ocf) throws IOException;

    /**
     * Read and return the full entry at the current read position of this reader. The entry will be removed once all
     * active readers have read the entry.
     *
     * @param ocf Factory used to create {@link DBBPool.BBContainer} as destinations for the entry data
     * @return {@link Entry} containing data entry and any extra header associated with it or {@code null} if there is
     *         no entry
     * @throws IOException
     */
    public Entry<M> pollEntry(OutputContainerFactory ocf) throws IOException;

    /**
     * @param segmentIndex index of the segment to get schema from, -1 means get schema from current segment
     * @return The extra header metadata in the segment header or {@code null} if there is none
     * @throws IOException If an error occurs reading the extra header
     * @deprecated use {@link #pollEntry(OutputContainerFactory)} to retrieve full entry
     */
    @Deprecated
    public BBContainer getExtraHeader(long segmentIndex) throws IOException;

    /**
     * Number of bytes left to read for this reader.
     * @return number of bytes left to read for this reader.
     * @throws IOException
     */
    public long sizeInBytes() throws IOException;

    /**
     *  Number of objects left to read for this reader.
     * @return number of objects left to read for this reader
     * @throws IOException
     */
    public int getNumObjects() throws IOException;

    /**
     * Returns true if this reader still has entries to read. False otherwise
     * @return true if this reader still has entries to read. False otherwise
     * @throws IOException
     */
    public boolean isEmpty() throws IOException;

    /**
     * Is the object this reader going to read the first object of segment?
     * @return true if the object this reader going to read is the first object of segment
     * throws IOException
     */
    @Deprecated
    public boolean isStartOfSegment() throws IOException;

    /**
     * Returns the index of the segment that reader currently reads on
     * @return
     */
    @Deprecated
    public long getSegmentIndex();

    /**
     * Entry class to hold all metadata and data associated with an entry in a {@link BinaryDeque}
     *
     * @param <M> Type of extra header metadata
     */
    public interface Entry<M> {
        /**
         * @return any associated extra header metadata. May return {@code null} if there was none
         */
        M getExtraHeader();

        /**
         * @return A {@link ByteBuffer} holding the entry data
         */
        ByteBuffer getData();

        /**
         * Indicates that this entry has been consumed and is eligible for deletion with respect to the
         * {@link BinaryDequeReader} which returned this entry.
         */
        void release();
    }
}