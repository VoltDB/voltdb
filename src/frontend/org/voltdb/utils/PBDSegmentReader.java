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

import java.io.Closeable;
import java.io.IOException;

import org.voltcore.utils.DBBPool;

/**
 * Represents a reader for a segment. Multiple readers may be active
 * at any point in time, reading from different locations in the segment.
 */
interface PBDSegmentReader<M> extends Closeable {
    /**
     * Are there any more entries to read from this segment for this reader
     *
     * @return true if there are still more entries to be read. False otherwise.
     * @throws IOException if the reader was closed or on any error trying to read from the segment file.
     */
    public boolean hasMoreEntries() throws IOException;

    /**
     * @return {@code true} if any entries have been read and discarded from this segment
     */
    public boolean anyReadAndDiscarded();

    /**
     * @return {@code true} if ALL entries have been read and discarded from this segment by this reader
     */
    public boolean allReadAndDiscarded();

    /**
     * @return {@code true} if any entries have been polled but not freed
     */
    boolean hasOutstandingEntries();

    /**
     * Read the next entry from the segment for this reader.
     * Returns null if all entries in this segment were already read by this reader.
     *
     * @param factory
     * @return BBContainer with the bytes read or {@code null} if all entries have been consumed
     * @throws IOException
     */
    public DBBPool.BBContainer poll(BinaryDeque.OutputContainerFactory factory, int maxSize) throws IOException;

    /**
     * @return A {@link DBBPool.BBContainer} with the extra header supplied for the segment or {@code null} if one was
     *         not supplied
     * @throws IOException If an error occurs while reading the extra header
     */
    @Deprecated
    public DBBPool.BBContainer getExtraHeader() throws IOException;

    //Don't use size in bytes to determine empty, could potentially
    //diverge from object count on crash or power failure
    //although incredibly unlikely
    /**
     * Returns the number of bytes that are left to read in this segment for this reader.
     *
     * @throws IOException
     */
    public int uncompressedBytesToRead() throws IOException;

    /**
     * Returns the current read offset for this reader in this segment.
     */
    public long readOffset();

    /**
     * Entry that this reader will read next.
     * @return
     */
    public int readIndex();

    /**
     * Rewinds the read offset for this reader by the specified number of bytes.
     */
    public void rewindReadOffset(int byBytes);

    /**
     * Reopen a previously closed reader. Re-opened reader still keeps the original read offset.
     */
    public void reopen() throws IOException;

    /**
     * Close this reader and release any resources.
     */
    @Override
    public void close() throws IOException;

    /**
     * Close this reader and release any resources. Difference between this and {@link #close()} is that
     * this keeps track of the closed reader. This call is used only internally when a reader is polling through segments.
     * {@link PBDSegment#getReader(String)} will still return this reader until the segment is closed or until all buffers are acked.
     */
    public void closeAndSaveReaderState() throws IOException;

    /**
     * Has this reader been closed.
     */
    public boolean isClosed();

    /**
     * Mark all entries left to read in this segment as read and discarded.
     * If there were any entries that were read, but not discarded, they will be left as undiscarded.
     *
     * @throws IOException
     */
    public void markRestReadAndDiscarded() throws IOException;
}
