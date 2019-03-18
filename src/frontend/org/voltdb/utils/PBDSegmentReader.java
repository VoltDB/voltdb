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

import org.voltcore.utils.DBBPool;

/**
 * Represents a reader for a segment. Multiple readers may be active
 * at any point in time, reading from different locations in the segment.
 */
interface PBDSegmentReader {
    /**
     * Are there any more entries to read from this segment for this reader
     *
     * @return true if there are still more entries to be read. False otherwise.
     * @throws IOException if the reader was closed or on any error trying to read from the segment file.
     */
    public boolean hasMoreEntries() throws IOException;

    /**
     * Have all the entries in this segment been read by this reader and
     * acknowledged as ready for discarding.
     *
     * @return true if all entries have been read and discarded by this reader. False otherwise.
     * @throws IOException if the reader was closed
     */
    public boolean allReadAndDiscarded() throws IOException;

    /**
     * Read the next entry from the segment for this reader.
     * Returns null if all entries in this segment were already read by this reader.
     *
     * @param factory
     * @param checkCRC
     * @return BBContainer with the bytes read
     * @throws IOException
     */
    public DBBPool.BBContainer poll(BinaryDeque.OutputContainerFactory factory,
            boolean checkCRC) throws IOException;

    public DBBPool.BBContainer getSchema(boolean checkCRC) throws IOException;

    //Don't use size in bytes to determine empty, could potentially
    //diverge from object count on crash or power failure
    //although incredibly unlikely
    /**
     * Returns the number of bytes that are left to read in this segment
     * for this reader.
     */
    public int uncompressedBytesToRead();

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
     * Set the current read offset for this reader in this segment to given value
     */
    public void setReadOffset(long readOffset);

    /**
     * Reopen a previously closed reader. Re-opened reader still keeps the original read offset.
     */
    public void reopen(boolean forWrite, boolean emptyFile) throws IOException;

    /**
     * Close this reader and release any resources.
     * <code>getReader</code> will still return this reader until the segment is closed.
     */
    public void close() throws IOException;

    /**
     * Has this reader been closed.
     */
    public boolean isClosed();
}
