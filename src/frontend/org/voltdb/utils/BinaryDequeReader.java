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

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;

/**
 * Reader interface used to read entries from the deque. Multiple readers may be active at the same time,
 * each of them maintaining their own read location within the deque.
 */
public interface BinaryDequeReader {
    /**
     * Read and return the object at the current read position of this reader.
     * The entry will be removed once all active readers have read the entry.
     * @param ocf
     * @param checkCRC
     * @return BBContainer with the bytes read. Null if there is nothing left to read.
     * @throws IOException
     */
    public BBContainer poll(OutputContainerFactory ocf, boolean checkCRC) throws IOException;

    /**
     * Read and return the schema of table located in the segment header
     * @param segmentIndex index of the segment to get schema from, -1 means get schema from current segment
     * @param updateReaderOffset whether to restore reader's original read offset after polling schema
     * @param checkCRC check PBD header CRC while getting schema
     * @return
     * @throws IOException
     */
    public BBContainer getSchema(long segmentIndex, boolean restoreReaderOffset, boolean checkCRC) throws IOException;

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
    public boolean isStartOfSegment() throws IOException;

    /**
     * Returns the index of the segment that reader currently reads on
     * @return
     */
    public long getSegmentIndex();
}