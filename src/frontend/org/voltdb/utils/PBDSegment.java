/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;

import java.io.File;
import java.io.IOException;

public interface PBDSegment {
    int NO_FLAGS = 0;
    int FLAG_COMPRESSED = 1;

    int COUNT_OFFSET = 0;
    int SIZE_OFFSET = 4;

    int CHUNK_SIZE = (1024 * 1024) * 64;
    int OBJECT_HEADER_BYTES = 8;
    int SEGMENT_HEADER_BYTES = 8;

    long segmentId();
    File file();

    void reset();

    int getNumEntries() throws IOException;

    boolean isBeingPolled();

    int readIndex();

    void open(boolean forWrite) throws IOException;

    void closeAndDelete() throws IOException;

    boolean isClosed();

    void close() throws IOException;

    void sync() throws IOException;

    boolean hasMoreEntries() throws IOException;

    boolean isEmpty() throws IOException;

    boolean offer(DBBPool.BBContainer cont, boolean compress) throws IOException;

    DBBPool.BBContainer poll(BinaryDeque.OutputContainerFactory factory) throws IOException;

    /*
     * Don't use size in bytes to determine empty, could potentially
     * diverge from object count on crash or power failure
     * although incredibly unlikely
     */
    int uncompressedBytesToRead();

    /**
     * Count and size may be cached in the object. If the segment is truncated out-of-band,
     * call this method to update the cached values.
     * @param count    The new count
     * @param size     The new size
     */
    void updateCachedCountAndSize(int count, int size);
}
