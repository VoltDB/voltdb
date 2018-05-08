/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;

/**
 * Specialized deque interface for storing binary objects. Objects can be provided as a buffer chain
 * and will be returned as a single buffer. Technically not a deque because removal at
 * the end is not supported.
 *
 */
public interface BinaryDeque {
    /*
     * Allocator for storage coming out of the BinaryDeque. Only
     * used if copying is necessary, otherwise a slice is returned
     */
    public static interface OutputContainerFactory {
        public BBContainer getContainer(int minimumSize);
    }

    /**
     * Store a buffer chain as a single object in the deque. IOException may be thrown if the object
     * is larger then the implementation defined max. 64 megabytes in the case of PersistentBinaryDeque.
     * If there is an exception attempting to write the buffers then all the buffers will be discarded
     * @param object
     * @throws IOException
     */
    void offer(BBContainer object) throws IOException;

    /**
     * Store a buffer chain as a single object in the deque. IOException may be thrown if the object
     * is larger then the implementation defined max. 64 megabytes in the case of PersistentBinaryDeque.
     * If there is an exception attempting to write the buffers then all the buffers will be discarded
     * @param object
     * @param allowCompression
     * @throws IOException
     */
    void offer(BBContainer object, boolean allowCompression) throws IOException;

    int offer(DeferredSerialization ds) throws IOException;

    /**
     * A push creates a new file each time to be "the head" so it is more efficient to pass
     * in all the objects you want to push at once so that they can be packed into
     * as few files as possible. IOException may be thrown if the object
     * is larger then the implementation defined max. 64 megabytes in the case of PersistentBinaryDeque.
     * If there is an exception attempting to write the buffers then all the buffers will be discarded
     * @param objects Array of buffers representing the objects to be pushed to the head of the queue
     * @throws java.io.IOException
     */
    public void push(BBContainer objects[]) throws IOException;

    /**
     * Start a BinaryDequeReader for reading, positioned at the start of the deque.
     * @param cursorId a String identifying the cursor. If a cursor is already open for this id,
     * the existing cursor will be returned.
     * @return a BinaryDequeReader for this cursorId
     * @throws IOException on any errors trying to read the PBD files
     */
    public BinaryDequeReader openForRead(String cursorId) throws IOException;

    /**
     * Close a BinaryDequeReader for reader, also close the SegmentReader for the segment if it is reading one
     * @param cursorId a String identifying the cursor.
     * @throws IOException on any errors trying to close the SegmentReader if it is the last one for the segment
     */
    public void closeCursor(String cursorId);

    /**
     * Persist all objects in the queue to the backing store
     * @throws IOException
     */
    public void sync() throws IOException;

    public void parseAndTruncate(BinaryDequeTruncator truncator) throws IOException;

    /**
     * Release all resources (open files) held by the back store of the queue. Continuing to use the deque
     * will result in an exception
     * @throws IOException
     */
    public void close() throws IOException;

    public boolean initializedFromExistingFiles();

    public Pair<Integer, Long> getBufferCountAndSize() throws IOException;

    public void closeAndDelete() throws IOException;

    /**
     * Reader class used to read entries from the deque. Multiple readers may be active at the same time,
     * each of them maintaining their own read location within the deque.
     */
    public interface BinaryDequeReader {
        /**
         * Read and return the object at the current read position of this reader.
         * The entry will be removed once all active readers have read the entry.
         * @param ocf
         * @return BBContainer with the bytes read. Null if there is nothing left to read.
         * @throws IOException
         */
        public BBContainer poll(OutputContainerFactory ocf) throws IOException;

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
    }

    public static class TruncatorResponse {
        public enum Status {
            FULL_TRUNCATE,
            PARTIAL_TRUNCATE
        }
        public final Status status;
        public TruncatorResponse(Status status) {
            this.status = status;
        }

        public int getTruncatedBuffSize() throws IOException {
            throw new UnsupportedOperationException("Must implement this for partial object truncation");
        }

        public int writeTruncatedObject(ByteBuffer output) throws IOException {
            throw new UnsupportedOperationException("Must implement this for partial object truncation");
        }
    }

    /*
     * A binary deque truncator parses all the objects in a binary deque
     * from head to tail until it find the truncation point. At the truncation
     * point it can return a version of the last object passed to it that will be updated in place.
     * Everything after that object in the deque will be truncated and deleted.
     */
    public interface BinaryDequeTruncator {
        /*
         * Invoked by parseAndTruncate on every object in the deque from head to tail
         * until parse returns a non-null ByteBuffer. The returned ByteBuffer can be length 0 or it can contain
         * an object to replace the last object that was passed to the binary deque. If the length is 0
         * then the last object passed to parse will be truncated out of the deque. Part of the object
         * or a new object can be returned to replace it.
         */
        public TruncatorResponse parse(BBContainer bb);
    }
}
