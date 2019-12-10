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

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;

/**
 * Specialized deque interface for storing binary objects. Objects can be provided as a buffer chain and will be
 * returned as a single buffer. Technically not a deque because removal at the end is not supported.
 *
 * @param <M> Type of extra header metadata which can be associated with entries
 */
public interface BinaryDeque<M> {
    /**
     * The different types of retention policies supported by BinaryDeque.
     */
    public static enum RetentionPolicyType {
        TIME_MS,
        MAX_BYTES;
    }

    /*
     * Allocator for storage coming out of the BinaryDeque. Only
     * used if copying is necessary, otherwise a slice is returned
     */
    public static interface OutputContainerFactory {
        public BBContainer getContainer(int minimumSize);
    }

    /**
     * Update the extraHeader associated with this instance. This updated metadata will be associated with all entries
     * added after this point but does not affect entries previously written.
     *
     * @param extraHeader new extra header metadata.
     * @throws IOException If an error occurs while updating the extraHeader
     */
    void updateExtraHeader(M extraHeader) throws IOException;

    /**
     * Store a buffer chain as a single object in the deque.
     * If there is an exception attempting to write the buffers then all the buffers will be discarded
     * @param BBContainer with the bytes to store
     * @return the number of bytes written. If compression is enabled, the number of compressed bytes
     *         written is returned, which will be different from the number of bytes passed in.
     * @throws IOException if the object is larger then the implementation defined max,
     *         64 megabytes in the case of PersistentBinaryDeque.
     */
    int offer(BBContainer object) throws IOException;

    int offer(DeferredSerialization ds) throws IOException;

    /**
     * A push creates a new file each time to be "the head" so it is more efficient to pass in all the objects you want
     * to push at once so that they can be packed into as few files as possible. IOException may be thrown if the object
     * is larger then the implementation defined max. 64 megabytes in the case of PersistentBinaryDeque. If there is an
     * exception attempting to write the buffers then all the buffers will be discarded
     * <p>
     * The current extraHeader metadata if any will be associated with these entries.
     *
     * @param objects Array of buffers representing the objects to be pushed to the head of the queue
     * @throws IOException
     */
    public void push(BBContainer objects[]) throws IOException;

    /**
     * A push creates a new file each time to be "the head" so it is more efficient to pass in all the objects you want
     * to push at once so that they can be packed into as few files as possible. IOException may be thrown if the object
     * is larger then the implementation defined max. 64 megabytes in the case of PersistentBinaryDeque. If there is an
     * exception attempting to write the buffers then all the buffers will be discarded
     *
     * @param objects     Array of buffers representing the objects to be pushed to the head of the queue
     * @param extraHeader header metadata to associate with entries.
     * @throws java.io.IOException
     */
    public void push(BBContainer objects[], M extraHeader) throws IOException;

    /**
     * Start a BinaryDequeReader for reading, positioned at the start of the deque.
     * @param cursorId a String identifying the cursor. If a cursor is already open for this id,
     * the existing cursor will be returned.
     * @return a BinaryDequeReader for this cursorId
     * @throws IOException on any errors trying to read the PBD files
     */
    public BinaryDequeReader<M> openForRead(String cursorId) throws IOException;

    /**
     * Close a BinaryDequeReader reader, also close the SegmentReader for the segment if it is reading one.
     * @param cursorId a String identifying the cursor.
     * @throws IOException on any errors trying to close the SegmentReader if it is the last one for the segment
     */
    default public void closeCursor(String cursorId) {
        closeCursor(cursorId, false);
    }

    /**
     * Close a BinaryDequeReader reader, optionally purging the segments on the last reader closing.
     * <p>
     * Purging segments on last reader closing is a DR-specific requirement, see implementation.
     *
     * @param cursorId
     * @param purgeOnLastCursor true if segment purge is requested on last reader closing.
     */
    public void closeCursor(String cursorId, boolean purgeOnLastCursor);

    public int countCursors();

    /**
     * Persist all objects in the queue to the backing store
     * @throws IOException
     */
    public void sync() throws IOException;

    public void parseAndTruncate(BinaryDequeTruncator truncator) throws IOException;

    public void scanEntries(BinaryDequeScanner scanner) throws IOException;

    public boolean deletePBDSegment(BinaryDequeValidator<M> checker) throws IOException;

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
     * Sets the retention policy to use on the PBD data.
     * As of the current implementation, only one retention policy may be set on a PBD.
     *
     * @param policyType the retention policy type
     * @param params parameters specific to the retention policy type. For example, for time based
     *        retention policy, the retain time in the correct unit may be the parameter.
     */
    public void setRetentionPolicy(RetentionPolicyType policyType, Object... params);

    /**
     * Indicates that the BinaryDeque can now start retention policy enforcement.
     * This will be typically called after the BinaryDeque is setup and required initializations
     * are completed.
     */
    public void startRetentionPolicyEnforcement();

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

        public int writeTruncatedObject(ByteBuffer output, int entryId) throws IOException {
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

    public interface BinaryDequeScanner {
        public void scan(BBContainer bb);
    }

    public interface BinaryDequeValidator<M> {
        public boolean isStale(M extraHeader);
    }
}
