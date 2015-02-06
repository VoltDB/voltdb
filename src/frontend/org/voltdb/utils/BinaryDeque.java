/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
    public void offer(BBContainer object) throws IOException;

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
     * Remove and return the object at the head of the queue
     * @param ocf
     * @return
     * @throws IOException
     */
    public BBContainer poll(OutputContainerFactory ocf) throws IOException;

    /**
     * Persist all objects in the queue to the backing store
     * @throws IOException
     */
    public void sync() throws IOException;

    /**
     * Release all resources (open files) held by the back store of the queue. Continuing to use the deque
     * will result in an exception
     * @throws IOException
     */
    public void close() throws IOException;

    public boolean isEmpty() throws IOException;

    public long sizeInBytes();
    public int getNumObjects();

    public void closeAndDelete() throws IOException;

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
        public ByteBuffer parse(ByteBuffer b);
    }

    public void parseAndTruncate(BinaryDequeTruncator truncator) throws IOException;
}
