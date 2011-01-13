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
package org.voltdb.utils;

import java.io.IOException;

import org.voltdb.utils.DBBPool.BBContainer;

/**
 * Specialized deque interface for storing binary objects. Objects can be provided as a buffer chain
 * and will be returned as a single buffer. Technically not a deque because removal at
 * the end is not supported.
 *
 */
public interface BinaryDeque {
    /**
     * Store a buffer chain as a single object in the deque. IOException may be thrown if the object
     * is larger then the implementation defined max. 64 megabytes in the case of PersistentBinaryDeque.
     * If there is an exception attempting to write the buffers then all the buffers will be discarded
     * @param objects
     * @throws IOException
     */
    public void offer(BBContainer object[]) throws IOException;

    /**
     * A push creates a new file each time to be "the head" so it is more efficient to pass
     * in all the objects you want to push at once so that they can be packed into
     * as few files as possible. IOException may be thrown if the object
     * is larger then the implementation defined max. 64 megabytes in the case of PersistentBinaryDeque.
     * If there is an exception attempting to write the buffers then all the buffers will be discarded
     * @param objects Array of buffer chains representing the objects to be pushed to the head of the queue
     */
    public void push(BBContainer objects[][]) throws IOException;

    /**
     * Remove and return the object at the head of the queue
     * @return
     * @throws IOException
     */
    public BBContainer poll() throws IOException;

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
}
