/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
 * Interface for serializing an Object at a later time and place. At serialization
 * time a memory pool will be provided.
 *
 */
public interface DeferredSerialization {
    /**
     * Serialize the Object contained in this DeferredSerialization
     * @param p A memory pool to provide memory for storing the serialized output
     * @return Serialized representation of the object stored in a ByteBuffer provided by the memory pool.
     * @throws IOException Thrown here because FastSerialzier throws IOException
     */
    BBContainer serialize(DBBPool p) throws IOException;

    /**
     * A deferred serialization might not be able to take place if a stream is closed
     * so a method for canceling the serialization and freeing associated resources must be provided.
     */
    void cancel();
}
