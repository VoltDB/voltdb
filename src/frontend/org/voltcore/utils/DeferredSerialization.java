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

package org.voltcore.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for serializing an Object at a later time and place. At serialization
 * time a memory pool will be provided.
 *
 */
public interface DeferredSerialization {
    /**
     * Value to return for an empty message
     */
    public static final int EMPTY_MESSAGE_LENGTH = -1;

    /**
     * Serialize the Object contained in this DeferredSerialization
     * @return Serialized representation of the object stored
     * @throws IOException Thrown here because FastSerialzier throws IOException
     */
    void serialize(ByteBuffer buf) throws IOException;

    /**
     * A deferred serialization might not be able to take place if a stream is closed
     * so a method for canceling the serialization and freeing associated resources must be provided.
     */
    void cancel();

    int getSerializedSize() throws IOException;
}
