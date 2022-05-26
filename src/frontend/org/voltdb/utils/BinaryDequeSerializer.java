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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for reading and writing objects of a specific type.
 *
 * @param <T> Type of object which this serializer reads and writes
 */
public interface BinaryDequeSerializer<T> {
    /**
     * @param object to be written
     * @return The maximum size of the buffer required by {@link #write(Object, ByteBuffer)} to write {@code object}
     */
    int getMaxSize(T object) throws IOException;

    /**
     * Write {@code object} to {@code buffer}
     *
     * @param object to write
     * @param buffer buffer to hold serialized data
     */
    void write(T object, ByteBuffer buffer) throws IOException;

    /**
     * @param buffer from which to read object
     * @return instance of {@code T} read from {@code buffer}
     */
    T read(ByteBuffer buffer) throws IOException;
}
