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

import org.voltcore.utils.DeferredSerialization;

/**
 * Simple base class for handling the writing of any {@link DeferredSerialization} object
 *
 * @param <T> Type of {@link DeferredSerialization} which this serializer operates on
 */
public abstract class BinaryDequeDeferredSerializer<T extends DeferredSerialization>
        implements BinaryDequeSerializer<T> {
    @Override
    public int getMaxSize(T object) throws IOException {
        return object.getSerializedSize();
    };

    @Override
    public void write(T object, ByteBuffer buffer) throws IOException {
        object.serialize(buffer);
    }
}
