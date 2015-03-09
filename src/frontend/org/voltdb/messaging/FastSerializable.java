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

package org.voltdb.messaging;

import java.io.IOException;

/**
 * <p>A class that implements <code>FastSerializable</code can be flattened
 * very much like the <code>Externalizable</code> interface. The reason
 * this system is faster is that the code requesting the deserialization
 * needs to the know the exact class of the object being deserialized. This
 * saves writing the classname to the stream and also saves searching for
 * the class by name during deserialization.</p>
 *
 * <p>Additionally, specialized input and output streams are used to
 * specifically serialize to a byte array in memory.</p>
 *
 */
public interface FastSerializable {

    /**
     * Read this object from the byte stream.
     *
     * @param in <code>FastDeserializer</code> instance to read data
     * from.
     * @throws IOException Rethrows any IOExceptions thrown.
     */
    public void readExternal(FastDeserializer in) throws IOException;

    /**
     * Write this object to the byte stream.
     *
     * @param out <code>FastSerializer</code> instance to write data
     * to.
     * @throws IOException Rethrows any IOExceptions thrown.
     */
    public void writeExternal(FastSerializer out) throws IOException;
}
