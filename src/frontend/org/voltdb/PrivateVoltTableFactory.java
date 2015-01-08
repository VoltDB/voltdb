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

package org.voltdb;

import java.nio.ByteBuffer;

/**
 * Deliberately awkward access to package-private constructors of
 * VoltTable. End users shouldn't call the constructors, but VoltDB
 * needs to call them from many packages. By going through this proxy,
 * it makes it very hard for someone to unintentionally call them.
 *
 */
public abstract class PrivateVoltTableFactory {

    public static VoltTable createUninitializedVoltTable() {
        return new VoltTable();
    }

    public static VoltTable createVoltTableFromBuffer(ByteBuffer backing, boolean readOnly) {
        return new VoltTable(backing, readOnly);
    }

    public static VoltTable createVoltTableFromSharedBuffer(ByteBuffer shared) {
        VoltTable vt = new VoltTable();
        vt.initFromBuffer(shared);
        return vt;
    }

    /**
     * End users should not call this method.
     * Obtain a reference to the table's underlying buffer.
     * The returned reference's position and mark are independent of
     * the table's buffer position and mark. The returned buffer has
     * no mark and is at position 0.
     */
    public static ByteBuffer getTableDataReference(VoltTable vt) {
        ByteBuffer buf = vt.m_buffer.duplicate();
        buf.rewind();
        return buf;
    }

    public static byte[] getSchemaBytes(VoltTable vt) {
        if (vt.getRowCount() > 0) {
            throw new RuntimeException("getSchemaBytes() Only works if the table is empty");
        }
        ByteBuffer dup = vt.m_buffer.duplicate();
        dup.limit(dup.limit() - 4);
        dup.position(0);
        byte retvalBytes[] = new byte[dup.remaining()];
        dup.get(retvalBytes);
        return retvalBytes;
    }

    /**
     * End users should not call this method.
     * @return Underlying buffer size
     */
    public static int getUnderlyingBufferSize(VoltTable vt) {
        return vt.m_buffer.position();
    }
}
