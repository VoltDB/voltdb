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
}
