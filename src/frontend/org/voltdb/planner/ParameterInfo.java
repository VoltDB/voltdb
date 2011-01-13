/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.planner;

import java.io.IOException;
import org.voltdb.VoltType;
import org.voltdb.messaging.*;

/**
 *
 *
 */
public class ParameterInfo implements FastSerializable {
    public int index;
    public VoltType type;

    @Override
    public String toString() {
        return "P" + String.valueOf(index) + ":" + type.name();
    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        index = in.readInt();
        type = VoltType.get(in.readByte());
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        out.writeInt(index);
        out.writeByte(type.getValue());
    }
}
