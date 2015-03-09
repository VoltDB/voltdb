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

package org.voltdb.types;

import java.util.*;

/**
 *
 *
 */
public enum PartitionMethodType {
    INVALID     (0),
    HASH        (1),
    RANGE       (2),
    MAP         (3),
    REPLICATION (4);

    PartitionMethodType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, PartitionMethodType> idx_lookup = new HashMap<Integer, PartitionMethodType>();
    protected static final Map<String, PartitionMethodType> name_lookup = new HashMap<String, PartitionMethodType>();
    static {
        for (PartitionMethodType vt : EnumSet.allOf(PartitionMethodType.class)) {
            PartitionMethodType.idx_lookup.put(vt.ordinal(), vt);
            PartitionMethodType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, PartitionMethodType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, PartitionMethodType> getNameMap() {
        return name_lookup;
    }


    public static PartitionMethodType get(Integer idx) {
        PartitionMethodType ret = PartitionMethodType.idx_lookup.get(idx);
        return (ret == null ? PartitionMethodType.INVALID : ret);
    }

    public static PartitionMethodType get(String name) {
        PartitionMethodType ret = PartitionMethodType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? PartitionMethodType.INVALID : ret);
    }
}
