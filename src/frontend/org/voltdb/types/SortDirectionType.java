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
 */
public enum SortDirectionType {
    INVALID      (0),
    ASC          (1),
    DESC            (2);

    SortDirectionType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, SortDirectionType> idx_lookup = new HashMap<Integer, SortDirectionType>();
    protected static final Map<String, SortDirectionType> name_lookup = new HashMap<String, SortDirectionType>();
    static {
        for (SortDirectionType vt : EnumSet.allOf(SortDirectionType.class)) {
            SortDirectionType.idx_lookup.put(vt.ordinal(), vt);
            SortDirectionType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, SortDirectionType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, SortDirectionType> getNameMap() {
        return name_lookup;
    }

    public static SortDirectionType get(Integer idx) {
        assert(idx >= 0);
        SortDirectionType ret = SortDirectionType.idx_lookup.get(idx);
        return (ret == null ? SortDirectionType.INVALID : ret);
    }

    public static SortDirectionType get(String name) {
        SortDirectionType ret = SortDirectionType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? SortDirectionType.INVALID : ret);
    }
}
