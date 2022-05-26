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

package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public enum JoinType {
    INVALID     (0), // For Parsing...
    INNER       (1),
    LEFT        (2),
    FULL        (3),
    RIGHT       (4);

    JoinType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, JoinType> idx_lookup = new HashMap<Integer, JoinType>();
    protected static final Map<String, JoinType> name_lookup = new HashMap<String, JoinType>();
    static {
        for (JoinType vt : EnumSet.allOf(JoinType.class)) {
            JoinType.idx_lookup.put(vt.ordinal(), vt);
            JoinType.name_lookup.put(vt.name().toLowerCase(), vt);
        }
    }

    public static Map<Integer, JoinType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, JoinType> getNameMap() {
        return name_lookup;
    }

    public static JoinType get(Integer idx) {
        JoinType ret = JoinType.idx_lookup.get(idx);
        return (ret == null ? JoinType.INVALID : ret);
    }

    public static JoinType get(String name) {
        JoinType ret = JoinType.name_lookup.get(name.toLowerCase());
        return (ret == null ? JoinType.INVALID : ret);
    }

}
