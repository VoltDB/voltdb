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
public enum ResultType {
    INVALID     (0), // For Parsing...
    NOOP        (1),
    OK          (2),
    ABORTED     (3),
    ERROR       (4),
    UNKNOWN     (5);

    ResultType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, ResultType> idx_lookup = new HashMap<Integer, ResultType>();
    protected static final Map<String, ResultType> name_lookup = new HashMap<String, ResultType>();
    static {
        for (ResultType vt : EnumSet.allOf(ResultType.class)) {
            ResultType.idx_lookup.put(vt.ordinal(), vt);
            ResultType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, ResultType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, ResultType> getNameMap() {
        return name_lookup;
    }

    public static ResultType get(Integer idx) {
        ResultType ret = ResultType.idx_lookup.get(idx);
        return (ret == null ? ResultType.INVALID : ret);
    }

    public static ResultType get(String name) {
        ResultType ret = ResultType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? ResultType.INVALID : ret);
    }
}
