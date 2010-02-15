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

package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public enum QueryType {
    INVALID     (0), // used for parsing
    NOOP        (1),
    SELECT      (2),
    INSERT      (3),
    UPDATE      (4),
    DELETE      (5);

    QueryType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, QueryType> idx_lookup = new HashMap<Integer, QueryType>();
    protected static final Map<String, QueryType> name_lookup = new HashMap<String, QueryType>();
    static {
        for (QueryType vt : EnumSet.allOf(QueryType.class)) {
            QueryType.idx_lookup.put(vt.ordinal(), vt);
            QueryType.name_lookup.put(vt.name().intern(), vt);
        }
    }

    public static Map<Integer, QueryType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, QueryType> getNameMap() {
        return name_lookup;
    }

    public static QueryType get(Integer idx) {
        QueryType ret = QueryType.idx_lookup.get(idx);
        return (ret == null ? QueryType.INVALID : ret);
    }

    public static QueryType get(String name) {
        QueryType ret = QueryType.name_lookup.get(name.intern());
        return (ret == null ? QueryType.INVALID : ret);
    }

}
