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

import java.util.*;

/**
 * Comparison Expression Quantifier EXPR = ANY/ALL (EXPR)
 */
public enum QuantifierType {
    NONE      (0),
    ANY       (1),
    ALL       (2);

    QuantifierType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, QuantifierType> idx_lookup = new HashMap<Integer, QuantifierType>();
    protected static final Map<String, QuantifierType> name_lookup = new HashMap<String, QuantifierType>();
    static {
        for (QuantifierType vt : EnumSet.allOf(QuantifierType.class)) {
            QuantifierType.idx_lookup.put(vt.ordinal(), vt);
            QuantifierType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, QuantifierType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, QuantifierType> getNameMap() {
        return name_lookup;
    }

    public static QuantifierType get(Integer idx) {
        assert(idx >= 0);
        QuantifierType ret = QuantifierType.idx_lookup.get(idx);
        return (ret == null ? QuantifierType.NONE : ret);
    }

    public static QuantifierType get(String name) {
        QuantifierType ret = QuantifierType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? QuantifierType.NONE : ret);
    }
}
