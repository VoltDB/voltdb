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
 * Specifies how to lookup on Table Index.
 */
public enum IndexLookupType {
    INVALID (0, "?"),
    EQ      (1, "="),
    GT      (2, ">"),
    GTE     (3, ">="),
    LT      (4, "<"),
    LTE     (5, "<="),
    GEO_CONTAINS (6, "contains");

    private final String m_symbol;

    IndexLookupType(int val, String symbol) {
        m_symbol = symbol;
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    public String getSymbol() {
        return m_symbol;
    }

    protected static final Map<Integer, IndexLookupType> idx_lookup = new HashMap<Integer, IndexLookupType>();
    protected static final Map<String, IndexLookupType> name_lookup = new HashMap<String, IndexLookupType>();
    static {
        for (IndexLookupType vt : EnumSet.allOf(IndexLookupType.class)) {
            IndexLookupType.idx_lookup.put(vt.ordinal(), vt);
            IndexLookupType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, IndexLookupType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, IndexLookupType> getNameMap() {
        return name_lookup;
    }

    public static IndexLookupType get(Integer idx) {
        IndexLookupType ret = IndexLookupType.idx_lookup.get(idx);
        return (ret == null ? IndexLookupType.INVALID : ret);
    }

    public static IndexLookupType get(String name) {
        IndexLookupType ret = IndexLookupType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? IndexLookupType.INVALID : ret);
    }
}
