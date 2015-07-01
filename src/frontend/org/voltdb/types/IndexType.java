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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public enum IndexType {
    INVALID         (0),
    BALANCED_TREE   (1),
    HASH_TABLE      (2),
    BTREE           (3);

    IndexType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, IndexType> idx_lookup = new HashMap<Integer, IndexType>();
    protected static final Map<String, IndexType> name_lookup = new HashMap<String, IndexType>();
    static {
        for (IndexType vt : EnumSet.allOf(IndexType.class)) {
            IndexType.idx_lookup.put(vt.ordinal(), vt);
            IndexType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, IndexType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, IndexType> getNameMap() {
        return name_lookup;
    }

    public static IndexType get(Integer idx) {
        IndexType ret = IndexType.idx_lookup.get(idx);
        return (ret == null ? IndexType.INVALID : ret);
    }

    public static IndexType get(String name) {
        IndexType ret = IndexType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? IndexType.INVALID : ret);
    }

    /**
     * As VoltDB currently doesn't add much functionality to HSQL's DDL
     * parsing support, it checks for certain strings in index names to
     * determine what kind of index it is. This is a giant hack, but
     * works until the team can put more resources into DDL parsing.
     * TODO: This should probably be an enum attribute.
     */
    public static String getSQLSuffix(int type)
    {
        IndexType it = get(type);
        return it.getSQLSuffix();
    }

    public String getSQLSuffix() {
        switch (this) {
        case BALANCED_TREE:
            return "_TREE";
        case BTREE:
        case HASH_TABLE:
            return "";
        }
        return null;
    }

    /**
     * Distinguish whether the IndexType with the given value supports ordered indexing.
     * TODO: This should probably be an enum attribute.
     */
    public static boolean isScannable(int type)
    {
        IndexType it = get(type);
        assert(it != INVALID);
        return it.isScannable();
    }

    private boolean isScannable() {
        switch (this) {
        case BALANCED_TREE:
        case BTREE:
            return true;
        case HASH_TABLE:
        case INVALID:
            return false;
        }
        return false;
    }
}
