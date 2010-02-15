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

package org.voltdb.planner;

/**
 * This enum describes the possible ways to use an Index in an
 * access path.
 *
 */
public enum IndexUseType {

    /**
     * The index key matches the predicate completely.
     * Unique indexes return one or no tuples.
     * Non-unique indexes may return many tuples.
     *
     * All index types should work.
     */
    COVERING_UNIQUE_EQUALITY,

    /**
     * Given a start and stop searchkey,
     * Scan from the smallest key > start
     * to the largest key < end.
     *
     * This only works for trees.
     */
    INDEX_SCAN,

    /**
     * If the range of keys to scan is fixed and small,
     * try equality searching on all possible keys.
     *
     * This works for all indexes.
     * Not yet supported (needs union executor).
     */
    REPEATED_SCAN,

}
