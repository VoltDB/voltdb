/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.compiler;

import java.util.LinkedHashMap;

public class AdHocStatementCache extends LinkedHashMap<String, AdHocPlannedStatement>{

    /**
     * define a LinkedHashMap based LRU cache bounds by both entry number and entry value on-heap size
     * without changing Map.Entry, only works for value of type AdHocPlannedStatement
     * only extend put, remove,clear and removeEldestEntry methods to account weight
     */

    private static final long serialVersionUID = 2988383448026641836L;
    final int maxEntries;
    final long maxMemory; // in bytes
    long currentMemory;   // in bytes

    public AdHocStatementCache() {
        // default max entry of 1000
        // default max value size of 32MB
        this(1000, 32 * 1024 * 1024);
    }

    public AdHocStatementCache(final int maxEntries) {
        this(maxEntries, 32 * 1024 * 1024);
    }

    public AdHocStatementCache(final int maxEntries, final long maxMemory) {
        // set accessOrder to true for LRU
        super(maxEntries * 2, .75f, true);
        this.maxEntries = maxEntries;
        this.maxMemory = maxMemory;
        this.currentMemory = 0;
    }

    @Override
    public AdHocPlannedStatement put(String key, AdHocPlannedStatement value) {
        this.currentMemory += value.getSerializedSize();
        return super.put(key,value);
    }

    @Override
    public AdHocPlannedStatement remove(Object key) {
        AdHocPlannedStatement value = super.remove(key);
        if (value != null) {
            this.currentMemory -= value.getSerializedSize();
        }
        return value;
    }

    @Override
    public void clear() {
        super.clear();
        this.currentMemory = 0;
    }
}