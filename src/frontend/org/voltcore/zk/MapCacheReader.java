/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltcore.zk;

import java.util.concurrent.ExecutionException;
import org.json_voltpatches.JSONObject;
import com.google.common.collect.ImmutableMap;

/**
 * A read-only interface to MapCache for consumers that do not
 * perform writes.
 */
public interface MapCacheReader {
    public void start(boolean block) throws InterruptedException, ExecutionException;
    public void shutdown() throws InterruptedException;
    public ImmutableMap<String, JSONObject> pointInTimeCache();
    public JSONObject get(String key);
}

