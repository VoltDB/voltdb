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

package org.voltdb.client.VoltBulkLoader;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.voltdb.client.ClientImpl;

/**
 * Global objects shared by all VoltBulkLoader instances operating under a single Client
 */
public class BulkLoaderState {
    // Client instance shared by all VoltBulkLoaders
    final ClientImpl m_clientImpl;
    // Maps a table name to the list of VoltBulkLoaders that are operating on that table
    final Map<String, List<VoltBulkLoader>> m_TableNameToLoader =
            Collections.synchronizedMap(new TreeMap<String, List<VoltBulkLoader>>());

    public BulkLoaderState(ClientImpl clientImpl) {
        m_clientImpl = clientImpl;
    }
}
