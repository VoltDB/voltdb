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

package org.voltdb.client.VoltBulkLoader;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collections;

import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Impl;

/**
 * Global state shared by all VoltBulkLoader instances operating
 * under a single client. Supports old or new clients, but not
 * both at the same time.
 * <p>
 * A single instance of the BulkLoaderState corresponds
 * to a single lower-level client, and can be shared by
 * more than one bulk loader. Each instance of a
 * <code>VoltBulkLoader</code> is allocated using the
 * {@link #newBulkLoader} method.
 */
public class BulkLoaderState {

    // Client instance shared by all VoltBulkLoaders
    final ClientImpl m_clientImpl;
    final Client2Impl m_client2Impl;

    // Maps a table name to the list of VoltBulkLoaders that are operating on that table
    final Map<String, List<VoltBulkLoader>> m_TableNameToLoader =
            Collections.synchronizedMap(new TreeMap<String, List<VoltBulkLoader>>());

    /**
     * Construct a <code>BulkLoaderState</code> for use with
     * a {@link org.voltdb.client.Client} object.
     *
     * @param client the <code>Client</code>
     */
    public BulkLoaderState(Client client) {
        m_clientImpl = (ClientImpl) client;
        m_client2Impl = null;
    }

    /**
     * Construct a <code>BulkLoaderState</code> for use with
     * a {@link org.voltdb.client.Client2} object.
     *
     * @param client the <code>Client2</code>
     */
    public BulkLoaderState(Client2 client) {
        m_clientImpl = null;
        m_client2Impl = (Client2Impl) client;
    }

    /**
     * Convenience method for connecting to the VoltDB
     * cluster, if this <code>BulkLoaderState</code>
     * was constructed from an unconnected client.
     * <p>
     * This is a simple passthrough to either
     * {@link org.voltdb.client.Client#createAnyConnection(String,long,long)} or
     * {@link org.voltdb.client.Client2#connectSync(String,long,long,TimeUnit)},
     * as appropriate.
     *
     * @param servers list of servers, each as host and optional port
     * @param timeout overall timeout
     * @param delay time between retries
     * @param unit units in which <code>timeout</code> and <code>delay</code> are expressed
     * @throws IOException server communication error
     */
    public void connect(String servers, long timeout, long delay, TimeUnit unit)
        throws IOException {
        if (m_client2Impl != null) {
            m_client2Impl.connectSync(servers, timeout, delay, unit);
        }
        else {
            m_clientImpl.createAnyConnection(servers, unit.toMillis(timeout), unit.toMillis(delay));
        }
    }

    /**
     * Creates a new instance of a {@link VoltBulkLoader} that is bound
     * to this <code>BulkLoaderState</code>.
     * <p>
     * Multiple instances of a <code>VoltBulkLoader</code> created by a single
     * <code>BulkLoaderState</code> will share some resources, particularly if they
     * are inserting into the same table.
     *
     * @param tableName name of table to which bulk inserts are applied
     * @param maxBatchSize batch size to collect for the table before pushing a bulk insert
     * @param upsertMode set to true if caller wants upsert instead of insert
     * @param failureCallback callback for notification on any failures
     * @return instance of <code>VoltBulkLoader</code>
     * @throws Exception if tableName can't be found in the catalog.
     */
    public synchronized VoltBulkLoader newBulkLoader(String tableName, int maxBatchSize, boolean upsertMode,
                                                     BulkLoaderFailureCallBack failureCallback) throws Exception {
        return new VoltBulkLoader(this, tableName, maxBatchSize, upsertMode,
                                  failureCallback);
    }

    /**
     * Creates a new instance of a {@link VoltBulkLoader} that is bound
     * to this <code>BulkLoaderState</code>.
     * <p>
     * Multiple instances of a <code>VoltBulkLoader</code> created by a single
     * <code>BulkLoaderState</code> will share some resources, particularly if they
     * are inserting into the same table.
     *
     * @param tableName name of table to which bulk inserts are applied
     * @param maxBatchSize batch size to collect for the table before pushing a bulk insert
     * @param upsertMode set to true if caller wants upsert instead of insert
     * @param failureCallback callback for notification on any failures
     * @param successCallback callback for notification on successful load operations
     * @return instance of <code>VoltBulkLoader</code>
     * @throws Exception if tableName can't be found in the catalog.
     */
    public synchronized VoltBulkLoader newBulkLoader(String tableName, int maxBatchSize, boolean upsertMode,
                                                     BulkLoaderFailureCallBack failureCallback,
                                                     BulkLoaderSuccessCallback successCallback) throws Exception {
        return new VoltBulkLoader(this, tableName, maxBatchSize, upsertMode,
                                  failureCallback, successCallback);
    }
}
