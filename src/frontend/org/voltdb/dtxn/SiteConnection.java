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

package org.voltdb.dtxn;

import java.util.List;
import java.util.Map;
import org.voltdb.VoltTable;

/**
 * <p>Created by a worker site to interact with the Transaction System.
 * Decides what the next job is for the site and lets the site create
 * new jobs.</p>
 *
 * <p><code>SiteConnection</code> is an abstract base class which is an
 * interface to the part of the pluggable transaction system. Different
 * transaction systems with provide their own subclasses.</p>
 */
public abstract class SiteConnection {

    /**
     * A globally unique identifier for the site that is using this
     * connection. This id matches up with the a site in the catalog.
     */
    protected final int m_siteId;
    public final int getSiteId() { return m_siteId; }

    /**
     * Lets <code>TransactionInitiator</code> create a new
     * <code>SiteConnection<code> object.
     *
     * @param siteId The identifier for the site creating this connection.
     */
    protected SiteConnection(int siteId) {
        this.m_siteId = siteId;
    }

    public abstract void shutdown() throws InterruptedException;

    public abstract Map<Integer,List<VoltTable>>
    recursableRun(TransactionState txnState);
}
