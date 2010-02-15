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
import org.voltdb.messages.FragmentTask;

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

    public int getSiteId() { return m_siteId; }

    /**
     * Lets <code>TransactionInitiator</code> create a new
     * <code>SiteConnection<code> object.
     *
     * @param siteId The identifier for the site creating this connection.
     */
    protected SiteConnection(int siteId) {
        this.m_siteId = siteId;
    }

    /**
     * Instruct the DTXN to send a FragmentTask to all partitions participating
     * in the transaction.
     *
     * @param task Fragment work to be sent to all participating partitions.
     */
    public abstract void createAllParticipatingWork(FragmentTask task);

    /**
     * Instruct the DTXN to have the local site/partition do work described
     * by a FragmentTask message.
     *
     * @param task The FragmentTask describing the work to do.
     */
    public abstract void createLocalWork(FragmentTask task, boolean nonTransactional);

    /**
     * Instruct the DTXN to send a FragmentTask to a specific list of
     * partitions and ensure they do the work requested of them.
     *
     * @param partitions A list of partition ids to send the work to.
     * @param task The FragmentTask describing the work to do.
     */
    public abstract boolean createWork(int[] partitions, FragmentTask task);

    /**
     * Instruct the DTXN that when the following dependencies have been met,
     * the procedure that was interrupted to do fragment work may resume. This
     * is done via a stack frame drop (the "return" statement) from the core
     * loop in the <code>ExecutionSite</code>
     *
     * @param dependencies The list of ids of dependencies that must be
     * satisfied before the procedure can resume.
     */
    public abstract void setupProcedureResume(boolean isFinal, int... dependencies);

    public abstract void shutdown() throws InterruptedException;

    public abstract Map<Integer,List<VoltTable>> recursableRun(boolean shutdownAllowed);

    /**
     * Get an unused dependency id for this transaction.
     * @return An unused dependency id
     */
    public abstract int getNextDependencyId();

    /**
     * Get an array of unused dependency ids of a specific length
     * @param len Length of the array desired
     * @return An array of unused dependency ids.
     */
    public int[] getNextDepenendcyArray(int len) {
        assert(len > 0);
        int[] retval = new int[len];
        for (int i = 0; i < len; i++)
            retval[i] = getNextDependencyId();
        return retval;
    }
}
