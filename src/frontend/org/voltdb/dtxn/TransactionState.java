/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.voltdb.ExecutionSite;
import org.voltdb.VoltTable;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.TransactionInfoBaseMessage;

/**
 * Controls the state of a transaction. Encapsulates from the SimpleDTXNConnection
 * all the logic about what needs to happen next in a transaction. The DTXNConn just
 * pumps events into the TransactionState and it takes the appropriate actions,
 * ultimately it will return true from finished().
 *
 */
public abstract class TransactionState extends OrderableTransaction  {
    public final int coordinatorSiteId;
    protected final boolean m_isReadOnly;
    protected int m_nextDepId = 1;
    protected final Mailbox m_mbox;
    protected final SiteTransactionConnection m_site;
    protected boolean m_done = false;
    protected long m_beginUndoToken;
    protected boolean m_needsRollback = false;

    /**
     * Set up the final member variables from the parameters. This will
     * be called exclusively by subclasses.
     *
     * @param mbox The mailbox for the site.
     * @param notice The information about the new transaction.
     */
    protected TransactionState(Mailbox mbox,
                               ExecutionSite site,
                               TransactionInfoBaseMessage notice)
    {
        super(notice.getTxnId(), notice.getInitiatorSiteId());
        m_mbox = mbox;
        m_site = site;
        coordinatorSiteId = notice.getCoordinatorSiteId();
        m_isReadOnly = notice.isReadOnly();
        m_beginUndoToken = ExecutionSite.kInvalidUndoToken;
    }

    final public boolean isDone() {
        return m_done;
    }

    public boolean isInProgress() {
        return false;
    }

    public boolean isReadOnly()
    {
        return m_isReadOnly;
    }

    /**
     * Indicate whether or not the transaction represented by this
     * TransactionState is single-partition.  Should be overridden to provide
     * sane results by subclasses.
     */
    public abstract boolean isSinglePartition();

    public abstract boolean isCoordinator();

    public abstract boolean isBlocked();

    public abstract boolean hasTransactionalWork();

    public abstract boolean doWork(boolean recovering);

    public boolean shouldResumeProcedure() {
        return false;
    }

    public void setBeginUndoToken(long undoToken)
    {
        m_beginUndoToken = undoToken;
    }

    public long getBeginUndoToken()
    {
        return m_beginUndoToken;
    }

    public boolean needsRollback()
    {
        return m_needsRollback;
    }

    public void createFragmentWork(int[] partitions, FragmentTaskMessage task) {
        String msg = "The current transaction context of type " + this.getClass().getName();
        msg += " doesn't support creating fragment tasks.";
        throw new UnsupportedOperationException(msg);
    }

    public void createAllParticipatingFragmentWork(FragmentTaskMessage task) {
        String msg = "The current transaction context of type " + this.getClass().getName();
        msg += " doesn't support creating fragment tasks.";
        throw new UnsupportedOperationException(msg);
    }

    public void createLocalFragmentWork(FragmentTaskMessage task, boolean nonTransactional) {
        String msg = "The current transaction context of type " + this.getClass().getName();
        msg += " doesn't support accepting fragment tasks.";
        throw new UnsupportedOperationException(msg);
    }

    public void setupProcedureResume(boolean isFinal, int[] dependencies) {
        String msg = "The current transaction context of type " + this.getClass().getName();
        msg += " doesn't support receiving dependencies.";
        throw new UnsupportedOperationException(msg);
    }

    public void processRemoteWorkResponse(FragmentResponseMessage response) {
        String msg = "The current transaction context of type ";
        msg += this.getClass().getName();
        msg += " doesn't support receiving fragment responses.";
        throw new UnsupportedOperationException(msg);
    }

    public void processCompleteTransaction(CompleteTransactionMessage complete)
    {
        String msg = "The current transaction context of type ";
        msg += this.getClass().getName();
        msg += " doesn't support receiving CompleteTransactionMessages.";
        throw new UnsupportedOperationException(msg);
    }

    public void
    processCompleteTransactionResponse(CompleteTransactionResponseMessage response)
    {
        String msg = "The current transaction context of type ";
        msg += this.getClass().getName();
        msg += " doesn't support receiving CompleteTransactionResponseMessages.";
        throw new UnsupportedOperationException(msg);
    }

    public Map<Integer, List<VoltTable>> getPreviousStackFrameDropDependendencies() {
        String msg = "The current transaction context of type ";
        msg += this.getClass().getName();
        msg += " doesn't support collecting stack frame drop dependencies.";
        throw new UnsupportedOperationException(msg);
    }

    public int getNextDependencyId() {
        return m_nextDepId++;
    }

    /**
     * Process the failure of failedSites.
     * @param globalCommitPoint greatest committed transaction id in the cluster
     * @param failedSites list of execution and initiator sites that have failed
     */
    public abstract void handleSiteFaults(HashSet<Integer> failedSites);
}
