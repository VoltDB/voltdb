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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class MpTransactionState extends TransactionState
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /**
     *  This is thrown by the TransactionState instance when something
     *  goes wrong mid-fragment, and execution needs to back all the way
     *  out to the stored procedure call.
     */
    // IZZY Consolidate me with MultiPartitionParticipantTransactionState
    // and perhaps make me more descriptive
    public static class FragmentFailureException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    final Iv2InitiateTaskMessage m_task;

    LinkedBlockingDeque<FragmentResponseMessage> m_newDeps =
        new LinkedBlockingDeque<FragmentResponseMessage>();
    Map<Integer, Set<Long>> m_remoteDeps;
    Map<Integer, List<VoltTable>> m_remoteDepTables =
        new HashMap<Integer, List<VoltTable>>();
    Map<Integer, Set<Long>> m_localDeps;
    Set<Integer> m_finalDeps;
    List<Long> m_useHSIds;
    long m_buddyHSId;
    FragmentTaskMessage m_remoteWork = null;
    FragmentTaskMessage m_localWork = null;

    MpTransactionState(Mailbox mailbox, long txnId,
                       TransactionInfoBaseMessage notice,
                       List<Long> useHSIds, long buddyHSId)
    {
        super(txnId, mailbox, notice);
        m_task = (Iv2InitiateTaskMessage)notice;
        m_useHSIds = useHSIds;
        m_buddyHSId = buddyHSId;
    }

    @Override
    public boolean isSinglePartition()
    {
        return false;
    }

    @Override
    public boolean isCoordinator()
    {
        return true;
    }

    @Override
    public boolean isBlocked()
    {
        // Not clear this method is useful in the new world?
        return false;
    }

    @Override
    public boolean hasTransactionalWork()
    {
        return false;
    }

    @Override
    public boolean doWork(boolean recovering)
    {
        return false;
    }

    @Override
    public StoredProcedureInvocation getInvocation()
    {
        return null;
    }

    @Override
    public void handleSiteFaults(HashSet<Long> failedSites)
    {
    }

    // Overrides needed by MpProcedureRunner
    @Override
    public void setupProcedureResume(boolean isFinal, int[] dependencies)
    {
        // Create some record of expected dependencies for tracking
        m_finalDeps = new HashSet<Integer>();
        for (int dep : dependencies) {
            m_finalDeps.add(dep);
        }
    }

    @Override
    public void createLocalFragmentWork(FragmentTaskMessage task, boolean nonTransactional)
    {
        m_localWork = task;
        // Create some record of expected dependencies for tracking
        m_localDeps = createTrackedDependenciesFromTask(task, new ArrayList<Long>(0));
    }

    @Override
    public void createAllParticipatingFragmentWork(FragmentTaskMessage task)
    {
        // Don't generate remote work or dependency tracking or anything if
        // there are no fragments to be done in this message
        // At some point maybe ProcedureRunner.slowPath() can get smarter
        if (task.getFragmentCount() > 0) {
            m_remoteWork = task;
            // Distribute fragments to remote destinations.
            long[] non_local_hsids = new long[m_useHSIds.size()];
            for (int i = 0; i < m_useHSIds.size(); i++) {
                non_local_hsids[i] = m_useHSIds.get(i);
            }
            try {
                // send to all non-local sites
                // IZZY: This needs to go through mailbox.deliver()
                // so that fragments could get replicated for k>0
                if (non_local_hsids.length > 0) {
                    m_mbox.send(non_local_hsids, m_remoteWork);
                }
            }
            catch (MessagingException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            m_remoteWork = null;
        }
    }

    private Map<Integer, Set<Long>>
    createTrackedDependenciesFromTask(FragmentTaskMessage task,
                                      List<Long> expectedHSIds)
    {
        Map<Integer, Set<Long>> depMap = new HashMap<Integer, Set<Long>>();
        for (int i = 0; i < task.getFragmentCount(); i++) {
            int dep = task.getOutputDepId(i);
            Set<Long> scoreboard = new HashSet<Long>();
            depMap.put(dep, scoreboard);
            for (long hsid : expectedHSIds) {
                scoreboard.add(hsid);
            }
        }
        return depMap;
    }

    @Override
    public Map<Integer, List<VoltTable>> recursableRun(SiteProcedureConnection siteConnection)
    {
        // Do distributed fragments, if any
        if (m_remoteWork != null) {
            // Create some record of expected dependencies for tracking
            m_remoteDeps = createTrackedDependenciesFromTask(m_remoteWork,
                                                             m_useHSIds);
            // if there are remote deps, block on them
            // FragmentResponses indicating failure will throw an exception
            // which will propagate out of handleReceivedFragResponse and
            // cause ProcedureRunner to do the right thing and cause rollback.
            while (!checkDoneReceivingFragResponses()) {
                try {
                    FragmentResponseMessage msg = m_newDeps.take();
                    handleReceivedFragResponse(msg);
                } catch (InterruptedException e) {
                    // this is a valid shutdown path.
                    hostLog.warn("Interrupted coordinating a multi-partition transaction. " +
                            "Terminating the transaction. " + e);
                    terminateTransaction();
                }
            }
        }

        BorrowTaskMessage borrowmsg = new BorrowTaskMessage(m_localWork);
        borrowmsg.addInputDepMap(m_remoteDepTables);
        try {
            m_mbox.send(m_buddyHSId, borrowmsg);
        }
        catch (MessagingException me) {
            throw new RuntimeException(me);
        }

        FragmentResponseMessage msg = null;
        try {
            msg = m_newDeps.take();
        }
        catch (InterruptedException e) {
            // this is a valid shutdown path.
            hostLog.warn("Interrupted coordinating a multi-partition transaction. " +
                         "Terminating the transaction. " + e);
            terminateTransaction();
        }

        // If the final fragment caused an error we'll need to trip rollback
        // This is duped from handleReceivedFragResponse, consolidate later
        if (msg.getStatusCode() != FragmentResponseMessage.SUCCESS) {
            m_needsRollback = true;
            if (msg.getException() != null) {
                throw msg.getException();
            } else {
                throw new FragmentFailureException();
            }
        }
        // Build results from the FragmentResponseMessage
        // This is similar to dependency tracking...maybe some
        // sane way to merge it
        Map<Integer, List<VoltTable>> results =
            new HashMap<Integer, List<VoltTable>>();
        for (int i = 0; i < msg.getTableCount(); i++) {
            int this_depId = msg.getTableDependencyIdAtIndex(i);
            VoltTable this_dep = msg.getTableAtIndex(i);
            List<VoltTable> tables = results.get(this_depId);
            if (tables == null) {
                tables = new ArrayList<VoltTable>();
                results.put(this_depId, tables);
            }
            tables.add(this_dep);
        }

        // Need some sanity check that we got all of the expected output dependencies?
        return results;
    }

    private void trackDependency(long hsid, int depId, VoltTable table)
    {
        // Remove the distributed fragment for this site from remoteDeps
        // for the dependency Id depId.
        Set<Long> localRemotes = m_remoteDeps.get(depId);
        Object needed = localRemotes.remove(hsid);
        if (needed != null) {
            // add table to storage
            List<VoltTable> tables = m_remoteDepTables.get(depId);
            if (tables == null) {
                tables = new ArrayList<VoltTable>();
                m_remoteDepTables.put(depId, tables);
            }
            tables.add(table);
        }
        else {
            // TODO: need an error path here.
            System.out.println("No remote dep for local site: " + hsid);
        }
    }

    private void handleReceivedFragResponse(FragmentResponseMessage msg)
    {
        if (msg.getStatusCode() != FragmentResponseMessage.SUCCESS) {
            m_needsRollback = true;
            if (msg.getException() != null) {
                throw msg.getException();
            } else {
                throw new FragmentFailureException();
            }
        }
        for (int i = 0; i < msg.getTableCount(); i++)
        {
            int this_depId = msg.getTableDependencyIdAtIndex(i);
            VoltTable this_dep = msg.getTableAtIndex(i);
            long src_hsid = msg.getExecutorSiteId();
            trackDependency(src_hsid, this_depId, this_dep);
        }
    }

    private boolean checkDoneReceivingFragResponses()
    {
        boolean done = true;
        for (Set<Long> depid : m_remoteDeps.values()) {
            if (depid.size() != 0) {
                done = false;
            }
        }
        return done;
    }

    // Runs from Mailbox's network thread
    public void offerReceivedFragmentResponse(FragmentResponseMessage message)
    {
        // push into threadsafe queue
        m_newDeps.offer(message);
    }

    /**
     * Kill a transaction - maybe shutdown mid-transaction? Or a timeout
     * collecting fragments? This is a don't-know-what-to-do-yet
     * stub.
     * TODO: fix this.
     */
    void terminateTransaction()
    {
        throw new RuntimeException("terminateTransaction is not yet implemented.");
    }
}
