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

package org.voltdb.dtxn;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.CatalogContext;
import org.voltdb.ExecutionSite;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskLogMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

public class MultiPartitionParticipantTxnState extends TransactionState {

    protected final ArrayDeque<WorkUnit> m_readyWorkUnits = new ArrayDeque<WorkUnit>();
    protected boolean m_isCoordinator;
    protected final long m_hsId;
    protected long[] m_nonCoordinatingSites;
    protected boolean m_shouldResumeProcedure = false;
    protected boolean m_hasStartedWork = false;
    protected HashMap<Integer, WorkUnit> m_missingDependencies = null;
    protected ArrayList<WorkUnit> m_stackFrameDropWUs = null;
    protected Map<Integer, List<VoltTable>> m_previousStackFrameDropDependencies = null;
    protected final StoredProcedureInvocation m_invocation; // for DR sending purposes

    //protected ArrayList<FragmentTaskMessage> m_loggedFragmentTasks = null;
    protected FragmentTaskLogMessage m_loggedFragments = null;

    private InitiateResponseMessage m_response;
    private HashSet<Long> m_outstandingAcks = null;
    private final java.util.concurrent.atomic.AtomicBoolean m_durabilityFlag;
    private final InitiateTaskMessage m_task;
    private final CatalogContext m_context;

    // ENG-3288 - Support mismatched results leniency for certain queries.
    private boolean m_allowMismatchedResults = false;

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /**
     *  This is thrown by the TransactionState instance when something
     *  goes wrong mid-fragment, and execution needs to back all the way
     *  out to the stored procedure call.
     */
    public static class FragmentFailureException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    public MultiPartitionParticipantTxnState(Mailbox mbox, ExecutionSite site,
                                             TransactionInfoBaseMessage notice)
    {
        super(mbox, site, notice);
        m_hsId = site.getSiteId();
        m_nonCoordinatingSites = null;
        m_isCoordinator = false;
        m_context = site.m_context;

        //Check to make sure we are the coordinator, it is possible to get an intiate task
        //where we aren't the coordinator because we are a replica of the coordinator.
        if (notice instanceof InitiateTaskMessage) {
            // keep this around for DR purposes
            m_invocation = ((InitiateTaskMessage) notice).getStoredProcedureInvocation();

            // Determine if mismatched results are okay.
            if (m_invocation != null) {
                String procName = m_invocation.getProcName();
                if (procName.startsWith("@AdHoc")) {
                    // For now the best we can do with ad hoc is to always allow mismatched results.
                    // We don't know if it's non-deterministic or not. But the main use case for
                    // being lenient is "SELECT * FROM TABLE LIMIT n", typically run as ad hoc.
                    m_allowMismatchedResults = true;
                } else {
                    // Walk through the statements to see if any are non-deterministic.
                    if (m_context != null && m_context.procedures != null) {
                        Procedure proc = m_context.procedures.get(procName);
                        if (proc != null) {
                            CatalogMap<Statement> stmts = proc.getStatements();
                            if (stmts != null) {
                                for (Statement stmt : stmts) {
                                    if (!stmt.getIscontentdeterministic() || !stmt.getIsorderdeterministic()) {
                                        m_allowMismatchedResults = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (notice.getCoordinatorHSId() == m_hsId) {
                m_isCoordinator = true;
                m_task = (InitiateTaskMessage) notice;
                m_durabilityFlag = m_task.getDurabilityFlagIfItExists();
                SiteTracker tracker = site.getSiteTracker();
                m_readyWorkUnits.add(new WorkUnit(tracker, m_task,
                                                  null, m_hsId,
                                                  null, false, m_allowMismatchedResults));

                /*
                 * ENG-3374: Get the set of non-coordinator sites for this
                 * transaction from the initiator and from the local site.
                 *
                 * Compare them and find any sites the initiator knows about that
                 * the local site doesn't know about. These are either new
                 * rejoining sites that need to be included, or they are
                 * failed sites that don't need to be included. Determine, for
                 * each extra site, which case it's in and either add it to
                 * the transaction or not.
                 *
                 * Note that if the local site has extra sites in the transaction,
                 * that can be ignored because the local site can count on a
                 * failure notice coming.
                 *
                 * Also note that having these two lists differ should be rare,
                 * and only when cluster membership changes.
                 */
                m_nonCoordinatingSites = m_task.getNonCoordinatorSites();

                // note, tracker should be non-null outside of tests
                if (tracker != null) {
                    long[] myArraySites = tracker.getAllSitesExcluding(m_hsId);

                    Set<Long> mySites = new TreeSet<Long>();
                    for (long hsid : myArraySites) {
                        mySites.add(hsid);
                    }

                    // match is true if all initiator-reported sites are locally known
                    boolean match = true;
                    for (long hsid : m_nonCoordinatingSites) {
                        if (!mySites.contains(hsid)) {
                            match = false;
                            if (!m_site.isActiveOrPreviouslyKnownSiteId(hsid)) {
                                mySites.add(hsid);
                            }
                        }
                    }

                    // if no match, rebuild the list according to the block above
                    if (!match) {
                        m_nonCoordinatingSites = new long[mySites.size()];
                        int i = 0;
                        for (long siteId : mySites) {
                            m_nonCoordinatingSites[i++] = siteId;
                        }
                    }
                }
            }
            else {
                m_durabilityFlag = ((InitiateTaskMessage)notice).getDurabilityFlagIfItExists();
                m_task = null;
            }
        }
        else {
            m_task = null;
            m_durabilityFlag = null;
            m_invocation = null;
        }
    }

    @Override
    public String toString() {
        return "MultiPartitionParticipantTxnState initiator: " + initiatorHSId +
            " coordinator: " + m_isCoordinator +
            " in-progress: " + m_hasStartedWork +
            " txnId: " + TransactionIdManager.toString(txnId);
    }

    @Override
    public boolean isInProgress() {
        return m_hasStartedWork;
    }

    @Override
    public boolean isSinglePartition()
    {
        return false;
    }

    @Override
    public boolean isBlocked()
    {
        return m_readyWorkUnits.isEmpty();
    }

    @Override
    public boolean isCoordinator()
    {
        return m_isCoordinator;
    }

    @Override
    public boolean hasTransactionalWork()
    {
        // this check is a little bit of a lie.  It depends not only on
        // whether or not any of the pending WorkUnits touch tables in the EE,
        // but also whether or not these pending WorkUnits are part of the final
        // batch of SQL statements that a stored procedure is going to execute
        // (Otherwise, we might see no transactional work remaining in this
        // batch but the stored procedure could send us another batch that
        // not only touches tables but does writes to them).  The
        // WorkUnit.nonTransactional boolean ends up ultimately being gated on this
        // condition in VoltProcedure.slowPath().  This is why the null case
        // below is pessimistic.
        if (m_missingDependencies == null)
        {
            return true;
        }

        boolean has_transactional_work = false;
        for (WorkUnit pendingWu : m_missingDependencies.values()) {
            if (pendingWu.nonTransactional == false) {
                has_transactional_work = true;
                break;
            }
        }
        return has_transactional_work;
    }

    @Override
    public boolean doWork(boolean rejoining) {
        if (rejoining && (m_rejoinState == RejoinState.NORMAL)) {
            m_rejoinState = RejoinState.REJOINING;
            if (!m_isReadOnly) {
                m_loggedFragments = new FragmentTaskLogMessage(
                        m_notice.getInitiatorHSId(), m_notice.getCoordinatorHSId(), m_notice.getTxnId());
                assert(m_loggedFragments.getTxnId() == getNotice().getTxnId());
            }
        }

        if (!m_hasStartedWork) {
            m_site.beginNewTxn(this);
            m_hasStartedWork = true;
        }

        if (m_done) {
            return true;
        }

        while (!isBlocked())
        {
            WorkUnit wu = m_readyWorkUnits.poll();
            if (wu.shouldResumeProcedure()) {
                assert(m_stackFrameDropWUs != null);
                m_stackFrameDropWUs.remove(wu);

                m_shouldResumeProcedure = true;
                m_previousStackFrameDropDependencies = wu.getDependencies();

                for (WorkUnit sfd : m_stackFrameDropWUs) {
                    sfd.m_stackCount--;
                    if (sfd.allDependenciesSatisfied()) {
                        m_readyWorkUnits.add(sfd);
                    }
                }

                return m_done;
            }

            VoltMessage payload = wu.getPayload();

            if (payload instanceof InitiateTaskMessage) {
                initiateProcedure((InitiateTaskMessage) payload);
            }
            else if (payload instanceof FragmentTaskMessage) {
                if ((m_rejoinState == RejoinState.REJOINING) && (wu.nonTransactional == false)) {
                    processRejoiningFragmentWork((FragmentTaskMessage) payload, wu.getDependencies());
                }
                else {
                    // when recovering, still do non-transactional work
                    processFragmentWork((FragmentTaskMessage) payload, wu.getDependencies());
                }
            }
        }

        return m_done;
    }

    @Override
    public boolean shouldResumeProcedure() {
        if (m_shouldResumeProcedure) {
            m_shouldResumeProcedure = false;
            return true;
        }
        return false;
    }

    public CompleteTransactionMessage createCompleteTransactionMessage(boolean rollback,
                                                                       boolean requiresAck)
    {
        CompleteTransactionMessage ft =
            new CompleteTransactionMessage(initiatorHSId,
                                           coordinatorSiteId,
                                           txnId,
                                           true,
                                           getHash(),
                                           rollback,
                                           requiresAck,
                                           false,
                                           false);

        return ft;
    }

    void initiateProcedure(InitiateTaskMessage itask) {
        assert(m_isCoordinator);
        // all MP txns are replayed as fragments
        assert(m_rejoinState != RejoinState.REPLAYING);

        // Cache the response locally and create accounting
        // to track the outstanding acks.
        m_response = m_site.processInitiateTask(this, itask);
        if (!m_response.shouldCommit()) {
            if (m_missingDependencies != null)
            {
                m_missingDependencies.clear();
            }
            m_needsRollback = true;
        }

        m_outstandingAcks = new HashSet<Long>();
        // if this transaction was readonly then don't require acks.
        // We still need to send the completion message, however,
        // since there are a number of circumstances where the coordinator
        // is the only site that knows whether or not the transaction has
        // completed.
        if (!isReadOnly())
        {
            for (long hsid : m_nonCoordinatingSites)
            {
                m_outstandingAcks.add(hsid);
            }
        }
        // send commit notices to everyone
        CompleteTransactionMessage complete_msg =
            createCompleteTransactionMessage(m_response.shouldCommit() == false,
                                             !isReadOnly());

        m_mbox.send(m_nonCoordinatingSites, complete_msg);

        if (m_outstandingAcks.size() == 0)
        {
            m_mbox.send(initiatorHSId, m_response);
            m_done = true;
        }
    }

    void processFragmentWork(FragmentTaskMessage ftask, HashMap<Integer, List<VoltTable>> dependencies) {
        assert(ftask.getFragmentCount() > 0);

        FragmentResponseMessage response = m_site.processFragmentTask(this, dependencies, ftask);
        if (response.getStatusCode() != FragmentResponseMessage.SUCCESS)
        {
            if (m_missingDependencies != null)
                m_missingDependencies.clear();
            m_readyWorkUnits.clear();

            if (m_isCoordinator)
            {
                // throw an exception which will back the runtime all the way
                // to the stored procedure invocation call, triggering undo
                // at that point
                if (response.getException() != null) {
                    throw response.getException();
                } else {
                    throw new FragmentFailureException();
                }
            }
            else
            {
                m_needsRollback = true;
                m_done = true;
            }
        }

        if (m_isCoordinator &&
            (response.getDestinationSiteId() == response.getExecutorSiteId()))
        {
            processFragmentResponseDependencies(response);
        }
        else
        {
            m_mbox.send(response.getDestinationSiteId(), response);
            // If we're not the coordinator, the transaction is read-only,
            // and this was the final task, then we can try to move on after
            // we've finished this work.
            if (!isCoordinator() && isReadOnly() && ftask.isFinalTask()) {
                m_done = true;
            }
        }
    }

    private void processRejoiningFragmentWork(FragmentTaskMessage ftask, HashMap<Integer, List<VoltTable>> dependencies) {
        assert(ftask.getFragmentCount() > 0);
        assert(m_rejoinState == RejoinState.REJOINING);

        FragmentResponseMessage response = new FragmentResponseMessage(ftask, m_hsId);
        response.setRecovering(true);
        response.setStatus(FragmentResponseMessage.SUCCESS, null);

        // log the work done for replay
        if (!ftask.isReadOnly() && !ftask.isSysProcTask()) {
            assert(m_notice.isReadOnly() == false);
            assert(m_loggedFragments != null);
            m_loggedFragments.appendFragmentTask(ftask);
        }

        // add a dummy table for all of the expected dependency ids
        for (int i = 0; i < ftask.getFragmentCount(); i++) {
            response.addDependency(ftask.getOutputDepId(i),
                    new VoltTable(new VoltTable.ColumnInfo("DUMMY", VoltType.BIGINT)));
        }

        m_mbox.send(response.getDestinationSiteId(), response);

        // If we're not the coordinator, the transaction is read-only,
        // and this was the final task, then we can try to move on after
        // we've finished this work.
        if (!isCoordinator() && isReadOnly() && ftask.isFinalTask()) {
            m_done = true;
        }
    }

    @Override
    public void setupProcedureResume(boolean isFinal, int[] dependencies) {
        assert(dependencies != null);
        assert(dependencies.length > 0);

        WorkUnit w = new WorkUnit(m_site.getSiteTracker(), null, dependencies,
                                  m_hsId, m_nonCoordinatingSites, true, m_allowMismatchedResults);
        if (isFinal)
            w.nonTransactional = true;
        for (int depId : dependencies) {
            if (m_missingDependencies == null) {
                m_missingDependencies = new HashMap<Integer, WorkUnit>();
            }
            // We are missing the dependency: record this fact
            assert(!m_missingDependencies.containsKey(depId));
            m_missingDependencies.put(depId, w);
        }
        if (m_stackFrameDropWUs == null)
            m_stackFrameDropWUs = new ArrayList<WorkUnit>();
        for (WorkUnit sfd : m_stackFrameDropWUs)
            sfd.m_stackCount++;
        m_stackFrameDropWUs.add(w);

        // Find any stack frame drop work marked ready in the ready set,
        // and if it's not really ready, take it out.
        for (WorkUnit wu : m_readyWorkUnits) {
            if (wu.shouldResumeProcedure()) {
                if (wu.m_stackCount > 0)
                    m_readyWorkUnits.remove(wu);
            }
        }
    }

    @Override
    public void createAllParticipatingFragmentWork(FragmentTaskMessage task) {
        assert(m_isCoordinator); // Participant can't set m_nonCoordinatingSites
        // send to all non-coordinating sites
        m_mbox.send(m_nonCoordinatingSites, task);
        // send to this site
        createLocalFragmentWork(task, false);
    }

    @Override
    public void createLocalFragmentWork(FragmentTaskMessage task, boolean nonTransactional) {
        if (task.getFragmentCount() <= 0) return;

        WorkUnit w = new WorkUnit(m_site.getSiteTracker(), task,
                                  task.getAllUnorderedInputDepIds(),
                                  m_hsId, m_nonCoordinatingSites, false,
                                  m_allowMismatchedResults);
        w.nonTransactional = nonTransactional;

        for (int i = 0; i < task.getFragmentCount(); i++) {
            ArrayList<Integer> inputDepIds = task.getInputDepIds(i);
            if (inputDepIds == null) continue;
            for (int inputDepId : inputDepIds) {
                if (m_missingDependencies == null)
                    m_missingDependencies = new HashMap<Integer, WorkUnit>();
                assert(!m_missingDependencies.containsKey(inputDepId));
                m_missingDependencies.put(inputDepId, w);
            }
        }

        if (w.allDependenciesSatisfied())
            m_readyWorkUnits.add(w);
    }

    @Override
    public void processRemoteWorkResponse(FragmentResponseMessage response) {
        // if we've already decided that we're rolling back, then we just
        // want to discard any incoming FragmentResponses that were
        // possibly in flight
        if (m_needsRollback)
        {
            return;
        }

        if (response.getStatusCode() != FragmentResponseMessage.SUCCESS)
        {
            if (m_missingDependencies != null)
                m_missingDependencies.clear();
            m_readyWorkUnits.clear();

            if (m_isCoordinator)
            {
                // throw an exception which will back the runtime all the way
                // to the stored procedure invocation call, triggering undo
                // at that point
                if (response.getException() != null) {
                    throw response.getException();
                }
                else {
                    throw new FragmentFailureException();
                }
            }
            else
            {
                m_needsRollback = true;
                m_done = true;
            }
        }

        processFragmentResponseDependencies(response);
    }

    private void processFragmentResponseDependencies(FragmentResponseMessage response)
    {
        int depCount = response.getTableCount();
        for (int i = 0; i < depCount; i++) {
            int dependencyId = response.getTableDependencyIdAtIndex(i);
            VoltTable payload = response.getTableAtIndex(i);
            assert(payload != null);

            // if we're getting a dependency, i hope we know about it
            assert(m_missingDependencies != null);

            WorkUnit w = m_missingDependencies.get(dependencyId);
            if (w == null) {
                String msg = "Unable to find WorkUnit for dependency: " +
                             dependencyId +
                             " as part of TXN: " + txnId +
                             " received from execution site: " +
                             response.getExecutorSiteId();
                hostLog.warn(msg);
                //throw new FragmentFailureException();
                return;
            }

            // if the node is recovering, it doesn't matter if the payload matches
            if (response.isRecovering()) {
                w.putDummyDependency(dependencyId, response.getExecutorSiteId());
            }
            else {
                w.putDependency(dependencyId, response.getExecutorSiteId(), payload, m_site.getSiteTracker());
            }
            if (w.allDependenciesSatisfied()) {
                handleWorkUnitComplete(w);
            }
        }
    }

    @Override
    public void processCompleteTransaction(CompleteTransactionMessage complete)
    {
        m_done = true;
        if (complete.isRollback()) {
            if (m_missingDependencies != null) {
                m_missingDependencies.clear();
            }
            m_readyWorkUnits.clear();
            m_needsRollback = true;
        }
        if (complete.requiresAck())
        {
            CompleteTransactionResponseMessage ctrm =
                new CompleteTransactionResponseMessage(complete, m_hsId);
            m_mbox.send(complete.getCoordinatorHSId(), ctrm);
        }
    }

    @Override
    public void
    processCompleteTransactionResponse(CompleteTransactionResponseMessage response)
    {
        // need to do ack accounting
        m_outstandingAcks.remove(response.getExecutionSiteId());
        if (m_outstandingAcks.size() == 0)
        {
            m_mbox.send(initiatorHSId, m_response);
            m_done = true;
        }
    }

    void handleWorkUnitComplete(WorkUnit w)
    {
        for (int depId : w.getDependencyIds()) {
            m_missingDependencies.remove(depId);
        }
        // slide this new stack frame drop into the right position
        // (before any other stack frame drops)
        if ((w.shouldResumeProcedure()) &&
                (m_readyWorkUnits.peekLast() != null) &&
                (m_readyWorkUnits.peekLast().shouldResumeProcedure())) {

            ArrayDeque<WorkUnit> deque = new ArrayDeque<WorkUnit>();
            while ((m_readyWorkUnits.peekLast() != null) &&
                    (m_readyWorkUnits.peekLast().shouldResumeProcedure())) {
                deque.add(m_readyWorkUnits.pollLast());
            }
            deque.add(w);
            while (deque.size() > 0)
                m_readyWorkUnits.add(deque.pollLast());
        }
        else {
            m_readyWorkUnits.add(w);
        }
    }

    public void checkWorkUnits()
    {
        // Find any workunits with previously unmet dependencies
        // that may now be satisfied.  We can't remove them from
        // the map in this loop because we induce a
        // ConcurrentModificationException
        if (m_missingDependencies != null)
        {
            ArrayList<WorkUnit> done_wus = new ArrayList<WorkUnit>();
            for (WorkUnit w : m_missingDependencies.values())
            {
                if (w.allDependenciesSatisfied()) {
                    done_wus.add(w);
                }
            }

            for (WorkUnit w : done_wus)
            {
                handleWorkUnitComplete(w);
            }
        }

        // Also, check to see if we're just waiting on acks from
        // participants, and, if so, if the fault we just experienced
        // freed us up.
        if (m_outstandingAcks != null)
        {
            if (m_outstandingAcks.size() == 0)
            {
                m_mbox.send(initiatorHSId, m_response);
                m_done = true;
            }
        }
    }

    // for test only
    @Deprecated
    public long[] getNonCoordinatingSites() {
        return m_nonCoordinatingSites;
    }

    @Override
    public Map<Integer, List<VoltTable>> getPreviousStackFrameDropDependendencies() {
        return m_previousStackFrameDropDependencies;
    }

    public InitiateTaskMessage getInitiateTaskMessage() {
        return m_task;
    }

    /**
     * Clean up internal state in response to a set of failed sites.
     * Note that failedSites contains both initiators and execution
     * sites. An external agent will drive the deletion/fault or
     * commit of this transaction state block. Only responsibility
     * here is to make internal book keeping right.
     */
    @Override
    public void handleSiteFaults(HashSet<Long> failedSites)
    {
        // remove failed sites from the non-coordinating lists
        // and decrement expected dependency response count
        if (m_nonCoordinatingSites != null) {
            ArrayDeque<Long> newlist = new ArrayDeque<Long>(m_nonCoordinatingSites.length);
            int removed = 0;
            for (int i=0; i < m_nonCoordinatingSites.length; ++i) {
                if (!failedSites.contains(m_nonCoordinatingSites[i])) {
                    newlist.addLast(m_nonCoordinatingSites[i]);
                }
                else {
                    removed++;
                }
            }
            //assert(removed == failedSites.size());

            m_nonCoordinatingSites = new long[newlist.size()];
            for (int i=0; i < m_nonCoordinatingSites.length; ++i) {
                m_nonCoordinatingSites[i] = newlist.removeFirst();
            }
        }

        // Remove failed sites from all of the outstanding work units that
        // may be expecting dependencies from now-dead sites.
        if (m_missingDependencies != null)
        {
            for (WorkUnit wu : m_missingDependencies.values())
            {
                for (Long site_id : failedSites)
                {
                    wu.removeSite(site_id);
                }
            }
        }

        // Also, if we're waiting on transaction completion acks from
        // participants, remove any sites that failed that we still may
        // be waiting on.
        if (m_outstandingAcks != null)
        {
            for (Long site_id : failedSites)
            {
                m_outstandingAcks.remove(site_id);
            }
        }
    }

    // STUFF BELOW HERE IS REALY ONLY FOR SYSPROCS
    // SOME DAY MAYBE FOR MORE GENERAL TRANSACTIONS

    @Override
    public void createFragmentWork(long[] partitions, FragmentTaskMessage task) {
        // send to all specified sites (possibly including this one)
        m_mbox.send(partitions, task);
    }

    @Override
    public boolean isDurable() {
        return m_durabilityFlag == null ? true : m_durabilityFlag.get();
    }

    @Override
    public StoredProcedureInvocation getInvocation() {
        return m_invocation;
    }

    @Override
    public TransactionInfoBaseMessage getTransactionInfoBaseMessageForRejoinLog() {
        assert(m_rejoinState == RejoinState.REJOINING);
        assert(m_loggedFragments != null);
        // skip txns that didn't do any work, or did sysproc work
        if (m_loggedFragments.getFragmentTasks().size() == 0) {
            return null;
        }
        return m_loggedFragments;
    }
}
