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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.ExecutionSite;
import org.voltdb.VoltTable;
import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState;
import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState.WorkUnitState;
import org.voltdb.messages.FragmentResponse;
import org.voltdb.messages.FragmentTask;
import org.voltdb.messages.InitiateResponse;
import org.voltdb.messages.InitiateTask;
import org.voltdb.messages.MembershipNotice;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.VoltMessage;

public class MultiPartitionParticipantTxnState extends TransactionState {

    private final ArrayDeque<WorkUnit> m_readyWorkUnits = new ArrayDeque<WorkUnit>();
    private boolean m_isCoordinator;
    private int[] m_nonCoordinatingSites;
    private boolean m_shouldResumeProcedure = false;
    private boolean m_hasStartedWork = false;
    private HashMap<Integer, WorkUnit> m_missingDependencies = null;
    private ArrayList<WorkUnit> m_stackFrameDropWUs = null;
    private Map<Integer, List<VoltTable>> m_previousStackFrameDropDependencies = null;
    private boolean m_dirty = false;

    /**
     *  This is thrown by the TransactionState instance when something
     *  goes wrong mid-fragment, and execution needs to back all the way
     *  out to the stored procedure call.
     */
    public static class FragmentFailureException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    public MultiPartitionParticipantTxnState(Mailbox mbox, ExecutionSite site,
                                             MembershipNotice notice)
    {
        super(mbox, site, notice);
        m_nonCoordinatingSites = null;
        m_isCoordinator = false;
        if (notice instanceof InitiateTask)
        {
            m_isCoordinator = true;
            InitiateTask task = (InitiateTask) notice;
            m_nonCoordinatingSites = task.getNonCoordinatorSites();
            m_readyWorkUnits.add(new WorkUnit(site.m_context, task, null, false));
        }
    }

    @Override
    public boolean doWork() {
        if (m_done)
            return true;

        if (!m_hasStartedWork) {
            m_site.beginNewTxn(txnId, isReadOnly);
            m_hasStartedWork = true;
        }

        /*if (!m_isCoordinator)
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }*/

        WorkUnit wu = m_readyWorkUnits.poll();
        while (wu != null) {
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

                // returning false means there is more work to do
                return false;
            }

            VoltMessage payload = wu.getPayload();

            // null payload implies this is a commit message
            if (payload == null) {
                if ((m_dirty == false) || (wu.commitEvenIfDirty))
                    m_done = true;
            }
            else if (payload instanceof InitiateTask)
                initiateProcedure((InitiateTask) payload);
            else if (payload instanceof FragmentTask)
                processFragmentWork((FragmentTask) payload, wu.getDependencies());

            // get the next workunit from the ready list
            wu = m_readyWorkUnits.poll();

            // if this proc is blocked...
            // check if it's safe to try to run another proc while waiting
            if ((!m_done) && (m_isCoordinator) && (wu == null) && (isReadOnly == true)) {
                assert(m_missingDependencies != null);

                boolean hasTransactionalWork = false;
                for (WorkUnit pendingWu : m_missingDependencies.values()) {
                    if (pendingWu.nonTransactional == false)
                        hasTransactionalWork = true;
                }

                // if no transactional work, we can interleave
                if (!hasTransactionalWork) {

                    // repeat until no eligible procs or other work is ready
                    boolean success = false;
                    do {
                        success = m_site.tryToSneakInASinglePartitionProcedure();
                    }
                    while(success && ((wu = m_readyWorkUnits.poll()) == null));
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

    void initiateProcedure(InitiateTask itask) {
        assert(m_isCoordinator);

        InitiateResponse response = m_site.processInitiateTask(itask);

        // send commit notices to everyone
        FragmentTask ftask = new FragmentTask(
                initiatorSiteId,
                coordinatorSiteId,
                txnId,
                true,
                new long[] {},
                null,
                new ByteBuffer[] {},
                true);
        ftask.setShouldUndo(response.shouldCommit() == false);
        assert(ftask.isFinalTask() == true);

        try {
            m_mbox.send(m_nonCoordinatingSites, 0, ftask);
        }
        catch (MessagingException e) {
            throw new RuntimeException(e);
        }

        if (!response.shouldCommit()) {
            if (m_missingDependencies != null)
                m_missingDependencies.clear();
            if (!isReadOnly) {
                m_site.rollbackTransaction(isReadOnly);
            }
        }

        try {
            m_mbox.send(initiatorSiteId, 0, response);
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

        m_done = true;
    }

    void processFragmentWork(FragmentTask ftask, HashMap<Integer, List<VoltTable>> dependencies) {
        assert(ftask.getFragmentCount() > 0);

        FragmentResponse response = m_site.processFragmentTask(dependencies, ftask);
        // mark this transaction as dirty
        if (response.getDirtyFlag())
            m_dirty = true;

        if (response.getStatusCode() != FragmentResponse.SUCCESS)
        {
            m_dirty = true;
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
                m_site.rollbackTransaction(isReadOnly);
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
            try {
                m_mbox.send(response.getDestinationSiteId(), 0, response);
            } catch (MessagingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void setupProcedureResume(boolean isFinal, int[] dependencies) {
        //System.out.printf("Setting up procedure resume with dependency %d\n", dependencies[0]);
        //System.out.flush();

        assert(dependencies != null);
        assert(dependencies.length > 0);

        WorkUnit w = new WorkUnit(m_site.m_context, null, dependencies, true);
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
    public void createAllParticipatingFragmentWork(FragmentTask task) {
        assert(m_isCoordinator); // Participant can't set m_nonCoordinatingSites
        try {
            // send to all non-coordinating sites
            m_mbox.send(m_nonCoordinatingSites, 0, task);
            // send to this site
            createLocalFragmentWork(task, false);
        }
        catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createLocalFragmentWork(FragmentTask task, boolean nonTransactional) {
        // handle the undo case
        if (task.shouldUndo()) {
            if (m_missingDependencies != null)
                m_missingDependencies.clear();
            m_readyWorkUnits.clear();
            m_site.rollbackTransaction(isReadOnly);
            m_done = true;
            return;
        }

        if (task.getFragmentCount() > 0)
            createLocalFragmentWorkDependencies(task, nonTransactional);

        // if this txn is a participant and this is a final task...
        // if it's empty, then it's always a commit message
        // if it's got work in it, then it's only a commit message if
        //    the transaction is clean (and stays clean after this work)
        if ((!m_isCoordinator) && (task.isFinalTask())) {
            // add a workunit that will commit the txn
            WorkUnit wu = new WorkUnit(m_site.m_context, null, null, false);
            wu.commitEvenIfDirty = task.getFragmentCount() == 0;
            m_readyWorkUnits.add(wu);
        }
    }

    private void createLocalFragmentWorkDependencies(FragmentTask task, boolean nonTransactional)
    {
        if (task.getFragmentCount() <= 0) return;

        WorkUnit w = new WorkUnit(m_site.m_context, task, task.getAllUnorderedInputDepIds(), false);
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
    public void processRemoteWorkResponse(FragmentResponse response) {
        if (response.getStatusCode() != FragmentResponse.SUCCESS)
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
                m_site.rollbackTransaction(isReadOnly);
                m_done = true;
            }
        }

        processFragmentResponseDependencies(response);
    }

    private void processFragmentResponseDependencies(FragmentResponse response)
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
                throw new FragmentFailureException();
            }

            w.putDependency(dependencyId, response.getExecutorSiteId(),
                            payload);
            if (w.allDependenciesSatisfied()) {
                for (int depId : w.getDependencyIds())
                    m_missingDependencies.remove(depId);

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
        }
    }

    @Override
    public void getDumpContents(StringBuilder sb) {
        sb.append("  Multi Partition Txn State with id ").append(txnId);
        //sb.append("")
    }

    @Override
    public ExecutorTxnState getDumpContents() {
        ExecutorTxnState retval = new ExecutorTxnState();
        retval.txnId = txnId;
        retval.coordinatorSiteId = coordinatorSiteId;
        retval.initiatorSiteId = initiatorSiteId;
        retval.hasUndoWorkUnit = false;
        retval.isReadOnly = isReadOnly;
        retval.nonCoordinatingSites = m_nonCoordinatingSites;

        if (m_missingDependencies != null) {
            retval.workUnits = new WorkUnitState[m_missingDependencies.size()];
            int i = 0;
            for (WorkUnit wu : m_missingDependencies.values()) {
                retval.workUnits[i++] = wu.getDumpContents();
            }
        }

        return retval;
    }

    @Override
    public Map<Integer, List<VoltTable>> getPreviousStackFrameDropDependendencies() {
        return m_previousStackFrameDropDependencies;
    }

    // STUFF BELOW HERE IS REALY ONLY FOR SYSPROCS
    // SOME DAY MAYBE FOR MORE GENERAL TRANSACTIONS

    @Override
    public void createFragmentWork(int[] partitions, FragmentTask task) {
        try {
            // send to all specified sites (possibly including this one)
            m_mbox.send(partitions, 0, task);
        }
        catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }
}
