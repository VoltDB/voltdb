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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.ParameterSet;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.utils.LogKeys;

public class MpTransactionState extends TransactionState
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    final Iv2InitiateTaskMessage m_task;
    final Mailbox m_mailbox;
    LinkedBlockingDeque<FragmentResponseMessage> m_newDeps;
    Map<Integer, Set<Long>> m_remoteDeps;
    Map<Integer, List<VoltTable>> m_remoteDepTables;
    Map<Integer, Set<Long>> m_localDeps;
    Set<Integer> m_finalDeps;
    long[] m_useHSIds;
    long m_localHSId;
    FragmentTaskMessage m_remoteWork = null;
    FragmentTaskMessage m_localWork = null;

    MpTransactionState(Mailbox mailbox, long txnId,
                       TransactionInfoBaseMessage notice,
                       long[] useHSIds, long localHSId)
    {
        super(txnId, notice);
        m_mailbox = mailbox;
        m_task = (Iv2InitiateTaskMessage)notice;
        m_useHSIds = useHSIds;
        m_localHSId = localHSId;
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
        // TODO Auto-generated method stub
        // Not clear this method is useful in the new world?
        return false;
    }

    @Override
    public boolean hasTransactionalWork()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean doWork(boolean recovering)
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public StoredProcedureInvocation getInvocation()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void handleSiteFaults(HashSet<Long> failedSites)
    {
        // TODO Auto-generated method stub

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
        m_localDeps = createTrackedDependenciesFromTask(task, new long[] {0});
    }

    @Override
    public void createAllParticipatingFragmentWork(FragmentTaskMessage task)
    {
        m_remoteWork = task;
        // Distribute fragments
        // Short-term(?) hack.  Pull the local HSId out of the list, we'll
        // short-cut that fragment task later
        long[] non_local_hsids = new long[m_useHSIds.length - 1];
        int index = 0;
        for (int i = 0; i < m_useHSIds.length; i++) {
            if (m_useHSIds[i] != m_localHSId)
            {
                non_local_hsids[index] = m_useHSIds[i];
                ++index;
            }
        }
        try {
            // send to all non-local sites (for now)
            m_mbox.send(non_local_hsids, task);
        }
        catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Integer, Set<Long>>
    createTrackedDependenciesFromTask(FragmentTaskMessage task,
                                      long[] expectedHSIds)
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
        if (m_remoteWork != null)
        {
            // Create some record of expected dependencies for tracking
            m_remoteDeps = createTrackedDependenciesFromTask(m_remoteWork,
                                                             m_useHSIds);
            // Add code to do local remote work.
            // need to modify processLocalFragmentTask() to be useable here too
            boolean doneWithRemoteDeps = false;
            while (!doneWithRemoteDeps)
            {
                FragmentResponseMessage msg = m_newDeps.poll();
                doneWithRemoteDeps = handleReceivedFragResponse(msg);
            }
        }

        // Next do local fragment stuff
        // Inject input deps for the local frags into the EE
        // use siteConnection.stashWorkUnitDependencies()
        siteConnection.stashWorkUnitDependencies(m_remoteDepTables);

        // Then execute the fragment task.  Looks like ExecutionSite.processFragmentTask(),
        // kinda, at least for now while we're executing stuff locally.
        // Probably don't need to generate a FragmentResponse
        Map<Integer, List<VoltTable>> results =
            processLocalFragmentTask(siteConnection);
        return results;
    }

    private boolean handleReceivedFragResponse(FragmentResponseMessage msg)
    {
        for (int i = 0; i < msg.getTableCount(); i++)
        {
            int this_depId = msg.getTableDependencyIdAtIndex(i);
            VoltTable this_dep = msg.getTableAtIndex(i);
            long src_hsid = msg.getExecutorSiteId();
            // check me for null for sanity
            Object needed = m_remoteDeps.remove(src_hsid);
            if (needed != null) {
                // add table to storage
                List<VoltTable> tables = m_remoteDepTables.get(this_depId);
                if (tables == null) {
                    tables = new ArrayList<VoltTable>();
                    m_remoteDepTables.put(this_depId, tables);
                }
                tables.add(this_dep);
            }
            else {
                // Bad things...deal with latah
            }
        }

        boolean done = true;
        for (Set<Long> depid : m_remoteDeps.values()) {
            if (depid.size() != 0) {
                done = false;
            }
        }
        return done;
    }

    // Cut-and-pasted from ExecutionSite.processFragmentTask().
    // Very similar to FragmentTask.processFragmentTask()...consider future
    // consolidation.
    private Map<Integer, List<VoltTable>>
    processLocalFragmentTask(SiteProcedureConnection siteConnection)
    {
        Map<Integer, List<VoltTable>> depResults =
            new HashMap<Integer, List<VoltTable>>();

        for (int frag = 0; frag < m_localWork.getFragmentCount(); frag++)
        {
            final long fragmentId = m_localWork.getFragmentId(frag);
            final int outputDepId = m_localWork.getOutputDepId(frag);

            // this is a horrible performance hack, and can be removed with small changes
            // to the ee interface layer.. (rtb: not sure what 'this' encompasses...)
            ParameterSet params = null;
            final ByteBuffer paramData = m_localWork.getParameterDataForFragment(frag);
            if (paramData != null) {
                final FastDeserializer fds = new FastDeserializer(paramData);
                try {
                    params = fds.readObject(ParameterSet.class);
                }
                catch (final IOException e) {
                    hostLog.l7dlog(Level.FATAL,
                                   LogKeys.host_ExecutionSite_FailedDeserializingParamsForFragmentTask.name(), e);
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
            }
            else {
                params = new ParameterSet();
            }

            if (m_localWork.isSysProcTask()) {
                throw new RuntimeException("IV2: Sysprocs not yet supported");
//                return processSysprocFragmentTask(txnState, dependencies, fragmentId,
//                                                  currentFragResponse, params);
            }
            else {
                final int inputDepId = m_localWork.getOnlyInputDepId(frag);

                // The try/catch from ExecutionSite goes away here, and
                // we let the exceptions bubble up to ProcedureRunner.call()
                // for handling?
                // IZZY: skeptical, need to test exception on final
                // fragment rollback
                final VoltTable dependency =
                    siteConnection.executePlanFragment(fragmentId,
                                                       inputDepId,
                                                       params,
                                                       txnId,
                                                       isReadOnly());
                List<VoltTable> tables = depResults.get(outputDepId);
                if (tables == null) {
                    tables = new ArrayList<VoltTable>();
                    m_remoteDepTables.put(outputDepId, tables);
                }
                tables.add(dependency);
                // IZZY: Keep the handled exceptions around for now until we
                // verify functionality
                //} catch (final EEException e) {
                //    hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
                //    currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                //    break;
                //} catch (final SQLException e) {
                //    hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
                //    currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
                //    break;
                //}
            }
        }

        return depResults;
    }

    // Runs from Mailbox's network thread
    public void offerReceivedFragmentResponse(FragmentResponseMessage message)
    {
        // push into threadsafe queue
        m_newDeps.offer(message);
    }
}