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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.Pair;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class MpTransactionState extends TransactionState
{
    final Iv2InitiateTaskMessage m_task;
    final Mailbox m_mailbox;
    LinkedBlockingDeque<FragmentResponseMessage> m_newDeps;
    Map<Integer, List<Pair<Integer, FragmentResponseMessage>>> m_remoteDeps;
    Map<Integer, List<Pair<Integer, FragmentResponseMessage>>> m_localDeps;
    Set<Integer> m_finalDeps;
    int[] m_usePartitions;
    FragmentTaskMessage m_remoteWork = null;
    FragmentTaskMessage m_localWork = null;

    MpTransactionState(Mailbox mailbox, long txnId,
                       TransactionInfoBaseMessage notice,
                       int[] usePartitions)
    {
        super(txnId, notice);
        m_mailbox = mailbox;
        m_task = (Iv2InitiateTaskMessage)notice;
        m_usePartitions = usePartitions;
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
        m_localDeps = createTrackedDependenciesFromTask(task, new int[] {0});
    }

    @Override
    public void createAllParticipatingFragmentWork(FragmentTaskMessage task)
    {
        m_remoteWork = task;
//        // Distribute fragments
//        try {
//            // send to all non-coordinating sites
//            m_mbox.send(m_nonCoordinatingSites, task);
//            // send to this site
//            // IZZY: DO THE RIGHT THING HERE
//        }
//        catch (MessagingException e) {
//            throw new RuntimeException(e);
//        }
    }

    private Map<Integer, List<Pair<Integer, FragmentResponseMessage>>>
    createTrackedDependenciesFromTask(FragmentTaskMessage task,
                                      int[] expectedPartitions)
    {
        Map<Integer, List<Pair<Integer, FragmentResponseMessage>>> depMap =
            new HashMap<Integer, List<Pair<Integer, FragmentResponseMessage>>>();
        for (int i = 0; i < task.getFragmentCount(); i++) {
            int dep = task.getOutputDepId(i);
            ArrayList<Pair<Integer, FragmentResponseMessage>> scoreboard =
                new ArrayList<Pair<Integer, FragmentResponseMessage>>();
            depMap.put(dep, scoreboard);
            for (int partition : expectedPartitions) {
                scoreboard.add(new Pair<Integer, FragmentResponseMessage>(partition, null));
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
                                                             m_usePartitions);
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
        Map<Integer, List<VoltTable>> deps = extractDepTablesFromResponses();
        siteConnection.stashWorkUnitDependencies(deps);

        // Then execute the fragment task.  Looks like ExecutionSite.processFragmentTask(),
        // kinda, at least for now while we're executing stuff locally.
        // Probably don't need to generate a FragmentResponse
        Map<Integer, List<VoltTable>> results = processLocalFragmentTask();
        return results;
    }

    private boolean handleReceivedFragResponse(FragmentResponseMessage msg)
    {
        // WRITE ME
        return false;
    }

    private Map<Integer, List<VoltTable>> extractDepTablesFromResponses()
    {
        Map<Integer, List<VoltTable>> depTables =
            new HashMap<Integer, List<VoltTable>>();

        // WRITE ME


        return depTables;
    }

    private Map<Integer, List<VoltTable>> processLocalFragmentTask()
    {
        Map<Integer, List<VoltTable>> depResults =
            new HashMap<Integer, List<VoltTable>>();

        // WRITE ME


        return depResults;
    }

    // Runs from Mailbox's network thread
    public void offerReceivedFragmentResponse(FragmentResponseMessage message)
    {
        // push into threadsafe queue
        m_newDeps.offer(message);
    }
}