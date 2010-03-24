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

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Site;
import org.voltdb.dtxn.MultiPartitionParticipantTxnState;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messages.FragmentTask;
import org.voltdb.messaging.FastSerializer;

/**
 *  System procedures extend VoltSystemProcedure and use its utility
 *  methods to create work in the system.  This functionality is not
 *  available to standard user procedures (which extend VoltProcedure).
 */
public abstract class VoltSystemProcedure extends VoltProcedure {

    /**
     * Allow sysprocs to update m_currentTxnState manually. User procedures
     * are passed this state in call(); sysprocs have other entry points
     * on non-coordinator sites.
     */
    public void setTransactionState(TransactionState txnState) {
        m_currentTxnState = txnState;
    }

    /** Bundles the data needed to describe a plan fragment. */
    public static class SynthesizedPlanFragment {
        public int siteId = -1;
        public long fragmentId = -1;
        public int outputDepId = -1;
        public int inputDepIds[] = null;
        public ParameterSet parameters = null;
        public boolean multipartition = false;   /** true if distributes to all executable partitions */
        public boolean nonExecSites = false;     /** true if distributes once to each node */
        /**
         * Used to tell the DTXN to suppress duplicate results from sites that replicate
         * a partition.  Most system procedures don't want this, but, for example,
         * adhoc queries actually do want duplicate suppression like user sysprocs.
         */
        public boolean suppressDuplicates = false;
    }

    abstract public DependencyPair executePlanFragment(HashMap<Integer,List<VoltTable>> dependencies,
                                                      long fragmentId,
                                                      ParameterSet params,
                                                      ExecutionSite.SystemProcedureExecutionContext context);

    /**
     * Produce work units, possibly on all sites, for a list of plan fragments.
     * The final plan fragment must aggregate intermediate results and produce
     * a single output dependency. This aggregate output is returned as the result.
     *
     * @param pfs an array of synthesized plan fragments
     * @param aggregatorOutputDependencyId dependency id produced by the aggregation pf
     *        The id of the table returned as the result of this procedure.
     * @return the resulting VoltTable as a length-one array.
     */
    protected VoltTable[] executeSysProcPlanFragments(SynthesizedPlanFragment pfs[],
                                                      int aggregatorOutputDependencyId)
    {
        VoltTable[] results = new VoltTable[1];
        executeSysProcPlanFragmentsAsync(pfs);

        // the stack frame drop terminates the recursion and resumes
        // execution of the current stored procedure.
        assert(m_currentTxnState != null);
        assert(m_currentTxnState instanceof MultiPartitionParticipantTxnState);
        m_currentTxnState.setupProcedureResume(false, new int[] {aggregatorOutputDependencyId});

        // execute the tasks that just got queued.
        // recursively call recurableRun and don't allow it to shutdown
        Map<Integer,List<VoltTable>> mapResults =
            m_site.recursableRun(m_currentTxnState);

        List<VoltTable> matchingTablesForId = mapResults.get(aggregatorOutputDependencyId);
        if (matchingTablesForId == null) {
            assert (mapResults.size() == 0);
            results[0] = null;
        }
        else {
            results[0] = matchingTablesForId.get(0);
        }

        return results;
    }

    /**
     * Produce work units, possibly on all sites, for a list of plan fragments.
     * The final plan fragment must aggregate intermediate results and produce
     * a single output dependency. This aggregate output is returned as the result.
     *
     * @param pfs an array of synthesized plan fragments
     * @param aggregatorOutputDependencyId dependency id produced by the aggregation pf
     *        The id of the table returned as the result of this procedure.
     */
    protected void executeSysProcPlanFragmentsAsync(SynthesizedPlanFragment pfs[])
    {
        for (SynthesizedPlanFragment pf : pfs) {
            // check mutually exclusive flags
            assert(!(pf.multipartition && pf.nonExecSites));
            assert(pf.parameters != null);

            // serialize parameters
            ByteBuffer parambytes = null;
            if (pf.parameters != null) {
                FastSerializer fs = new FastSerializer();
                try {
                    fs.writeObject(pf.parameters);
                } catch (IOException e) {
                    e.printStackTrace();
                    assert(false);
                }
                parambytes = fs.getBuffer();
            }

            FragmentTask task = new FragmentTask(
                    m_currentTxnState.initiatorSiteId,
                    m_site.getSiteId(),
                    m_currentTxnState.txnId,
                    false,
                    new long[] { pf.fragmentId },
                    new int[] { pf.outputDepId },
                    new ByteBuffer[] { parambytes },
                    false);
            if (pf.inputDepIds != null) {
                for (int depId : pf.inputDepIds) {
                    task.addInputDepId(0, depId);
                }
            }
            task.setFragmentTaskType(FragmentTask.SYS_PROC_PER_SITE);
            if (pf.suppressDuplicates)
            {
                task.setFragmentTaskType(FragmentTask.SYS_PROC_PER_PARTITION);
            }

            if (pf.multipartition) {
                // create a workunit for every execution site
                m_currentTxnState.createAllParticipatingFragmentWork(task);
            }
            else if (pf.nonExecSites) {
                // create a workunit for one arbitrary site on each host.
                final HashMap<Host,String> foundHosts = new HashMap<Host,String>();
                final ArrayList<Integer> sites = new ArrayList<Integer>();
                for (Site site : m_cluster.getSites()) {
                    if (site.getIsexec() && (foundHosts.containsKey(site.getHost()) == false)) {
                        foundHosts.put(site.getHost(), site.getTypeName());
                        int siteId = Integer.parseInt(site.getTypeName());
                        sites.add(siteId);
                    }
                }
                int[] destinations = new int[sites.size()];
                for (int i = 0; i < sites.size(); i++)
                    destinations[i] = sites.get(i);

                m_currentTxnState.createFragmentWork(destinations, task);
            }
            else {
                // create one workunit for the current site
                if (pf.siteId == -1)
                    m_currentTxnState.createLocalFragmentWork(task, false);
                else
                    m_currentTxnState.createFragmentWork(new int[] { pf.siteId }, task);
            }
        }
    }
}
