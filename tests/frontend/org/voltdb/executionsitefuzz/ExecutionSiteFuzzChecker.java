/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.executionsitefuzz;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.voltdb.logging.VoltLogger;

public class ExecutionSiteFuzzChecker
{
    private static final VoltLogger testLog = new VoltLogger("TEST");

    HashMap<Integer, HashMap<Integer, SiteLog>> m_replicaSets;
    ArrayList<SiteLog> m_failedSites;
    // a list of replica sets (partition IDs) on which progress can be made
    HashSet<Integer> m_unblockedSets;
    // a list of replica sets which are blocked on a multipartition TXN
    HashSet<Integer> m_blockedSets;
    // a list of replica sets which are done (have no more log output)
    HashSet<Integer> m_doneSets;

    ArrayList<SiteLog> m_liveSites;
    ArrayList<SiteLog> m_doneSites;

    long m_lastTxnId = Long.MIN_VALUE;

    public ExecutionSiteFuzzChecker()
    {
        m_replicaSets = new HashMap<Integer, HashMap<Integer, SiteLog>>();
        m_failedSites = new ArrayList<SiteLog>();
        m_unblockedSets = new HashSet<Integer>();
        m_blockedSets = new HashSet<Integer>();
        m_doneSets = new HashSet<Integer>();

        m_liveSites = new ArrayList<SiteLog>();
        m_doneSites = new ArrayList<SiteLog>();
    }

    public void addSite(int siteId, int partitionId, StringWriter logBuffer)
    {
        //System.out.println("Adding siteID: " + siteId + ", partitionID: " + partitionId);
        if (!m_replicaSets.containsKey(partitionId))
        {
            m_replicaSets.put(partitionId, new HashMap<Integer, SiteLog>());
        }
        SiteLog new_site = new SiteLog(siteId, partitionId, logBuffer);
        m_replicaSets.get(partitionId).put(siteId, new_site);

        m_liveSites.add(new_site);
    }

    public void dumpLogs()
    {
        for (Integer part_id : m_replicaSets.keySet())
        {
            for (Integer site_id : m_replicaSets.get(part_id).keySet())
            {
                SiteLog this_log = m_replicaSets.get(part_id).get(site_id);
                this_log.logComplete();
//                while (!this_log.isDone())
//                {
//                    System.out.println("" + part_id + ", " + site_id + ": " + this_log.currentTxn());
//                    this_log.advanceLog();
//                }
//                this_log.reset();
            }
        }
    }

    Set<SiteLog> getLowestTxnIdSet()
    {
        Set<SiteLog> retval = null;
        Long min_txn_id = Long.MAX_VALUE;
        for (SiteLog site : m_liveSites)
        {
            //System.out.println("lowestTxnIdSet checking site: " + site.getSiteId());
            //System.out.println("min_txn_id: " + min_txn_id + ", curr_txn_id: " + site.currentTxn().getTxnId());
            if (site.currentTxn().getTxnId().compareTo(min_txn_id) < 0)
            {
                min_txn_id = site.currentTxn().getTxnId();
                retval = new HashSet<SiteLog>();
                retval.add(site);
            }
            else if (site.currentTxn().getTxnId().compareTo(Long.MAX_VALUE) == 0)
            {
                // if the TXN ID is actually Long.MAX_VALUE, then this is a bogus
                // transaction record, just ignore it.  Makes the empty
                // list case when a site fails IMMEDIATELY and has no transactions
                // in it work
                // DO NOTHING
            }
            else if (site.currentTxn().getTxnId().compareTo(min_txn_id) == 0)
            {
                retval.add(site);
            }
        }

        StringBuilder sb = new StringBuilder();

        sb.append("Next TXN ID set:  TXN ID: " + min_txn_id).append(" ");
        sb.append("  Members: ");
        for (SiteLog site : retval)
        {
            sb.append(site.getSiteId()).append(", ");
        }
        testLog.info(sb.toString());
        return retval;
    }

    boolean validateSinglePartitionSet(Set<SiteLog> sites)
    {
        // Single-partition txns are easy to verify, just
        // ensure that all the members of the set are equal.
        boolean valid = true;
        TransactionRecord model_txn = null;
        for (SiteLog site : sites)
        {
            testLog.info("" + site.getPartitionId() + ", " + site.getSiteId() + ": " + site.currentTxn());
            if (model_txn == null)
            {
                model_txn = site.currentTxn();
            }
            else if (!model_txn.equals(site.currentTxn()))
            {
                testLog.error("VALIDATION FAILURE, MISMATCHED TRANSACTIONS");
                valid = false;
            }
        }
        return valid;
    }

    boolean validateMultiPartitionSet(Set<SiteLog> sites)
    {
        // validate this set as follows:
        // - The elements of the set should all match
        // - If the elements are a single-part TXN, there should be one per surviving replica (defer check?)
        // - If the elements are a multi-part TXN:
        // -- If there is one per surviving site, pass
        // -- If there is fewer than one per surviving site, they must all be rollback
        // --- This is the case where one or more sites never even start the
        // --- transaction because of the coordinator failure
        boolean valid = true;

        // Should all match
        TransactionRecord model_txn = null;
        TransactionRecord coord_txn = null;
        HashSet<Integer> failed_sites = new HashSet<Integer>();
        boolean saw_rollback = false;

        // Run through the list once and extract coordinator and failure info.
        for (SiteLog site : sites)
        {
            testLog.info("" + site.getPartitionId() + ", " + site.getSiteId() + ": " + site.currentTxn());
            // If this site saw any failures during this TXN, add
            // them to the failed_sites set.  Also, check to see if any
            // of the state upon which we currently rely is on this failed site
            if (site.currentTxn().sawFailure())
            {
                failed_sites.addAll(site.currentTxn().getFailedSites());
            }
            if (site.currentTxn().isCoordinator())
            {
                coord_txn = site.currentTxn();
            }
        }

        for (SiteLog site : sites)
        {
            if (failed_sites.contains((Integer)site.getSiteId()))
            {
                // The coordinator cannot fail early during a read-write multi-partition txn
                if (site.currentTxn().isCoordinator() && !site.currentTxn().isReadOnly())
                {
                    testLog.error("VALIDATION FAILURE, " +
                                  "MULTIPARTITION COORDINATOR FAILED " +
                                  "DURING READ/WRITE TRANSACTION BUT SOMEHOW COMPLETED");
                    valid = false;
                }
                else
                {
                    testLog.info("Site: " + site.getSiteId() + " failed before " +
                                 "TXN " + site.currentTxn().getTxnId() + " was resolved, ignoring");
                }
                continue;
            }

            if (site.currentTxn().rolledBack())
            {
                saw_rollback = true;
            }

            if (model_txn == null)
            {
                if (coord_txn == null)
                {
                    model_txn = site.currentTxn();
                }
                else
                {
                    model_txn = coord_txn;
                }
            }
            else if (!model_txn.isConsistent(site.currentTxn()))
            {
                testLog.error("VALIDATION FAILURE, MISMATCHED TRANSACTIONS");
                valid = false;
            }
        }

        // check quantity/type/other global requirements
        if (valid)
        {
            // There are cases where some sites will have started a multi-partition
            // TXN before the coordinator fails, but other sites will not have
            // started it, and so they will never start it.  In any case
            // where the number of sites that run a TXN is less than the number
            // of sites that are still live, we need to have rolled back that transaction.
            // Only matters if the transaction is not read-only
            if (model_txn.isMultiPart() && !model_txn.isReadOnly() &&
                sites.size() != m_liveSites.size() && !model_txn.rolledBack())
            {
                testLog.error("VALIDATION FAILURE, PARTIALLY COMMITTED TXN");
                valid = false;
            }
            // If we're multi-partition and anyone rolled back, the coordinator
            // better have rolled back.  Note that we might not
            // have a coordinator because it may be a failed node
            if (coord_txn != null && coord_txn.isMultiPart() && !coord_txn.rolledBack() && saw_rollback)
            {
                testLog.error("VALIDATION FAILURE, COORDINATOR COMMITTED WHEN PARTICIPANT ROLLED BACK");
                valid = false;
            }
        }

        return valid;
    }

    public boolean validateSet(Set<SiteLog> sites)
    {
        long curr_txn_id = sites.iterator().next().currentTxn().getTxnId();
        if (curr_txn_id <= m_lastTxnId)
        {
            testLog.error("VALIDATION FAILURE, TXN_ID FAILED TO ADVANCE");
            return false;
        }
        else
        {
            m_lastTxnId = curr_txn_id;
        }

        if (sites.iterator().next().currentTxn().isMultiPart())
        {
            return validateMultiPartitionSet(sites);
        }
        else
        {
            return validateSinglePartitionSet(sites);
        }
    }

    public boolean validateLogs()
    {
        // new validation algorithm:
        //
        // across the head of each site log, prune any failed sites (self-fail in this TXN)
        // across the heads of the remaining site logs, find the lowest TXN ID set
        // validate this set as follows:
        // - The elements of the set should all match
        // - If the elements are a single-part TXN, there should be one per surviving replica (defer check?)
        // - If the elements are a multi-part TXN:
        // -- If there is one per surviving site, pass
        // -- If there is fewer than one per surviving site, they must all be rollback
        // Advance the sites in this set
        boolean valid = true;
        boolean done = false;

        while (!done && valid)
        {
            // prune any failed sites
            boolean pruned = false;
            while (!m_liveSites.isEmpty() && !pruned)
            {
                pruned = true;
                Iterator<SiteLog> site_iter = m_liveSites.iterator();
                while (site_iter.hasNext())
                {
                    SiteLog site = site_iter.next();
                    if (site.currentTxn().failed())
                    {
                        testLog.info("Pruning site: " + site.getSiteId());
                        site_iter.remove();
                        m_failedSites.add(site);
                        pruned = false;
                    }
                }
            }

            // validate the heads of remaining live sites
            if (!m_liveSites.isEmpty())
            {
                Set<SiteLog> next_set = getLowestTxnIdSet();
                valid = validateSet(next_set);
                // advance the sites in this set
                for (SiteLog site : next_set)
                {
                    site.advanceLog();
                    if (site.isDone())
                    {
                        m_liveSites.remove(site);
                        m_doneSites.add(site);
                    }
                }
            }
            else
            {
                done = true;
            }
        }
        return valid;
    }
}
