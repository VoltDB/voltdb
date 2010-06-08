/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

public class ExecutionSiteFuzzChecker
{
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
            else if (site.currentTxn().getTxnId().compareTo(min_txn_id) == 0)
            {
                retval.add(site);
            }
        }

        StringBuilder sb = new StringBuilder();

        sb.append("Next TXN ID set:  TXN ID: " + min_txn_id).append("\n");
        sb.append("  Members: ");
        for (SiteLog site : retval)
        {
            sb.append(site.getSiteId()).append(", ");
        }
        System.out.println(sb.toString());
        return retval;
    }

    boolean validateSet(Set<SiteLog> sites)
    {
        // validate this set as follows:
        // - The elements of the set should all match
        // - If the elements are a single-part TXN, there should be one per surviving replica (defer check?)
        // - If the elements are a multi-part TXN:
        // -- If there is one per surviving site, pass
        // -- If there is fewer than one per surviving site, they must all be rollback
        boolean valid = true;

        // Should all match
        TransactionRecord model_txn = null;
        for (SiteLog site : sites)
        {
            System.out.println("" + site.getPartitionId() + ", " + site.getSiteId() + ": " + site.currentTxn());
            if (model_txn == null)
            {
                model_txn = site.currentTxn();
            }
            else if (!model_txn.equals(site.currentTxn()))
            {
                System.out.println("VALIDATION FAILURE, MISMATCHED TRANSACTIONS");
                junit.framework.Assert.assertTrue(false);
                valid = false;
            }
        }

        // check quantity/type requirements
        if (valid)
        {
            if (model_txn.isMultiPart() && sites.size() != m_liveSites.size())
            {
                System.out.println("POTENTIAL FAILURE/ROLLBACK HICCUP!");
            }
            if (model_txn.isMultiPart() && sites.size() != m_liveSites.size() && !model_txn.rolledBack())
            {
                System.out.println("VALIDATION FAILURE, PARTIALLY COMMITTED TXN");
                junit.framework.Assert.assertTrue(false);
                valid = false;
            }
        }

        return valid;
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
                        System.out.println("Pruning site: " + site.getSiteId());
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
