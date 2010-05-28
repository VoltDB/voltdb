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

package org.voltdb;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ExecutionSiteFuzzChecker
{
    // Icky, too much hardwired stuff in here.  Fix later.
    class LogString
    {
        String m_text;

        LogString(String text)
        {
            m_text = text;
        }

        public boolean equals(Object o)
        {
            if (o == this)
            {
                return true;
            }
            if (!(o instanceof LogString))
            {
                return false;
            }
            LogString other = (LogString) o;
            return m_text.equals(other.m_text);
        }

        public String toString()
        {
            return m_text;
        }

        boolean isFuzz()
        {
            return m_text.contains("FUZZTEST");
        }

        boolean isTxnStart()
        {
            return m_text.contains("beginNewTxn");
        }

        int getTxnId()
        {
            if (!isTxnStart() || !isTxnEnd() || !isRollback())
            {
                throw new RuntimeException("getTxnId called on inappropriate message");
            }
            return Integer.valueOf(m_text.substring(m_text.indexOf("FUZZTEST")).split(" ")[2]);
        }

        boolean isSinglePart()
        {
            return m_text.contains("single");
        }

        boolean isMultiPart()
        {
            return m_text.contains("multi");
        }

        boolean isOtherFault()
        {
            return m_text.contains("handleNodeFault");
        }

        boolean isSelfFault()
        {
            // XXX ADD APPROPRIATE CHECK HERE WHEN WE ADD LOG TO FUZZ TEST FRAMEWORK
            return false;
        }

        int getFaultNode()
        {
            if (!isOtherFault())
            {
                throw new RuntimeException("getFaultNode called on non-fault message");
            }
            return Integer.valueOf(m_text.substring(m_text.indexOf("FUZZTEST")).split(" ")[2]);
        }

        boolean isRollback()
        {
            return m_text.contains("rollbackTransaction");
        }

        boolean isTxnEnd()
        {
            return m_text.contains("completeTransaction");
        }
    }

    class SiteLog
    {
        private Integer m_siteId;
        private StringWriter m_logBuffer;
        private String[] m_logStrings;
        private int m_logIndex;

        SiteLog(Integer siteId, StringWriter logBuffer)
        {
            m_siteId = siteId;
            m_logBuffer = logBuffer;
            m_logIndex = 0;
        }

        void reset()
        {
            m_logIndex = 0;
        }

        Integer getSiteId()
        {
            return m_siteId;
        }

        void logComplete()
        {
            m_logStrings = m_logBuffer.toString().split("\n");
            findNextNonFailFuzz();
        }

        LogString currentLog()
        {
            if (isDone())
            {
                return new LogString(m_logStrings[m_logStrings.length - 1]);
            }
            return new LogString(m_logStrings[m_logIndex]);
        }

        boolean isDone()
        {
            return (m_logIndex >= m_logStrings.length);
        }

        // Will advance over non-interesting log messages.  Will not move
        // if the current message is interesting.  Also no-op if we're at the
        // end of the log
        void findNextNonFailFuzz()
        {
            boolean found = false;
            while (!isDone() && !found)
            {
                if (!currentLog().isFuzz())
                {
                    m_logIndex++;
                }
                else if (currentLog().isOtherFault())
                {
                    // XXX RECORD OTHER FAILURE
                    m_logIndex++;
                }
                else if (currentLog().isSelfFault())
                {
                    // XXX RECORD SELF FAILURE
                    m_logIndex++;
                }
                else
                {
                    found = true;
                }
            }
        }

        // Contrive to stay at the last log message if we've reached the end.
        void advanceLog()
        {
            if (!isDone())
            {
                m_logIndex++;
                //System.out.println("Advancing index for site : " + m_siteId + " to " + m_logIndex);
                findNextNonFailFuzz();
            }
        }
    }

    HashMap<Integer, HashMap<Integer, SiteLog>> m_replicaSets;
    // a list of replica sets (partition IDs) on which progress can be made
    HashSet<Integer> m_unblockedSets;
    // a list of replica sets which are blocked on a multipartition TXN
    HashSet<Integer> m_blockedSets;
    // a list of replica sets which are done (have no more log output)
    HashSet<Integer> m_doneSets;

    public ExecutionSiteFuzzChecker()
    {
        m_replicaSets = new HashMap<Integer, HashMap<Integer, SiteLog>>();
        m_unblockedSets = new HashSet<Integer>();
        m_blockedSets = new HashSet<Integer>();
        m_doneSets = new HashSet<Integer>();
    }

    public void addSite(int siteId, int partitionId, StringWriter logBuffer)
    {
        //System.out.println("Adding siteID: " + siteId + ", partitionID: " + partitionId);
        if (!m_replicaSets.containsKey(partitionId))
        {
            m_replicaSets.put(partitionId, new HashMap<Integer, SiteLog>());
        }
        m_replicaSets.get(partitionId).put(siteId, new SiteLog(siteId, logBuffer));
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
//                    System.out.println("" + part_id + ", " + site_id + ": " + this_log.currentLog());
//                    this_log.advanceLog();
//                }
//                this_log.reset();
            }
        }
    }

    void sortBlocked()
    {
        for (Integer part_id : m_replicaSets.keySet())
        {
            SiteLog site_log = m_replicaSets.get(part_id).values().iterator().next();
            if (site_log.currentLog().isMultiPart())
            {
                System.out.println("PARTITION BLOCKED: " + part_id);
                m_unblockedSets.remove((Integer) part_id);
                m_blockedSets.add(part_id);
            }
            else if (site_log.currentLog().isSinglePart())
            {
                System.out.println("PARTITION UNBLOCKED: " + part_id);
                m_blockedSets.remove((Integer) part_id);
                m_unblockedSets.add(part_id);
            }
        }
        System.out.println("INITIAL UNBLOCKED SETS: " + m_unblockedSets);
        System.out.println("INITIAL BLOCKED SETS: " + m_blockedSets);
    }

    boolean processReplicaSetUntilBlockedOrDone(Integer partitionId)
    {
        boolean valid = true;
        boolean blocked = false;

        while (valid && !blocked)
        {
            LogString curr_log = null;
            // Check the current log message on each replica for a match
            for (SiteLog site_log : m_replicaSets.get(partitionId).values())
            {
                System.out.println("LOG: " + partitionId + ", " + site_log.getSiteId() + ": " + site_log.currentLog());
                if (curr_log == null)
                {
                    curr_log = site_log.currentLog();
                }
                else
                {
                    if (!site_log.currentLog().equals(curr_log))
                    {
                        System.out.println("SOMETHING BARFED IN REPLICA SET: " + partitionId);
                        junit.framework.Assert.assertFalse(true);
                        valid = false;
                    }
                }
            }
            // Advance state and check for termination conditions (multi-part block, end of log)
            for (SiteLog site_log : m_replicaSets.get(partitionId).values())
            {
                //System.out.println("Trying to advance: " + site_log.getSiteId());
                site_log.advanceLog();
                if (site_log.isDone())
                {
                    System.out.println("LOG DONE: " + site_log.getSiteId());
                    m_unblockedSets.remove((Integer) partitionId);
                    m_doneSets.add(partitionId);
                    blocked = true;
                }
                else if (site_log.currentLog().isMultiPart())
                {
                    System.out.println("LOG BLOCKED: " + site_log.getSiteId());
                    m_unblockedSets.remove((Integer) partitionId);
                    m_blockedSets.add(partitionId);
                    blocked = true;
                }
                else
                {
                    System.out.println("LOG ADVANCING: " + site_log.getSiteId());
                }
            }
        }
        return valid;
    }

    boolean processMultiPartitionTransaction()
    {
        boolean valid = true;
        boolean done = false;
        while (!done && valid)
        {
            LogString curr_log = null;
            for (Integer partition_id : m_blockedSets)
            {
                for (SiteLog site_log : m_replicaSets.get(partition_id).values())
                {
                    System.out.println("LOG: " + partition_id + ", " + site_log.getSiteId() + ": " + site_log.currentLog());
                    if (curr_log == null)
                    {
                        curr_log = site_log.currentLog();
                    }
                    else
                    {
                        if (!site_log.currentLog().equals(curr_log))
                        {
                            System.out.println("SOMETHING BARFED IN MULTI-PART");
                            junit.framework.Assert.assertFalse(true);
                            valid = false;
                        }
                    }
                }
            }

            ArrayList<Integer> sets_to_remove = new ArrayList<Integer>();
            for (Integer partition_id : m_blockedSets)
            {
                // Advance state and check for termination conditions (multi-part block, end of log)
                for (SiteLog site_log : m_replicaSets.get(partition_id).values())
                {
                    //System.out.println("Trying to advance: " + site_log.getSiteId());
                    site_log.advanceLog();
                    if (site_log.isDone())
                    {
                        System.out.println("LOG DONE: " + site_log.getSiteId());
                        //m_blockedSets.remove((Integer) partition_id);
                        sets_to_remove.add(partition_id);
                        m_doneSets.add(partition_id);
                        done = true;
                    }
                    else if (site_log.currentLog().isTxnStart() &&
                             site_log.currentLog().isSinglePart())
                    {
                        System.out.println("LOG UNBLOCKED: " + site_log.getSiteId());
                        //m_blockedSets.remove((Integer) partition_id);
                        sets_to_remove.add(partition_id);
                        m_unblockedSets.add(partition_id);
                        done = true;
                    }
                    else
                    {
                        System.out.println("LOG ADVANCING: " + site_log.getSiteId());
                    }
                }
            }
            for (Integer part_id : sets_to_remove)
            {
                m_blockedSets.remove((Integer) part_id);
            }
        }
        return valid;
    }

    public boolean validateLogs()
    {
        boolean valid = false;
        boolean done = false;
        // Rough sketch of validation:
        //
        // Put all replica sets in the unblocked list
        // For each replica set in the unblocked list:
        //   For each site in the replica set:
        //      Advance to the next non-failure output trace:
        //         if the output is the failure of another site:
        //            record somewhere that this site has seen that site's failure
        //            and advance the log index and then continue here
        //         if the output is the failure of this site:
        //            remove this site from the replica set
        //            do yet-to-be-determined failure accounting
        //            (probably how many other sites still live)
        //      Check that the logging at the current log index matches for
        //         the surviving replica sites.
        //      if the output is the start of a multi-partition txn, move this
        //         replica set to the blocked list and move on to next set
        //      if the output is the start of a single-partition txn:
        //         go up to Advance and repeat
        // Repeat until all sets are blocked or there are no more log messages
        // If all sets are blocked:
        //    Complete this multi-partition txn:
        //       Verify that the log message at each log index matches
        //       Advance each site to the next non-failure output message as above
        //       Repeat until multi-part txn is complete (or massive fail)
        //    Move all replica sets back to the unblocked list
        //

        sortBlocked();

        while (!done)
        {
            while (!m_unblockedSets.isEmpty())
            {
                System.out.println("UNBLOCKED SETS: " + m_unblockedSets);
                processReplicaSetUntilBlockedOrDone(m_unblockedSets.iterator().next());
            }
            if (m_blockedSets.isEmpty())
            {
                System.out.println("DONE!");
                done = true;
            }
            else
            {
                System.out.println("BLOCKED SETS: " + m_blockedSets);
                processMultiPartitionTransaction();
            }
        }

        return valid;
    }
}
