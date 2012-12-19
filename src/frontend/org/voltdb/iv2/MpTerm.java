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

import java.lang.InterruptedException;

import java.util.ArrayList;

import java.util.concurrent.ExecutionException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.voltcore.logging.VoltLogger;

import org.voltcore.utils.CoreUtils;

import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

import com.google.common.collect.ImmutableMap;

public class MpTerm implements Term
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final ZooKeeper m_zk;
    private final TreeSet<Long> m_knownLeaders = new TreeSet<Long>();

    // Initialized in start() -- when the term begins.
    protected LeaderCache m_leaderCache;

    // runs on the babysitter thread when a replica changes.
    // simply forward the notice to the initiator mailbox; it controls
    // the Term processing.
    // NOTE: The contract with LeaderCache is that it always
    // returns a full cache (one entry for every partition).  Returning a
    // partially filled cache is NotSoGood(tm).
    LeaderCache.Callback m_leadersChangeHandler = new LeaderCache.Callback()
    {
        @Override
        public void run(ImmutableMap<Integer, Long> cache)
        {
            Set<Long> updatedLeaders = new HashSet<Long>();
            for (Long HSId : cache.values()) {
                updatedLeaders.add(HSId);
            }
            List<Long> leaders = new ArrayList<Long>(updatedLeaders);
            tmLog.debug(m_whoami + "updating leaders: " + CoreUtils.hsIdCollectionToString(leaders));
            tmLog.debug(m_whoami
                      + "LeaderCache change handler updating leader list to: "
                      + CoreUtils.hsIdCollectionToString(leaders));
            m_knownLeaders.clear();
            m_knownLeaders.addAll(updatedLeaders);
            m_mailbox.updateReplicas(leaders);
        }
    };

    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public MpTerm(ZooKeeper zk, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami)
    {
        m_zk = zk;
        m_mailbox = mailbox;
        m_whoami = whoami;
    }

    /**
     * Start a new Term.  This starts watching followers via ZK.  Block on an
     * appropriate repair algorithm to watch final promotion to leader.
     */
    @Override
    public void start()
    {
        try {
            m_leaderCache = new LeaderCache(m_zk, VoltZK.iv2masters, m_leadersChangeHandler);
            m_leaderCache.start(true);
        }
        catch (ExecutionException ee) {
            VoltDB.crashLocalVoltDB("Unable to create babysitter starting term.", true, ee);
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Unable to create babysitter starting term.", true, e);
        }
    }

    @Override
    public void shutdown()
    {
        if (m_leaderCache != null) {
            try {
                m_leaderCache.shutdown();
            } catch (InterruptedException e) {
                // We're shutting down...this may jsut be faster.
            }
        }
    }

    @Override
    public List<Long> getInterestingHSIds()
    {
        return new ArrayList<Long>(m_knownLeaders);
    }
}
