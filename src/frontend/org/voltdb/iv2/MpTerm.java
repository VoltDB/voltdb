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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import org.voltcore.logging.VoltLogger;

import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.MapCache;
import org.voltcore.zk.MapCache.Callback;

import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class MpTerm implements Term
{
    VoltLogger tmLog = new VoltLogger("TM");
    private final String m_whoami;

    private final InitiatorMailbox m_mailbox;
    private final ZooKeeper m_zk;
    private final CountDownLatch m_missingStartupSites;
    private final TreeSet<Long> m_knownLeaders = new TreeSet<Long>();

    // Initialized in start() -- when the term begins.
    protected MapCache m_mapCache;

    // runs on the babysitter thread when a replica changes.
    // simply forward the notice to the initiator mailbox; it controls
    // the Term processing.
    // NOTE: The contract with MapCache is that it always
    // returns a full cache (one entry for every partition).  Returning a
    // partially filled cache is NotSoGood(tm).
    Callback m_leadersChangeHandler = new Callback()
    {
        @Override
        public void run(ImmutableMap<String, JSONObject> cache)
        {
            Set<Long> updatedLeaders = new HashSet<Long>();
            for (JSONObject thing : cache.values()) {
                try {
                    updatedLeaders.add(Long.valueOf(thing.getLong("hsid")));
                } catch (JSONException e) {
                    VoltDB.crashLocalVoltDB("Corrupt ZK MapCache data.", true, e);
                }
            }
            List<Long> leaders = new ArrayList<Long>(updatedLeaders);
            tmLog.info(m_whoami + "updating leaders: " + CoreUtils.hsIdCollectionToString(leaders));
            // Need to handle startup separately from runtime updates.
            if (MpTerm.this.m_missingStartupSites.getCount() > 0) {

                Sets.SetView<Long> removed = Sets.difference(MpTerm.this.m_knownLeaders, updatedLeaders);
                if (!removed.isEmpty()) {
                    tmLog.error(m_whoami
                            + "leader(s) failed during startup. Initialization can not complete."
                            + " Failed leaders: " + removed);
                    VoltDB.crashLocalVoltDB("Leaders failed during startup.", true, null);
                    return;
                }
                Sets.SetView<Long> added = Sets.difference(updatedLeaders, MpTerm.this.m_knownLeaders);
                int newLeaders = added.size();
                m_knownLeaders.clear();
                m_knownLeaders.addAll(updatedLeaders);
                m_mailbox.updateReplicas(leaders);
                for (int i=0; i < newLeaders; i++) {
                    MpTerm.this.m_missingStartupSites.countDown();
                }
                tmLog.info(m_whoami +
                        "continuing leader promotion.  Waiting for " +
                        m_missingStartupSites.getCount() + " more for configured k-safety.");
            }
            else {
                // remove the leader; convert to hsids; deal with the replica change.
                tmLog.info(m_whoami
                        + "MapCache change handler updating leader list to: "
                        + CoreUtils.hsIdCollectionToString(leaders));
                m_knownLeaders.clear();
                m_knownLeaders.addAll(updatedLeaders);
                m_mailbox.updateReplicas(leaders);
            }
        }
    };

    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public MpTerm(CountDownLatch missingStartupSites, ZooKeeper zk,
            long initiatorHSId, InitiatorMailbox mailbox,
            String whoami)
    {
        m_zk = zk;
        m_mailbox = mailbox;

        if (missingStartupSites != null) {
            m_missingStartupSites = missingStartupSites;
        }
        else {
            m_missingStartupSites = new CountDownLatch(0);
        }

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
            m_mapCache = new MapCache(m_zk, VoltZK.iv2masters, m_leadersChangeHandler);
            m_mapCache.start(true);
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
        if (m_mapCache != null) {
            try {
                m_mapCache.shutdown();
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
