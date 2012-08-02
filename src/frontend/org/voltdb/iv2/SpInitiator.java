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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import java.util.List;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import org.voltcore.messaging.HostMessenger;

import org.voltcore.utils.Pair;

import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.MapCache;
import org.voltcore.zk.MapCache.Callback;

import org.voltcore.zk.MapCacheWriter;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

import com.google.common.collect.ImmutableMap;

/**
 * Subclass of Initiator to manage single-partition operations.
 * This class is primarily used for object construction and configuration plumbing;
 * Try to avoid filling it with lots of other functionality.
 */
public class SpInitiator extends BaseInitiator
{
    final private MapCache m_mapCache;
    private boolean m_promoted = false;

    Callback m_leadersChangeHandler = new Callback()
    {
        @Override
        public void run(ImmutableMap<String, JSONObject> cache)
        {
            for (JSONObject thing : cache.values()) {
                try {
                    if (Long.valueOf(thing.getLong("appointee")) == getInitiatorHSId()) {
                        if (!m_promoted) {
                            acceptPromotion();
                            m_promoted = true;
                        }
                        break;
                    }
                } catch (JSONException e) {
                    VoltDB.crashLocalVoltDB("Corrupt ZK MapCache data.", true, e);
                }
            }
        }
    };

    public SpInitiator(HostMessenger messenger, Integer partition)
    {
        super(VoltZK.iv2masters, messenger, partition,
                new SpScheduler(new SiteTaskerQueue()),
                "SP");
        m_mapCache = new MapCache(messenger.getZK(), VoltZK.iv2appointees, m_leadersChangeHandler);
    }

    @Override
    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          int kfactor, CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          boolean createForRejoin)
        throws KeeperException, InterruptedException, ExecutionException
    {
        try {
            m_mapCache.start(true);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to configure SpInitiator.", true, e);
        }
        super.configureCommon(backend, serializedCatalog, catalogContext,
                kfactor + 1, csp, numberOfPartitions,
                createForRejoin && isRejoinable());
        // add ourselves to the ephemeral node list which BabySitters will watch for this
        // partition
        LeaderElector.createParticipantNode(m_messenger.getZK(),
                LeaderElector.electionDirForPartition(m_partitionId),
                Long.toString(getInitiatorHSId()), null);
    }

    @Override
    public void acceptPromotion()
    {
        try {
            long startTime = System.currentTimeMillis();
            Boolean success = false;
            m_term = createTerm(null, m_messenger.getZK(),
                    m_partitionId, getInitiatorHSId(), m_initiatorMailbox,
                    m_whoami);
            m_term.start();
            while (!success) {
                RepairAlgo repair = null;
                repair = createPromoteAlgo(m_term.getInterestingHSIds(),
                        m_initiatorMailbox, m_whoami);

                m_initiatorMailbox.setRepairAlgo(repair);
                // term syslogs the start of leader promotion.
                Pair<Boolean, Long> result = repair.start().get();
                success = result.getFirst();
                if (success) {
                    m_initiatorMailbox.setLeaderState(result.getSecond());
                    tmLog.info(m_whoami
                            + "finished leader promotion. Took "
                            + (System.currentTimeMillis() - startTime) + " ms.");

                    // THIS IS where map cache should be updated, not
                    // in the promotion algorithm.
                    MapCacheWriter iv2masters = new MapCache(m_messenger.getZK(),
                            m_zkMailboxNode);
                    iv2masters.put(Integer.toString(m_partitionId),
                            new JSONObject("{hsid:" + m_initiatorMailbox.getHSId() + "}"));
                }
                else {
                    // The only known reason to fail is a failed replica during
                    // recovery; that's a bounded event (by k-safety).
                    // CrashVoltDB here means one node failure causing another.
                    // Don't create a cascading failure - just try again.
                    tmLog.info(m_whoami
                            + "interrupted during leader promotion after "
                            + (System.currentTimeMillis() - startTime) + " ms. of "
                            + "trying. Retrying.");
                }
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Terminally failed leader promotion.", true, e);
        }
    }

    /**
     * SpInitiator has userdata that must be rejoined.
     */
    @Override
    public boolean isRejoinable()
    {
        return true;
    }

    @Override
    public Term createTerm(CountDownLatch missingStartupSites, ZooKeeper zk,
            int partitionId, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami)
    {
        return new SpTerm(zk, partitionId, initiatorHSId, mailbox, whoami);
    }

    @Override
    public RepairAlgo createPromoteAlgo(List<Long> survivors, InitiatorMailbox mailbox,
            String whoami)
    {
        return new SpPromoteAlgo(m_term.getInterestingHSIds(), m_initiatorMailbox, m_whoami);
    }
}
