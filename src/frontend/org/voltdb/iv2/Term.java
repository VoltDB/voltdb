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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.List;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import org.voltcore.logging.VoltLogger;
import org.voltcore.zk.BabySitter;
import org.voltcore.zk.BabySitter.Callback;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.MapCache;
import org.voltcore.zk.MapCacheWriter;

import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

/**
 * Term encapsulates the process/algorithm of becoming
 * a new PI and the leadership responsibilities while performing that
 * role.
 */
public class Term
{
    VoltLogger hostLog = new VoltLogger("HOST");

    private final InitiatorMailbox m_mailbox;
    private final int m_partitionId;
    private final long m_initiatorHSId;
    private final ZooKeeper m_zk;

    // Initialized in start() -- when the term begins.
    private BabySitter m_babySitter;

    Callback m_replicasChangeHandler = new Callback()
    {
        @Override
        public void run(List<String> children) {
            hostLog.info("Babysitter for zkLeaderNode: " +
                    LeaderElector.electionDirForPartition(m_partitionId) + ":");
            hostLog.info("children: " + children);
            // make an HSId array out of the children
            // The list contains the leader, skip it
            List<Long> replicas = new ArrayList<Long>(children.size() - 1);
            for (String child : children) {
                long HSId = Long.parseLong(LeaderElector.getPrefixFromChildName(child));
                if (HSId != m_initiatorHSId)
                {
                    replicas.add(HSId);
                }
            }
            hostLog.info("Updated replicas: " + replicas);
            m_mailbox.updateReplicas(replicas);
        }
    };

    public static class InaugurationFuture implements Future<Boolean>
    {
        private CountDownLatch m_doneLatch = new CountDownLatch(1);
        private ExecutionException m_exception = null;

        private void setException(Exception e)
        {
            m_exception = new ExecutionException(e);
        }

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public boolean isDone() {
			return false;
		}

		@Override
		public Boolean get() throws InterruptedException, ExecutionException {
			m_doneLatch.await();
            if (m_exception != null) {
                throw m_exception;
            }
            return true;
		}

		@Override
		public Boolean get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			m_doneLatch.await(timeout, unit);
            if (m_exception != null) {
                throw m_exception;
            }
            return true;
		}
    }

    /**
     * Setup a new Term but don't take any action to take responsibility.
     */
    public Term(ZooKeeper zk, int partitionId, long initiatorHSId, InitiatorMailbox mailbox)
        throws ExecutionException, InterruptedException, KeeperException
    {
        m_zk = zk;
        m_partitionId = partitionId;
        m_initiatorHSId = initiatorHSId;
        m_mailbox = mailbox;
    }

    /**
     * Start a new Term. Returns a future that is done when the leadership has
     * been fully assumed and all surviving replicas have been repaired.
     *
     * @param kfactorForStartup If running for startup and not for fault
     * recovery, pass the kfactor required to proceed. For fault recovery,
     * pass any negative value as kfactorForStartup.
     */
    public Future<?> start(int kfactorForStartup)
    {
        InaugurationFuture result = new InaugurationFuture();
        try {
            m_babySitter = new BabySitter(m_zk,
                    LeaderElector.electionDirForPartition(m_partitionId),
                    m_replicasChangeHandler, true);
            if (kfactorForStartup >= 0) {
                prepareForStartup(kfactorForStartup);
            }
            else {
                prepareForFaultRecovery();
            }
            declareReadyAsLeader();
        } catch (Exception e) {
            result.setException(e);
        }
        result.m_doneLatch.countDown();
        return result;
    }


    public Future<?> cancel()
    {
        return null;
    }

    public void shutdown()
    {
        if (m_babySitter != null) {
            m_babySitter.shutdown();
        }
    }

    public List<String> lastSeenChildren()
    {
        return m_babySitter.lastSeenChildren();
    }

    void prepareForStartup(int kfactor)
    {
        // This block-on-all-the-replicas-at-startup thing sucks.  Hopefully this can
        // go away when we get rejoin working.
        List<String> children = m_babySitter.lastSeenChildren();
        while (children.size() < kfactor + 1) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
            }
            children = m_babySitter.lastSeenChildren();
        }
    }

    void prepareForFaultRecovery()
    {
    }

    // with leadership election complete, update the master list
    // for non-initiator components that care.
    void declareReadyAsLeader()
    {
        hostLog.info("Registering " +  m_partitionId + " as new master.");
        try {
            MapCacheWriter iv2masters = new MapCache(m_zk, VoltZK.iv2masters);
            iv2masters.put(Integer.toString(m_partitionId),
                    new JSONObject("{hsid:" + m_mailbox.getHSId() + "}"));
        } catch (KeeperException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        } catch (JSONException e) {
            VoltDB.crashLocalVoltDB("Bad news: failed to declare leader.", true, e);
        }
    }


}
