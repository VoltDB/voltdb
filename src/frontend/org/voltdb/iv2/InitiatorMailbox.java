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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.zk.BabySitter;
import org.voltcore.zk.BabySitter.Callback;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.LeaderNoticeHandler;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.messaging.InitiateTaskMessage;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class InitiatorMailbox implements Mailbox, LeaderNoticeHandler
{
    VoltLogger hostLog = new VoltLogger("HOST");
    private final int m_partitionId;
    private final InitiatorMessageHandler m_msgHandler;
    private final HostMessenger m_messenger;
    private long m_hsId;

    // hacky temp txnid
    AtomicLong m_txnId = new AtomicLong(0);

    //
    // Half-backed replication stuff
    //
    InitiatorRole m_role;
    private LeaderElector m_elector;
    // only primary initiator has the following two set
    private BabySitter m_babySitter = null;
    private volatile long[] m_replicas = null;
    Callback m_membershipChangeHandler = new Callback()
    {
        @Override
        public void run(List<String> children)
        {
            if (children == null || children.isEmpty()) {
                return;
            }

            // The list includes the leader, exclude it
            long[] tmpArray = new long[children.size() - 1];
            int i = 0;
            for (String child : children) {
                try {
                    long HSId = Long.parseLong(child.split("_")[0]);
                    if (HSId != m_hsId) {
                        tmpArray[i++] = HSId;
                    }
                }
                catch (NumberFormatException e) {
                    hostLog.error("Unable to get the HSId of initiator replica " + child);
                    return;
                }
            }
            m_replicas = tmpArray;
            ((PrimaryRole) m_role).setReplicas(m_replicas);
        }
    };


    public InitiatorMailbox(InitiatorMessageHandler msgHandler, HostMessenger messenger,
            int partitionId, PartitionClerk partitionClerk)
    {
        m_msgHandler = msgHandler;
        m_messenger = messenger;
        m_partitionId = partitionId;
        m_messenger.createMailbox(null, this);
        m_msgHandler.setMailbox(this);
    }

    /**
     * Start leader election
     * @throws Exception
     */
    public void start(int totalReplicasForPartition) throws Exception
    {
        // by this time, we should have our HSId
        m_role = new ReplicatedRole(m_hsId);

        String electionDirForPartition = VoltZK.electionDirForPartition(m_partitionId);
        m_elector = new LeaderElector(
                m_messenger.getZK(),
                electionDirForPartition,
                Long.toString(this.m_hsId), // prefix
                null,
                this);
        // This will invoke becomeLeader()
        m_elector.start(true);

        if (m_elector.isLeader()) {
            // barrier to wait for all replicas to be ready
            boolean success = false;
            for (int ii = 0; ii < 4000; ii++) {
                List<String> children = m_babySitter.lastSeenChildren();
                if (children == null || children.size() < totalReplicasForPartition) {
                    Thread.sleep(5);
                } else {
                    success = true;
                    break;
                }
            }
            if (!success) {
                VoltDB.crashLocalVoltDB("Not all replicas for partition " +
                        m_partitionId + " are ready in time", false, null);
            }
        }
    }

    public void shutdown() throws InterruptedException, KeeperException
    {
        if (m_babySitter != null) {
            m_babySitter.shutdown();
        }
        if (m_elector != null) {
            m_elector.shutdown();
        }
    }

    @Override
    public void send(long destHSId, VoltMessage message) throws MessagingException
    {
        message.m_sourceHSId = this.m_hsId;
        m_messenger.send(destHSId, message);
    }

    @Override
    public void send(long[] destHSIds, VoltMessage message) throws MessagingException
    {
        message.m_sourceHSId = this.m_hsId;
        m_messenger.send(destHSIds, message);
    }

    @Override
    public synchronized void deliver(VoltMessage message)
    {
        m_msgHandler.deliver(message);
    }

    /**
     * Forwards the initiate task message to the replicas. Only the primary
     * initiator has to do this.
     *
     * @param message
     */
    private void replicateInitiation(InitiateTaskMessage message)
    {
        if (m_replicas != null) {
            try {
                send(m_replicas, message);
            }
            catch (MessagingException e) {
                hostLog.error("Failed to replicate initiate task.", e);
            }
        }
    }

    @Override
    public VoltMessage recv()
    {
        return m_role.poll();
    }

    @Override
    public void deliverFront(VoltMessage message)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking()
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(long timeout)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recv(Subject[] s)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s, long timeout)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public long getHSId()
    {
        return m_hsId;
    }

    @Override
    public void setHSId(long hsId)
    {
        this.m_hsId = hsId;
    }

    @Override
    public void becomeLeader()
    {
        String electionDirForPartition = VoltZK.electionDirForPartition(m_partitionId);
        m_role = new PrimaryRole();
        m_babySitter = new BabySitter(m_messenger.getZK(),
                                    electionDirForPartition,
                                    m_membershipChangeHandler);
        // It's not guaranteed that we'll have all the children at this time
        m_membershipChangeHandler.run(m_babySitter.lastSeenChildren());
    }
}
