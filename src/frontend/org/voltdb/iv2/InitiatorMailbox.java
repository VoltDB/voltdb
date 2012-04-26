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

import org.apache.zookeeper_voltpatches.KeeperException;
import org.voltcore.logging.VoltLogger;
import org.voltcore.agreement.BabySitter;
import org.voltcore.agreement.BabySitter.Callback;
import org.voltcore.agreement.LeaderElector;
import org.voltcore.zk.LeaderNoticeHandler;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class InitiatorMailbox implements Mailbox, LeaderNoticeHandler
{
    VoltLogger hostLog = new VoltLogger("HOST");
    private final int partitionId;
    private final HostMessenger messenger;
    private long hsId;

    InitiatorRole role;
    private LeaderElector elector;
    // only primary initiator has the following two set
    private BabySitter babySitter = null;
    private volatile long[] replicas = null;
    Callback membershipChangeHandler = new Callback()
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
                    if (HSId != hsId) {
                        tmpArray[i++] = HSId;
                    }
                }
                catch (NumberFormatException e) {
                    hostLog.error("Unable to get the HSId of initiator replica " + child);
                    return;
                }
            }
            replicas = tmpArray;
            ((PrimaryRole) role).setReplicas(replicas);
        }
    };

    public InitiatorMailbox(HostMessenger messenger, int partitionId)
    {
        this.messenger = messenger;
        this.partitionId = partitionId;
    }

    /**
     * Start leader election
     * @throws Exception
     */
    public void start(int totalReplicasForPartition) throws Exception
    {
        // by this time, we should have our HSId
        role = new ReplicatedRole(hsId);

        String electionDirForPartition = VoltZK.electionDirForPartition(partitionId);
        elector = new LeaderElector(
                messenger.getZK(),
                electionDirForPartition,
                Long.toString(this.hsId), // prefix
                null,
                this);
        // This will invoke becomeLeader()
        elector.start(true);

        if (elector.isLeader()) {
            // barrier to wait for all replicas to be ready
            boolean success = false;
            for (int ii = 0; ii < 4000; ii++) {
                List<String> children = babySitter.lastSeenChildren();
                if (children == null || children.size() < totalReplicasForPartition) {
                    Thread.sleep(5);
                } else {
                    success = true;
                    break;
                }
            }
            if (!success) {
                VoltDB.crashLocalVoltDB("Not all replicas for partition " +
                        partitionId + " are ready in time", false, null);
            }
        }
    }

    public void shutdown() throws InterruptedException, KeeperException
    {
        if (babySitter != null) {
            babySitter.shutdown();
        }
        if (elector != null) {
            elector.shutdown();
        }
    }

    @Override
    public void send(long destHSId, VoltMessage message) throws MessagingException
    {
        message.m_sourceHSId = this.hsId;
        messenger.send(destHSId, message);
    }

    @Override
    public void send(long[] destHSIds, VoltMessage message) throws MessagingException
    {
        message.m_sourceHSId = this.hsId;
        messenger.send(destHSIds, message);
    }

    @Override
    public void deliver(VoltMessage message)
    {
        if (message instanceof InitiateTaskMessage) {
            // this will assign txnId if we are the leader
            synchronized (role) {
                role.offerInitiateTask((InitiateTaskMessage)message);
                if (elector.isLeader()) {
                    replicateInitiation((InitiateTaskMessage)message);
                }
            }
        }
        else if (message instanceof InitiateResponseMessage) {
            InitiateResponseMessage response = (InitiateResponseMessage)message;
            Pair<Long, InitiateResponseMessage> nextResponse = role.offerResponse(response);
            /*
             * In case the response needs to be forwarded. Replicas forward the
             * responses to the primary initiator. Primary initiator forwards
             * responses to the client interface.
             */
            if (nextResponse != null) {
                try {
                    send(nextResponse.getFirst(), nextResponse.getSecond());
                }
                catch (MessagingException e) {
                    hostLog.error("Failed to deliver response from execution site.", e);
                }
            }
        }
    }

    /**
     * Forwards the initiate task message to the replicas. Only the primary
     * initiator has to do this.
     *
     * @param message
     */
    private void replicateInitiation(InitiateTaskMessage message)
    {
        if (replicas != null) {
            try {
                send(replicas, message);
            }
            catch (MessagingException e) {
                hostLog.error("Failed to replicate initiate task.", e);
            }
        }
    }

    @Override
    public VoltMessage recv()
    {
        return role.poll();
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
        return hsId;
    }

    @Override
    public void setHSId(long hsId)
    {
        this.hsId = hsId;
    }

    @Override
    public void becomeLeader()
    {
        String electionDirForPartition = VoltZK.electionDirForPartition(partitionId);
        role = new PrimaryRole();
        babySitter = new BabySitter(messenger.getZK(),
                                    electionDirForPartition,
                                    membershipChangeHandler);
        // It's not guaranteed that we'll have all the children at this time
        membershipChangeHandler.run(babySitter.lastSeenChildren());
    }
}
