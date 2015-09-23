/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.zk;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/*
 * This state machine coexists with other SynchronizedStateManagers by using different branches of the
 * ZooKeeper tree. This class provides a service to the StateMachineInstance class that reports changes
 * to the membership. The ZooKeeper tree hierarchy is (quoted are reserved node names):
 *     |- rootNode
 *     |\
 *     | |- 'MEMBERS'
 *     |  \
 *     |   |- memberId
 *     \
 *      |- instanceName
 *      |\
 *      | |- 'LOCK_CONTENDERS'
 *      |\
 *      | |- 'BARRIER_PARTICIPANTS'
 *       \
 *        |- 'BARRIER_RESULTS'
 *
 * This hierarchy supports multiple topologies so one SynchronizedStatesManager could be used to track
 * cluster membership while another could be used to track partition membership. Each SynchronizedStatesManager
 * instance has a memberId that must be unique to the member community it is participating in. As the
 * SynchronizedStatesManager is a shared service that can provide membership changes of other
 * StateMachineInstances joining or leaving the community, all instances are considered tightly bound to the
 * SynchronizedStatesManager. This means that the SynchronizedStatesManager will not join a community until
 * all instances using that manager have registered with the SynchronizedStatesManager. Thus when a
 * SynchronizedStatesManager informs its instances that a new member has joined the community (the set of
 * nodes under the 'MEMBERS' node), the instance can be assured that it's peer instance is also there. This
 * implies that StateMachineInstances can only coexist in the same SynchronizedStatesManager if all instances
 * can be initialized (registered) before any individual instance needs to access the shared state.
 */
public class SynchronizedStatesManager {
    final AtomicBoolean m_done = new AtomicBoolean(false);
    public final static String MEMBERS = "MEMBERS";
    private Set<String> m_groupMembers = new HashSet<String>();
    private StateMachineInstance m_registeredStateMachines[];
    private int m_registeredStateMachineInstances = 0;
    static final ListeningExecutorService m_shared_es = CoreUtils.getListeningExecutorService("SSM Daemon", 1);
    // We assume that we are far enough along that the HostMessenger is up and running. Otherwise add to constructor.
    final ZooKeeper m_zk;
    final String m_ssmRootNode;
    final String m_stateMachineRoot;
    final String m_stateMachineMemberPath;
    final String m_memberId;

    protected boolean addIfMissing(String absolutePath, CreateMode createMode, byte[] data)
            throws KeeperException, InterruptedException {
        return ZKUtil.addIfMissing(m_zk, absolutePath, createMode, data);
    }

    protected String getMemberId() {
        return m_memberId;
    }

    public SynchronizedStatesManager(ZooKeeper zk, String rootPath, String ssmNodeName, String memberId) {
        m_zk = zk;
        // We will not add ourselves as members in ZooKeeper until all StateMachineInstances have registered
        m_ssmRootNode = ssmNodeName;
        m_stateMachineRoot = ZKUtil.joinZKPath(rootPath, ssmNodeName);
        m_stateMachineMemberPath = ZKUtil.joinZKPath(m_stateMachineRoot, MEMBERS);
        m_memberId = memberId;
    }

    // Used only for Mocking StateMachineInstance
    public SynchronizedStatesManager() {
        m_zk = null;
        m_ssmRootNode = "MockRootForZooKeeper";
        m_stateMachineRoot = "MockRootForSSM";
        m_stateMachineMemberPath = "MockRootMembershipNode";
        m_memberId = "MockMemberId";
    }

    public void initialize(int registeredInstances) throws KeeperException, InterruptedException {
        ByteBuffer numberOfInstances = ByteBuffer.allocate(4);
        numberOfInstances.putInt(registeredInstances);
        m_registeredStateMachines = new StateMachineInstance[registeredInstances];
        addIfMissing(m_stateMachineRoot, CreateMode.PERSISTENT, numberOfInstances.array());
    }

    public void ShutdownSynchronizedStatesManager() throws InterruptedException {
        ListenableFuture<?> disableComplete = m_shared_es.submit(disableInstances);
        try {
            disableComplete.get();
        }
        catch (ExecutionException e) {
            Throwables.propagate(e.getCause());
        }

    }

    private final Runnable disableInstances = new Runnable() {
        @Override
        public void run() {
            try {
                for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                    stateMachine.disableMembership();
                }
                m_done.set(true);
                m_zk.delete(ZKUtil.joinZKPath(m_stateMachineMemberPath, m_memberId), -1);
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                m_done.set(true);
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                m_done.set(true);
            } catch (InterruptedException e) {
                m_done.set(true);
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in SynchronizedStatesManager.", true, e);
                m_done.set(true);
            }
        }
    };

    private final Runnable membershipEventHandler = new Runnable() {
        @Override
        public void run() {
            try {
                checkForMembershipChanges();
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
            } catch (InterruptedException e) {
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in SynchronizedStatesManager.", true, e);
            }
        }
    };

    private class MembershipWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            try {
                if (!m_done.get()) {
                    for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                        stateMachine.checkMembership();
                    }
                    m_shared_es.submit(membershipEventHandler);
                }
            } catch (RejectedExecutionException e) {
            }
        }
    }

    private final MembershipWatcher m_membershipWatcher = new MembershipWatcher();

    private final Runnable initializeInstances = new Runnable() {
        @Override
        public void run() {
            try {
                byte[] expectedInstances = m_zk.getData(m_stateMachineRoot, false, null);
                assert(m_registeredStateMachineInstances == ByteBuffer.wrap(expectedInstances).getInt());
                // First become a member of the community
                addIfMissing(m_stateMachineMemberPath, CreateMode.PERSISTENT, null);
                // This could fail because of an old ephemeral that has not aged out yet but assume this does not happen
                addIfMissing(ZKUtil.joinZKPath(m_stateMachineMemberPath, m_memberId), CreateMode.EPHEMERAL, null);

                m_groupMembers = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, m_membershipWatcher));
                // Then initialize each instance
                for (StateMachineInstance instance : m_registeredStateMachines) {
                    instance.initializeStateMachine(m_groupMembers);
                }
            } catch (KeeperException.SessionExpiredException e) {
                // lost the full connection. some test cases do this...
                // means zk shutdown without the elector being shutdown.
                // ignore.
                m_done.set(true);
            } catch (KeeperException.ConnectionLossException e) {
                // lost the full connection. some test cases do this...
                // means shutdown without the elector being
                // shutdown; ignore.
                m_done.set(true);
            } catch (InterruptedException e) {
                m_done.set(true);
            } catch (Exception e) {
                org.voltdb.VoltDB.crashLocalVoltDB(
                        "Unexepected failure in initializeInstances.", true, e);
                m_done.set(true);
            }
        }
    };

    public synchronized StateMachineInstance getStateMachine(int idx) throws InterruptedException {
        assert(idx < m_registeredStateMachines.length);
        return m_registeredStateMachines[idx];
    }

    public synchronized void registerStateMachine(StateMachineInstance machine) throws InterruptedException {
        assert(m_registeredStateMachineInstances < m_registeredStateMachines.length);

        m_registeredStateMachines[m_registeredStateMachineInstances] = machine;
        m_registeredStateMachineInstances++;
        if (m_registeredStateMachineInstances == m_registeredStateMachines.length) {
            if (machine.m_log.isDebugEnabled()) {
                // lets make sure all the state machines are using unique paths
                Set<String> instanceNames = new HashSet<String>();
                for (StateMachineInstance instance : m_registeredStateMachines) {
                    if (!instanceNames.add(instance.m_statePath)) {
                        machine.m_log.error(": Multiple state machine instances with the same instanceName [" +
                                instance.m_statePath + "]");
                    }
                }
            }
            // Do all the initialization and notifications on the executor to avoid a race with
            // the group membership changes
            ListenableFuture<?> initComplete = m_shared_es.submit(initializeInstances);
            try {
                initComplete.get();
            }
            catch (ExecutionException e) {
                Throwables.propagate(e.getCause());
            }
        }
    }

    /*
     * Track state machine membership. If it changes, notify all state machine instances
     */
    private void checkForMembershipChanges() throws KeeperException, InterruptedException {
        Set<String> children = ImmutableSet.copyOf(m_zk.getChildren(m_stateMachineMemberPath, m_membershipWatcher));

        Set<String> removedMembers;
        Set<String> addedMembers;
        if (m_registeredStateMachineInstances == m_registeredStateMachines.length && !m_groupMembers.equals(children)) {
            removedMembers = Sets.difference(m_groupMembers, children);
            addedMembers = Sets.difference(children, m_groupMembers);
            m_groupMembers = children;
            for (StateMachineInstance stateMachine : m_registeredStateMachines) {
                stateMachine.membershipChanged(m_groupMembers, addedMembers, removedMembers);
            }
        }
    }

}
