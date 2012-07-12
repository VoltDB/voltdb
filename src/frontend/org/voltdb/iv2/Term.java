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
import java.util.Collection;
import java.util.Comparator;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.BabySitter;
import org.voltcore.zk.BabySitter.Callback;
import org.voltcore.zk.LeaderElector;
import org.voltcore.zk.MapCache;
import org.voltcore.zk.MapCacheWriter;

import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

import com.google.common.collect.Sets;

// Some comments on threading and organization.
//   start() returns a future. Block on this future to get the final answer.
//
//   deliver() runs in the initiator mailbox deliver() context and triggers
//   all repair work.
//
//   replica change handler runs in the babysitter thread context.
//   replica change handler invokes a method in init.mbox that also
//   takes the init.mbox deliver lock
//
//   it is important that repair work happens with the deliver lock held
//   and that updatereplicas also holds this lock -- replica failure during
//   repair must happen unambigously before or after each local repair action.
//
//   A Term can be cancelled by initiator mailbox while the deliver lock is
//   held. Repair work must check for cancellation before producing repair
//   actions to the mailbox.
//
//   Note that a term can not prevent messages being delivered post cancellation.
//   RepairLog requests therefore use a requestId to dis-ambiguate responses
//   for cancelled requests that are filtering in late.


/**
 * Term encapsulates the process/algorithm of becoming
 * a new PI and the consequent ZK observers for performing that
 * role.
 */
public interface Term
{
    VoltLogger tmLog = new VoltLogger("TM");

    /**
     * Start a new Term. Returns a future that is done when the leadership has
     * been fully assumed and all surviving replicas have been repaired.
     *
     * @param kfactorForStartup If running for startup and not for fault
     * recovery, pass the kfactor required to proceed. For fault recovery,
     * pass any negative value as kfactorForStartup.
     */
    abstract public Future<Boolean> start();

    abstract public boolean cancel();

    abstract public void shutdown();

    /** Process a new repair log response */
    abstract public void deliver(VoltMessage message);

    /** Have all survivors supplied a full repair log? */
    abstract public boolean areRepairLogsComplete();

    /** Send missed-messages to survivors. Exciting! */
    abstract public void repairSurvivors();
}
