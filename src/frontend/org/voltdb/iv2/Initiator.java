/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.iv2;

import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CommandLog;
import org.voltdb.MemoryStats;
import org.voltdb.ProducerDRGateway;
import org.voltdb.StartAction;
import org.voltdb.StatsAgent;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;

/**
 * Abstracts the top-level interface to create and configure an Iv2
 * MP or SP initiator.
 */
public interface Initiator
{
    /** Configure an Initiator and prepare it for work */
    public void configure(BackendTarget backend,
                          CatalogContext catalogContext,
                          String serializedCatalog,
                          int numberOfPartitions,
                          StartAction startAction,
                          StatsAgent agent,
                          MemoryStats memStats,
                          CommandLog cl,
                          String coreBindIds,
                          boolean isLowestSiteId)
        throws KeeperException, InterruptedException, ExecutionException;

    /** Create DR gateway */
    public void initDRGateway(StartAction startAction,
                              ProducerDRGateway nodeDRGateway,
                              boolean createMpDRGateway);

    /** Shutdown an Initiator and its sub-components. */
    public void shutdown();

    /** Ask for the partition ID this initiator is assigned to */
    public int getPartitionId();

    /** Ask for the HSId used to address this Initiator. */
    public long getInitiatorHSId();

    /** This initiator participates in rejoin processing */
    public boolean isRejoinable();

    /** Create a Term implementation appropriate for the subclass */
    public Term createTerm(ZooKeeper zk, int partitionId, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami);

    /** Write a viable replay set to the command log */
    public void enableWritingIv2FaultLog();

    /** Assign or remove a listener to/from the spScheduler for notification of CommandLogged (durable) UniqueIds */
    public void configureDurableUniqueIdListener(DurableUniqueIdListener listener, boolean install);
}
