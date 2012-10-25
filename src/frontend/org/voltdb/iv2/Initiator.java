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
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.MemoryStats;
import org.voltdb.NodeDRGateway;
import org.voltdb.CommandLog;
import org.voltdb.StatsAgent;
import org.voltdb.VoltDB;

/**
 * Abstracts the top-level interface to create and configure an Iv2
 * MP or SP initiator.
 */
public interface Initiator
{
    /** Configure an Initiator and prepare it for work */
    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          int kfactor, CatalogSpecificPlanner csp,
                          int numberOfPartitions,
                          VoltDB.START_ACTION startAction,
                          StatsAgent agent,
                          MemoryStats memStats,
                          CommandLog cl,
                          NodeDRGateway nodeDRGateway)
        throws KeeperException, InterruptedException, ExecutionException;

    /** Shutdown an Initiator and its sub-components. */
    public void shutdown();

    /** Ask for the HSId used to address this Initiator. */
    public long getInitiatorHSId();

    /** This initiator participates in rejoin processing */
    public boolean isRejoinable();

    /** Create a Term implementation appropriate for the subclass */
    public Term createTerm(ZooKeeper zk, int partitionId, long initiatorHSId, InitiatorMailbox mailbox,
            String whoami);

    /** Create a Promotion implementation appropriate for the subclass */
    public RepairAlgo createPromoteAlgo(List<Long> survivors, InitiatorMailbox mailbox,
            String whoami);

    /** Write a viable replay set to the command log */
    public void enableWritingIv2FaultLog();
}
