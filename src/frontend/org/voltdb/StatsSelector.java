/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
package org.voltdb;

public enum StatsSelector {
    TABLE,            // invoked as @stat table
    INDEX,            // invoked as @stat index
    PROCEDURE,        // invoked as @stat procedure
    STARVATION,
    QUEUE,
    INITIATOR,        // invoked as @stat initiator
    LATENCY,          // invoked as @stat latency
    LATENCY_COMPRESSED,  // before V7.3 this was @Statistics LATENCY
    LATENCY_HISTOGRAM,
    PARTITIONCOUNT,
    IOSTATS,
    MEMORY,           // info about node's memory usage
    LIVECLIENTS,      // info about the currently connected clients
    PLANNER,          // info about planner and EE performance and cache usage
    MANAGEMENT,       // Returns pretty much everything
    PROCEDUREPROFILE, // performs an aggregation of the procedure statistics
    SNAPSHOTSTATUS,
    PROCEDUREINPUT,
    PROCEDUREOUTPUT,
    PROCEDUREDETAIL,  // provides more granular statistics for procedure calls at a per-statement level.

    /*
     * DRPRODUCERPARTITION and DRPRODUCERNODE are internal names
     * Externally the selector is "DRPRODUCER", or just "DR"
     */
    DR,
    DRPRODUCER,
    DRPRODUCERPARTITION,
    DRPRODUCERNODE,

    DRCONSUMER,
    DRCONSUMERNODE,
    DRCONSUMERPARTITION,

    DRROLE,

    TOPO,           // return leader and site info for iv2
    TTL,            // return time to live info
    REBALANCE,      // return elastic rebalance progress
    KSAFETY,        // return ksafety coverage information
    CPU,            // return CPU Stats
    GC,             // return GC Stats

    COMMANDLOG,     // return number of outstanding bytes and txns on this node
    IMPORTER
}
