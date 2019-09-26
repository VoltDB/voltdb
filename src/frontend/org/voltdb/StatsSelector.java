/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

/**
 * Defines which stats should be collected when this enum is passed as an argument to the procedure {@code @Statistics}
 */
public enum StatsSelector {
    TABLE,
    INDEX,
    PROCEDURE,
    STARVATION,
    QUEUE,
    IDLETIME(STARVATION),
    INITIATOR,
    LATENCY(false),
    LATENCY_COMPRESSED,  // before V7.3 this was @Statistics LATENCY
    LATENCY_HISTOGRAM,
    PARTITIONCOUNT,
    IOSTATS,
    MEMORY,           // info about node's memory usage
    LIVECLIENTS,      // info about the currently connected clients
    PLANNER,          // info about planner and EE performance and cache usage
    CPU,            // return CPU Stats
    MANAGEMENT(MEMORY, INITIATOR, PROCEDURE, IOSTATS, TABLE, INDEX, STARVATION, QUEUE, CPU), // Returns pretty much everything
    SNAPSHOTSTATUS(false),
    PROCEDUREPROFILE(PROCEDURE), // performs an aggregation of the procedure statistics
    PROCEDUREINPUT(PROCEDURE),
    PROCEDUREOUTPUT(PROCEDURE),
    PROCEDUREDETAIL(PROCEDURE),  // provides more granular statistics for procedure calls at a per-statement level.

    /*
     * DRPRODUCERPARTITION and DRPRODUCERNODE are internal names
     * Externally the selector is "DRPRODUCER", or just "DR"
     */
    DRPRODUCERPARTITION(false),
    DRPRODUCERNODE(false),
    DR(DRPRODUCERPARTITION, DRPRODUCERNODE),
    DRPRODUCER(DR.subSelectors()),

    DRCONSUMERNODE(false),
    DRCONSUMERPARTITION(false),
    DRCONSUMER(DRCONSUMERNODE, DRCONSUMERPARTITION),

    DRROLE(false),

    TOPO,           // return leader and site info for iv2
    TTL,            // return time to live info
    REBALANCE,      // return elastic rebalance progress
    KSAFETY,        // return ksafety coverage information
    GC,             // return GC Stats

    COMMANDLOG(false),     // return number of outstanding bytes and txns on this node
    IMPORTER,       // synonym as IMPORT for backward compatibility
    IMPORT(IMPORTER),
    EXPORT,
    TASK(false),
    TASK_SCHEDULER(TASK),
    TASK_PROCEDURE(TASK);

    /** Whether or not this stat supports interval collection */
    private final boolean m_supportsInterval;
    /** Mapping to actual stat(s) to collect which are registered in the system */
    private final StatsSelector[] m_subSelctors;

    private StatsSelector(boolean supportsInterval, StatsSelector... subSelectors) {
        m_supportsInterval = supportsInterval;
        m_subSelctors = subSelectors == null ? new StatsSelector[] { this } : subSelectors;
    }

    private StatsSelector(boolean supportsInterval) {
        this(supportsInterval, (StatsSelector[]) null);
    }

    private StatsSelector(StatsSelector... subSelectors) {
        this(true, subSelectors);
    }

    private StatsSelector() {
        this(true);
    }

    public boolean interval(boolean interval) {
        return m_supportsInterval && interval;
    }

    StatsSelector[] subSelectors() {
        return m_subSelctors;
    }

    public static StatsSelector[] getAllStatsCollector() {
        return StatsSelector.class.getEnumConstants();
    }
}
