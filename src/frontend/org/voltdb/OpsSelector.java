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
package org.voltdb;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;

import static org.voltcore.messaging.HostMessenger.CLOCK_SKEW_COLLECTOR_ID;

/**
 * Conveniently encapsulate some of the data required for each OPS selector.
 */
public enum OpsSelector {
    SNAPSHOTDELETE(SnapshotDeleteAgent.class, HostMessenger.SNAPSHOTSCAN_SITE_ID),
    SNAPSHOTSCAN(SnapshotScanAgent.class, HostMessenger.SNAPSHOTDELETE_SITE_ID),
    STATISTICS(StatsAgent.class, HostMessenger.STATS_SITE_ID),
    SYSTEMCATALOG(SystemCatalogAgent.class, HostMessenger.SYSCATALOG_SITE_ID),
    SYSTEMINFORMATION(SystemInformationAgent.class, HostMessenger.SYSINFO_SITE_ID),
    TRACE(TraceAgent.class, HostMessenger.TRACE_SITE_ID),
    SYSTEM_CLOCK_SKEW(ClockSkewCollectorAgent.class, CLOCK_SKEW_COLLECTOR_ID);

    // OpsAgent subclass providing the implementation for this OPS selector
    private final Class<? extends OpsAgent> m_agentClass;
    // Well-known site ID for this OPS selector (I want to make this go away, but, no time)
    private final int m_siteId;

    private <T extends OpsAgent> OpsSelector(Class<T> agentClass, int siteId)
    {
        m_agentClass = agentClass;
        m_siteId = siteId;
    }

    public Class<? extends OpsAgent> getAgentClass()
    {
        return m_agentClass;
    }

    public int getSiteId()
    {
        return m_siteId;
    }

    /**
     * Convenience method: given the provided hostId, return the HSId for this OPS selector
     */
    public long getHSId(int hostId)
    {
        return CoreUtils.getHSIdFromHostAndSite(hostId, m_siteId);
    }
}
