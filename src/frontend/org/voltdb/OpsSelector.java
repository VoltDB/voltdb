/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

public enum OpsSelector {
    // IZZY: UNMAGIC ME
    SNAPSHOTSCAN(SnapshotScanAgent.class, -7),
    STATISTICS(StatsAgent.class, HostMessenger.STATS_SITE_ID),
    SYSTEMCATALOG(SystemCatalogAgent.class, HostMessenger.SYSCATALOG_SITE_ID),
    SYSTEMINFORMATION(SystemInformationAgent.class, HostMessenger.SYSINFO_SITE_ID);

    private final Class<?> m_agentClass;
    private final int m_siteId;

    private OpsSelector(Class<?> agentClass, int siteId)
    {
        m_agentClass = agentClass;
        m_siteId = siteId;
    }

    public Class<?> getAgentClass()
    {
        return m_agentClass;
    }

    public int getSiteId()
    {
        return m_siteId;
    }

    public long getHSId(int hostId)
    {
        return CoreUtils.getHSIdFromHostAndSite(hostId, m_siteId);
    }
}
