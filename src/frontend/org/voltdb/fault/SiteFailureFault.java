/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.fault;

import java.util.List;

import org.voltcore.utils.CoreUtils;

public class SiteFailureFault extends VoltFault
{
    public static int SITE_FAILURE_FOREIGN_HOST = 0;
    public static int SITE_FAILURE_CATALOG = 1;
    public static int SITE_FAILURE_INITIATOR = 2;
    public static int SITE_FAILURE_EXECUTION_SITE = 3;


    public SiteFailureFault(List<Long> siteIds)
    {
        super(FaultType.SITE_FAILURE);
        m_siteIds = siteIds;
    }

    public List<Long> getSiteIds()
    {
        return m_siteIds;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("SiteFailureFault:\n");
        sb.append("  Site Ids: " + CoreUtils.hsIdCollectionToString(m_siteIds) + "\n");
        sb.append(super.toString());

        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof SiteFailureFault) {
            SiteFailureFault ofault = (SiteFailureFault)other;
            return ofault.getSiteIds().equals(m_siteIds);
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (m_hashCode != null) {
            return m_hashCode;
        }
        int hashCode = 0;
        for (Long sid : m_siteIds) {
            hashCode += sid.hashCode();
        }
        m_hashCode = hashCode;
        return m_hashCode;
    }

    private Integer m_hashCode;
    private final List<Long> m_siteIds;
}
