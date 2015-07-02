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

package org.voltdb;

import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.SystemStatsCollector.Datum;

public class ResourceUsageMonitor implements Runnable
{
    private int m_rssLimit;

    public ResourceUsageMonitor(SystemSettingsType systemSettings)
    {
        if (systemSettings!=null && systemSettings.getMemorylimit()!=null) {
            // configured value is in GB. Convert it to MB
            m_rssLimit = systemSettings.getMemorylimit().getSize()*1024;
        }
    }

    public boolean hasResourceLimitsConfigured()
    {
        return (m_rssLimit>0);
    }

    @Override
    public void run()
    {
        if (VoltDB.instance().getMode()!=OperationMode.RUNNING) {
            return;
        }

        Datum datum = SystemStatsCollector.getRecentSample();
        if (datum==null) { // this will be null if stats has not run yet
            return;
        }

        if (datum.rss>=m_rssLimit && VoltDB.instance().getMode()==OperationMode.RUNNING) {
            VoltDB.instance().getClientInterface().getInternalConnectionHandler().callProcedure(0, "@Pause");
        }
    }
}
