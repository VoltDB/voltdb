/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.importer;

import org.voltdb.CatalogContext;

/**
 * For regression test use only. An ImportManager that has fewer dependencies and can be used standalone for testing.
 * Created by bshaw on 5/26/17.
 */
public class ImportManagerWithMocks extends ImportManager {

    /** Creates partially mocked-out ImportManager for standalone use.
     * @param myHostId Fake Host ID (0 is fine)
     * @param mockChannelDistributer Fake ChannelDistributer. The test can use it to simulate mesh events, and this eliminates a dependency on Zookeeper.
     * @param myStatsCollector Real ImporterStatsCollector that test can use to get data if it wants. This class has no onerous dependencies so no need to mock it.
     */
    public ImportManagerWithMocks(int myHostId, CatalogContext initialCatalogCtxt, ChannelDistributer mockChannelDistributer, ImporterStatsCollector myStatsCollector) {
        super(myHostId, null, myStatsCollector);

        // from ImportManager.initialize()
        m_self = this;
        create(myHostId, initialCatalogCtxt);
    }


    @Override
    protected void initializeChannelDistributer()  {
        if (m_distributer != null) return;

        m_distributer = new MockChannelDistributer(String.valueOf(m_myHostId));
        m_distributer.registerCallback("__IMPORT_MANAGER__", this);
    }
}
