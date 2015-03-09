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

package org.voltdb.iv2;

import java.io.IOException;

import org.voltdb.PartitionDRGateway;

import org.voltdb.rejoin.TaskLog;
import org.voltdb.SiteProcedureConnection;

/**
 * Pokes PartitionDRGateway once in a while to see if there are any data that
 * needs to be sent.
 */
public class DRTask extends SiteTasker {
    private final PartitionDRGateway m_gateway;

    public DRTask(PartitionDRGateway gateway) {
        m_gateway = gateway;
    }

    @Override
    public void run(SiteProcedureConnection siteConnection) {
        // IV2 doesn't use the txnId passed to tick, so pass a dummy value
        m_gateway.tick(0);
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
    throws IOException
    {
        // no-op during rejoin
    }

}
