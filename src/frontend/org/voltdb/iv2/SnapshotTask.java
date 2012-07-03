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

import org.voltcore.logging.VoltLogger;

import org.voltdb.SiteProcedureConnection;

public class SnapshotTask extends SiteTasker
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public SnapshotTask()
    {
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
        siteConnection.doSnapshotWork(false);
    }

    @Override
    public void runForRejoin(SiteProcedureConnection siteConnection)
    {
        throw new RuntimeException("Snapshot task attempted snapshot on partial rejoin state.");
    }

    @Override
    public int priority()
    {
        return 0;
    }

}
