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

package org.voltdb;

import java.util.HashMap;

import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.dtxn.SiteTracker;

public interface SystemProcedureExecutionContext {
    public Database getDatabase();

    public Cluster getCluster();

    public long getLastCommittedTxnId();

    public long getCurrentTxnId();

    public long getNextUndo();

    public HashMap<String, ProcedureRunner> getProcedures();

    public long getSiteId();

    // does this site have "lowest site id" responsibilities.
    public boolean isLowestSiteId();

    public int getHostId();

    public int getPartitionId();

    public long getCatalogCRC();

    public SiteTracker getSiteTracker();

    // Separate SiteTracker accessor for IV2 use.
    // Snapshot services that need this can get a SiteTracker in IV2, but
    // all other calls to the regular getSiteTracker() are going to throw.
    public SiteTracker getSiteTrackerForSnapshot();

    public int getNumberOfPartitions();

    public SiteProcedureConnection getSiteProcedureConnection();

    public SiteSnapshotConnection getSiteSnapshotConnection();

    public void updateBackendLogLevels();

    public boolean updateCatalog(String catalogDiffCommands, CatalogContext context);
}
