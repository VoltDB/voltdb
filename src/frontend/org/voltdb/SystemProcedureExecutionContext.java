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
import org.voltdb.jni.ExecutionEngine;

public interface SystemProcedureExecutionContext {
    public Database getDatabase();

    public Cluster getCluster();

    public ExecutionEngine getExecutionEngine();

    public long getLastCommittedTxnId();

    public long getCurrentTxnId();

    public long getNextUndo();

    public ExecutionSite getExecutionSite();

    public HashMap<String, ProcedureRunner> getProcedures();

    public long getSiteId();

    public int getHostId();

    public int getPartitionId();

    public SiteTracker getSiteTracker();

    public int getNumberOfPartitions();
}
