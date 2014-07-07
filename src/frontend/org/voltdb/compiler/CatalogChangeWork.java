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

package org.voltdb.compiler;

import org.voltdb.client.ProcedureInvocationType;

public class CatalogChangeWork extends AsyncCompilerWork {
    private static final long serialVersionUID = -5257248292283453286L;

    final byte[] catalogBytes;
    final String deploymentString;
    final String[] adhocDDLStmts;

    public CatalogChangeWork(
            long replySiteId,
            long clientHandle, long connectionId, String hostname, boolean adminConnection,
            Object clientData, byte[] catalogBytes, String deploymentString,
            ProcedureInvocationType type, long originalTxnId, long originalUniqueId,
            boolean onReplica, boolean useAdhocDDL,
            AsyncCompilerWorkCompletionHandler completionHandler)
    {
        super(replySiteId, false, clientHandle, connectionId, hostname,
              adminConnection, clientData, type, originalTxnId, originalUniqueId,
              onReplica, useAdhocDDL,
              completionHandler);
        if (catalogBytes != null) {
            this.catalogBytes = catalogBytes.clone();
        }
        else {
            this.catalogBytes = null;
        }
        this.deploymentString = deploymentString;
        adhocDDLStmts = null;
    }

    /**
     * To process adhoc DDL, we want to convert the AdHocPlannerWork we received from the
     * ClientInterface into a CatalogChangeWork object for the AsyncCompilerAgentHelper to
     * grind on.
     */
    public CatalogChangeWork(AdHocPlannerWork adhocDDL)
    {
        super(adhocDDL.replySiteId,
              adhocDDL.shouldShutdown,
              adhocDDL.clientHandle,
              adhocDDL.connectionId,
              adhocDDL.hostname,
              adhocDDL.adminConnection,
              adhocDDL.clientData,
              adhocDDL.invocationType,
              adhocDDL.originalTxnId,
              adhocDDL.originalUniqueId,
              adhocDDL.onReplica,
              adhocDDL.useAdhocDDL,
              adhocDDL.completionHandler);
        // AsyncCompilerAgentHelper will fill in the current catalog bytes later.
        this.catalogBytes = null;
        // Ditto for deployment string
        this.deploymentString = null;
        this.adhocDDLStmts = adhocDDL.sqlStatements;
    }
}
