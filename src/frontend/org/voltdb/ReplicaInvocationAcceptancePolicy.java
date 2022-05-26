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

import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Procedure;

/**
 * An invocation acceptance policy targeting all types of invocations in
 * secondary cluster. Secondary cluster only accepts read-only procedures from
 * normal clients, and write procedures from DR agent.
 */
public class ReplicaInvocationAcceptancePolicy extends InvocationValidationPolicy {
    public ReplicaInvocationAcceptancePolicy(boolean isOn) {
        super(isOn);  // isOn == TRUE means this is a Replica cluster.
    }


    private ClientResponseImpl shouldAcceptHelper(
            AuthUser user,
            StoredProcedureInvocation invocation,
            boolean isReadOnly)
    {
        if (!isOn) {
            return null;
        }

        // This path is only executed before the AdHoc statement is run through the planner. After the
        // Planner, the client interface will figure out what kind of statement this is.
        if (invocation.getProcName().equals("@AdHoc")) {
            return null;
        }

        if (isReadOnly) {
            return null;
        }

        return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                new VoltTable[0],
                "Write procedure " + invocation.getProcName() +
                " is not allowed in replica cluster",
                invocation.clientHandle);
    }

    @Override
    public ClientResponseImpl shouldAccept(AuthUser user, StoredProcedureInvocation invocation,
                                Procedure proc) {
        if (invocation == null || proc == null) {
            return null;
        }

        // Duplicate hack from ClientInterface
        String procName = invocation.getProcName();
        if (procName.equalsIgnoreCase("@UpdateClasses")) {
            procName = "@UpdateApplicationCatalog";
        }

        if (!procName.equalsIgnoreCase("@AdHoc")) {
            Config sysProc = SystemProcedureCatalog.listing.get(procName);
            if (sysProc != null && sysProc.allowedInReplica) {
                // white-listed sysprocs, adhoc is a special case
                return null;
            }
        }

        return shouldAcceptHelper(user, invocation, proc.getReadonly());
    }
}
