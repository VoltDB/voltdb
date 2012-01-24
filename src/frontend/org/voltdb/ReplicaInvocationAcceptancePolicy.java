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

import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.network.WriteStream;

/**
 * An invocation acceptance policy targeting all types of invocations in
 * secondary cluster. Secondary cluster only accepts read-only procedures from
 * normal clients, and write procedures from DR agent.
 */
public class ReplicaInvocationAcceptancePolicy extends InvocationAcceptancePolicy {
    public ReplicaInvocationAcceptancePolicy(boolean isOn) {
        super(isOn);
    }

    private ClientResponseImpl shouldAcceptHelper(AuthUser user, StoredProcedureInvocation invocation,
                                 boolean isReadOnly) {
        if (invocation.getType() == ProcedureInvocationType.ORIGINAL) {
            if (!isOn) {
                return null;
            }

            // hackish way to check if an adhoc query is read-only
            if (invocation.procName.equals("@AdHoc")) {
                String sql = (String) invocation.getParams().toArray()[0];
                isReadOnly = sql.trim().toLowerCase().startsWith("select");
            }

            if (isReadOnly) {
                return null;
            } else {
                return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        "Write procedure " + invocation.procName +
                        " is not allowed in replica cluster",
                        invocation.clientHandle);
            }
        } else {
            if (!isOn) {
                return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        "Replicated procedure " + invocation.procName +
                        " is dropped from cluster",
                        invocation.clientHandle);
            }

            if (isReadOnly) {
                return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        "Read replicated procedure " + invocation.procName +
                        " is dropped from replica cluster",
                        invocation.clientHandle);
            } else {
                return null;
            }
        }
    }

    @Override
    public ClientResponseImpl shouldAccept(AuthUser user, StoredProcedureInvocation invocation,
                                Procedure proc) {
        if (invocation == null || proc == null) {
            return null;
        }
        return shouldAcceptHelper(user, invocation, proc.getReadonly());
    }

    @Override
    public ClientResponseImpl shouldAccept(AuthUser user, StoredProcedureInvocation invocation, Config sysProc) {
        if (invocation.getType() == ProcedureInvocationType.ORIGINAL && sysProc.allowedInReplica &&
            !invocation.procName.equalsIgnoreCase("@AdHoc")) {
            // white-listed sysprocs, adhoc is a special case
            return null;
        }
        return shouldAcceptHelper(user, invocation, sysProc.readOnly);
    }
}
