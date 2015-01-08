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

import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ProcedureInvocationType;

/**
 * An invocation acceptance policy targeting all types of invocations in
 * secondary cluster. Secondary cluster only accepts read-only procedures from
 * normal clients, and write procedures from DR agent.
 */
public class ReplicaInvocationAcceptancePolicy extends InvocationValidationPolicy {
    public ReplicaInvocationAcceptancePolicy(boolean isOn) {
        super(isOn);  // isOn == TRUE means this is a Replica cluster.
    }


    private ClientResponseImpl shouldAcceptHelper(AuthUser user, StoredProcedureInvocation invocation,
                                 boolean isReadOnly) {
        // NOT a dragent invocation.
        if (invocation.getType() == ProcedureInvocationType.ORIGINAL) {
            if (!isOn) {
                return null;
            }

            // hackish way to check if an adhoc query is read-only
            //??? What keeps @AdHoc from invoking a read/write mixed batch that starts with a 'select'?
            //??? Or what would prevent these writes from getting missed here?
            if (invocation.procName.equals("@AdHoc") || invocation.procName.equals("@AdHocSpForTest")) {
                String sql = (String) invocation.getParams().toArray()[0];
                String initial = sql.trim().substring(0, 1);
                // Match "SELECT ... , "select ..." and the likes of "(select ... ) union ... "
                isReadOnly = "sS(".contains(initial);
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
        }

        // IS a dragent invocation
        else {
            if (!isOn) {
                return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        "Master cluster rejected dragent transaction " + invocation.procName
                        + ". A DR master cluster will not accept transactions from the dragent.",
                        invocation.clientHandle);
            }

            if (isReadOnly) {
                // This should be impossible since the dragent isn't passed read-only txns.
                return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        "Read-only procedure " + invocation.procName +
                        " was not replayed on replica cluster. " +
                        "Reads are not replicated across DR connections.",
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
        } else if (invocation.getType() == ProcedureInvocationType.ORIGINAL &&
                !invocation.procName.equalsIgnoreCase("@AdHoc")) {
            Config sysProc = SystemProcedureCatalog.listing.get(invocation.getProcName());
            if (sysProc != null && sysProc.allowedInReplica) {
                // white-listed sysprocs, adhoc is a special case
                return null;
            }
        }
        return shouldAcceptHelper(user, invocation, proc.getReadonly());
    }
}
