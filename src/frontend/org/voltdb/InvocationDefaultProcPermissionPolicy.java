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

import org.voltcore.logging.Level;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.catalog.Procedure;
import org.voltcore.logging.VoltLogger;
import org.voltdb.common.Permission;
import org.voltdb.utils.LogKeys;

/**
 * Checks if a user has permission to call a procedure.
 */
public class InvocationDefaultProcPermissionPolicy extends InvocationPermissionPolicy {
    private static final VoltLogger authLog = new VoltLogger("AUTH");

    public InvocationDefaultProcPermissionPolicy() {
    }

    /**
     *
     * @see org.voltdb.InvocationAcceptancePolicy#shouldAccept(org.voltdb.AuthSystem.AuthUser,
     *      org.voltdb.StoredProcedureInvocation, org.voltdb.catalog.Procedure,
     *      org.voltcore.network.WriteStream)
     */
    @Override
    public PolicyResult shouldAccept(AuthUser user, StoredProcedureInvocation invocation, Procedure proc) {

        if (proc.getDefaultproc()) {
            boolean res = false;
            if (!proc.getReadonly()) {
                res = user.hasPermission(Permission.DEFAULTPROC);
            } else {
                res = user.hasPermission(Permission.DEFAULTPROCREAD);
            }
            if (!res) {
                return PolicyResult.DENY;
            }
            return PolicyResult.ALLOW;
        }

        return PolicyResult.NOT_APPLICABLE;
    }

    @Override
    public ClientResponseImpl getErrorResponse(AuthUser user, StoredProcedureInvocation invocation, Procedure procedure) {
        authLog.l7dlog(Level.INFO,
                LogKeys.auth_ClientInterface_LackingPermissionForProcedure.name(),
                new String[] {user.m_name, invocation.procName}, null);
        return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                new VoltTable[0], "User does not have permission to invoke " + invocation.procName,
                invocation.clientHandle);
    }

}
