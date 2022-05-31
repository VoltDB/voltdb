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

import org.voltcore.logging.VoltLogger;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.catalog.Procedure;
import org.voltdb.common.Permission;

/**
 * Checks if a user has permission to call a procedure.
 */
public class InvocationSysprocPermissionPolicy extends InvocationPermissionPolicy {
    private static final VoltLogger authLog = new VoltLogger("AUTH");

    public InvocationSysprocPermissionPolicy() {
    }

    /**
     *
     * @param user whose permission needs to be checked.
     * @param invocation invocation associated with this request
     * @param proc procedure associated.
     * @return PolicyResult ALLOW, DENY or NOT_APPLICABLE
     * @see org.voltdb.InvocationAcceptancePolicy#shouldAccept(org.voltdb.AuthSystem.AuthUser,
     *      org.voltdb.StoredProcedureInvocation, org.voltdb.catalog.Procedure,
     *      org.voltcore.network.WriteStream)
     */
    @Override
    public PolicyResult shouldAccept(AuthUser user, StoredProcedureInvocation invocation, Procedure proc) {

        //Since AdHoc perms are diff we only check sysprocs other than AdHoc
        if (proc.getSystemproc() && !invocation.getProcName().startsWith("@AdHoc")) {
            if (!user.hasPermission(Permission.ADMIN) && !proc.getReadonly()) {
                return PolicyResult.DENY;
            }
            return PolicyResult.ALLOW;
        }
        return PolicyResult.NOT_APPLICABLE;
    }

    @Override
    public ClientResponseImpl getErrorResponse(AuthUser user, StoredProcedureInvocation invocation, Procedure procedure) {
        authLog.infoFmt("User %s lacks permission to invoke admin %s",
                        user.m_name, invocation.getProcName());
        return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                new VoltTable[0],
                "User " + user.m_name + " does not have admin permission",
                invocation.clientHandle);
    }

}
