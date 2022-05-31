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

public class InvocationSqlPermissionPolicy extends InvocationPermissionPolicy {
    private static final VoltLogger authLog = new VoltLogger("AUTH");

    public InvocationSqlPermissionPolicy() {
    }

    /**
     *
     * @see org.voltdb.InvocationAcceptancePolicy#shouldAccept(org.voltdb.AuthSystem.AuthUser,
     *      org.voltdb.StoredProcedureInvocation, org.voltdb.catalog.Procedure,
     *      org.voltcore.network.WriteStream)
     */
    @Override
    public PolicyResult shouldAccept(AuthUser user, StoredProcedureInvocation invocation, Procedure proc) {
        if (proc.getSystemproc() && invocation.getProcName().startsWith("@AdHoc_RW")) {
            if (user.hasPermission(Permission.SQL)) {
                return PolicyResult.ALLOW;
            }
            return PolicyResult.DENY;
        }
        if (proc.getSystemproc() && invocation.getProcName().startsWith("@AdHoc")) {
            if (user.hasPermission(Permission.SQLREAD)) {
                return PolicyResult.ALLOW;
            }
            return PolicyResult.DENY;
        }

        return PolicyResult.NOT_APPLICABLE;
    }

    @Override
    public ClientResponseImpl getErrorResponse(AuthUser user, StoredProcedureInvocation invocation, Procedure procedure) {
        authLog.infoFmt("User %s lacks permission to invoke @AdHoc", user.m_name);
        return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                new VoltTable[0], "User does not have SQL read/write permission",
                invocation.clientHandle);
    }
}
