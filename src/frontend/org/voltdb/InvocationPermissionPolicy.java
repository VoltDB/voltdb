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

package org.voltdb;

import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Procedure;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.LogKeys;

/**
 * Checks if a user has permission to call a procedure.
 */
public class InvocationPermissionPolicy extends InvocationAcceptancePolicy {
    private static final VoltLogger authLog = new VoltLogger("AUTH");

    public InvocationPermissionPolicy(boolean isOn) {
        super(isOn);
    }

    /**
     * Determine whether or not the current user has permission to call this procedure.
     * AuthSystem.hasPermission() handles both user-written procedures and default
     * auto-generated ones.
     *
     * @see org.voltdb.InvocationAcceptancePolicy#shouldAccept(org.voltdb.AuthSystem.AuthUser,
     *      org.voltdb.StoredProcedureInvocation, org.voltdb.catalog.Procedure,
     *      org.voltcore.network.WriteStream)
     */
    @Override
    public ClientResponseImpl shouldAccept(AuthUser user,
            StoredProcedureInvocation invocation,
            Procedure proc) {
        if (proc.getSystemproc()) {
            if (invocation.procName.startsWith("@AdHoc")) {
                // AdHoc requires unique permission. Then has to plan in a separate thread.
                if (!user.hasAdhocPermission()) {
                    authLog.l7dlog(Level.INFO,
                            LogKeys.auth_ClientInterface_LackingPermissionForAdhoc.name(),
                            new String[] {user.m_name}, null);
                    return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                            new VoltTable[0], "User does not have @AdHoc permission",
                            invocation.clientHandle);
                }
            } else if (!user.hasSystemProcPermission()) {
                authLog.l7dlog(Level.INFO,
                        LogKeys.auth_ClientInterface_LackingPermissionForSysproc.name(),
                        new String[] { user.m_name, invocation.procName },
                        null);
                return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        "User " + user.m_name + " does not have sysproc permission",
                        invocation.clientHandle);
            }
        } else {
            if (!user.hasPermission(proc)) {
                authLog.l7dlog(Level.INFO,
                        LogKeys.auth_ClientInterface_LackingPermissionForProcedure.name(),
                        new String[] { user.m_name, invocation.procName }, null);
                return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        "User does not have permission to invoke " + invocation.procName,
                        invocation.clientHandle);
            }
        }
        return null;
    }
}
