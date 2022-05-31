/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import org.voltdb.utils.CatalogUtil;

/**
 * Checks if a user has permission to call a compound procedure.
 */
public class InvocationCompoundProcedurePermissionPolicy extends InvocationPermissionPolicy {
    private static final VoltLogger authLog = new VoltLogger("AUTH");

    public InvocationCompoundProcedurePermissionPolicy() {
    }

    @Override
    public PolicyResult shouldAccept(AuthUser user, StoredProcedureInvocation invocation, Procedure proc) {

        if (!CatalogUtil.isCompoundProcedure(proc)) {
            return PolicyResult.NOT_APPLICABLE;
        }

        if (!user.hasPermission(Permission.COMPOUNDPROC)) {
            return PolicyResult.DENY;
        }
        return PolicyResult.ALLOW;
    }

    @Override
    public ClientResponseImpl getErrorResponse(AuthUser user, StoredProcedureInvocation invocation, Procedure procedure) {
        authLog.infoFmt("User %s lacks permission to invoke procedure %s",
                        user.m_name, invocation.getProcName());
        return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                new VoltTable[0],
                "User does not have permission to invoke compound procedure " + invocation.getProcName(),
                invocation.clientHandle);
    }

}
