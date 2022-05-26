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
import org.voltdb.catalog.Procedure;

/**
 * Checks if a user has permission to call a procedure.
 */
public class InvocationPermissionPolicy {

    enum PolicyResult {
        ALLOW,
        DENY,
        NOT_APPLICABLE,
    };

    public InvocationPermissionPolicy() {
    }

    /**
     *
     * shouldAccept will return ALLOW, DENY or NOT_APPLICABLE based on what is being evaluated with predicates.
     * @return ClientResponse or null if accepted.
     * @see org.voltdb.InvocationAcceptancePolicy#shouldAccept(org.voltdb.AuthSystem.AuthUser,
     *      org.voltdb.StoredProcedureInvocation, org.voltdb.catalog.Procedure,
     *      org.voltcore.network.WriteStream)
     */
    public PolicyResult shouldAccept(AuthUser user, StoredProcedureInvocation invocation, Procedure proc) {
        return null;
    }

    public ClientResponseImpl getErrorResponse(AuthUser user, StoredProcedureInvocation invocation, Procedure procedure) {
        return null;
    }

}
