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

import java.util.ArrayList;
import java.util.List;
import org.voltdb.catalog.Procedure;

/**
 * Validator that validates permissions.
 */
public class PermissionValidator {

    /**
     * Policies used to determine if we can accept an invocation based on given permission.
     */
    private final List<InvocationPermissionPolicy> m_permissionpolicies = new ArrayList<InvocationPermissionPolicy>();

    public PermissionValidator() {
        m_permissionpolicies.add(new InvocationSysprocPermissionPolicy());
        m_permissionpolicies.add(new InvocationSqlPermissionPolicy());
        m_permissionpolicies.add(new InvocationDefaultProcPermissionPolicy());
        m_permissionpolicies.add(new InvocationUserDefinedProcedurePermissionPolicy());
    }

    //Check permission policies first check all if any ALLOW go through DENY counts only if we didnt allow.
    //For auth disabled user the first policy will return ALLOW breaking the loop.
    public ClientResponseImpl shouldAccept(String name, AuthSystem.AuthUser user,
                                  final StoredProcedureInvocation task,
                                  final Procedure catProc) {
        if (user.isAuthEnabled()) {
            InvocationPermissionPolicy deniedPolicy = null;
            InvocationPermissionPolicy.PolicyResult res = InvocationPermissionPolicy.PolicyResult.DENY;
            for (InvocationPermissionPolicy policy : m_permissionpolicies) {
                res = policy.shouldAccept(user, task, catProc);
                if (res == InvocationPermissionPolicy.PolicyResult.ALLOW) {
                    deniedPolicy = null;
                    break;
                }
                if (res == InvocationPermissionPolicy.PolicyResult.DENY) {
                    if (deniedPolicy == null) {
                        //Take first denied response only.
                        deniedPolicy = policy;
                    }
                }
            }
            if (deniedPolicy != null) {
                return deniedPolicy.getErrorResponse(user, task, catProc);
            }
            //We must have an explicit allow on of the policy must grant access.
            assert(res == InvocationPermissionPolicy.PolicyResult.ALLOW);
            return null;
        }
        //User authentication is disabled. (auth disabled user)
        return null;
    }
}
