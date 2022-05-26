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
 * Policy on whether or not to accept a type of stored procedure invocations. It
 * is used by the ClientInterface to determine whether or not to process an
 * invocation when it comes in from client. This is used for non-permission policies.
 *
 * Policy can be turned on or off. If a policy is off, it should always accept
 * all invocations.
 */
public abstract class InvocationValidationPolicy {
    protected volatile boolean isOn;

    /**
     * Turn the policy on or off.
     * @param isOn Whether this policy should be on
     */
    public void setMode(boolean isOn) {
        this.isOn = isOn;
    }

    /**
     * Constructor
     * @param isOn Whether this policy should be on by default
     */
    public InvocationValidationPolicy(boolean isOn) {
        this.isOn = isOn;
    }

    /**
     * Determine if we should accept a procedure invocation.
     *
     * Inheriting class should override this method if it's targeting a user
     * procedure invocation.
     *
     * @param user Current user
     * @param invocation The invocation
     * @param proc The procedure catalog object
     * @return ClientResponseImpl with error or null if accepted
     */
    public ClientResponseImpl shouldAccept(AuthUser user, StoredProcedureInvocation invocation,
            Procedure proc) {
        return null;
    }
}
