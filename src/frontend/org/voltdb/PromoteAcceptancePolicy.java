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

/**
 * Acceptance policy for @Promote, simplistic for now since @Promote has a simple interface.
 */
public class PromoteAcceptancePolicy extends InvocationAcceptancePolicy {

    /**
     * Constructor
     * @param isOn Whether this policy should be on by default
     */
    public PromoteAcceptancePolicy(boolean isOn) {
        super(isOn);
    }

    /* (non-Javadoc)
     * @see org.voltdb.InvocationAcceptancePolicy#shouldAccept(org.voltdb.AuthSystem.AuthUser, org.voltdb.StoredProcedureInvocation, org.voltdb.SystemProcedureCatalog.Config)
     */
    @Override
    public ClientResponseImpl shouldAccept(AuthUser user,
            StoredProcedureInvocation invocation, Config sysProc) {
        if (!invocation.procName.equals("@Promote")) {
            return null;
        }

        // There are no parameters to validate. So for now we can blindly accept.
        return null;
    }

}
