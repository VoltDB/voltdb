/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

/**
 * Check ad hoc query parameters.
 */
public class AdHocAcceptancePolicy extends InvocationAcceptancePolicy {
    public AdHocAcceptancePolicy(boolean isOn) {
        super(isOn);
    }

    @Override
    public ClientResponseImpl shouldAccept(AuthUser user,
            StoredProcedureInvocation invocation,
            Procedure sysProc) {

        if (!invocation.procName.equals("@AdHoc")) {
            return null;
        }

        ParameterSet params = invocation.getParams();
        // Make sure there is at least 1 parameter!  ENG-4921
        // Note the second secret param, so 1 or 2 params is legal.
        if (params.toArray().length < 1 || params.toArray().length > 2) {
            return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                    new VoltTable[0], "Adhoc system procedure requires exactly one or two parameters, " +
                    "the SQL statement to execute with an optional partitioning value.",
                    invocation.clientHandle);
        }

        // check the types are both strings
        if ((params.toArray()[0] instanceof String) == false) {
            return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "Adhoc system procedure requires sql in the String type only.",
                    invocation.clientHandle);
        }

        return null;
    }

}
