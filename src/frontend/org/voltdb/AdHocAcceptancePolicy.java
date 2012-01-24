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
import org.voltdb.network.WriteStream;

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
            Config sysProc) {

        if (!invocation.procName.equals("@AdHoc")) {
            return null;
        }

        ParameterSet params = invocation.getParams();
        // note the second secret param
        if (params.m_params.length > 2) {
            return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                    new VoltTable[0], "Adhoc system procedure requires exactly one or two parameters, " +
                    "the SQL statement to execute with an optional partitioning value.",
                    invocation.clientHandle);
        }
        // check the types are both strings
        if ((params.m_params[0] instanceof String) == false) {
            return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "Adhoc system procedure requires sql in the String type only.",
                    invocation.clientHandle);
        }

        return null;
    }

}
