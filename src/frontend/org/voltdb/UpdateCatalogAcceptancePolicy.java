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
import org.voltdb.utils.Encoder;

/**
 * Check update catalog parameters.
 */
public class UpdateCatalogAcceptancePolicy extends InvocationAcceptancePolicy {
    public UpdateCatalogAcceptancePolicy(boolean isOn) {
        super(isOn);
    }

    @Override
    public ClientResponseImpl shouldAccept(AuthUser user,
                                StoredProcedureInvocation invocation,
                                Config sysProc) {
        if (!invocation.procName.equals("@UpdateApplicationCatalog")) {
            return null;
        }

        ParameterSet params = invocation.getParams();
        if (params.m_params.length != 2 ||
            params.m_params[0] == null ||
            params.m_params[1] == null)
        {
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0],
                    "UpdateApplicationCatalog system procedure requires exactly " +
                    "two parameters, the catalog bytes and the deployment file " +
                    "string.",
                    invocation.clientHandle);
        }

        boolean isHex = false;
        if (params.m_params[0] instanceof String) {
            isHex = Encoder.isHexEncodedString((String) params.m_params[0]);
        }
        if (!isHex && !(params.m_params[0] instanceof byte[])) {
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0],
                    "UpdateApplicationCatalog system procedure takes the " +
                    "catalog bytes as a byte array. The received parameter " +
                    "is of type " + params.m_params[0].getClass() + ".",
                    invocation.clientHandle);
        }

        return null;
    }
}
