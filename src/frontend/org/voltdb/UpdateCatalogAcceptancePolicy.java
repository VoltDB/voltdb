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
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Encoder;

/**
 * Check update catalog parameters.
 */
public class UpdateCatalogAcceptancePolicy extends InvocationAcceptancePolicy {

    public static final String COMMUNITY_MISSING_UAC_ERROR_MSG =
            "@UpdateApplicationCatalog is an Enterprise-only feature. " +
            "It is not supported in the VoltDB Community Edition.";

    public UpdateCatalogAcceptancePolicy(boolean isOn) {
        super(isOn);
    }

    @Override
    public ClientResponseImpl shouldAccept(AuthUser user,
                                StoredProcedureInvocation invocation,
                                Procedure sysProc) {
        if (!invocation.procName.equals("@UpdateApplicationCatalog")) {
            return null;
        }

        // give a nice error message for the community edition
        if (VoltDB.instance().getConfig().m_isEnterprise == false) {
            return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    COMMUNITY_MISSING_UAC_ERROR_MSG,
                    invocation.clientHandle);
        }

        ParameterSet params = invocation.getParams();
        // deployment string can be null, indicating that the user
        // doesn't want to alter that, and we'll use the previous deployment
        if (params.toArray().length != 2 ||
            params.toArray()[0] == null)
        {
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0],
                    "UpdateApplicationCatalog system procedure requires exactly " +
                    "two parameters, the catalog bytes and the deployment file " +
                    "string (which may be null).",
                    invocation.clientHandle);
        }

        boolean isHex = false;
        if (params.toArray()[0] instanceof String) {
            isHex = Encoder.isHexEncodedString((String) params.toArray()[0]);
        }
        if (!isHex && !(params.toArray()[0] instanceof byte[])) {
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0],
                    "UpdateApplicationCatalog system procedure takes the " +
                    "catalog bytes as a byte array. The received parameter " +
                    "is of type " + params.toArray()[0].getClass() + ".",
                    invocation.clientHandle);
        }

        return null;
    }
}
