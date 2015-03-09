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

import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Encoder;

/**
 * Check @UpdateClasses parameters.
 */
public class UpdateClassesAcceptancePolicy extends InvocationValidationPolicy {

    public UpdateClassesAcceptancePolicy(boolean isOn) {
        super(isOn);
    }

    @Override
    public ClientResponseImpl shouldAccept(AuthUser user,
                                StoredProcedureInvocation invocation,
                                Procedure sysProc) {
        if (!invocation.procName.equals("@UpdateClasses")) {
            return null;
        }

        ParameterSet params = invocation.getParams();
        // Either the jar bytes or the deletion can be null, indicating
        // that the user doesn't want to change that component.
        if (params.toArray().length != 2)
        {
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0],
                    "UpdateClasses system procedure requires exactly " +
                    "two parameters, the bytes of the jarfile containing new class files " +
                    "and the string containing class names to be deleted " +
                    "(either of which may be null).",
                    invocation.clientHandle);
        }
        if (params.toArray()[0] != null)
        {
            boolean isHex = false;
            if (params.toArray()[0] instanceof String) {
                isHex = Encoder.isHexEncodedString((String) params.toArray()[0]);
            }
            if (!isHex && !(params.toArray()[0] instanceof byte[])) {
                return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                        new VoltTable[0],
                        "UpdateClasses system procedure takes the " +
                        "jarfile bytes as a byte array. The received parameter " +
                        "is of type " + params.toArray()[0].getClass() + ".",
                        invocation.clientHandle);
            }
        }
        if (params.toArray()[1] != null && !(params.toArray()[1] instanceof String)) {
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0],
                    "UpdateClasses system procedure takes the list of classes to be " +
                    "deleted as a string. The received parameter " +
                    "is of type " + params.toArray()[0].getClass() + ".",
                    invocation.clientHandle);
        }
        return null;
    }
}
