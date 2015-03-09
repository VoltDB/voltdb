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

package org.voltdb.client;

/**
 * The type of procedure invocation. Currently there are only two types of
 * invocations, original invocation and replicated invocation. The replicated
 * invocation carries the txn ID of the original txn and the new txn ID it is
 * assigned to in the new cluster.
 *
 * The type is embedded in the version byte of procedure invocation when send
 * across the wire.
 */
public enum ProcedureInvocationType {
    ORIGINAL((byte) 0),
    REPLICATED ((byte) (1 << 7));

    private final byte value;

    private ProcedureInvocationType(byte val) {
        value = val;
    }

    public byte getValue() {
        return value;
    }

    public static ProcedureInvocationType typeFromByte(byte b) {
        byte bit = (byte) (b >> 7);
        if (bit == 0) {
            return ORIGINAL;
        } else {
            return REPLICATED;
        }
    }

    @Override
    public String toString() {
        return "ProcedureInvocationType." + name();
    }
}