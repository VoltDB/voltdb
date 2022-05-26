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
    ORIGINAL((byte) 0),              // original version pre 6.7
    VERSION1((byte) 1),              // version with individual timeout support (pre 6.7)
    VERSION2((byte) 2);               // slightly extensible v6.7 and up
    // REPLICATED ((byte) (1 << 7)); // -128 - no longer used - will throw RuntimeException

    private final byte m_value;

    private ProcedureInvocationType(byte val) {
        m_value = val;
    }

    public byte getValue() {
        return m_value;
    }

    public static ProcedureInvocationType typeFromByte(byte b) {
        switch(b) {
        case 0:
            return ORIGINAL;
        case 1:
            return VERSION1;
        case 2:
            return VERSION2;
        default:
            throw new RuntimeException("Unknown ProcedureInvocationType " + b);
        }
    }

    @Override
    public String toString() {
        return "ProcedureInvocationType." + name();
    }
}
