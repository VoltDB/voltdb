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
 * The type is embedded in the version byte of procedure invocation when send
 * across the wire.
 */
public enum BatchTimeoutType {
    NO_BATCH_TIMEOUT ((byte) -1),
    HAS_BATCH_TIMEOUT ((byte) 1);

    private final byte value;

    private BatchTimeoutType(byte val) {
        value = val;
    }

    public byte getValue() {
        return value;
    }

    public static final int BATCH_TIMEOUT_VERSION = 1;
    public static final int NO_TIMEOUT = NO_BATCH_TIMEOUT.getValue(); // -1

    public static BatchTimeoutType typeFromByte(byte b) {
        switch (b) {
        case -1:
            return NO_BATCH_TIMEOUT;
        case 1:
            return HAS_BATCH_TIMEOUT;
        default:
            throw new RuntimeException("Unkonwn BatchTimeoutType " + b);
        }
    }

    public static boolean isUserSetTimeout(int timeout) {
        if (timeout >= 0) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "BatchTimeoutType." + name();
    }

    public static String toString (int timeout) {
        if (timeout > 0) {
            return HAS_BATCH_TIMEOUT.toString() + " with value(millis): " + timeout;
        } else if (timeout == 0) {

        }
        return NO_BATCH_TIMEOUT.toString();
    }

}