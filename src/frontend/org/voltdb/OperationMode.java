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

import java.nio.charset.StandardCharsets;

/**
 * All the operational modes VoltDB can be in
 */
public enum OperationMode {
    INITIALIZING, RUNNING, PAUSED, SHUTTINGDOWN;

    private final byte [] bytes;

    OperationMode() {
        bytes = name().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Get the operation mode from its ordinal value.
     * @param val
     * @return
     */
    public static OperationMode get(byte val) {
        for (OperationMode mode : OperationMode.values()) {
            if (mode.ordinal() == val) {
                return mode;
            }
        }
        throw new AssertionError("Unknown mode: " + val);
    }

    public byte [] getBytes() {
        return bytes;
    }

    public static OperationMode valueOf(byte [] bytes) {
        return valueOf(new String(bytes, StandardCharsets.UTF_8));
    }
}
