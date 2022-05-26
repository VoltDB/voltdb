/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
 * Priority definitions for use in procedure invocations.
 *
 * Shared between client-side ProcedureInvocation and
 * server-side StoredProcedureInvocation.
 *
 * Not for direct application use; there are symbols in
 * Client2Config for applications (assigned from the
 * values in this class, not cut'n'pasted).
 *
 * See also DeploymentFileSchema.xsd if you're changing
 * the range allowed here.
 */
public class Priority {

    // Maximum and minimum priority levels that clients can set.
    // Numerically lower values mean higher priority.
    // All values in [highest, lowest] are allowed.
    // Highest client priority is elsewhere assumed to be 1.
    // Serialized to a byte, signed in Java, therefore cannot exceed
    // 127 in numerical value.
    public static final int HIGHEST_PRIORITY = 1;
    public static final int LOWEST_PRIORITY = 8;

    public static final int DEFAULT_PRIORITY = LOWEST_PRIORITY / 2;

    // This is the highest effective priority
    public static final int SYSTEM_PRIORITY = 0;
}
