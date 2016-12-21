/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltdb.compiler.deploymentfile.ReadlevelType;

/**
 * Note: A shortcut read (FAST) is a read operation sent to any replica and completed with no
 * confirmation or communication with other replicas. In a partition scenario, it's
 * possible to read an unconfirmed transaction's writes that will be lost.
 */
public abstract class Consistency {

    public enum ReadLevel {
        FAST (0),   // send reads everywhere, no waiting or queue, return response to clients immediately.
        SAFE (1);   // send reads to primary, do not replicate them to replicas, but don't lose them to
                    // clients until any previous writes have been ack-ed of the repair log.


        private final int m_value;

        /** Constructor for non-JDBC-visible types.
         * This can safely stub out any attributes that are only used by jdbc. */
        private ReadLevel(int value) {
            this.m_value = value;
        }

        public int toInt() {
            return m_value;
        }

        public static ReadLevel fromInt(int value) {
            if (value == FAST.m_value) {
                return FAST;
            }
            if (value == SAFE.m_value) {
                return SAFE;
            }
            throw new IllegalArgumentException(
                    String.format("No Consistency.ReadLevel with value: %d", value));
        }

        public static ReadLevel fromReadLevelType(ReadlevelType value) {
            if (value == ReadlevelType.FAST) {
                return FAST;
            }
            if (value == ReadlevelType.SAFE) {
                return SAFE;
            }
            throw new IllegalArgumentException(
                    String.format("No Consistency.ReadLevel with value: %s", value.toString()));
        }

        public ReadlevelType toReadLevelType() {
            if (this == FAST) {
                return ReadlevelType.FAST;
            }
            if (this == SAFE) {
                return ReadlevelType.SAFE;
            }
            throw new IllegalArgumentException(
                    String.format("No ReadlevelType mapping for Consistency.ReadLevel: %s", toString()));
        }
    }
}
