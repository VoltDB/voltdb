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

public abstract class Consistency {

    public enum ReadLevel {
        FAST (0),   // send reads everywhere, no waiting or queue, return response to clients immediately.
        SAFE (1),   // like writes, send reads to primary, replicates them to replica, and check hash response.
        SAFE_1 (2), // send reads everywhere but don't lose them to clients
                    // until any previous writes have benn ack-ed of the repair log.
        SAFE_2 (3), // send reads to primary only, not replicating to replicas like writes.
                    // have responses to clients block on any unsafe reads waiting to go to clients in front of them.
        SAFE_3 (4); // like SAFE_3, but evenly distributed the reads between primary and replicas.

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
            if (value == SAFE_1.m_value) {
                return SAFE_1;
            }
            if (value == SAFE_2.m_value) {
                return SAFE_2;
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
            if (value == ReadlevelType.SAFE_1) {
                return SAFE_1;
            }
            if (value == ReadlevelType.SAFE_2) {
                return SAFE_2;
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
            if (this == SAFE_1) {
                return ReadlevelType.SAFE_1;
            }
            if (this == SAFE_2) {
                return ReadlevelType.SAFE_2;
            }

            throw new IllegalArgumentException(
                    String.format("No ReadlevelType mapping for Consistency.ReadLevel: %s", toString()));
        }

        public boolean hasShortcutRead() {
            if (m_value == ReadLevel.FAST.toInt() || m_value == ReadLevel.SAFE_1.toInt()) {
                return true;
            }

            return false;
        }
    }
}
