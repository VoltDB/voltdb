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
        FAST (0),
        SAFE (1);

        private final int value;

        /** Constructor for non-JDBC-visible types.
         * This can safely stub out any attributes that are only used by jdbc. */
        private ReadLevel(int value) {
            this.value = value;
        }

        public int toInt() {
            return value;
        }

        public static ReadLevel fromInt(int value) {
            if (value == FAST.value) {
                return FAST;
            }
            else if (value == SAFE.value) {
                return SAFE;
            }
            else {
                throw new IllegalArgumentException(
                        String.format("No Consistency.ReadLevel with value: %d", value));
            }
        }

        public static ReadLevel fromReadLevelType(ReadlevelType value) {
            if (value == ReadlevelType.FAST) {
                return FAST;
            }
            else if (value == ReadlevelType.SAFE) {
                return SAFE;
            }
            else {
                throw new IllegalArgumentException(
                        String.format("No Consistency.ReadLevel with value: %s", value.toString()));
            }
        }

        public ReadlevelType toReadLevelType() {
            if (this == FAST) {
                return ReadlevelType.FAST;
            }
            else if (this == SAFE) {
                return ReadlevelType.SAFE;
            }
            else {
                throw new IllegalArgumentException(
                        String.format("No ReadlevelType mapping for Consistency.ReadLevel: %s", toString()));
            }
        }
    }
}
