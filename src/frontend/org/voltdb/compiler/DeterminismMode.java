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

package org.voltdb.compiler;

public enum DeterminismMode {
    FASTER, // Pick the fastest plan without regard for determinism
    SAFER;   // Pick a fast plan that is more likely to be deterministic
            //  In practice, this means avoiding table scans, but could
            //  still fail on non-unique indexes
    //SAFE  // Not yet added, but could add an order-by all over the place
            //  if that made sense.

    char toChar() {
        if (this == FASTER) {
            return 'F';
        }
        if (this == SAFER) {
            return 'S';
        }
        throw new RuntimeException("This shouldn't happend in DeterminismMode.java");
    }

    static DeterminismMode fromChar(char c) {
        if (c == 'F') {
            return FASTER;
        }
        if (c == 'S') {
            return SAFER;
        }
        return null;
    }

    static DeterminismMode fromStr(String str) {
        if ((str == null) || (str.isEmpty())) {
            return null;
        }
        return fromChar(str.toUpperCase().charAt(0));
    }
}
