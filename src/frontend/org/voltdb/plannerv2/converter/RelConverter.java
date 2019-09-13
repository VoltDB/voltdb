/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2.converter;

import org.apache.calcite.rel.core.JoinRelType;
import org.voltdb.types.JoinType;

public class RelConverter {
    /**
     * Convert Calcite Join type to a corresponding Volt one
     * @param joinType
     * @return
     */
    public static JoinType convertJointType(JoinRelType joinType) {
        switch (joinType) {
            case FULL: return JoinType.FULL;
            case LEFT: return JoinType.LEFT;
            case RIGHT: return JoinType.RIGHT;
            case INNER: return JoinType.INNER;
            // to passify the compiler
            default: return JoinType.INNER;
        }
    }
}
