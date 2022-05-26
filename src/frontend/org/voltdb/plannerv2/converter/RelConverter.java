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

package org.voltdb.plannerv2.converter;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.voltdb.planner.ParsedUnionStmt;
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

    /**
     * Convert Calcite SetOp type to a corresponding Volt one
     * @param kind
     * @param all
     * @return ParsedUnionStmt.UnionType
     */
    public static ParsedUnionStmt.UnionType convertSetOpType(SqlKind kind, boolean all) {
        switch (kind) {
            case UNION: return (all)? ParsedUnionStmt.UnionType.UNION_ALL : ParsedUnionStmt.UnionType.UNION;
            case EXCEPT: return (all)? ParsedUnionStmt.UnionType.EXCEPT_ALL : ParsedUnionStmt.UnionType.EXCEPT;
            case INTERSECT: return (all)? ParsedUnionStmt.UnionType.INTERSECT_ALL : ParsedUnionStmt.UnionType.INTERSECT;
            default: return ParsedUnionStmt.UnionType.NOUNION;
        }
    }
}
