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

package org.voltdb.plannerv2.utils;

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannerv2.converter.RelConverter;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.types.SortDirectionType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VoltDBRexUtil {
    /**
     * Convert a collation into an OrderByPlanNode.
     *
     * @param collation
     * @param collationFieldExps
     * @return
     */
    public static OrderByPlanNode collationToOrderByNode(RelCollation collation, List<RexNode> collationFieldExps) {
        Preconditions.checkNotNull(collation);
        OrderByPlanNode opn = new OrderByPlanNode();

        // Convert ORDER BY Calcite expressions to VoltDB
        List<AbstractExpression> voltExprList = new ArrayList<>();
        for (RexNode expr : collationFieldExps) {
            AbstractExpression voltExpr = RexConverter.convert(expr);
            voltExprList.add(voltExpr);
        }
        List<Pair<Integer, SortDirectionType>> collFields = RelConverter.convertCollation(collation);
        Preconditions.checkArgument(voltExprList.size() == collFields.size());
        int index = 0;
        for (Pair<Integer, SortDirectionType> collField : collFields) {
            opn.getSortExpressions().add(voltExprList.get(index++));
            opn.getSortDirections().add(collField.right);
        }
        return opn;
    }
}
