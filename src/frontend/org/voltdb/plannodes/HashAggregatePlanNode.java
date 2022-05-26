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

package org.voltdb.plannodes;

import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;

/**
 * Plan node representing an Aggregate with a Hash based implementation of grouping.
 *
 */
public class HashAggregatePlanNode extends AggregatePlanNode {
    public HashAggregatePlanNode() {
        super();
    }

    public HashAggregatePlanNode(HashAggregatePlanNode origin) {
        super();
        m_isCoordinatingAggregator = origin.m_isCoordinatingAggregator;
        if (origin.m_prePredicate != null) {
            m_prePredicate = origin.m_prePredicate.clone();
        }
        if (origin.m_postPredicate != null) {
            m_postPredicate = origin.m_postPredicate.clone();
        }
        for (AbstractExpression expr : origin.m_groupByExpressions) {
            addGroupByExpression(expr);
        }
        List<ExpressionType> aggregateTypes = origin.m_aggregateTypes;
        List<Integer> aggregateDistinct = origin.m_aggregateDistinct;
        List<Integer> aggregateOutputColumns = origin.m_aggregateOutputColumns;
        List<AbstractExpression> aggregateExpressions = origin.mAggregateExpressions;
        for (int i = 0; i < origin.getAggregateTypesSize(); i++) {
            addAggregate(aggregateTypes.get(i),
                    aggregateDistinct.get(i) == 1,
                    aggregateOutputColumns.get(i),
                    aggregateExpressions.get(i));
        }
        setOutputSchema(origin.getOutputSchema());
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.HASHAGGREGATE;
    }

}
