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
            m_prePredicate = (AbstractExpression) origin.m_prePredicate.clone();
        }
        if (origin.m_postPredicate != null) {
            m_postPredicate = (AbstractExpression) origin.m_postPredicate.clone();
        }
        for (AbstractExpression expr : origin.m_groupByExpressions) {
            addGroupByExpression(expr);
        }
        List<ExpressionType> aggregateTypes = origin.m_aggregateTypes;
        List<Integer> aggregateDistinct = origin.m_aggregateDistinct;
        List<Integer> aggregateOutputColumns = origin.m_aggregateOutputColumns;
        List<AbstractExpression> aggregateExpressions = origin.m_aggregateExpressions;
        for (int i = 0; i < origin.getAggregateTypesSize(); i++) {
            addAggregate(aggregateTypes.get(i),
                    aggregateDistinct.get(i) == 1 ? true : false,
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
