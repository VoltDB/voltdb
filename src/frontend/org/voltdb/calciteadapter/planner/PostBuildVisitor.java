/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.calciteadapter.planner;

import java.util.HashSet;
import java.util.Set;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractPlanNodeVisitor;
import org.voltdb.plannodes.LimitPlanNode;

/**
 * A Visitor to iterate over a built plan to collect various information about the plan
 *
 */
public class PostBuildVisitor extends AbstractPlanNodeVisitor {

    private boolean m_hasLimitOffset = false;
    private boolean m_isOrderDeterministic = true;
    private Set<ParameterValueExpression> m_pveSet = new HashSet<>();

    @Override
    public void visitNode(AbstractPlanNode node) {
        if (node instanceof LimitPlanNode) {
            m_hasLimitOffset = true;
        }
        // Collect pve
        Set<AbstractExpression> pves = new HashSet<>();
        node.findAllExpressionsOfClass(ParameterValueExpression.class, pves);
        for(AbstractExpression pve : pves) {
            m_pveSet.add((ParameterValueExpression)pve);
        }

        super.visitNode(node);
    }

    public boolean hasLimitOffset() {
        return m_hasLimitOffset;
    }

    public boolean isOrderDeterministic() {
        return m_isOrderDeterministic;
    }

    ParameterValueExpression[] getParameterValueExpressions() {
        return m_pveSet.toArray(new ParameterValueExpression[m_pveSet.size()]);
    }
}
