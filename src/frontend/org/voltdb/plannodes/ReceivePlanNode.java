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

import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class ReceivePlanNode extends AbstractReceivePlanNode {

    public ReceivePlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.RECEIVE;
    }

    @Override
    public void generateOutputSchema(Database db) {
        // default behavior: just copy the input schema
        // to the output schema
        super.generateOutputSchema(db);
        // except, while technically the resulting output schema is just a pass-through,
        // when the plan gets fragmented, this receive node will be at the bottom of the
        // fragment and will need its own serialized copy of its (former) child's output schema.
        m_hasSignificantOutputSchema = true;
    }

    @Override
    public void resolveColumnIndexes()
    {
        resolveColumnIndexes(m_outputSchema);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "RECEIVE FROM ALL PARTITIONS";
    }

    @Override
    public boolean isOutputOrdered (List<AbstractExpression> sortExpressions, List<SortDirectionType> sortDirections) {
        return false;
    }

}
