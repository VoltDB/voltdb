/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
package org.voltdb.planner.microoptimizations;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.PlanNodeType;

public class RemoveUnnecessaryProjectNodes extends MicroOptimization {

    /**
     * Sometimes when a plan is a single partition plan we end up
     * with an extra, unnecessary projection node.  We can't actually
     * easily remove this in the planner.  The projection node occurs
     * when:
     * <ol>
     *   <li>We push a RECEIVE/SEND pair onto a subplan.  This subplan
     *       would be a join tree or scan, or maybe a subquery.</li>
     *   <li>We push a projection node on top of the R/S pair to calculate
     *       the select list expressions.</li>
     *   <li>We decide the plan is a single partition plan anyway, and
     *       that the R/S pair is not really needed after all.  So, we
     *       eliminate the pair.</li>
     * </ol>
     * At none of these steps do we have enough information to know if
     * the projection node is needed or if it can be eliminated.  So
     * we need to look for these unnecessary projection nodes after
     * the plans are constructed, which means we need a micro-optimization.
     *
     * Now, we can't apply a microoptimization as we usually do, while selecting
     * the best plan, because the output schemas have not been generated
     * when the microoptimizations are being applied.  So we need a
     * special microoptimization which is applied after the plan is
     * in its final state.
     */
    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode plan) {
        AbstractPlanNode answer = null;;
        for (AbstractPlanNode node = plan, child = null;
                node != null;
                node = child) {
            child = (node.getChildCount() > 0) ? node.getChild(0) : null;
            if (node.getPlanNodeType() == PlanNodeType.PROJECTION) {
                assert(child != null);
                NodeSchema childSchema = child.getOutputSchema();
                assert(childSchema != null);
                if (((ProjectionPlanNode)node).isIdentity(childSchema)) {
                    AbstractPlanNode parent = (node.getParentCount() > 0) ? node.getParent(0) : null;
                    if (parent == null) {
                        answer = child;
                    } else {
                        node.removeFromGraph();
                        child.clearParents();
                        parent.clearChildren();
                        parent.addAndLinkChild(child);
                    }
                }
            }
        }
        if (answer != null) {
            return answer;
        }
        return plan;
    }

    @Override
    MicroOptimizationRunner.Phases getPhase() {
        return MicroOptimizationRunner.Phases.AFTER_BEST_SELECTION;
    }
}
