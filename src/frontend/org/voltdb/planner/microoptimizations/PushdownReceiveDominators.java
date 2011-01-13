/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner.microoptimizations;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.DistinctPlanNode;
import org.voltdb.types.PlanNodeType;

public class PushdownReceiveDominators implements MicroOptimization {

    @Override
    public List<CompiledPlan> apply(CompiledPlan plan) {

        ArrayList<CompiledPlan> retval = new ArrayList<CompiledPlan>();

        AbstractPlanNode planGraph = plan.fragments.get(0).planGraph;
        planGraph.calculateDominators();

        ArrayList<AbstractPlanNode> receiveNodes =
            planGraph.findAllNodesOfType(PlanNodeType.RECEIVE);

        for (AbstractPlanNode pn : receiveNodes) {
            if (processReceiveNode(pn)) {

                // could make the graph transformation more complex
                // to avoid this recalculation; however, we expect graphs to
                // be relatively small and the total set of transformations
                // performed to also be small, making the cost of recalculation
                // an okay trade-off v. the complexity of a more efficient
                // implementation
                planGraph.calculateDominators();
            }
        }

        // modified plan in place
        retval.add(plan);
        return retval;
    }

    /**
     * @param receive
     * @return true of a transformation requiring recalculation
     * of the dominator state occurred.
     */
    private boolean processReceiveNode(AbstractPlanNode receive) {
        boolean modifiedGraph = false;

        // walk the dominators for the receive node and move them
        // after the receive/send pair as possible
        for (AbstractPlanNode pn : receive.getDominators()) {
            if (pn.getPlanNodeType() == PlanNodeType.DISTINCT) {
                modifiedGraph = pushdownDistinct(receive, pn) || modifiedGraph;
            }
        }
        return modifiedGraph;
    }


    /**
     * If a RECEIVE is dominated by a DISTINCT, that DISTINCT can be executed
     * by the remote partition. If the DISTINCT includes a unique key, the
     * dominating DISTINCT can be fully removed. Otherwise, it must remain as
     * a post filter on possibly duplicate data from the children.
     *
     * @param receive RECEIVE node being pushed past
     * @param distinct DISTINCT node transformed
     * @return
     */
    private boolean pushdownDistinct(AbstractPlanNode receive, AbstractPlanNode distinct) {
        // distinct must be an immediate parent of receive
        if (distinct.hasChild(receive) == false)
            return false;

        // distinct must have a single parent.
        AbstractPlanNode distinct_parent = distinct.getParent(0);
        if (distinct.getParentCount() > 1)
            return false;

        // receive must have a send child
        AbstractPlanNode send = receive.getChild(0);
        if (send.getPlanNodeType() != PlanNodeType.SEND) {
            assert(false) : "receive without send child?";
            return false;
        }

        // passes requirements to transform!

        // TODO: Determine if distinct.getDistinctColumnIndex() is a uniquely-valued column
        boolean distinct_on_unique_column = false;
        if (distinct_on_unique_column) {
            distinct.removeFromGraph();
            distinct_parent.addAndLinkChild(receive);
            send.addIntermediary(distinct);
        }
        else {
            assert(distinct.isInline() == false);
            DistinctPlanNode new_distinct = ((DistinctPlanNode)distinct).produceCopyForTransformation();
            send.addIntermediary(new_distinct);
        }
        return true;
    }

}
