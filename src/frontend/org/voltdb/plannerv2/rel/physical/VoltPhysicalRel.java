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

package org.voltdb.plannerv2.rel.physical;

import java.util.Objects;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.guards.CalcitePlanningException;
import org.voltdb.plannodes.AbstractPlanNode;

import com.google.common.base.Preconditions;

public interface VoltPhysicalRel extends RelNode {
    Convention CONVENTION = new Convention.Impl("CONVENTION", VoltPhysicalRel.class) {
    };

    /**
     * Convert VoltPhysicalRel and its descendant(s) to a AbstractPlanNode tree
     * This is the key piece that bridges between Calcite planner and VoltDB planner.
     * @return AbstractPlanNode
     */
    default AbstractPlanNode toPlanNode() {
        throw new CalcitePlanningException("Not implemented!");
    }

    /**
     * Return a child VoltDBRel node in a specified position
     *
     * @param node         Parent Node
     * @param childOrdinal Child position
     * @return VoltDBRel
     */
    default VoltPhysicalRel getInputNode(RelNode node, int childOrdinal) {
        RelNode inputNode = node.getInput(childOrdinal);
        if (inputNode != null) {
            if (inputNode instanceof RelSubset) {
                inputNode = ((RelSubset) inputNode).getBest();
                Objects.requireNonNull(inputNode);

            }
            Preconditions.checkArgument(inputNode instanceof VoltPhysicalRel);
        }
        return (VoltPhysicalRel) inputNode;
    }

    /**
     * Convert a child VoltDBRel node in a specified position to an AbstractPlanNode
     *
     * @param node
     * @param childOrdinal
     * @return AbstractPlanNode
     */
    default AbstractPlanNode inputRelNodeToPlanNode(RelNode node, int childOrdinal) {
        VoltPhysicalRel inputNode = getInputNode(node, childOrdinal);
        Objects.requireNonNull(inputNode);
        return inputNode.toPlanNode();
    }
}
