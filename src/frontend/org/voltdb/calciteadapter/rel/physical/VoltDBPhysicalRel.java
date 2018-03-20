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

package org.voltdb.calciteadapter.rel.physical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannodes.AbstractPlanNode;

public interface VoltDBPhysicalRel extends RelNode  {
    final static Convention VOLTDB_PHYSICAL = new Convention.Impl("VOLTDB_PHYSICAL", VoltDBPhysicalRel.class) {
        public boolean canConvertConvention(Convention toConvention) {
            return true;
          }

          public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits,
              RelTraitSet toTraits) {
            return true;
          }

    };

    AbstractPlanNode toPlanNode();

    /**
     * Return a child VoltDBRel node in a specified position
     * @param node Parent Node
     * @param childOrdinal Child position
     * @return VoltDBRel
     */
    default VoltDBPhysicalRel getInputNode(RelNode node, int childOrdinal) {
        RelNode inputNode = node.getInput(childOrdinal);
        if (inputNode != null) {
            if (inputNode instanceof RelSubset) {
                inputNode = ((RelSubset) inputNode).getBest();
                assert (inputNode != null);
            }
            assert(inputNode instanceof VoltDBPhysicalRel);
        }
        return (VoltDBPhysicalRel) inputNode;
    }

    /**
     * Convert a child VoltDBRel node in a specified position to an AbstractPlanNode
     * @param node
     * @param childOrdinal
     * @return AbstractPlanNode
     */
    default AbstractPlanNode inputRelNodeToPlanNode(RelNode node, int childOrdinal) {
        VoltDBPhysicalRel inputNode = getInputNode(node, childOrdinal);
        assert(inputNode != null);
        return inputNode.toPlanNode();
    }

}
