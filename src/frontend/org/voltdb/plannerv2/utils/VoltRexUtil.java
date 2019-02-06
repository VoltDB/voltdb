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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;

public class VoltRexUtil {

    /**
     * Convert a {@link #RelDistribution} of HASH type into a new {@link #RelDistribution} where partitioning column index
     * is adjusted for a given projection. If the partitioning column is projected out, the output index array will be empty
     *
     * If the input distribution is not HAS_DISTRIBUTED it is returned unmodified.
     *
     * @param builder - RexBuilder
     * @param program - RexProgram
     * @param distribution - initial RelDistribution
     * @return RelDistribution
     */
    public static RelDistribution adjustRelDistributionForProgram(
            RexBuilder builder,
            RexProgram program,
            RelDistribution distribution) {
        assert (program != null);
        if (distribution.getType() != RelDistribution.Type.HASH_DISTRIBUTED) {
            return distribution;
        }

        List<Integer> distColumnIndexes = distribution.getKeys();
        if (distColumnIndexes.isEmpty()) {
            return distribution;
        }
        // VoltDB supports only one partition column per table.
        assert(distColumnIndexes.size() == 1);
        final int partitionIndex = distColumnIndexes.get(0);
        List<Integer> newPartitionIndexes = new ArrayList<>(1);

        // Iterate over the project list to find a corresponding input reference
        // which index matches the partitioning column index from the input distribution
        // The index of this project will be the new index of a partitioning column
        for (int idx = 0; idx < program.getProjectList().size(); ++idx) {
            RexLocalRef projRef = program.getProjectList().get(idx);
            int source = projRef.getIndex();
            assert(source < program.getExprList().size());
            RexNode rexNode = program.getExprList().get(source);
            if (rexNode instanceof RexInputRef) {
                // This is an actual field
                int exprInputIndex = ((RexInputRef)rexNode).getIndex();
                if (partitionIndex == exprInputIndex) {
                    newPartitionIndexes.add(idx);
                    break;
                }
            }
        }
        return RelDistributions.hash(newPartitionIndexes);
    }

    /**
     * Return field index if a {@link #RexNode} is an input field or a local reference that points to a one. -1 otherwise
     * @param program
     * @param node
     * @param allowCast
     * @return
     */
    public static int getReferenceOrAccessIndex(RexProgram program, RexNode node, boolean allowCast) {
        if (allowCast && node.isA(SqlKind.CAST)){
            RexCall call = (RexCall) node;
            node =  call.operands.get(0);
            return getReferenceOrAccessIndex(program, node, false);
        }
        if (node instanceof RexLocalRef && program != null) {
            int idx = ((RexLocalRef) node).getIndex();
            node = program.getExprList().get(idx);
            return getReferenceOrAccessIndex(program, node, true);
        }
        return getReferenceOrAccessIndex(node, false);
    }

    private static int getReferenceOrAccessIndex(RexNode node, boolean allowCast) {
        if (node instanceof RexInputRef) {
            return ((RexInputRef)node).getIndex();
        } else if (node instanceof RexFieldAccess) {
            return ((RexFieldAccess) node).getField().getIndex();
        } else if (allowCast && node.isA(SqlKind.CAST)){
            RexCall call = (RexCall) node;
            return getReferenceOrAccessIndex(call.operands.get(0), false);
        }
        return -1;
    }

}
