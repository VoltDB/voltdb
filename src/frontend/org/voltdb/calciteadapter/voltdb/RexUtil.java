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

package org.voltdb.calciteadapter.voltdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltcore.utils.Pair;
import org.voltdb.types.SortDirectionType;

public class RexUtil {

    /**
     * Convert a collation into a new collation which column indexes are adjusted for a possible projection.
     * Adopted from the RexProgram.deduceCollations
     *
     * @param program - IndexScan node program
     * @param inputCollation - index collation
     * @return RelCollation
     */
    public static RelCollation adjustIndexCollation(
            RexProgram program,
            RelCollation inputCollation) {
        assert (program != null);

        int sourceCount = program.getInputRowType().getFieldCount();
        List<RexLocalRef> refs = program.getProjectList();
        int[] targets = new int[sourceCount];
        Arrays.fill(targets, -1);
        for (int i = 0; i < refs.size(); i++) {
            final RexLocalRef ref = refs.get(i);
            final int source = ref.getIndex();
            if ((source < sourceCount) && (targets[source] == -1)) {
                targets[source] = i;
            }
        }
        final List<RelFieldCollation> fieldCollations = new ArrayList<>(0);
        for (RelFieldCollation fieldCollation : inputCollation.getFieldCollations()) {
            final int source = fieldCollation.getFieldIndex();
            final int target = targets[source];
            if (target < 0) {
                // Stop at the first mismatched field
                return RelCollations.of(fieldCollations);
            }
            fieldCollations.add(
                    fieldCollation.copy(target));
        }

        // Success -- all of the source fields of this key are mapped
        // to the output.
        return RelCollations.of(fieldCollations);
    }

    /**
     * Reverse the direction for each collation field in the input collation
     * ASCENDING -> DESCENDING
     * STRICTLY_ASCENDING -> STRICTLY_DESCENDING
     * DESCENDING -> ASCENDING
     * STRICTLY_DESCENDING -> STRICTLY_ASCENDING
     *
     * @param inputCollation
     * @returned RelCollation
     */
    public static RelCollation reverseCollation(RelCollation inputCollation) {
        final List<RelFieldCollation> fieldCollations = new ArrayList<>(0);
        for (RelFieldCollation fieldCollation : inputCollation.getFieldCollations()) {
            final int fieldIndex = fieldCollation.getFieldIndex();
            final Direction direction = fieldCollation.getDirection();
            RelFieldCollation newfieldCollation;
            if (direction == Direction.ASCENDING) {
                newfieldCollation = new RelFieldCollation(fieldIndex, Direction.DESCENDING);
            } else if (direction == Direction.STRICTLY_ASCENDING) {
                newfieldCollation = new RelFieldCollation(fieldIndex, Direction.STRICTLY_DESCENDING);
            } else if (direction == Direction.DESCENDING) {
                newfieldCollation = new RelFieldCollation(fieldIndex, Direction.ASCENDING);
            } else if (direction == Direction.STRICTLY_DESCENDING) {
                newfieldCollation = new RelFieldCollation(fieldIndex, Direction.STRICTLY_ASCENDING);
            } else {
                newfieldCollation = new RelFieldCollation(fieldIndex, direction);
            }
            fieldCollations.add(newfieldCollation);
        }
        return RelCollations.of(fieldCollations);
    }

    /**
     * Compare sort and index collations to determine whether they are compatible or not
     *  - both collations are not empty
     *  - the collation direction is the same for all fields for each collation
     *  - all fields from the sort collation have matching fields from the index collation
     * If collations are compatible return a pair containing the sort direction (ASC, DESC) and
     * the boolean indicating whether the index direction needs to be reversed or not.
     *
     * If collations are not compatible the returned sort direction is INVALID.
     *
     * @param sortCollation
     * @param indexCollation
     * @return Pair<SortDirectionType, Boolean>
     */
    public static Pair<SortDirectionType, Boolean> areCollationsCompartible(RelCollation sortCollation, RelCollation indexCollation) {
        if (sortCollation == RelCollations.EMPTY || indexCollation == RelCollations.EMPTY) {
            return Pair.of(SortDirectionType.INVALID, false);
        }

        List<RelFieldCollation> collationFields1 = sortCollation.getFieldCollations();
        List<RelFieldCollation> collationFields2 = indexCollation.getFieldCollations();
        if (collationFields2.size() < collationFields1.size()) {
            return Pair.of(SortDirectionType.INVALID, false);
        }
        assert(collationFields1.size() > 0);
        RelFieldCollation.Direction collationDirection1 = collationFields1.get(0).getDirection();
        assert(collationFields2.size() > 0);
        RelFieldCollation.Direction collationDirection2 = collationFields2.get(0).getDirection();

        for (int i = 0; i < collationFields1.size(); ++i) {
            RelFieldCollation fieldCollation1 = collationFields1.get(i);
            RelFieldCollation fieldCollation2 = collationFields2.get(i);
            if (fieldCollation1.direction != collationDirection1 ||
                    fieldCollation2.direction != collationDirection2 ||
                    fieldCollation1.getFieldIndex() != fieldCollation2.getFieldIndex()) {
                return Pair.of(SortDirectionType.INVALID, false);
            }
        }
        SortDirectionType sortDirection =
                (Direction.ASCENDING == collationDirection1 || Direction.STRICTLY_ASCENDING == collationDirection1) ?
                SortDirectionType.ASC : SortDirectionType.DESC;
        boolean needReverseCollation = collationDirection1 != collationDirection2;
        return Pair.of(sortDirection, needReverseCollation);
    }

    /**
     * Merges two programs together.
     *
     * @param topProgram    Top program. Its expressions are in terms of the
     *                      outputs of the bottom program.
     * @param bottomProgram Bottom program.
     * @param rexBuilder    Rex builder
     * @return Merged program
     */
    public static RexProgram mergeProgram(RexProgram bottomProgram, RexProgram topProgram, RexBuilder rexBuilder) {
        assert(topProgram != null);
        RexProgram mergedProgram;
        if (bottomProgram != null) {
            // Merge two programs topProgram / bottomProgram into a new merged program
            mergedProgram = RexProgramBuilder.mergePrograms(
                    topProgram,
                    bottomProgram,
                   rexBuilder);
            assert(topProgram.getOutputRowType().equals(bottomProgram.getOutputRowType()));
        } else {
            mergedProgram = topProgram;
        }
        return mergedProgram;
    }

}
