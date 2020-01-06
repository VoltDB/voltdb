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

package org.voltdb.plannerv2.rel.util;

import java.util.stream.IntStream;

import org.apache.calcite.rel.core.SetOp;
import org.voltdb.plannerv2.converter.RelConverter;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.UnionPlanNode;

/**
 * Helper utilities to determine Plan Cost.
 */
public final class PlanNodeUtil {

    public static AbstractPlanNode setOpToPlanNode(VoltPhysicalRel setOpNode) {
        if (setOpNode instanceof SetOp) {
            SetOp setOp = (SetOp) setOpNode;
            final UnionPlanNode upn = new UnionPlanNode(RelConverter.convertSetOpType(setOp.kind, setOp.all));
            IntStream.range(0, setOp.getInputs().size())
            .forEach(i -> {
                final AbstractPlanNode child = setOpNode.inputRelNodeToPlanNode(setOp, i);
                upn.addAndLinkChild(child);
            });
            return upn;
        }
        return null;
    }
}
