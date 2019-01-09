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

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;

import com.google.common.base.Preconditions;

public class VoltRelUtil {

    /**
     * Add a new RelTrait to a RelNode and its descendants.
     *
     * @param rel
     * @param newTrait
     * @return
     */
    public static RelNode addTraitRecurcively(RelNode rel, RelTrait newTrait) {
        Preconditions.checkNotNull(rel);
        RelTraitShuttle traitShuttle = new RelTraitShuttle(newTrait);
        return rel.accept(traitShuttle);
    }

    public static int decideSplitCount(RelNode rel) {
        return (rel instanceof VoltPhysicalRel) ?
                ((VoltPhysicalRel) rel).getSplitCount() : 1;
    }


}
