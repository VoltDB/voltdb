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

package org.voltdb.plannerv2.rel;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;

/**
 * Contains implementation for creating various VoltPhysical nodes.
 *
 */
public class VoltPRelFactories {

    public static ProjectFactory VOLT_PHYSICAL_PROJECT_FACTORY =
        new VoltPProjectFactoryImpl();

    // @TODO Add factories for ALL Volt Physical Relations

    /**
     * Implementation of ProjectFactory
     * {@link org.apache.calcite.rel.core.RelFactories.ProjectFactory}
     * that returns a VoltPhysicalCalc
     * {@link org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc}.
     */
    private static class VoltPProjectFactoryImpl implements ProjectFactory {

        public RelNode createProject(RelNode input,
                List<? extends RexNode> childExprs, List<String> fieldNames) {

            final RelDataType outputRowType =
                    RexUtil.createStructType(input.getCluster().getTypeFactory(), childExprs,
                        fieldNames, SqlValidatorUtil.F_SUGGESTER);

            final RexProgram program =
                    RexProgram.create(
                            input.getRowType(),
                            childExprs,
                            null,
                            outputRowType,
                            input.getCluster().getRexBuilder());

            return new VoltPhysicalCalc(input.getCluster(),
                    input.getTraitSet(),
                    input,
                    program,
                    1);
        }
    }

}
