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

package org.voltdb.newplanner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.voltdb.newplanner.rules.logical.VoltDBLAggregateRule;
import org.voltdb.newplanner.rules.logical.VoltDBLCalcRule;
import org.voltdb.newplanner.rules.logical.VoltDBLSortRule;
import org.voltdb.newplanner.rules.logical.VoltDBLTableScanRule;

/**
 * reference https://www.slideshare.net/JordanHalterman/introduction-to-apache-calcite
 * <p>
 * How Calcite optimizer work? (I could be wrong, someone please verify it)
 * <p>
 * steps:
 * 1. Optimize logical plan (the SQL query can directly translate to a initial logical plan,
 * then we optimize it to a better logical plan)
 * 2. Convert logical plan into a physical plan (represents the physical execution stages)
 * <p>
 * common optimizations:
 * Prune unused fields, Merge projections, Convert subqueries to joins, Reorder joins,
 * Push down projections, Push down filters
 * <p>
 * Key Concepts:
 * <p>
 * # {@link org.apache.calcite.rel.RelNode} represents a relational expression
 * Sort, Join, Project, Filter, Scan...
 * Exp:
 * select col1 as id, col2 as name from foo where col1=21;
 * <p>
 * Project ( id = [$0], name = [$1] ) <-- expression
 * Filter (condition=[= ($0, 21)])   <-- children/input
 * TableScan (table = [foo])
 * <p>
 * # {@link org.apache.calcite.rex.RexNode} represents a row-level expression:
 * = scalar expr
 * Projection fields, conditions
 * Input column reference  -->  RexInputRef
 * Literal                 -->  RexLiteral
 * Struct field access     -->  RexFieldAccess
 * Function call           -->  RexCall
 * Window expression       -->  RexOver
 * <p>
 * # traits
 * Defined by the {@link org.apache.calcite.plan.RelTrait} interface
 * Traits are used to validate plan output
 * {@link org.apache.calcite.plan.Convention}
 * {@link org.apache.calcite.rel.RelCollation}
 * {@link org.apache.calcite.rel.RelDistribution}
 * <p>
 * ## Convention
 * Convention is a type of RelTrait, it is associated with a
 * RelNode interface
 * <p>
 * Conventions are used to represent a single data source.
 * <p>
 * describing how the expression passes data to its consuming relational expression
 * <p>
 * Inputs to a relational expression must be in the same convention.
 * <p>
 * # Rules
 * Rules are used to modify query plans.
 * <p>
 * Defined by the {@link org.apache.calcite.plan.RelOptRule} interface
 * <p>
 * Rules are matched to elements of a query plan using pattern matching
 * {@link org.apache.calcite.plan.RelOptRuleOperand}
 * <p>
 * ## Converter
 * {@link org.apache.calcite.rel.convert.ConverterRule}
 * convert() is called for matched rules
 * <p>
 * {@link org.apache.calcite.rel.convert.Converter}
 * can convert from one convention to another
 * via convert()
 * <p>
 * Q: when ConverterRule and when Converter?
 * <p>
 * ## Transformer
 * onMatch() is called for matched rules
 * <p>
 * call.transformTo()
 * <p>
 * # Planners
 * {@link org.apache.calcite.plan.volcano.VolcanoPlanner}
 * {@link org.apache.calcite.plan.hep.HepPlanner}
 * <p>
 * # Program
 * {@link org.apache.calcite.tools.Program}
 * <p>
 * Program that transforms a relational expression into another relational expression.
 * A planner is a sequence of programs, each of which is sometimes called a "phase".
 * <p>
 * The most typical program is an invocation of the volcano planner with a particular RuleSet.
 */
public class VoltDBRules {

    public static RelOptRule[] VOLCANO_RULES_0 = {
            // Calcite's Logical Rules

            /*
            LogicalCalc
            proj
            filter
            calc = proj & filter
            A relational expression which computes project expressions and also filters.
            This relational expression combines the functionality of LogicalProject and LogicalFilter.
            It should be created in the later stages of optimization,
            by merging consecutive LogicalProject and LogicalFilter nodes together.

            The following rules relate to LogicalCalc:

            FilterToCalcRule creates this from a LogicalFilter
            ProjectToCalcRule creates this from a LogicalFilter
            FilterCalcMergeRule merges this with a LogicalFilter
            ProjectCalcMergeRule merges this with a LogicalProject
            CalcMergeRule merges two LogicalCalcs
             */
            CalcMergeRule.INSTANCE
            , FilterCalcMergeRule.INSTANCE
            , FilterToCalcRule.INSTANCE
            , ProjectCalcMergeRule.INSTANCE
            , ProjectToCalcRule.INSTANCE
            , ProjectMergeRule.INSTANCE
            , FilterProjectTransposeRule.INSTANCE

            // VoltDBLogical Conversion Rules
            , VoltDBLSortRule.INSTANCE
            , VoltDBLTableScanRule.INSTANCE
            , VoltDBLCalcRule.INSTANCE
            , VoltDBLAggregateRule.INSTANCE
    };


    public static Program VOLCANO_PROGRAM_0 = Programs.ofRules(
            VOLCANO_RULES_0
    );


    public static Program[] getVolcanoPrograms() {
        return new Program[]{VOLCANO_PROGRAM_0};
    }

}
