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

package org.voltdb.calciteadapter.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPAggregateScanMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPCalcAggregateMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPCalcScanMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPLimitMergeExchangeMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPLimitScanMergeRule;
import org.voltdb.calciteadapter.rules.inlining.VoltDBPLimitSortMergeRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLAggregateRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLCalcRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLSortRule;
import org.voltdb.calciteadapter.rules.logical.VoltDBLTableScanRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPAggregateExchangeTransposeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPAggregateRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPCalcExchangeTransposeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPCalcRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPCalcScanToIndexRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPLimitExchangeTransposeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPLimitRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSeqScanRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortConvertRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortExchangeTransposeRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortIndexScanRemoveRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortRule;
import org.voltdb.calciteadapter.rules.physical.VoltDBPSortScanToIndexRule;

/**
 *  reference https://www.slideshare.net/JordanHalterman/introduction-to-apache-calcite
 *
 *  How Calcite optimizer work? (I could be wrong, someone please verify it)
 *
 *  steps:
 *  1. Optimize logical plan (the SQL query can directly translate to a initial logical plan,
 *  then we optimize it to a better logical plan)
 *  2. Convert logical plan into a physical plan (represents the physical execution stages)
 *
 *  common optimizations:
 *  Prune unused fields, Merge projections, Convert subqueries to joins, Reorder joins,
 *  Push down projections, Push down filters
 *
 *  Key Concepts:
 *
 *  # {@link org.apache.calcite.rel.RelNode} represents a relational expression
 *    Sort, Join, Project, Filter, Scan...
 *    Exp:
 *    select col1 as id, col2 as name from foo where col1=21;
 *
 *    Project ( id = [$0], name = [$1] ) <-- expression
 *      Filter (condition=[= ($0, 21)])   <-- children/input
 *          TableScan (table = [foo])
 *
 *  # {@link org.apache.calcite.rex.RexNode} represents a row-level expression:
 *  = scalar expr
 *      Projection fields, conditions
 *      Input column reference  -->  RexInputRef
 *      Literal                 -->  RexLiteral
 *      Struct field access     -->  RexFieldAccess
 *      Function call           -->  RexCall
 *      Window expression       -->  RexOver
 *
 *  # traits
 *  Defined by the {@link org.apache.calcite.plan.RelTrait} interface
 *  Traits are used to validate plan output
 *  {@link org.apache.calcite.plan.Convention}
 *  {@link org.apache.calcite.rel.RelCollation}
 *  {@link org.apache.calcite.rel.RelDistribution}
 *
 *  ## Convention
 *      Convention is a type of RelTrait, it is associated with a
 *      RelNode interface
 *
 *      Conventions are used to represent a single data source.
 *
 *      describing how the expression passes data to its consuming relational expression
 *
 *      Inputs to a relational expression must be in the same convention.
 *
 *  # Rules
 *  Rules are used to modify query plans.
 *
 *  Defined by the {@link org.apache.calcite.plan.RelOptRule} interface
 *
 *  Rules are matched to elements of a query plan using pattern matching
 *  {@link org.apache.calcite.plan.RelOptRuleOperand}
 *
 *      ## Converter
 *      {@link org.apache.calcite.rel.convert.ConverterRule}
 *      convert() is called for matched rules
 *
 *          {@link org.apache.calcite.rel.convert.Converter}
 *          can convert from one convention to another
 *          via convert()
 *
 *          Q: when ConverterRule and when Converter?
 *
 *      ## Transformer
 *      onMatch() is called for matched rules
 *
 *      call.transformTo()
 *
 *  # Planners
 *  {@link org.apache.calcite.plan.volcano.VolcanoPlanner}
 *  {@link org.apache.calcite.plan.hep.HepPlanner}
 *
 *  # Program
 *  {@link org.apache.calcite.tools.Program}
 *
 *  Program that transforms a relational expression into another relational expression.
 *  A planner is a sequence of programs, each of which is sometimes called a "phase".
 *
 *  The most typical program is an invocation of the volcano planner with a particular RuleSet.
 *
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

    public static RelOptRule[] VOLCANO_RULES_1 = {
            // Calcite's Rules
            AbstractConverter.ExpandConversionRule.INSTANCE

            // TODO: why we apply CalcMergeRule again in this stage
            , CalcMergeRule.INSTANCE

            // VoltDB Logical Rules

            // VoltDB Physical Rules
            , VoltDBPSortScanToIndexRule.INSTANCE_1
            , VoltDBPSortScanToIndexRule.INSTANCE_2
            , VoltDBPCalcScanToIndexRule.INSTANCE
            , VoltDBPSortIndexScanRemoveRule.INSTANCE_1
            , VoltDBPSortIndexScanRemoveRule.INSTANCE_2

            // VoltDB Physical Conversion Rules
            , VoltDBPCalcRule.INSTANCE
            , VoltDBPSeqScanRule.INSTANCE
            , VoltDBPSortRule.INSTANCE
            , VoltDBPSortConvertRule.INSTANCE_NONE
            , VoltDBPSortConvertRule.INSTANCE_VOLTDB
            , VoltDBPLimitRule.INSTANCE
            , VoltDBPAggregateRule.INSTANCE

            // Exchage Rules
            , VoltDBPCalcExchangeTransposeRule.INSTANCE
            , VoltDBPLimitExchangeTransposeRule.INSTANCE
            , VoltDBPSortExchangeTransposeRule.INSTANCE
            , VoltDBPAggregateExchangeTransposeRule.INSTANCE_1
            , VoltDBPAggregateExchangeTransposeRule.INSTANCE_2
            , VoltDBPAggregateExchangeTransposeRule.INSTANCE_3
    };


    // TODO: this rule set never used
    public static RelOptRule[] INLINING_RULES = {
            // VoltDB Inline Rules. The rules order declaration
            // has to match the order of rels from a real plan produced by the previous stage.

            VoltDBPLimitMergeExchangeMergeRule.INSTANCE_1
            , VoltDBPLimitMergeExchangeMergeRule.INSTANCE_2
            , VoltDBPCalcAggregateMergeRule.INSTANCE
            , VoltDBPCalcScanMergeRule.INSTANCE
            , VoltDBPLimitSortMergeRule.INSTANCE_1
            , VoltDBPLimitSortMergeRule.INSTANCE_2
            , VoltDBPAggregateScanMergeRule.INSTANCE
            , VoltDBPLimitScanMergeRule.INSTANCE_1
            , VoltDBPLimitScanMergeRule.INSTANCE_2


    };

    public static Program VOLCANO_PROGRAM_0 = Programs.ofRules(
            VOLCANO_RULES_0
            );

    public static Program VOLCANO_PROGRAM_1 = Programs.ofRules(
            VOLCANO_RULES_1
            );

    public static Program INLINING_PROGRAM = Programs.ofRules(
            INLINING_RULES
            );

    public static Program[] getVolcanoPrograms() {
        return new Program[] {VOLCANO_PROGRAM_0, VOLCANO_PROGRAM_1};
    }

    public static Program getInliningProgram() {
        return INLINING_PROGRAM;
    }

}
