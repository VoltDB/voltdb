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

package org.voltdb.newplanner;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.voltdb.newplanner.metadata.VoltDBDefaultRelMetadataProvider;
import org.voltdb.newplanner.rules.PlannerPhase;
import org.voltdb.types.CalcitePlannerType;

/**
 * Some quick notes about "How Calcite Planner work".
 * reference https://www.slideshare.net/JordanHalterman/introduction-to-apache-calcite
 * steps:
 * 1. Optimize logical plan (the SQL query can directly translate to a initial logical plan,
 * then we optimize it to a better logical plan)
 * 2. Convert logical plan into a physical plan (represents the physical execution stages)
 *
 * common optimizations:
 * Prune unused fields, Merge projections, Convert subqueries to joins, Reorder joins,
 * Push down projections, Push down filters
 *
 * Key Concepts:
 *
 * # {@link org.apache.calcite.rel.RelNode} represents a relational expression
 * Sort, Join, Project, Filter, Scan...
 * Exp:
 * select col1 as id, col2 as name from foo where col1=21;
 *
 * Project ( id = [$0], name = [$1] ) <-- expression
 * Filter (condition=[= ($0, 21)])   <-- children/input
 * TableScan (table = [foo])
 *
 * # {@link org.apache.calcite.rex.RexNode} represents a row-level expression:
 * = scalar expr
 * Projection fields, conditions
 * Input column reference  -->  RexInputRef
 * Literal                 -->  RexLiteral
 * Struct field access     -->  RexFieldAccess
 * Function call           -->  RexCall
 * Window expression       -->  RexOver
 *
 * # traits
 * Defined by the {@link org.apache.calcite.plan.RelTrait} interface
 * Traits are used to validate plan output
 * {@link org.apache.calcite.plan.Convention}
 * {@link org.apache.calcite.rel.RelCollation}
 * {@link org.apache.calcite.rel.RelDistribution}
 *
 * ## Convention
 * Convention is a type of RelTrait, it is associated with a
 * RelNode interface
 *
 * Conventions are used to represent a single data source.
 *
 * describing how the expression passes data to its consuming relational expression
 *
 * Inputs to a relational expression must be in the same convention.
 *
 * # Rules
 * Rules are used to modify query plans.
 *
 * Defined by the {@link org.apache.calcite.plan.RelOptRule} interface
 *
 * Rules are matched to elements of a query plan using pattern matching
 * {@link org.apache.calcite.plan.RelOptRuleOperand}
 *
 * ## Converter
 * {@link org.apache.calcite.rel.convert.ConverterRule}
 * convert() is called for matched rules
 *
 * {@link org.apache.calcite.rel.convert.Converter}
 * By declaring itself to be a converter, a relational expression is telling the planner about this equivalence,
 * and the planner groups expressions which are logically equivalent but have different physical traits
 * into groups called RelSets.
 *
 * Q: why we need to put logically equivalent RelNode to a RelSet?
 * A: RelSet provides a level of indirection that allows Calcite to optimize queries.
 * If the input to a relational operator is an equivalence class, not a particular relational expression,
 * then Calcite has the freedom to choose the member of the equivalence class that has the cheapest cost.
 *
 *
 * ## Transformer
 * onMatch() is called for matched rules
 *
 * call.transformTo()
 *
 * # Planners
 * {@link org.apache.calcite.plan.volcano.VolcanoPlanner}
 * {@link org.apache.calcite.plan.hep.HepPlanner}
 *
 * # Program
 * {@link org.apache.calcite.tools.Program}
 *
 * Program that transforms a relational expression into another relational expression.
 * A planner is a sequence of programs, each of which is sometimes called a "phase".
 *
 * The most typical program is an invocation of the volcano planner with a particular RuleSet.
 */

/**
 * Util class to provide Calcite Planning methods.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class CalcitePlanner {
    /**
     * Transform RelNode to a new RelNode, targeting the provided set of traits.
     *
     * @param plannerType  The type of Planner to use.
     * @param phase        The transformation phase we're running.
     * @param input        The origianl RelNode
     * @param targetTraits The traits we are targeting for output.
     * @return The transformed RelNode.
     */
    static public RelNode transform(CalcitePlannerType plannerType, PlannerPhase phase, RelNode input,
                                    RelTraitSet targetTraits) {
        final RelTraitSet toTraits = targetTraits.simplify();
        final RelNode output;
        switch (plannerType) {
            case HEP: {
                final HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

                // add the ruleset to group, otherwise each rules will only apply once in order.
                hepProgramBuilder.addGroupBegin();
                phase.getRules().forEach(hepProgramBuilder::addRuleInstance);
                hepProgramBuilder.addGroupEnd();

                // create the HepPlanner.
                final HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
                // Set VoltDB Metadata Provider
                JaninoRelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(
                        VoltDBDefaultRelMetadataProvider.INSTANCE);
                RelMetadataQuery.THREAD_PROVIDERS.set(relMetadataProvider);

                // Modify RelMetaProvider for every RelNode in the SQL operator Rel tree.
                input = input.accept(new MetaDataProviderModifier(relMetadataProvider));
                planner.setRoot(input);
                if (!input.getTraitSet().equals(targetTraits)) {
                    planner.changeTraits(input, toTraits);
                }
                output = planner.findBestExp();
                break;
            }
            case VOLCANO: {
                // TODO: VOLCANO planner
                output = null;
                break;
            }
            default: {
                throw new RuntimeException("Dead branch.");
            }
        }

        return output;
    }

    /**
     * Transform RelNode to a new RelNode without changing any traits. Also will log the outcome.
     *
     * @param plannerType The type of Planner to use.
     * @param phase       The transformation phase we're running.
     * @param input       The origianl RelNode
     * @return The transformed RelNode.
     */
    static public RelNode transform(CalcitePlannerType plannerType, PlannerPhase phase, RelNode input) {
        return transform(plannerType, phase, input, input.getTraitSet());
    }

    public static class MetaDataProviderModifier extends RelShuttleImpl {
        private final RelMetadataProvider metadataProvider;

        public MetaDataProviderModifier(RelMetadataProvider metadataProvider) {
            this.metadataProvider = metadataProvider;
        }

        @Override
        public RelNode visit(TableScan scan) {
            scan.getCluster().setMetadataProvider(metadataProvider);
            return super.visit(scan);
        }

        @Override
        public RelNode visit(TableFunctionScan scan) {
            scan.getCluster().setMetadataProvider(metadataProvider);
            return super.visit(scan);
        }

        @Override
        public RelNode visit(LogicalValues values) {
            values.getCluster().setMetadataProvider(metadataProvider);
            return super.visit(values);
        }

        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            parent = super.visitChild(parent, i, child);
            parent.getCluster().setMetadataProvider(metadataProvider);
            return parent;
        }
    }
}
