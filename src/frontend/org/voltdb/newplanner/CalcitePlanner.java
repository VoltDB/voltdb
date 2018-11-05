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

public class CalcitePlanner {
    /**
     * Transform RelNode to a new RelNode, targeting the provided set of traits.
     *
     * @param plannerType The type of Planner to use.
     * @param phase The transformation phase we're running.
     * @param input The origianl RelNode
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
                phase.getRules().forEach(hepProgramBuilder::addRuleInstance);

                // create the HepPlanner.
                final HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
                // Set VoltDB Metadata Provider
                JaninoRelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(
                        VoltDBDefaultRelMetadataProvider.INSTANCE);
                RelMetadataQuery.THREAD_PROVIDERS.set(relMetadataProvider);

                // Modify RelMetaProvider for every RelNode in the SQL operator Rel tree.
                input.accept(new MetaDataProviderModifier(relMetadataProvider));
                planner.setRoot(input);
                if (!input.getTraitSet().equals(targetTraits)) {
                    planner.changeTraits(input, toTraits);
                }
                output = planner.findBestExp();
                break;
            }
            case VOLCANO:
            default: {
                // TODO: VOLCANO planner
                output = null;
                break;
            }
        }

        return output;
    }

    /**
     * Transform RelNode to a new RelNode without changing any traits. Also will log the outcome.
     *
     * @param plannerType
     *          The type of Planner to use.
     * @param phase
     *          The transformation phase we're running.
     * @param input
     *          The origianl RelNode
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
            child.accept(this);
            parent.getCluster().setMetadataProvider(metadataProvider);
            return parent;
        }
    }
}
