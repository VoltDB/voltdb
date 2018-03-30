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

package org.voltdb.calciteadapter.planner;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.calciteadapter.rel.logical.VoltDBLogicalRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPhysicalRel;
import org.voltdb.calciteadapter.rules.VoltDBRules;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SendPlanNode;

public class CalcitePlanner {

    private static SchemaPlus schemaPlusFromDatabase(Database db) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(false);
        for (Table table : db.getTables()) {
            rootSchema.add(table.getTypeName(), new VoltDBTable(table));
        }

        return rootSchema;
    }

    private static Planner getPlanner(SchemaPlus schema) {
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(schema)
                .programs(VoltDBRules.getProgram())
                .build();
          return Frameworks.getPlanner(config);

    }


    private static CompiledPlan calciteToVoltDBPlan(VoltDBPhysicalRel rel, CompiledPlan compiledPlan) {

        RexConverter.resetParameterIndex();

        AbstractPlanNode root = rel.toPlanNode();
        SendPlanNode sendNode = new SendPlanNode();
        sendNode.addAndLinkChild(root);
        root = sendNode;

        compiledPlan.rootPlanGraph = root;

        PostBuildVisitor postPlannerVisitor = new PostBuildVisitor();
        root.acceptVisitor(postPlannerVisitor);

        compiledPlan.setReadOnly(true);
        compiledPlan.statementGuaranteesDeterminism(
                postPlannerVisitor.hasLimitOffset(), // no limit or offset
                postPlannerVisitor.isOrderDeterministic(),  // is order deterministic
                null); // no details on determinism

        compiledPlan.setStatementPartitioning(StatementPartitioning.forceSP());

        compiledPlan.setParameters(postPlannerVisitor.getParameterValueExpressions());

        return compiledPlan;
    }


    public static CompiledPlan plan(Database db, String sql, String dirName, boolean isLargeQuery) {
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        SchemaPlus schema = schemaPlusFromDatabase(db);
        Planner planner = getPlanner(schema);
        CompiledPlan compiledPlan = new CompiledPlan(isLargeQuery);

        compiledPlan.sql = sql;

        SqlNode parsedSql = null;
        SqlNode validatedSql = null;
        RelNode convertedRel = null;
        RelTraitSet traitSet = null;
        RelNode phaseOneRel = null;
        RelNode phaseTwoRel = null;
        RelNode phaseThreeRel = null;
        String errMsg = null;

        try {
            // Parse the input sql
            parsedSql = planner.parse(sql);

            // Validate the input sql
            validatedSql = planner.validate(parsedSql);

            // Convert the input sql to a relational expression
            convertedRel = planner.rel(validatedSql).project();

            // Transform the relational expression

            // Apply Rule set 0 - standard Calcite transformations and convert to the VOLTDB Logical convention
            traitSet = prepareFinalTraitSet(planner, VoltDBLogicalRel.VOLTDB_LOGICAL, convertedRel.getTraitSet());
            phaseOneRel = planner.transform(0, traitSet, convertedRel);

            // Apply Rule Set 1 - VoltDB transformations
            // Add traits that the transformed relNode must have
            traitSet = prepareFinalTraitSet(planner, VoltDBPhysicalRel.VOLTDB_PHYSICAL, phaseOneRel.getTraitSet());
            phaseTwoRel = planner.transform(1, traitSet, phaseOneRel);

            // Apply Rule Set 2 - VoltDB transformations
            phaseThreeRel = planner.transform(2, traitSet, phaseTwoRel);

            // Convert To VoltDB plan
            calciteToVoltDBPlan((VoltDBPhysicalRel)phaseThreeRel, compiledPlan);

            String explainPlan = compiledPlan.rootPlanGraph.toExplainPlanString();

            compiledPlan.explainedPlan = explainPlan;
            // Renumber the plan node ids to start with 1
            compiledPlan.resetPlanNodeIds(1);

            PlanDebugOutput.outputPlanFullDebug(compiledPlan, compiledPlan.rootPlanGraph,
                    dirName, "JSON");
            PlanDebugOutput.outputExplainedPlan(compiledPlan, dirName, "CALCITE");
        }
        catch (Throwable e) {
            errMsg = e.getMessage();
            System.out.println("For some reason planning failed!..And here's the error:");
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.out.println("And here's how far we have gotten:\n");
            throw new PlanningErrorException(e.getMessage());
        } finally {
            planner.close();
            planner.reset();

            PlanDebugOutput.outputCalcitePlanningDetails(
                    sql, dirName, "DEBUG", errMsg,
                    parsedSql, validatedSql, convertedRel,
                    phaseOneRel, phaseTwoRel, phaseThreeRel);
        }
        return compiledPlan;
    }

    private static RelTraitSet prepareFinalTraitSet(
            Planner planner,
            Convention outConvention,
            RelTraitSet traceSetIn) {
        RelTraitSet traitSet = planner.getEmptyTraitSet().replace(outConvention);
        if (traceSetIn != null) {
            RelTrait collationTrait = traceSetIn.getTrait(RelCollationTraitDef.INSTANCE);
            if (collationTrait instanceof RelCollation) {
                traitSet = traitSet.plus(collationTrait);
            }
        }
        return traitSet;
    }
}
