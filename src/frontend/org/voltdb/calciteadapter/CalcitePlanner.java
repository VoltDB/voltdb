package org.voltdb.calciteadapter;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.voltdb.calciteadapter.rel.VoltDBRel;
import org.voltdb.calciteadapter.rules.VoltDBRules;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.ParameterValueExpression;
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


    private static CompiledPlan calciteToVoltDBPlan(VoltDBRel rel, CompiledPlan compiledPlan) {
        AbstractPlanNode root = rel.toPlanNode();
        SendPlanNode sendNode = new SendPlanNode();
        sendNode.addAndLinkChild(root);
        root = sendNode;

        compiledPlan.rootPlanGraph = root;

        compiledPlan.setReadOnly(true);
        compiledPlan.statementGuaranteesDeterminism(
                false, // no limit or offset
                true,  // is order deterministic
                null); // no details on determinism

        compiledPlan.setStatementPartitioning(StatementPartitioning.forceSP());
        compiledPlan.parameters = new ParameterValueExpression[0];

        return compiledPlan;
    }


    public static CompiledPlan plan(Database db, String sql) {
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        SchemaPlus schema = schemaPlusFromDatabase(db);
        Planner planner = getPlanner(schema);
        CompiledPlan compiledPlan = new CompiledPlan();

        compiledPlan.sql = sql;

        SqlNode parse = null;
        SqlNode validate = null;
        RelNode convert = null;
        RelTraitSet traitSet = null;
        RelNode transform = null;

        try {
            // Parse the input sql
            parse = planner.parse(sql);

            // Validate the input sql
            validate = planner.validate(parse);

            // Convert the input sql to a relational expression
            convert = planner.rel(validate).project();

            // Transform the relational expression
            traitSet = planner.getEmptyTraitSet()
                    .replace(VoltDBConvention.INSTANCE);
            transform = planner.transform(0, traitSet, convert);

            calciteToVoltDBPlan((VoltDBRel)transform, compiledPlan);

            String explainPlan = compiledPlan.rootPlanGraph.toExplainPlanString();

            compiledPlan.explainedPlan = explainPlan;
            // Renumber the plan node ids to start with 1
            compiledPlan.resetPlanNodeIds(1);

            PlanDebugOutput.outputPlanFullDebug(compiledPlan, compiledPlan.rootPlanGraph,
                    "1", "1");
            PlanDebugOutput.outputExplainedPlan(compiledPlan, "1", "1");
        }
        catch (Throwable e) {
            System.out.println("For some reason planning failed!..And here's the error:");
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.out.println("And here's how far we have gotten:\n");
            throw new PlanningErrorException(e.getMessage());
        } finally {
            planner.close();
            planner.reset();

            PlanDebugOutput.outputCalcitePlanningDetails(sql, parse, validate, convert, transform,
                    "1", "1");
        }
        return compiledPlan;
    }



}
