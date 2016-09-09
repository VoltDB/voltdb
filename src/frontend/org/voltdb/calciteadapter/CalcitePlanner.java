package org.voltdb.calciteadapter;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
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
                .programs(Programs.ofRules(VoltDBRules.getRules()))
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
        StringBuffer sb = new StringBuffer();

        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        SchemaPlus schema = schemaPlusFromDatabase(db);
        Planner planner = getPlanner(schema);
        CompiledPlan compiledPlan = new CompiledPlan();

        sb.append("\n*****************************************\n");
        sb.append("Calcite Planning Details\n");
        compiledPlan.sql = sql;

        try {
            // Parse the input sql
            SqlNode parse = planner.parse(sql);
            sb.append("\n**** Parsed stmt ****\n" + parse + "\n");

            // Validate the input sql
            SqlNode validate = planner.validate(parse);
            sb.append("\n**** Validated stmt ****\n" + validate + "\n");

            // Convert the input sql to a relational expression
            RelNode convert = planner.rel(validate).project();
            sb.append("\n**** Converted relational expression ****\n" +
                    RelOptUtil.toString(convert) + "\n");

            // Transform the relational expression
            RelTraitSet traitSet = planner.getEmptyTraitSet()
                    .replace(VoltDBConvention.INSTANCE);
            RelNode transform = planner.transform(0, traitSet, convert);

            sb.append("**** Optimized relational expression ****\n" +
                    RelOptUtil.toString(transform) + "\n");

            calciteToVoltDBPlan((VoltDBRel)transform, compiledPlan);

            planner.close();
            planner.reset();
            sb.append("*****************************************\n\n");

            String explainPlan = compiledPlan.rootPlanGraph.toExplainPlanString() + "\n\n"
                    + sb.toString();
            compiledPlan.explainedPlan = explainPlan;

        }
        catch (Throwable e) {
            throw new PlanningErrorException(e.getMessage());
        }


        return compiledPlan;
    }


}
