package org.voltdb.calciteadapter;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableThetaJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
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

    // unneeded for now???
    static class VoltDBProjectRule extends ConverterRule {
        VoltDBProjectRule() {
          super(LogicalProject.class, RelOptUtil.PROJECT_PREDICATE, Convention.NONE,
              VoltDBConvention.INSTANCE, "VoltDBProjectRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
          final LogicalProject project = (LogicalProject) rel;
          return VoltDBProject.create(
              convert(project.getInput(),
                  project.getInput().getTraitSet()
                      .replace(VoltDBConvention.INSTANCE)),
              project.getProjects(),
              project.getRowType());
        }
      }

    private static class VoltDBCalcScanMergeRule extends RelOptRule {

        public static final VoltDBCalcScanMergeRule INSTANCE = new VoltDBCalcScanMergeRule();

        private VoltDBCalcScanMergeRule() {
            super(operand(LogicalCalc.class, operand(VoltDBTableScan.class, none())));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalCalc calc = call.rel(0);
            VoltDBTableScan scan = call.rel(1);
            call.transformTo(scan.copy(calc.getProgram()));
        }
    }

    static class VoltDBJoinRule extends ConverterRule {

        public static final VoltDBJoinRule INSTANCE = new VoltDBJoinRule();

        VoltDBJoinRule() {
            super(
                    LogicalJoin.class,
                    Convention.NONE,
                    VoltDBConvention.INSTANCE,
                    "VoltDBJoinRule");
        }

        @Override public RelNode convert(RelNode rel) {
            LogicalJoin join = (LogicalJoin) rel;
            List<RelNode> newInputs = new ArrayList<>();
            for (RelNode input : join.getInputs()) {
              if (!(input.getConvention() instanceof VoltDBConvention)) {
                input =
                    convert(
                        input,
                        input.getTraitSet()
                            .replace(VoltDBConvention.INSTANCE));
              }
              newInputs.add(input);
            }
            final RelOptCluster cluster = join.getCluster();
            final RelTraitSet traitSet =
                join.getTraitSet().replace(VoltDBConvention.INSTANCE);
            final RelNode left = newInputs.get(0);
            final RelNode right = newInputs.get(1);
            if (join.getJoinType() != JoinRelType.INNER) {
                return null;
            }
            RelNode newRel;
              newRel = new VoltDBJoin(
                  cluster,
                  traitSet,
                  left,
                  right,
                  join.getCondition(),
                  join.getVariablesSet(),
                  join.getJoinType());
            return newRel;
          }
      }


    private static class VoltDBCalcJoinMergeRule extends RelOptRule {

        public static final VoltDBCalcJoinMergeRule INSTANCE = new VoltDBCalcJoinMergeRule();

        private VoltDBCalcJoinMergeRule() {
            super(operand(LogicalCalc.class, operand(VoltDBJoin.class, any())));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalCalc calc = call.rel(0);
            VoltDBJoin join = call.rel(1);
            call.transformTo(join.copy(calc.getProgram()));
        }

    }

    private static class VoltDBRules {
        //public static final ConverterRule PROJECT_RULE = new VoltDBProjectRule();
        //public static final RelOptRule PROJECT_SCAN_MERGE_RULE = new VoltDBProjectScanMergeRule();

        public static List<RelOptRule> getRules() {
            List<RelOptRule> rules = new ArrayList<>();

            rules.add(VoltDBCalcScanMergeRule.INSTANCE);

            rules.add(VoltDBJoinRule.INSTANCE);
            rules.add(VoltDBCalcJoinMergeRule.INSTANCE);

            rules.add(CalcMergeRule.INSTANCE);
            rules.add(FilterCalcMergeRule.INSTANCE);
            rules.add(FilterToCalcRule.INSTANCE);
            rules.add(ProjectCalcMergeRule.INSTANCE);
            rules.add(ProjectToCalcRule.INSTANCE);


            return rules;
        }
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

            System.out.println("\n**** Converted relational expression ****\n" +
                    RelOptUtil.toString(convert) + "\n");

            // Transform the relational expression
            RelTraitSet traitSet = planner.getEmptyTraitSet()
                    .replace(VoltDBConvention.INSTANCE);
            RelNode transform = planner.transform(0, traitSet, convert);

            sb.append("**** Optimized relational expression ****\n" +
                    RelOptUtil.toString(transform) + "\n");

            System.out.println("\n**** Optimized relational expression ****\n" +
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
