/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.calciteadapter;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Result;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;

import com.google.common.base.Supplier;

import junit.framework.TestCase;

public class TestCalcite extends TestCase {

    // Stolen from TestVoltCompiler
    String testout_jar;

    @Override
    public void setUp() {
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "testout.jar";
    }

    @Override
    public void tearDown() {
        File tjar = new File(testout_jar);
        tjar.delete();
    }

    private boolean compileDDL(String ddl, VoltCompiler compiler) {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas>" +
            "<schema path='" + schemaPath + "' />" +
            "</schemas>" +
            "<procedures/>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        return compiler.compileWithProjectXML(projectPath, testout_jar);
    }

    private static class VoltDBTable implements org.apache.calcite.schema.TranslatableTable {

        org.voltdb.catalog.Table m_catTable;

        public VoltDBTable(org.voltdb.catalog.Table table) {
            m_catTable = table;
        }

        public static RelDataType toRelDataType(RelDataTypeFactory typeFactory, VoltType vt, int prec) {
            SqlTypeName sqlTypeName = SqlTypeName.get(vt.toSQLString().toUpperCase());
            RelDataType rdt;
            switch (vt) {
            case STRING:
                // This doesn't seem quite right...
                rdt = typeFactory.createSqlType(sqlTypeName, prec);
                rdt = typeFactory.createTypeWithCharsetAndCollation(rdt, Charset.forName("UTF-8"), SqlCollation.IMPLICIT);
                break;
                default:
                    rdt = typeFactory.createSqlType(sqlTypeName);
            }
            return rdt;
        }

        @Override
        public TableType getJdbcTableType() {
            // TODO Auto-generated method stub
            return Schema.TableType.TABLE;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            FieldInfoBuilder builder = typeFactory.builder();
            List<Column> columns = CatalogUtil.getSortedCatalogItems(m_catTable.getColumns(), "index");
            for (Column catColumn : columns) {
                VoltType vt = VoltType.get((byte)catColumn.getType());
                RelDataType rdt = toRelDataType(typeFactory, vt, catColumn.getSize());
                rdt = typeFactory.createTypeWithNullability(rdt, catColumn.getNullable());
                builder.add(catColumn.getName(), rdt);
            }
            return builder.build();
        }

        @Override
        public Statistic getStatistic() {
            return new Statistic() {

                @Override
                public Double getRowCount() {
                    // TODO Auto-generated method stub
                    return null;
                }

                @Override
                public boolean isKey(ImmutableBitSet columns) {
                    // TODO Auto-generated method stub
                    return false;
                }

                @Override
                public List<RelCollation> getCollations() {
                    return new ArrayList<>();
                }

                @Override
                public RelDistribution getDistribution() {
                    // TODO Auto-generated method stub
                    return null;
                }

            };
        }

        @Override
        public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
            return new VoltDBTableScan(context.getCluster(), relOptTable, this);
        }
    }

    private CatalogMap<Table> getCatalogTables(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables();
    }

    public enum VoltDBConvention implements Convention {
        INSTANCE;

        /** Cost of an VoltDB node versus implementing an equivalent node in a
         * "typical" calling convention. */
        public static final double COST_MULTIPLIER = 1.0d;

        @Override public String toString() {
          return getName();
        }

        @Override
        public Class getInterface() {
          return VoltDBRel.class;
        }

        @Override
        public String getName() {
          return "VOLTDB";
        }

        @Override
        public RelTraitDef getTraitDef() {
          return ConventionTraitDef.INSTANCE;
        }

        @Override
        public boolean satisfies(RelTrait trait) {
          return this == trait;
        }

        @Override
        public void register(RelOptPlanner planner) {}

        @Override
        public boolean canConvertConvention(Convention toConvention) {
          return false;
        }

        @Override
        public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits,
            RelTraitSet toTraits) {
          return false;
        }
      }

    public static interface VoltDBRel extends RelNode  {
    }

    public static class VoltDBTableScan extends TableScan implements VoltDBRel {

        final VoltDBTable m_voltDBTable;

        protected VoltDBTableScan(RelOptCluster cluster, RelOptTable table,
                VoltDBTable voltDBTable) {
              super(cluster, cluster.traitSetOf(VoltDBConvention.INSTANCE), table);
              this.m_voltDBTable = voltDBTable;
            }
    }

public static class VoltDBProject extends Project implements VoltDBRel {

    public VoltDBProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
          super(cluster, traitSet, input, projects, rowType);
          assert getConvention() instanceof VoltDBConvention;
        }

        @Deprecated // to be removed before 2.0
        public VoltDBProject(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode input, List<? extends RexNode> projects, RelDataType rowType,
            int flags) {
          this(cluster, traitSet, input, projects, rowType);
          Util.discard(flags);
        }

        /** Creates an VoltDBProject, specifying row type rather than field
         * names. */
        public static VoltDBProject create(final RelNode input,
            final List<? extends RexNode> projects, RelDataType rowType) {
          final RelOptCluster cluster = input.getCluster();
          final RelMetadataQuery mq = RelMetadataQuery.instance();
          final RelTraitSet traitSet =
              cluster.traitSet().replace(VoltDBConvention.INSTANCE)
                  .replaceIfs(RelCollationTraitDef.INSTANCE,
                      new Supplier<List<RelCollation>>() {
                        @Override
                        public List<RelCollation> get() {
                          return RelMdCollation.project(mq, input, projects);
                        }
                      });
          return new VoltDBProject(cluster, traitSet, input, projects, rowType);
        }

        @Override
        public VoltDBProject copy(RelTraitSet traitSet, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
          return new VoltDBProject(getCluster(), traitSet, input,
              projects, rowType);
        }
}

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

static class VoltDBProjectScanMergeRule extends RelOptRule {

    public VoltDBProjectScanMergeRule() {
        super(operand(VoltDBProject.class, operand(VoltDBTableScan.class, none())));
        // TODO Auto-generated constructor stub
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // TODO Auto-generated method stub
        assert false;
    }

}

    static class VoltDBRules {
        public static final ConverterRule PROJECT_RULE = new VoltDBProjectRule();
        public static final ConverterRule PROJECT_SCAN_MERGE_RULE = new VoltDBProjectRule();
    }

    private SchemaPlus schemaPlusFromDDL(String ddl) {
        VoltCompiler compiler = new VoltCompiler();
        boolean success = compileDDL(ddl, compiler);

        assertTrue(success);

        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        Catalog cat = compiler.getCatalog();
        for (Table table : getCatalogTables(cat)) {
            rootSchema.add(table.getTypeName(), new VoltDBTable(table));
        }

        return rootSchema;
    }

    public void testSchema() {
        String ddl = "create table test_calcite ("
                + "i integer primary key, "
                + "si smallint, "
                + "ti tinyint,"
                + "bi bigint,"
                + "f float not null, "
                + "v varchar(32));";
        SchemaPlus rootSchema = schemaPlusFromDDL(ddl);
        assertTrue(rootSchema != null);
    }

    private Planner getCalcitePlanner(SchemaPlus schemaPlus) {
      final FrameworkConfig config = Frameworks.newConfigBuilder()
              .parserConfig(SqlParser.Config.DEFAULT)
              .defaultSchema(schemaPlus)
              //.programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
              //.programs(Programs.ofRules(EnumerableRules.ENUMERABLE_PROJECT_RULE))
              .programs(Programs.ofRules(VoltDBRules.PROJECT_RULE, VoltDBRules.PROJECT_SCAN_MERGE_RULE))
              .build();
        return Frameworks.getPlanner(config);
    }

    private void parseValidateAndPlan(Planner planner, String sql) throws SqlParseException, ValidationException, RelConversionException {
        System.out.println("*****************************************");

        // Parse the input sql
        SqlNode parse = planner.parse(sql);
        System.out.println("**** Parsed stmt ****\n" + parse + "\n");

        // Validate the input sql
        SqlNode validate = planner.validate(parse);
        System.out.println("**** Validated stmt ****\n" + validate + "\n");

        // Convert the input sql to a relational expression
        RelNode convert = planner.rel(validate).project();
        System.out.println("**** Converted relation expression ****\n" +
                RelOptUtil.toString(convert) + "\n");

        // Transform the relational expression
        RelTraitSet traitSet = planner.getEmptyTraitSet()
            .replace(VoltDBConvention.INSTANCE)
//                .replace(EnumerableConvention.INSTANCE)
                //.replace(Convention.NONE);
                ;
        RelNode transform = planner.transform(0, traitSet, convert);

        System.out.println("**** Optimized relation expression ****\n" +
                RelOptUtil.toString(transform) + "\n");
        planner.close();
        planner.reset();
        System.out.println("*****************************************\n\n");
    }

    public void testCalcitePlanner() throws Exception {
        String ddl = "create table test_calcite ("
                + "i integer primary key, "
                + "si smallint, "
                + "ti tinyint,"
                + "bi bigint,"
                + "f float not null, "
                + "v varchar(32));"
                + "create table t2 ("
                + "pk integer primary key, vc varchar(256));";
        Planner planner = getCalcitePlanner(schemaPlusFromDDL(ddl));
        assertTrue(planner != null);


        parseValidateAndPlan(planner, "select f from test_calcite");
//        parseValidateAndPlan(planner, "select * from test_calcite");
//        parseValidateAndPlan(planner, "select f from test_calcite where ti = 3");
//        parseValidateAndPlan(planner, "select f from test_calcite where ti = 3");
//        parseValidateAndPlan(planner, "select f, vc from test_calcite as tc "
//                + "inner join t2 on tc.i = t2.pk where ti = 3");
//        parseValidateAndPlan(planner, "select f, vc from (select i, ti, f * f as f from test_calcite where bi = 10) as tc "
//                + "inner join "
//                + "(select * from t2 where pk > 20) as t2 on tc.i = t2.pk where ti = 3");
    }
}
