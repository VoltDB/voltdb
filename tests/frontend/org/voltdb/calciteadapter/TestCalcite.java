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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.calciteadapter.CalcitePlanner.VoltDBRules;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.utils.BuildDirectoryUtils;

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

    private CatalogMap<Table> getCatalogTables(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables();
    }


    private static AbstractPlanNode calciteToVoltDBPlan(VoltDBRel rel) {
        AbstractPlanNode root = rel.toPlanNode();
        SendPlanNode sendNode = new SendPlanNode();
        sendNode.addAndLinkChild(root);
        return sendNode;
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
        System.out.println("**** Converted relational expression ****\n" +
                RelOptUtil.toString(convert) + "\n");

        // Transform the relational expression
        RelTraitSet traitSet = planner.getEmptyTraitSet()
            .replace(VoltDBConvention.INSTANCE)
//                .replace(EnumerableConvention.INSTANCE)
                //.replace(Convention.NONE);
                ;
        RelNode transform = planner.transform(0, traitSet, convert);

        System.out.println("**** Optimized relational expression ****\n" +
                RelOptUtil.toString(transform) + "\n");

        AbstractPlanNode plan = calciteToVoltDBPlan((VoltDBRel)transform);
        PlanNodeList nodelist = new PlanNodeList(plan);
        String jsonString = nodelist.toJSONString();
        System.out.println("**** Converted to VoltDB Plan ****\n" +
                plan.toExplainPlanString() + "\n\n" + jsonString+ "\n");

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
