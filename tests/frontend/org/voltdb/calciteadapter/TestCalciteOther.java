/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.utils.BuildDirectoryUtils;

import junit.framework.TestCase;

public class TestCalciteOther extends TestCase {

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
        return compiler.compileDDLString(ddl, testout_jar);
    }

    private CatalogMap<Table> getCatalogTables(Catalog catalog) {
        return catalog.getClusters().get("cluster").getDatabases().get("database").getTables();
    }

    private SchemaPlus schemaPlusFromDDL(String ddl) {
        VoltCompiler compiler = new VoltCompiler(true, false);
        boolean success = compileDDL(ddl, compiler);

        assertTrue(success);

        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        Catalog cat = compiler.getCatalog();
        for (Table table : getCatalogTables(cat)) {
            rootSchema.add(table.getTypeName(), new VoltDBTable(table));
        }

        return rootSchema;
    }

    private Catalog ddlToCatalog(String ddl) {
        VoltCompiler compiler = new VoltCompiler(true, false);
        boolean success = compileDDL(ddl, compiler);

        assertTrue(success);
        return compiler.getCatalog();
    }

//    public void testSchema() {
//        String ddl = "create table test_calcite ("
//                + "i integer primary key, "
//                + "si smallint, "
//                + "ti tinyint,"
//                + "bi bigint,"
//                + "f float not null, "
//                + "v varchar(32));";
//        SchemaPlus rootSchema = schemaPlusFromDDL(ddl);
//        assertTrue(rootSchema != null);
//    }

    public void testCalcitePlanner() throws Exception {
        String ddl = "create table test_calcite ("
                + "i integer primary key, "
                + "si smallint, "
                + "ti tinyint,"
                + "bi bigint,"
                + "f float not null, "
                + "v varchar(32));"
                + "create table test ("
                + "i integer primary key, "
                + "si smallint, "
                + "ti tinyint,"
                + "bi bigint,"
                + "f float not null, "
                + "v varchar(32));"
                + "create table t2 ("
                + "pk integer primary key, vc varchar(256));\n\n"
                + "create table partitioned ("
                + "i integer primary key not null, "
                + "si smallint, "
                + "ti tinyint,"
                + "bi bigint,"
                + "f float not null, "
                + "v varchar(32));\n"
                + "partition table partitioned on column i;";

        Catalog cat = ddlToCatalog(ddl);
        CompiledPlan plan;

        Database db = cat.getClusters().get("cluster").getDatabases().get("database");
//        plan = CalcitePlanner.plan(db, "select * from partitioned");
//        System.out.println(plan.explainedPlan);

//        plan = CalcitePlanner.plan(db, "select i from partitioned p where si = 345");
//        System.out.println(plan.explainedPlan);

//        PlanNodeTree planTree = new PlanNodeTree(plan.rootPlanGraph);
//        String planJson = planTree.toJSONString();
//        System.out.println(planJson);

//        plan = CalcitePlanner.plan(db, "select f from test_calcite");
//        System.out.println(plan.explainedPlan);
//        plan = CalcitePlanner.plan(db, "select * from test_calcite");
//        System.out.println(plan.explainedPlan);
//        plan = CalcitePlanner.plan(db, "select i from test_calcite where v = 'foo'");
//        plan = CalcitePlanner.plan(db, "select i from test_calcite where ti = 10");

        // should be t1-t2 because of an additional filer on t1 with everything else equal
//        plan = CalcitePlanner.plan(db,
//                "select test.i from test inner join " +
//                "test_calcite  on test_calcite.i = test.i where test_calcite.v = 'foo';");
//        System.out.println(plan.explainedPlan);

//        plan = CalcitePlanner.plan(db,
//                "select t1.v, t2.v "
//                + "from "
//                + "  (select * from test_calcite where v = 'foo') as t1 "
//                + "  inner join "
//                + "  (select * from test_calcite where f = 30.3) as t2 "
//                + "on t1.i = t2.i "
//                + "where t1.i = 3;"
//                );
//        System.out.println(plan.explainedPlan);
//
//        plan = CalcitePlanner.plan(db, "select * from partitioned;");
//        System.out.println(plan.explainedPlan);
//
//        plan = CalcitePlanner.plan(db, "select * from partitioned as p1 inner join partitioned as p2 on p1.i = p2.i;");
//        System.out.println(plan.explainedPlan);
//
//        plan = CalcitePlanner.plan(db, "select * from test_calcite as p1 inner join test_calcite as p2 on p1.i = p2.i;");
//        System.out.println(plan.explainedPlan);

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
