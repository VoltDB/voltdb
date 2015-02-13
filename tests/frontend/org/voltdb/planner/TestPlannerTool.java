/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.planner;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.CatalogContext;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestPlannerTool extends TestCase {

    PlannerTool m_pt = null;

    public void testSimple() throws IOException {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addAllDefaults();
        final File jar = new File("tpcc-oop.jar");
        jar.deleteOnExit();

        //long start = System.nanoTime();
        //for (int i = 0; i < 10000; i++) {
        builder.compile("tpcc-oop.jar");
        /*    long end = System.nanoTime();
            System.err.printf("Took %.3f seconds to compile.\n",
                    (end - start) / 1000000000.0);
            start = end;
        }*/

        byte[] bytes = MiscUtils.fileToBytes(new File("tpcc-oop.jar"));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes).getFirst());
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);
        CatalogContext context = new CatalogContext(0, 0, catalog, bytes, new byte[] {}, 0);

        m_pt = new PlannerTool(context.cluster, context.database, context.getCatalogHash());

        AdHocPlannedStatement result = null;
        result = m_pt.planSqlForTest("select * from warehouse;");
        System.out.println(result);

        // try many tables joins
        try {
            result = m_pt.planSqlForTest("select * from WAREHOUSE, DISTRICT, CUSTOMER, CUSTOMER_NAME, HISTORY, STOCK, ORDERS, NEW_ORDER, ORDER_LINE where " +
                "WAREHOUSE.W_ID = DISTRICT.D_W_ID and " +
                "WAREHOUSE.W_ID = CUSTOMER.C_W_ID and " +
                "WAREHOUSE.W_ID = CUSTOMER_NAME.C_W_ID and " +
                "WAREHOUSE.W_ID = HISTORY.H_W_ID and " +
                "WAREHOUSE.W_ID = STOCK.S_W_ID and " +
                "WAREHOUSE.W_ID = ORDERS.O_W_ID and " +
                "WAREHOUSE.W_ID = NEW_ORDER.NO_W_ID and " +
                "WAREHOUSE.W_ID = ORDER_LINE.OL_W_ID and " +
                "WAREHOUSE.W_ID = 0");
        }
        catch (Exception e) {
            // V4.5 supports multiple table joins
            fail();
        }

        // commented out code put the big stat
        /*int i = 0;
        while (i == 0) {
            long start = System.currentTimeMillis();*/
        // try just the right amount of tables
        try {
            result = m_pt.planSqlForTest("select * from CUSTOMER, STOCK, ORDERS, ORDER_LINE, NEW_ORDER where " +
                "CUSTOMER.C_W_ID = CUSTOMER.C_W_ID and " +
                "CUSTOMER.C_W_ID = STOCK.S_W_ID and " +
                "CUSTOMER.C_W_ID = ORDERS.O_W_ID and " +
                "CUSTOMER.C_W_ID = ORDER_LINE.OL_W_ID and " +
                "CUSTOMER.C_W_ID = NEW_ORDER.NO_W_ID and " +
                "CUSTOMER.C_W_ID = 0");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        /*    long duration = System.currentTimeMillis() - start;
            System.out.printf("Nasty query took %.2f seconds\n", duration / 1000.0);
        }*/

        // try garbage
        try {
            result = m_pt.planSqlForTest("ryan likes the yankees");
            fail();
        }
        catch (Exception e) {}

        try {
            Thread.sleep(500);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        try {
            result = m_pt.planSqlForTest("ryan likes the yankees");
            fail();
        }
        catch (Exception e) {}

        result = m_pt.planSqlForTest("select * from warehouse;");
        System.out.println(result);
    }

    public void testBadDDL() throws IOException
    {
        // semicolons in in-lined comments are bad
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE A (C1 BIGINT NOT NULL, PRIMARY KEY(C1)); -- this; is bad");
        builder.addPartitionInfo("A", "C1");
        // semicolons in string literals are bad
        builder.addLiteralSchema("create table t(id bigint not null, name varchar(5) default 'a;bc', primary key(id));");
        builder.addPartitionInfo("t", "id");
        // Add a newline string literal case just for fun
        builder.addLiteralSchema("create table s(id bigint not null, name varchar(5) default 'a\nb', primary key(id));");
        builder.addStmtProcedure("MakeCompileHappy",
                                 "SELECT * FROM A WHERE C1 = ?;",
                                 "A.C1: 0");

        final File jar = new File("testbadddl-oop.jar");
        jar.deleteOnExit();
        builder.compile("testbadddl-oop.jar");
        byte[] bytes = MiscUtils.fileToBytes(new File("testbadddl-oop.jar"));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes).getFirst());
        assertNotNull(serializedCatalog);
        Catalog c = new Catalog();
        c.execute(serializedCatalog);
        CatalogContext context = new CatalogContext(0, 0, c, bytes, new byte[] {}, 0);

        m_pt = new PlannerTool(context.cluster, context.database, context.getCatalogHash());

        // Bad DDL would kill the planner before it starts and this query
        // would return a Stream Closed error
        m_pt.planSqlForTest("select * from A;");
    }
}
