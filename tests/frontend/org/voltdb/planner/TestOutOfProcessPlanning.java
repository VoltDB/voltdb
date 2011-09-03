/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.CatalogUtil;

public class TestOutOfProcessPlanning extends TestCase {

    PlannerTool m_pt = null;

    class PlannerKillerThread extends Thread {
        AtomicBoolean m_shouldStop = new AtomicBoolean(false);
        String m_serializedCatalog = null;
        int m_timeout = 2000;

        public PlannerKillerThread(int timeout)
        {
            m_timeout = timeout;
        }

        @Override
        public void run() {
            while (m_shouldStop.get() == false) {
                //if (m_pt.expensiveIsRunningCheck() == false) {
                //  m_pt =
                //}

                if (m_pt.perhapsIsHung(m_timeout)) {
                    m_pt.kill();
                    m_pt = PlannerTool.createPlannerToolProcess(m_serializedCatalog);
                }

                Thread.yield();
            }
        }

    }

    public void testSimple() throws IOException {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addAllDefaults();
        builder.compile("tpcc-oop.jar");

        byte[] bytes = CatalogUtil.toBytes(new File("tpcc-oop.jar"));
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(bytes, null);

        m_pt = PlannerTool.createPlannerToolProcess(serializedCatalog);

        PlannerKillerThread ptKiller = new PlannerKillerThread(2000);
        ptKiller.m_serializedCatalog = serializedCatalog;
        ptKiller.start();

        PlannerTool.Result result = null;
        result = m_pt.planSql("select * from warehouse;", false);
        System.out.println(result);

        result = m_pt.planSql("select * from WAREHOUSE, DISTRICT, CUSTOMER, CUSTOMER_NAME, HISTORY, STOCK, ORDERS, NEW_ORDER, ORDER_LINE where " +
                "WAREHOUSE.W_ID = DISTRICT.D_W_ID and " +
                "WAREHOUSE.W_ID = CUSTOMER.C_W_ID and " +
                "WAREHOUSE.W_ID = CUSTOMER_NAME.C_W_ID and " +
                "WAREHOUSE.W_ID = HISTORY.H_W_ID and " +
                "WAREHOUSE.W_ID = STOCK.S_W_ID and " +
                "WAREHOUSE.W_ID = ORDERS.O_W_ID and " +
                "WAREHOUSE.W_ID = NEW_ORDER.NO_W_ID and " +
                "WAREHOUSE.W_ID = ORDER_LINE.OL_W_ID and " +
                "WAREHOUSE.W_ID = 0", false);
        System.out.println(result);

        result = m_pt.planSql("ryan likes the yankees", false);
        System.out.println(result);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        result = m_pt.planSql("ryan likes the yankees", false);
        System.out.println(result);

        result = m_pt.planSql("select * from warehouse;", false);
        System.out.println(result);

        final File jar = new File("tpcc-oop.jar");
        jar.delete();

        ptKiller.m_shouldStop.set(true);
        try {
            ptKiller.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

        builder.compile("testbadddl-oop.jar");
        byte[] bytes = CatalogUtil.toBytes(new File("testbadddl-oop.jar"));
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(bytes, null);
        assertNotNull(serializedCatalog);
        Catalog c = new Catalog();
        c.execute(serializedCatalog);
        m_pt = PlannerTool.createPlannerToolProcess(serializedCatalog);

        PlannerKillerThread ptKiller = new PlannerKillerThread(60000);
        ptKiller.m_serializedCatalog = serializedCatalog;
        ptKiller.start();

        // Bad DDL would kill the planner before it starts and this query
        // would return a Stream Closed error
        PlannerTool.Result result = null;
        result = m_pt.planSql("select * from A;", false);
        System.out.println(result);
        assertNotSame("Stream closed", result.getErrors());
        assertNull(result.getErrors());

        final File jar = new File("testbadddl-oop.jar");
        jar.delete();

        ptKiller.m_shouldStop.set(true);
        try {
            ptKiller.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
