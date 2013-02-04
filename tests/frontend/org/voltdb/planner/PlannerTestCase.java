/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.net.URL;
import java.util.List;

import junit.framework.TestCase;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;

public class PlannerTestCase extends TestCase {

    private PlannerTestAideDeCamp m_aide;
    private boolean m_byDefaultPlanForSinglePartition;

    protected void failToCompile(String sql, String... patterns)
    {
        int paramCount = 0;
        for (int ii = 0; ii < sql.length(); ii++) {
            // Yes, we ARE assuming that test queries don't contain quoted question marks.
            if (sql.charAt(ii) == '?') {
                paramCount++;
            }
        }
        List<AbstractPlanNode> pn = null;
        try {
            pn = m_aide.compile(sql, paramCount, m_byDefaultPlanForSinglePartition, null);
            fail();
        }
        catch (PlanningErrorException ex) {
            String result = ex.toString();
            for (String pattern : patterns) {
                if ( ! result.contains(pattern)) {
                    System.out.println("Did not find pattern '" + pattern + "' in error string '" + result + "'");
                    fail();
                }
            }
        }
    }

    protected CompiledPlan compileAdHocPlan(String sql) {
        CompiledPlan cp = null;
        try {
            cp = m_aide.compileAdHocPlan(sql);
            assertTrue(cp != null);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        return cp;
    }

    /** A helper here where the junit test can assert success */
    protected List<AbstractPlanNode> compileToFragments(String sql)
    {
        int paramCount = 0;
        for (int ii = 0; ii < sql.length(); ii++) {
            // Yes, we ARE assuming that test queries don't contain quoted question marks.
            if (sql.charAt(ii) == '?') {
                paramCount++;
            }
        }
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, paramCount, planForSinglePartitionFalse, null);
    }

    /** A helper here where the junit test can assert success */
    protected List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql, int paramCount, boolean planForSinglePartition, String joinOrder)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn = m_aide.compile(sql, paramCount, planForSinglePartition, joinOrder);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        if (planForSinglePartition) {
            assertTrue(pn.size() == 1);
        }
        return pn;
    }

    protected AbstractPlanNode compileWithJoinOrder(String sql, String joinOrder)
    {
        int paramCount = 0;
        for (int ii = 0; ii < sql.length(); ii++) {
            // Yes, we ARE assuming that test queries don't contain quoted question marks.
            if (sql.charAt(ii) == '?') {
                paramCount++;
            }
        }
        return compileWithJoinOrder(sql, paramCount, joinOrder);
    }


    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compileWithJoinOrder(String sql, int paramCount, String joinOrder)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn = compileWithJoinOrderToFragments(sql, paramCount, m_byDefaultPlanForSinglePartition, joinOrder);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        return pn.get(0);
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compile(String sql)
    {
        int paramCount = 0;
        for (int ii = 0; ii < sql.length(); ii++) {
            // Yes, we ARE assuming that test queries don't contain quoted question marks.
            if (sql.charAt(ii) == '?') {
                paramCount++;
            }
        }
        return compile(sql, paramCount);
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compile(String sql, int paramCount)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn = compileWithJoinOrderToFragments(sql, paramCount, m_byDefaultPlanForSinglePartition, null);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn.get(0) != null);
        return pn.get(0);
    }


    protected void setupSchema(URL ddlURL, String basename,
                               boolean planForSinglePartition) throws Exception
    {
        m_aide = new PlannerTestAideDeCamp(ddlURL, basename);
        m_byDefaultPlanForSinglePartition = planForSinglePartition;
    }

    protected void forceReplication()
    {
        // Set all tables to non-replicated.
        Cluster cluster = m_aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            // t.setIsreplicated(true);
            assertTrue(t.getIsreplicated());
        }
    }

    protected void forceReplicationExceptForOneTable(String table, String column)
    {
        // Set all tables to non-replicated.
        Cluster cluster = m_aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            assertTrue(t.getIsreplicated());
            if (t.getTypeName().equalsIgnoreCase(table)) {
                t.setIsreplicated(false);
                t.setPartitioncolumn(t.getColumns().get(column));
            }
        }
    }

    // TODO: Phase out this functionality, possibly by phasing out PlannerTestAideDeCamp in favor
    // of some other utility class -- one that supports inline PARTITION statements in the DDL.
    // It really is a hack because it creates an otherwise unlikely condition of
    // partitioned tables with no identified partitioning column.
    protected void forceHackPartitioning() {
        // Set all tables to non-replicated.
        Cluster cluster = m_aide.getCatalog().getClusters().get("cluster");
        CatalogMap<Table> tmap = cluster.getDatabases().get("database").getTables();
        for (Table t : tmap) {
            t.setIsreplicated(false);
        }
    }

    Database getDatabase() {
        return m_aide.getDatabase();
    }
}
