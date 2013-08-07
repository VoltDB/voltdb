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

import org.apache.commons.lang3.StringUtils;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.plannodes.AbstractPlanNode;

public class PlannerTestCase extends TestCase {

    private PlannerTestAideDeCamp m_aide;
    private boolean m_byDefaultPlanForSinglePartition;

    protected void failToCompile(String sql, String... patterns)
    {
        int paramCount = 0;
        int skip = 0;
        while(true) {
            // Yes, we ARE assuming that test queries don't contain quoted question marks.
            skip = sql.indexOf('?', skip);
            if (skip == -1) {
                break;
            }
            skip++;
            paramCount++;
        }
        try {
            m_aide.compile(sql, paramCount, m_byDefaultPlanForSinglePartition, null);
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
        return compileAdHocPlan(sql, DeterminismMode.SAFER);
    }

    protected CompiledPlan compileAdHocPlan(String sql, DeterminismMode detMode) {
        CompiledPlan cp = null;
        try {
            cp = m_aide.compileAdHocPlan(sql, detMode);
            assertTrue(cp != null);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        return cp;
    }

    final int paramCount = 0;
    String noJoinOrder = null;
    /** A helper here where the junit test can assert success */
    protected List<AbstractPlanNode> compileSinglePartitionToFragments(String sql)
    {
        boolean planForSinglePartitionTrue = true;
        return compileWithJoinOrderToFragments(sql, planForSinglePartitionTrue, noJoinOrder);
    }

    /** A helper here where the junit test can assert success */
    protected List<AbstractPlanNode> compileToFragments(String sql)
    {
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, planForSinglePartitionFalse, noJoinOrder);
    }

    /** A helper here where the junit test can assert success */
    protected List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql, String joinOrder)
    {
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, planForSinglePartitionFalse, joinOrder);
    }

    /** A helper here where the junit test can assert success */
    private List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql,
                                                                   boolean planForSinglePartition,
                                                                   String joinOrder)
    {
        int paramCount = 0;
        for (int ii = 0; ii < sql.length(); ii++) {
            // Yes, we ARE assuming that test queries don't contain quoted question marks.
            if (sql.charAt(ii) == '?') {
                paramCount++;
            }
        }
        return compileWithJoinOrderToFragments(sql, paramCount, planForSinglePartition, joinOrder);
    }

    /** A helper here where the junit test can assert success */
    private List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql, int paramCount,
                                                                   boolean planForSinglePartition,
                                                                   String joinOrder)
    {
        List<AbstractPlanNode> pn = m_aide.compile(sql, paramCount, planForSinglePartition, joinOrder);
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        if (planForSinglePartition) {
            assertTrue(pn.size() == 1);
        }
        return pn;
    }

    protected AbstractPlanNode compileWithJoinOrder(String sql, String joinOrder)
    {
        try {
            return compileWithCountedParamsAndJoinOrder(sql, joinOrder);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
            return null;
        }
    }

    protected AbstractPlanNode compileWithInvalidJoinOrder(String sql, String joinOrder) throws Exception
    {
        return compileWithCountedParamsAndJoinOrder(sql, joinOrder);
    }


    private AbstractPlanNode compileWithCountedParamsAndJoinOrder(String sql, String joinOrder) throws Exception
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
    private AbstractPlanNode compileWithJoinOrder(String sql, int paramCount, String joinOrder) throws Exception
    {
        List<AbstractPlanNode> pn = compileWithJoinOrderToFragments(sql, paramCount,
                                                                    m_byDefaultPlanForSinglePartition, joinOrder);
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        return pn.get(0);
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compile(String sql)
    {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
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

    Database getDatabase() {
        return m_aide.getDatabase();
    }

}
