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

import java.net.URL;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DeterminismMode;
import org.voltdb.plannodes.AbstractPlanNode;

public class PlannerTestCase extends TestCase {

    private PlannerTestAideDeCamp m_aide;
    private boolean m_byDefaultInferPartitioning = true;
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
            m_aide.compile(sql, paramCount, m_byDefaultInferPartitioning, m_byDefaultPlanForSinglePartition, null);
            fail();
        }
        catch (PlanningErrorException ex) {
            String result = ex.toString();
            for (String pattern : patterns) {
                if ( ! result.contains(pattern)) {
                    System.err.println("Did not find pattern '" + pattern + "' in error string '" + result + "'");
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
    protected List<AbstractPlanNode> compileToFragments(String sql)
    {
        boolean planForSinglePartitionFalse = false;
        return compileWithJoinOrderToFragments(sql, planForSinglePartitionFalse, noJoinOrder);
    }

    protected List<AbstractPlanNode> compileToFragmentsForSinglePartition(String sql)
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
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        return compileWithJoinOrderToFragments(sql, paramCount, planForSinglePartition, joinOrder);
    }

    /** A helper here where the junit test can assert success */
    private List<AbstractPlanNode> compileWithJoinOrderToFragments(String sql, int paramCount,
                                                                   boolean planForSinglePartition,
                                                                   String joinOrder)
    {
        List<AbstractPlanNode> pn = m_aide.compile(sql, paramCount, m_byDefaultInferPartitioning, m_byDefaultPlanForSinglePartition, joinOrder);
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        if (planForSinglePartition) {
            assertTrue(pn.size() == 1);
        }
        return pn;
    }

    protected AbstractPlanNode compileSPWithJoinOrder(String sql, String joinOrder)
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

    protected void compileWithInvalidJoinOrder(String sql, String joinOrder) throws Exception
    {
        compileWithJoinOrderToFragments(sql, paramCount, m_byDefaultPlanForSinglePartition, joinOrder);
    }


    private AbstractPlanNode compileWithCountedParamsAndJoinOrder(String sql, String joinOrder) throws Exception
    {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        return compileSPWithJoinOrder(sql, paramCount, joinOrder);
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compile(String sql)
    {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        return compileSPWithJoinOrder(sql, paramCount, null);
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compileForSinglePartition(String sql)
    {
        // Yes, we ARE assuming that test queries don't contain quoted question marks.
        int paramCount = StringUtils.countMatches(sql, "?");
        boolean m_infer = m_byDefaultInferPartitioning;
        boolean m_forceSP = m_byDefaultInferPartitioning;
        m_byDefaultInferPartitioning = false;
        m_byDefaultPlanForSinglePartition = true;

        AbstractPlanNode pn = compileSPWithJoinOrder(sql, paramCount, null);
        m_byDefaultInferPartitioning = m_infer;
        m_byDefaultPlanForSinglePartition = m_forceSP;
        return pn;
    }

    /** A helper here where the junit test can assert success */
    protected AbstractPlanNode compileSPWithJoinOrder(String sql, int paramCount, String joinOrder)
    {
        List<AbstractPlanNode> pns = null;
        try {
            pns = compileWithJoinOrderToFragments(sql, paramCount, m_byDefaultPlanForSinglePartition, joinOrder);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pns.get(0) != null);
        return pns.get(0);
    }


    protected void setupSchema(URL ddlURL, String basename,
                               boolean planForSinglePartition) throws Exception
    {
        m_aide = new PlannerTestAideDeCamp(ddlURL, basename);
        m_byDefaultPlanForSinglePartition = planForSinglePartition;
    }

    protected void setupSchema(boolean inferPartitioning, URL ddlURL, String basename) throws Exception
    {
        m_byDefaultInferPartitioning = inferPartitioning;
        m_aide = new PlannerTestAideDeCamp(ddlURL, basename);
    }


    Database getDatabase() {
        return m_aide.getDatabase();
    }

    protected void printExplainPlan(List<AbstractPlanNode> planNodes) {
        for (AbstractPlanNode apn: planNodes) {
            System.out.println(apn.toExplainPlanString());
        }
    }

    protected String buildExplainPlan(List<AbstractPlanNode> planNodes) {
        String explain = "";
        for (AbstractPlanNode apn: planNodes) {
            explain += apn.toExplainPlanString() + '\n';
        }
        return explain;
    }

    protected void checkQueriesPlansAreTheSame(String sql1, String sql2) {
        String explainStr1, explainStr2;
        List<AbstractPlanNode> pns = compileToFragments(sql1);
        explainStr1 = buildExplainPlan(pns);
        pns = compileToFragments(sql2);
        explainStr2 = buildExplainPlan(pns);

        assertEquals(explainStr1, explainStr2);
    }

    /** Given a list of Class objects for plan node subclasses, asserts
     * if the given plan doesn't contain instances of those classes.
     */
    static protected void assertClassesMatchNodeChain(
            List<Class<? extends AbstractPlanNode>> expectedClasses,
            AbstractPlanNode actualPlan) {
        AbstractPlanNode pn = actualPlan;
        for (Class<? extends AbstractPlanNode> c : expectedClasses) {
            assertFalse("Actual plan shorter than expected",
                    pn == null);
            assertTrue("Expected plan to contain an instance of " + c.getSimpleName() +", "
                    + "instead found " + pn.getClass().getSimpleName(),
                    c.isInstance(pn));
            if (pn.getChildCount() > 0)
                pn = pn.getChild(0);
            else
                pn = null;
        }

        assertTrue("Actual plan longer than expected", pn == null);
    }
}
