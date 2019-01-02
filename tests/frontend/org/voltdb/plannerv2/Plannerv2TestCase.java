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

package org.voltdb.plannerv2;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.test.SqlTests;
import org.voltdb.planner.PlannerTestCase;

/**
 * Base class for planner v2 test cases.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class Plannerv2TestCase extends PlannerTestCase {

    private VoltPlanner m_planner;
    private SchemaPlus m_schemaPlus;

    /**
     * Set things up.
     */
    protected void init() {
        m_schemaPlus = VoltSchemaPlus.from(getDatabase());
        m_planner = new VoltPlanner(m_schemaPlus);
    }

    protected RelTraitSet getEmptyTraitSet() {
        return m_planner.getEmptyTraitSet();
    }

    public abstract class Tester {
        SqlParserUtil.StringAndPos m_sap;
        String m_expectedException;
        String m_expectedPlan;
        String m_expectedTransform;
        SqlNode m_parsedNode;
        SqlNode m_validatedNode;
        RelRoot m_root;
        RelNode m_transformedNode;
        int m_ruleSetIndex;
        RelTraitSet m_requiredOutputTraits;

        void reset() {
            m_sap = null;
            m_expectedException = m_expectedPlan = m_expectedTransform = null;
            m_parsedNode = m_validatedNode = null;
            m_root = null;
            m_transformedNode = null;
        }

        public Tester sql(String sql) {
            reset();
            m_sap = SqlParserUtil.findPos(sql);
            return this;
        }

        public Tester exception(String expectedMsgPattern) {
            m_expectedException = expectedMsgPattern;
            return this;
        }

        public Tester plan(String expectedPlan) {
            m_expectedPlan = expectedPlan;
            return this;
        }

        public Tester phase(PlannerRules.Phase phase) {
            m_ruleSetIndex = phase.ordinal();
            return this;
        }

        public Tester traitSet(RelTraitSet traitSet) {
            m_requiredOutputTraits = traitSet;
            return this;
        }

        public Tester transform(String expectedTransform) {
            m_expectedTransform = expectedTransform;
            return this;
        }

        public RelNode getTransformedNode() {
            return m_transformedNode;
        }

        public void test() throws AssertionError {
            if (m_sap == null) {
                throw new AssertionError("Need to specify a SQL statement.");
            }
            if (m_planner == null) {
                init();
            } else {
                m_planner.reset();
            }
        }

        void checkEx(Exception ex) throws AssertionError {
            if (m_expectedException == null) {
                throw new AssertionError("Unexpected exception thrown: " + m_sap.sql, ex);
            } else if (ex instanceof SqlParseException){
                String errMessage = ex.getMessage();
                if (errMessage == null
                        || ! ex.getMessage().matches(m_expectedException)) {
                    throw new AssertionError("Exception thrown not matching the pattern: "
                            + m_expectedException, ex);
                }
            } else {
                SqlTests.checkEx(ex, m_expectedException, m_sap, SqlTests.Stage.VALIDATE);
            }
        }
    }

    public class ValidationTester extends Tester {
        @Override public void test() throws AssertionError {
            super.test();
            try {
                m_parsedNode = m_planner.parse(m_sap.sql);
                m_validatedNode = m_planner.validate(m_parsedNode);
            } catch (Exception ex) {
                checkEx(ex);
            }
        }
    }

    public class ConversionTester extends ValidationTester {
        @Override public void test() throws AssertionError {
            super.test();
            try {
                m_root = m_planner.rel(m_validatedNode);
                if (m_expectedPlan != null) {
                    assertEquals(m_expectedPlan, m_root.toString());
                }
            } catch (Exception ex) {
                checkEx(ex);
            }
        }
    }

    public class TransformationTester extends ConversionTester {
        @Override public void test() throws AssertionError {
            super.test();
            if (m_ruleSetIndex < 0) {
                throw new AssertionError("Need to specify a planner phase.");
            }
            if (m_requiredOutputTraits == null) {
                throw new AssertionError("Need to specify the output trait set.");
            }
            try {
                m_transformedNode = m_planner.transform(m_ruleSetIndex, m_requiredOutputTraits, m_root.rel);
                if (m_expectedTransform != null) {
                    String actualTransform = RelOptUtil.toString(m_transformedNode);
                    assertEquals(m_expectedTransform, actualTransform);
                }
            } catch (Exception ex) {
                checkEx(ex);
            }
        }
    }
}
