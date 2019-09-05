/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.test.SqlTests;
import org.voltdb.compiler.PlannerTool.JoinCounter;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.planner.PlannerTestCase;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannerv2.rules.PlannerRules;
import org.voltdb.plannerv2.rules.PlannerRules.Phase;
import org.voltdb.plannerv2.utils.VoltRelUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;

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

    protected SchemaPlus getSchemaPlus() {
        return m_schemaPlus;
    }

    public abstract class Tester {
        SqlParserUtil.StringAndPos m_sap;
        String m_expectedException;
        String m_expectedPlan;
        String m_expectedTransform;
        String m_expectedVoltPlanJson;
        SqlNode m_parsedNode;
        SqlNode m_validatedNode;
        RelRoot m_root;
        RelNode m_transformedNode;
        int m_ruleSetIndex = -1;
        boolean m_canCommuteJoins = false;

        void reset() {
            m_sap = null;
            m_expectedException = m_expectedPlan = m_expectedTransform = m_expectedVoltPlanJson = null;
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

        public Tester json(String expectedVoltPlanJson) {
            m_expectedVoltPlanJson = expectedVoltPlanJson;
            return this;
        }

        public Tester phase(PlannerRules.Phase phase) {
            m_ruleSetIndex = phase.ordinal();
            return this;
        }

        public Tester transform(String expectedTransform) {
            m_expectedTransform = expectedTransform;
            return this;
        }

        public RelNode getTransformedNode() {
            return m_transformedNode;
        }

        public void pass() throws AssertionError {
            if (m_sap == null) {
                throw new AssertionError("Need to specify a SQL statement.");
            }
            if (m_planner == null) {
                init();
            } else {
                m_planner.reset();
            }
        }

        public void fail() {
            PlannerTestCase.fail("Not implemented.");
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
        @Override public void pass() throws AssertionError {
            super.pass();
            try {
                m_parsedNode = m_planner.parse(m_sap.sql);
                m_validatedNode = m_planner.validate(m_parsedNode);
            } catch (Exception ex) {
                checkEx(ex);
            }
        }
    }

    public class SqlParserTester extends Tester {
        @Override public void pass() throws AssertionError {
            super.pass();
            try {
                m_parsedNode = m_planner.parse(m_sap.sql);
            } catch (Exception ex) {
                checkEx(ex);
            }
        }
    }

    public class ConversionTester extends ValidationTester {
        @Override public void pass() throws AssertionError {
            super.pass();
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

    public class LogicalRulesTester extends ConversionTester {
        @Override public void pass() throws AssertionError {
            super.pass();
            if (m_ruleSetIndex < 0) {
                throw new AssertionError("Need to specify a planner phase.");
            }

            JoinCounter scanCounter = new JoinCounter();
            m_root.rel.accept(scanCounter);
            m_canCommuteJoins = scanCounter.canCommuteJoins();

            m_transformedNode = m_planner.transform(PlannerRules.Phase.LOGICAL.ordinal(),
                    getEmptyTraitSet().replace(VoltLogicalRel.CONVENTION), m_root.rel);
            if (m_ruleSetIndex == PlannerRules.Phase.LOGICAL.ordinal() && m_expectedTransform != null) {
                String actualTransform = RelOptUtil.toString(m_transformedNode);
                assertEquals(m_expectedTransform, actualTransform);
            }
        }
    }

    public class MPFallbackTester extends LogicalRulesTester {
        private RelDistribution transform() {
            m_transformedNode = VoltRelUtil.addTraitRecursively(m_transformedNode, RelDistributions.ANY);
            m_planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
            m_transformedNode = VoltPlanner.transformHep(PlannerRules.Phase.MP_FALLBACK, m_transformedNode);
            return m_transformedNode.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
        }

        @Override public void pass() throws AssertionError {
            super.pass();
            final RelDistribution distribution = transform();
            assertTrue("Got SINGLETON distribution without partition equal value",
                    distribution.getIsSP());
        }

        @Override public void fail() {
            super.pass();
            try {
                final RelDistribution distribution = transform();
                assertFalse("Expected fall back:\nGot distribution type " +
                                distribution.getType().name() +
                                " with partition equal value = " +
                                (distribution.getPartitionEqualValue() == null ? "null" :
                                        distribution.getPartitionEqualValue().toString()),
                        distribution.getIsSP());
            } catch (PlanningErrorException e) {    // transform stage is allowed to throw:
                assertEquals(                       // See RelDistributionUtils#isJoinSP()
                        "SQL error while compiling query: This query is not plannable.  "
                                + "The planner cannot guarantee that all rows would be in a single partition.",
                        e.getMessage());
            }
        }
    }

    public class OuterJoinRulesTester extends MPFallbackTester {
        @Override public void pass() throws AssertionError {
            super.pass();
            if (m_ruleSetIndex < 0) {
                throw new AssertionError("Need to specify a planner phase.");
            }

            m_transformedNode = VoltPlanner.transformHep(PlannerRules.Phase.OUTER_JOIN, m_transformedNode);
            if (m_ruleSetIndex == PlannerRules.Phase.OUTER_JOIN.ordinal() && m_expectedTransform != null) {
                String actualTransform = RelOptUtil.toString(m_transformedNode);
                assertEquals(m_expectedTransform, actualTransform);
            }
        }
    }

    public class PhysicalConversionRulesTester extends OuterJoinRulesTester {
        @Override public void pass() throws AssertionError {
            super.pass();
            // Prepare the set of RelTraits required of the root node at the termination of the physical conversion phase.
            RelTraitSet physicalTraits = m_transformedNode.getTraitSet().replace(VoltPhysicalRel.CONVENTION).
                    replace(RelDistributions.ANY);
            Phase physicalPhase = (m_canCommuteJoins) ?
                    Phase.PHYSICAL_CONVERSION_WITH_JOIN_COMMUTE : Phase.PHYSICAL_CONVERSION;
            m_transformedNode = m_planner.transform(physicalPhase.ordinal(),
                    physicalTraits, m_transformedNode);
            if (m_ruleSetIndex == Phase.PHYSICAL_CONVERSION.ordinal() && m_expectedTransform != null) {
                String actualTransform = RelOptUtil.toString(m_transformedNode);
                assertEquals(m_expectedTransform, actualTransform);
            }
        }
    }

    public class InlineRulesTester extends PhysicalConversionRulesTester {
        @Override public void reset() {
            super.reset();
            AbstractPlanNode.resetPlanNodeIds();
        }

        @Override public void pass() throws AssertionError {
            super.pass();
            m_transformedNode = VoltPlanner.transformHep(PlannerRules.Phase.INLINE,
                    HepMatchOrder.ARBITRARY, m_transformedNode, true);
            if (m_ruleSetIndex == PlannerRules.Phase.INLINE.ordinal()) {
                if (m_expectedTransform != null) {
                    String actualTransform = RelOptUtil.toString(m_transformedNode);
                    // eliminate the HepRelVertex number which can be arbitrary
                    m_expectedTransform = m_expectedTransform.replaceAll("HepRelVertex#\\d+", "HepRelVertex");
                    actualTransform = actualTransform.replaceAll("HepRelVertex#\\d+", "HepRelVertex");
                    assertEquals(m_expectedTransform, actualTransform);
                }
                if (m_expectedVoltPlanJson != null) {
                    assertTrue(m_transformedNode instanceof VoltPhysicalRel);
                    AbstractPlanNode voltPlan = ((VoltPhysicalRel)m_transformedNode).toPlanNode();
                    PlanNodeList planNodeList = new PlanNodeList(voltPlan, false);
                    String actualPlan = planNodeList.toJSONString();
                    assertEquals(m_expectedVoltPlanJson, actualPlan);
                }
            }
        }
    }
}
