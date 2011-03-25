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
package org.voltdb.expressions;

import java.util.*;

import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.*;

import junit.framework.*;

public class TestExpressionUtil extends TestCase {
    /**
     * Used for traversing trees.
     *
     */
    static abstract class TestExpressionTreeWalker {

        /**
         * Expression node stack
         */
        private final Stack<AbstractExpression> m_stack = new Stack<AbstractExpression>();

        /**
         * How deep we are in the tree
         */
        private int m_depth = -1;

        /**
         * Depth first traveral
         * @param exp
         */
        public final void traverse(AbstractExpression exp) {
            m_stack.push(exp);
            m_depth++;
            if (exp.getLeft() != null) {
                traverse(exp.getLeft());
            }
            if (exp.getRight() != null) {
                traverse(exp.getRight());
            }
            AbstractExpression check_exp = m_stack.pop();
            assert(exp.equals(check_exp));
            callback(exp);
            m_depth--;
        }

        /**
         * Returns the parent of the current node in callback()
         * @return
         */
        protected final AbstractExpression getParent() {
            AbstractExpression ret = null;
            if (!m_stack.isEmpty()) ret = m_stack.peek();
            return ret;
        }

        /**
         * Returns the depth of the current callback() invocation
         * @return
         */
        protected final int getDepth() {
            return m_depth;
        }

        /**
         * This method will be called after the walker has explored a node's children
         * @param exp the Expression object to perform an operation on
         */
        public abstract void callback(AbstractExpression exp);
    }

    //
    //        AND
    //       /   \
    //    EQUAL  NOT
    //    /   \   |
    //   P   T  C
    //
    protected static final AbstractExpression ROOT_EXP = new ConjunctionExpression(ExpressionType.CONJUNCTION_AND);
    protected static final AbstractExpression CHILD_EXPS[] = { new ComparisonExpression(ExpressionType.COMPARE_EQUAL),
                                                               new ComparisonExpression(ExpressionType.OPERATOR_NOT),
                                                               new ParameterValueExpression(),
                                                               new TupleValueExpression(),
                                                               new ConstantValueExpression()
    };
    static {
        ROOT_EXP.setLeft(CHILD_EXPS[0]);
        ROOT_EXP.setRight(CHILD_EXPS[1]);
        CHILD_EXPS[0].setLeft(CHILD_EXPS[2]);
        CHILD_EXPS[0].setRight(CHILD_EXPS[3]);
        CHILD_EXPS[1].setLeft(CHILD_EXPS[4]);
        //ExpressionUtil.generateIds(ROOT_EXP);
    } // STATIC

    // ------------------------------------------------------------------
    // COMPARISON METHODS
    // ------------------------------------------------------------------

    protected static void compareExpressionTrees(AbstractExpression out_exp, AbstractExpression in_exp) {
        //
        // Make sure that compacted tree is exactly the same as the original
        // sub-tree that should have been preserved
        //
        final Vector<AbstractExpression> orig_list = new Vector<AbstractExpression>();
        new TestExpressionTreeWalker() {
            @Override
            public void callback(AbstractExpression exp) {
                orig_list.add(exp);
            }
        }.traverse(out_exp);
        assertFalse(orig_list.isEmpty());

        new TestExpressionTreeWalker() {
            @Override
            public void callback(AbstractExpression exp) {
                assertFalse(orig_list.isEmpty());
                AbstractExpression pop_exp = orig_list.remove(0);
                TestExpressionUtil.compareExpressions(pop_exp, exp);
            }
        }.traverse(in_exp);
    }

    protected static void compareExpressions(AbstractExpression out_exp, AbstractExpression in_exp) {
        //
        // ID
        //
        /*if (out_exp.getId() != null) {
            assertNotNull(in_exp.getId());
            if (!out_exp.getId().equals(in_exp.getId())) {
                System.err.println("OUT: " + out_exp);
                System.err.println("IN:  " + in_exp);
            }
            assertEquals(out_exp.getId(), in_exp.getId());
        } else {
            assertNull(in_exp.getId());
        }*/
        //
        // LEFT & RIGHT
        //
        assertEquals((out_exp.getLeft() == null), (in_exp.getLeft() == null));
        assertEquals((out_exp.getRight() == null), (in_exp.getRight() == null));
        //
        // VALUE TYPE
        //
        if (out_exp.getValueType() != in_exp.getValueType()) {
            System.err.println("OUT: " + out_exp.getValueType());
            System.err.println("IN:  " + in_exp.getValueType());
        }
        assertEquals(out_exp.getValueType(), in_exp.getValueType());
        //
        // Specialized Checks
        //
        switch (out_exp.getExpressionType()) {
            case VALUE_CONSTANT: {
                ConstantValueExpression out_const_exp = (ConstantValueExpression)out_exp;
                ConstantValueExpression in_const_exp = (ConstantValueExpression)in_exp;
                assertEquals(out_const_exp.getValue(), in_const_exp.getValue());
                break;
            }
            case VALUE_PARAMETER:
                ParameterValueExpression out_param_exp = (ParameterValueExpression)out_exp;
                ParameterValueExpression in_param_exp = (ParameterValueExpression)in_exp;
                assertEquals(out_param_exp.getParameterId(), in_param_exp.getParameterId());
                break;
            case VALUE_TUPLE:
                TupleValueExpression out_tuple_exp = (TupleValueExpression)out_exp;
                TupleValueExpression in_tuple_exp = (TupleValueExpression)in_exp;
                assertEquals(out_tuple_exp.getColumnIndex(), in_tuple_exp.getColumnIndex());
                break;
        } // SWITCH
        return;
    }

    // ------------------------------------------------------------------
    // TEST CASES
    // ------------------------------------------------------------------

    /**
     *  Clone Sub-Tree
     */
    public void testClone() {
        AbstractExpression cloned_exp = null;
        try {
            cloned_exp = ExpressionUtil.clone(ROOT_EXP);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        assertNotNull(cloned_exp);

        //
        // First build our lists of information about each node in the orignal tree
        // It is assumed that the tree will be traversed in the same order
        //
        final ArrayList<AbstractExpression> orig_exps = new ArrayList<AbstractExpression>();
        new TestExpressionTreeWalker() {
            @Override
            public void callback(AbstractExpression exp) {
                orig_exps.add(exp);
            }
        }.traverse(ROOT_EXP);
        //
        // Then walk through the cloned tree and make sure the objects are different
        // but the information is the same
        //
        new TestExpressionTreeWalker() {
            @Override
            public void callback(AbstractExpression exp) {
                assertFalse(orig_exps.isEmpty());
                AbstractExpression orig_exp = orig_exps.remove(0);
                //
                // We want to make sure that the cloned Expression doesn't
                // have the its ID set to its hashCode
                //
                assertTrue(orig_exp.hashCode() != exp.hashCode());
                //assertFalse(orig_exp.getId().equals(Integer.toString(exp.hashCode())));
                //
                // Use our general comparison method to check other things
                //
                TestExpressionUtil.compareExpressions(orig_exp, exp);
            }
        }.traverse(cloned_exp);
    }

    /**
     *
     *
     */
    public void testCombine() {
        //
        // We create a bunch of individual ComparisonExpression trees and we then
        // combine them into a single tree created with AND conjunctions
        //
        int num_of_subtrees = 5;
        final List<AbstractExpression> combine_exps = new ArrayList<AbstractExpression>();
        final Map<AbstractExpression, AbstractExpression> combine_exps_left = new HashMap<AbstractExpression, AbstractExpression>();
        final Map<AbstractExpression, AbstractExpression> combine_exps_right = new HashMap<AbstractExpression, AbstractExpression>();
        for (int ctr = 0; ctr < num_of_subtrees; ctr++) {
            AbstractExpression exps[] = { new ComparisonExpression(ExpressionType.COMPARE_EQUAL),
                                          new ParameterValueExpression(),
                                          new TupleValueExpression()
            };
            exps[0].setLeft(exps[1]);
            exps[0].setRight(exps[2]);
            //ExpressionUtil.generateIds(exps[0]);
            combine_exps.add(exps[0]);
            combine_exps_left.put(exps[0], exps[1]);
            combine_exps_right.put(exps[0], exps[2]);
        } // FOR

        AbstractExpression combined_exp = null;
        try {
            combined_exp = ExpressionUtil.combine(combine_exps);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        assertNotNull(combined_exp);
        assertEquals(combined_exp.getExpressionType(), ExpressionType.CONJUNCTION_AND);
        //System.err.println(combined_exp.toString(true));

        //
        // Checking whether this worked is a bit tricky because the ordering of the may
        // be different if the implementation changges. So we just need to check to make
        // sure that all of our sub-trees are contained within the new tree and that their
        // structure has not changed
        //
        new TestExpressionTreeWalker() {
            @Override
            public void callback(AbstractExpression exp) {
                //
                // This node was in our original tree
                //
                if (combine_exps.contains(exp)) {
                    assertTrue(combine_exps_left.containsKey(exp));
                    TestExpressionUtil.compareExpressions(exp.getLeft(), combine_exps_left.get(exp));
                    assertTrue(combine_exps_right.containsKey(exp));
                    TestExpressionUtil.compareExpressions(exp.getRight(), combine_exps_right.get(exp));
                    //
                    // Make sure our parent is a CONJUNCTION_AND expression node
                    //
                    assertNotNull(this.getParent());
                    assertEquals(this.getParent().getExpressionType(), ExpressionType.CONJUNCTION_AND);
                //
                // If this is a CONJUNCTION_AND that we added, make sure that both of its
                // children are not null
                //
                } else if (exp.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                    assertNotNull(exp.getLeft());
                    assertNotNull(exp.getRight());
                }
            }
        }.traverse(combined_exp);
    }

    // This is basically just a check that the rules in
    // VoltTypeUtil.determineImplicitCasting() push up through the aggregate
    // expression properly.  We'll just do a few of the corner cases and
    // call it good.
    public void testAssignOutputValueTypesRecursivelyForAggregateAvg()
    {
        AbstractExpression root =
            new AggregateExpression(ExpressionType.AGGREGATE_AVG);

        AbstractExpression op = new TupleValueExpression();
        root.setLeft(op);

        // Simple tuple value type gets pushed through
        op.setValueType(VoltType.FLOAT);
        ExpressionUtil.assignOutputValueTypesRecursively(root);
        assertEquals(VoltType.FLOAT, root.getValueType());

        op.setValueType(VoltType.INTEGER);
        ExpressionUtil.assignOutputValueTypesRecursively(root);
        assertEquals(VoltType.INTEGER, root.getValueType());

        op.setValueType(VoltType.DECIMAL);
        ExpressionUtil.assignOutputValueTypesRecursively(root);
        assertEquals(VoltType.DECIMAL, root.getValueType());

        op = new OperatorExpression();
        root.setLeft(op);

        AbstractExpression left = new TupleValueExpression();
        AbstractExpression right = new TupleValueExpression();

        op.setLeft(left);
        op.setRight(right);

        // FLOAT + int type gets promoted to FLOAT
        left.setValueType(VoltType.FLOAT);
        right.setValueType(VoltType.INTEGER);
        ExpressionUtil.assignOutputValueTypesRecursively(root);
        assertEquals(VoltType.FLOAT, root.getValueType());

        // random INT types get promoted to BIGINT
        left.setValueType(VoltType.TINYINT);
        right.setValueType(VoltType.INTEGER);
        ExpressionUtil.assignOutputValueTypesRecursively(root);
        assertEquals(VoltType.BIGINT, root.getValueType());

        // DECIMAL works, at least
        left.setValueType(VoltType.DECIMAL);
        right.setValueType(VoltType.DECIMAL);
        ExpressionUtil.assignOutputValueTypesRecursively(root);
        assertEquals(VoltType.DECIMAL, root.getValueType());
    }

    /** Base case test of NUMERIC literal processing */
    public void testAssignLiteralConstantTypes()
    {
        AbstractExpression lit_dec;
        AbstractExpression dec_lit;
        AbstractExpression lit;
        AbstractExpression dec;
        AbstractExpression bint;

        // convert NUMERIC to DECIMAL right/left
        lit = new ConstantValueExpression();
        lit.m_valueType = VoltType.NUMERIC;
        lit.m_valueSize = VoltType.NUMERIC.getLengthInBytesForFixedTypes();
        dec = new ConstantValueExpression();
        dec.m_valueType = VoltType.DECIMAL;
        dec.m_valueSize = VoltType.DECIMAL.getLengthInBytesForFixedTypes();
        lit_dec = new OperatorExpression(ExpressionType.OPERATOR_PLUS, lit, dec);

        ExpressionUtil.assignLiteralConstantTypesRecursively(lit_dec, VoltType.STRING);
        assertEquals(lit.m_valueType, VoltType.DECIMAL);
        assertEquals(lit.m_valueSize, VoltType.DECIMAL.getLengthInBytesForFixedTypes());
        assertEquals(dec.m_valueType, VoltType.DECIMAL);
        assertEquals(dec.m_valueSize, VoltType.DECIMAL.getLengthInBytesForFixedTypes());

        // convert NUMERIC to DECIMAL (left/right)
        lit = new ConstantValueExpression();
        lit.m_valueType = VoltType.NUMERIC;
        lit.m_valueSize = VoltType.NUMERIC.getLengthInBytesForFixedTypes();
        dec = new ConstantValueExpression();
        dec.m_valueType = VoltType.DECIMAL;
        dec.m_valueSize = VoltType.DECIMAL.getLengthInBytesForFixedTypes();
        dec_lit = new OperatorExpression(ExpressionType.OPERATOR_DIVIDE, dec, lit);

        ExpressionUtil.assignLiteralConstantTypesRecursively(dec_lit, VoltType.STRING);
        assertEquals(lit.m_valueType, VoltType.DECIMAL);
        assertEquals(lit.m_valueSize, VoltType.DECIMAL.getLengthInBytesForFixedTypes());
        assertEquals(dec.m_valueType, VoltType.DECIMAL);
        assertEquals(dec.m_valueSize, VoltType.DECIMAL.getLengthInBytesForFixedTypes());


        // convert numeric to float
        lit = new ConstantValueExpression();
        lit.m_valueType = VoltType.NUMERIC;
        lit.m_valueSize = VoltType.NUMERIC.getLengthInBytesForFixedTypes();
        bint = new ConstantValueExpression();
        bint.m_valueType = VoltType.BIGINT;
        bint.m_valueSize = VoltType.BIGINT.getLengthInBytesForFixedTypes();
        AbstractExpression lit_bint =
            new OperatorExpression(ExpressionType.OPERATOR_MINUS, lit, bint);

        ExpressionUtil.assignLiteralConstantTypesRecursively(lit_bint, VoltType.STRING);
        assertEquals(lit.m_valueType, VoltType.FLOAT);
        assertEquals(lit.m_valueSize, VoltType.FLOAT.getLengthInBytesForFixedTypes());
        assertEquals(bint.m_valueType, VoltType.BIGINT);
        assertEquals(bint.m_valueSize, VoltType.BIGINT.getLengthInBytesForFixedTypes());

        // test a larger tree
        lit = new ConstantValueExpression();
        lit.m_valueType = VoltType.NUMERIC;
        lit.m_valueSize = VoltType.NUMERIC.getLengthInBytesForFixedTypes();
        bint = new ConstantValueExpression();
        bint.m_valueType = VoltType.DECIMAL;
        bint.m_valueSize = VoltType.DECIMAL.getLengthInBytesForFixedTypes();
        lit_bint = new OperatorExpression(ExpressionType.OPERATOR_MINUS, lit, bint);

        AbstractExpression root = new OperatorExpression(ExpressionType.OPERATOR_MULTIPLY,
                lit_bint, new TupleValueExpression());
        ExpressionUtil.assignLiteralConstantTypesRecursively(root);
        assertEquals(lit.m_valueType, VoltType.DECIMAL);
        assertEquals(lit.m_valueSize, VoltType.DECIMAL.getLengthInBytesForFixedTypes());
        assertEquals(bint.m_valueType, VoltType.DECIMAL);
        assertEquals(bint.m_valueSize, VoltType.DECIMAL.getLengthInBytesForFixedTypes());
    }

    public void testSetOutputTypeForInsertExpressionWithCveAndTimestamp() throws Exception
    {
        ConstantValueExpression cve = new ConstantValueExpression();
        cve.setValue("4000000000");
        cve.setValueSize(8);
        cve.setValueType(VoltType.BIGINT);
        HashMap<Integer, VoltType> override_map = new HashMap<Integer, VoltType>();
        ExpressionUtil.setOutputTypeForInsertExpression(cve, VoltType.TIMESTAMP, 8, override_map);
        assertEquals(VoltType.TIMESTAMP, cve.getValueType());
        assertEquals(8, cve.getValueSize());
        System.out.println(override_map);

        cve = new ConstantValueExpression();
        cve.setValue("400000000");
        cve.setValueSize(4);
        cve.setValueType(VoltType.INTEGER);
        override_map = new HashMap<Integer, VoltType>();
        ExpressionUtil.setOutputTypeForInsertExpression(cve, VoltType.TIMESTAMP, 8, override_map);
        assertEquals(VoltType.TIMESTAMP, cve.getValueType());
        assertEquals(8, cve.getValueSize());

        cve = new ConstantValueExpression();
        cve.setValue("4000");
        cve.setValueSize(2);
        cve.setValueType(VoltType.SMALLINT);
        override_map = new HashMap<Integer, VoltType>();
        ExpressionUtil.setOutputTypeForInsertExpression(cve, VoltType.TIMESTAMP, 8, override_map);
        assertEquals(VoltType.TIMESTAMP, cve.getValueType());
        assertEquals(8, cve.getValueSize());

        cve = new ConstantValueExpression();
        cve.setValue("40");
        cve.setValueSize(1);
        cve.setValueType(VoltType.TINYINT);
        override_map = new HashMap<Integer, VoltType>();
        ExpressionUtil.setOutputTypeForInsertExpression(cve, VoltType.TIMESTAMP, 8, override_map);
        assertEquals(VoltType.TIMESTAMP, cve.getValueType());
        assertEquals(8, cve.getValueSize());
    }
}