/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Vector;

import junit.framework.TestCase;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.TimestampType;

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
    //   P     T  C
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
                assertEquals(out_param_exp.getParameterIndex(), in_param_exp.getParameterIndex());
                break;
            case VALUE_TUPLE:
                TupleValueExpression out_tuple_exp = (TupleValueExpression)out_exp;
                TupleValueExpression in_tuple_exp = (TupleValueExpression)in_exp;
                assertEquals(out_tuple_exp.getColumnIndex(), in_tuple_exp.getColumnIndex());
                break;
            default:
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
            if (ROOT_EXP != null) {
                cloned_exp = ROOT_EXP.clone();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        assertNotNull(cloned_exp);

        //
        // First build our lists of information about each node in the original tree
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
                // share components (having the same object identity).
                // This COULD still fail for wrapped primitive types
                // IF Expressions ever reference any and
                // IF the cloning method is not (somehow) working to avoid
                // pointers into a system-provided wrapped primitive cache.
                assertFalse(orig_exp == exp);
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
            combined_exp = ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, combine_exps);
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
        TestExpressionTreeWalker treeWalker = new TestExpressionTreeWalker() {
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
        };
        treeWalker.traverse(combined_exp);

        //
        // Test  variadic combine
        //
        final List<AbstractExpression> combine_exps1 = new ArrayList<AbstractExpression>();
        final List<AbstractExpression> combine_exps2 = new ArrayList<AbstractExpression>();
        final List<AbstractExpression> combine_exps3 = new ArrayList<AbstractExpression>();
        final List<AbstractExpression> combine_exps4 = null;
        for (int ctr = 0; ctr < num_of_subtrees/2; ctr++) {
            combine_exps1.add(combine_exps.get(ctr));
        }
        for (int ctr = num_of_subtrees/2; ctr < num_of_subtrees; ctr++) {
            combine_exps2.add(combine_exps.get(ctr));
        }
        AbstractExpression var_combined_exp = null;
        try {
            var_combined_exp = ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND,
                    combine_exps1, combine_exps2, combine_exps3, combine_exps4);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        assertNotNull(var_combined_exp);
        assertEquals(var_combined_exp.getExpressionType(), ExpressionType.CONJUNCTION_AND);
        treeWalker.traverse(var_combined_exp);
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
        ExpressionUtil.finalizeValueTypes(root);
        assertEquals(VoltType.FLOAT, root.getValueType());

        op.setValueType(VoltType.INTEGER);
        ExpressionUtil.finalizeValueTypes(root);
        assertEquals(VoltType.INTEGER, root.getValueType());

        op.setValueType(VoltType.DECIMAL);
        ExpressionUtil.finalizeValueTypes(root);
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
        ExpressionUtil.finalizeValueTypes(root);
        assertEquals(VoltType.FLOAT, root.getValueType());

        // random INT types get promoted to BIGINT
        left.setValueType(VoltType.TINYINT);
        right.setValueType(VoltType.INTEGER);
        ExpressionUtil.finalizeValueTypes(root);
        assertEquals(VoltType.BIGINT, root.getValueType());

        // DECIMAL works, at least
        left.setValueType(VoltType.DECIMAL);
        right.setValueType(VoltType.DECIMAL);
        ExpressionUtil.finalizeValueTypes(root);
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

        lit_dec.normalizeOperandTypes_recurse();
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

        dec_lit.normalizeOperandTypes_recurse();
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

        lit_bint.normalizeOperandTypes_recurse();
        assertEquals(lit.m_valueType, VoltType.DECIMAL);
        assertEquals(lit.m_valueSize, VoltType.DECIMAL.getLengthInBytesForFixedTypes());
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
        root.normalizeOperandTypes_recurse();
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
        cve.refineValueType(VoltType.TIMESTAMP, 8);
        assertEquals(VoltType.TIMESTAMP, cve.getValueType());
        assertEquals(8, cve.getValueSize());

        cve = new ConstantValueExpression();
        cve.setValue("400000000");
        cve.setValueSize(4);
        cve.setValueType(VoltType.INTEGER);
        cve.refineValueType(VoltType.TIMESTAMP, 8);
        assertEquals(VoltType.TIMESTAMP, cve.getValueType());
        assertEquals(8, cve.getValueSize());

        cve = new ConstantValueExpression();
        cve.setValue("4000");
        cve.setValueSize(2);
        cve.setValueType(VoltType.SMALLINT);
        cve.refineValueType(VoltType.TIMESTAMP, 8);
        assertEquals(VoltType.TIMESTAMP, cve.getValueType());
        assertEquals(8, cve.getValueSize());

        cve = new ConstantValueExpression();
        cve.setValue("40");
        cve.setValueSize(1);
        cve.setValueType(VoltType.TINYINT);
        cve.refineValueType(VoltType.TIMESTAMP, 8);
        assertEquals(VoltType.TIMESTAMP, cve.getValueType());
        assertEquals(8, cve.getValueSize());
    }

    public void testSetOutputTypeForInsertExpressionWithLiteralStringDates() throws Exception
    {
        ConstantValueExpression cve = new ConstantValueExpression();
        TimestampType ts = new TimestampType(999999999);
        cve.setValue(ts.toString());
        cve.setValueType(VoltType.STRING);
        cve.setValueSize(ts.toString().length());
        cve.refineValueType(VoltType.TIMESTAMP, 8);
        assertEquals(VoltType.TIMESTAMP, cve.getValueType());
        assertEquals("999999999", cve.m_value);
    }

    // Test interesting cases of indexable expressions and the query expressions they might match.
    // Technically, this tests AbstractExpression methods, not ExpressionUtil methods,
    // but do we really need to launch yet another JUnit suite? I think not.
    public void testIndexedExpressionBindings() throws Exception
    {
        List<AbstractExpression> arguments;

        TupleValueExpression extraColumn = new TupleValueExpression();
        extraColumn.setTableName("T1");
        extraColumn.setColumnName("extra");

        ConstantValueExpression constant = new ConstantValueExpression();
        constant.setValue("42");

        ConstantValueExpression otherConstant = new ConstantValueExpression();
        otherConstant.setValue("44");

        // Interesting indexable expressions include:
        // A) simple column, T1.A
        TupleValueExpression exprA = new TupleValueExpression();
        exprA.setTableName("T1");
        exprA.setColumnName("A");

        // B) math with columns, (T1.extra * T1.A)
        OperatorExpression exprB = new OperatorExpression(ExpressionType.OPERATOR_MULTIPLY, extraColumn, exprA);

       // C) a function of a column, ( functionName(T1.A) )
        FunctionExpression exprC = new FunctionExpression();
        exprC.setAttributes("functionName", "yesFunctionName", "optionalArg", 42);
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        exprC.setArgs(arguments);

        // D) math with a column and a constant ( 42 * T1.A )
        OperatorExpression exprD = new OperatorExpression(ExpressionType.OPERATOR_MULTIPLY, constant, exprA);

        // E) a function of a column and constants, ( anotherFunctionName( T1.A, 42, 44 ) )
        FunctionExpression exprE = new FunctionExpression();
        exprE.setAttributes("anotherFunctionName", "yesAnotherWhyNot", "optionalArgs", 44); // not 42, not that it much matters
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        arguments.add(constant);
        arguments.add(otherConstant);
        exprE.setArgs(arguments);

        List<AbstractExpression> result;
        //
        // Interesting matches for these include:
        //
        // Each of A through E should match an identical "cloned" expression,
        // with no "parameter binding" caveat.
        AbstractExpression likeA = exprA.clone();
        result = likeA.bindingToIndexedExpression(exprA);
        assertNotNull(result);
        assertTrue(result.isEmpty());

        AbstractExpression likeB = exprB.clone();
        result = likeB.bindingToIndexedExpression(exprB);
        assertNotNull(result);
        assertTrue(result.isEmpty());

        AbstractExpression likeC = exprC.clone();
        result = likeC.bindingToIndexedExpression(exprC);
        assertNotNull(result);
        assertTrue(result.isEmpty());

        AbstractExpression likeD = exprD.clone();
        result = likeD.bindingToIndexedExpression(exprD);
        assertNotNull(result);
        assertTrue(result.isEmpty());

        AbstractExpression likeE = exprE.clone();
        result = likeE.bindingToIndexedExpression(exprE);
        assertNotNull(result);
        assertTrue(result.isEmpty());

        // Each of D and E should match one-off expressions, differing only in that one or more
        // of their constants are replaced with Parameters having those identical constants as
        // their "original values".
        // Said parameters should (all) be listed in the resulting "parameter binding" caveats.

        ParameterValueExpression paramifiedConstant = new ParameterValueExpression();
        paramifiedConstant.setOriginalValue(constant);

        ParameterValueExpression otherParamifiedConstant = new ParameterValueExpression();
        otherParamifiedConstant.setOriginalValue(otherConstant);

        // D) math with a column and a constant ( 42 * T1.A ) works for ( ? * T1.A ) w/ ? == 42
        AbstractExpression paramifiedD = exprD.clone();
        paramifiedD.setLeft(paramifiedConstant);
        result = paramifiedD.bindingToIndexedExpression(exprD);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals(result.get(0), paramifiedConstant);

        // E) a function of a column and constants, ( anotherFunctionName( T1.A, 42, 44 ) )
        // works for ( anotherFunctionName( T1.A, ?, 44 ) ) where ? = 42 ...
        FunctionExpression paramifiedE = (FunctionExpression) exprE.clone();
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        arguments.add(paramifiedConstant);
        arguments.add(otherConstant);
        paramifiedE.setArgs(arguments);
        result = paramifiedE.bindingToIndexedExpression(exprE);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals(result.get(0), paramifiedConstant);

        // works for ( anotherFunctionName( T1.A, 42, ? ) ) where ? = 44
        FunctionExpression reparamifiedE = (FunctionExpression) exprE.clone();
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        arguments.add(constant);
        arguments.add(otherParamifiedConstant);
        reparamifiedE.setArgs(arguments);
        result = reparamifiedE.bindingToIndexedExpression(exprE);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals(result.get(0), otherParamifiedConstant);

        // works for ( anotherFunctionName( T1.A, ?, ? ) ) where ?, ? = 42, 44
        FunctionExpression everSoParamifiedE = (FunctionExpression) exprE.clone();
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        arguments.add(paramifiedConstant);
        arguments.add(otherParamifiedConstant);
        everSoParamifiedE.setArgs(arguments);
        result = everSoParamifiedE.bindingToIndexedExpression(exprE);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals(result.size(), 2);
        assertEquals(result.get(0), paramifiedConstant);
        assertEquals(result.get(1), otherParamifiedConstant);

        // Done positive match testing.

        ConstantValueExpression neitherConstant = new ConstantValueExpression();
        neitherConstant.setValue("86");

        ParameterValueExpression paramifiedNeitherConstant = new ParameterValueExpression();
        paramifiedNeitherConstant.setOriginalValue(neitherConstant);

        ParameterValueExpression actualUserProvidedParameter = new ParameterValueExpression();
        ParameterValueExpression otherUserProvidedParameter = new ParameterValueExpression();

        //
        // Interesting non-matches for these indexable expressions include:
        //
        // Each of A through E should fail to match a "clone" of any of the others,
        // all way too dissimilar.
        result = likeA.bindingToIndexedExpression(exprB);
        assertNull(result);
        result = likeA.bindingToIndexedExpression(exprC);
        assertNull(result);
        result = likeA.bindingToIndexedExpression(exprD);
        assertNull(result);
        result = likeA.bindingToIndexedExpression(exprE);
        assertNull(result);

        result = likeB.bindingToIndexedExpression(exprA);
        assertNull(result);
        result = likeB.bindingToIndexedExpression(exprC);
        assertNull(result);
        result = likeB.bindingToIndexedExpression(exprD);
        assertNull(result);
        result = likeB.bindingToIndexedExpression(exprE);
        assertNull(result);

        result = likeC.bindingToIndexedExpression(exprA);
        assertNull(result);
        result = likeC.bindingToIndexedExpression(exprB);
        assertNull(result);
        result = likeC.bindingToIndexedExpression(exprD);
        assertNull(result);
        result = likeC.bindingToIndexedExpression(exprE);
        assertNull(result);

        result = likeD.bindingToIndexedExpression(exprA);
        assertNull(result);
        result = likeD.bindingToIndexedExpression(exprB);
        assertNull(result);
        result = likeD.bindingToIndexedExpression(exprC);
        assertNull(result);
        result = likeD.bindingToIndexedExpression(exprE);
        assertNull(result);

        result = likeE.bindingToIndexedExpression(exprA);
        assertNull(result);
        result = likeE.bindingToIndexedExpression(exprB);
        assertNull(result);
        result = likeE.bindingToIndexedExpression(exprC);
        assertNull(result);
        result = likeE.bindingToIndexedExpression(exprD);
        assertNull(result);

        // Each of D and E should fail to match "near misses", one-offs of their parameterized selves,
        // specifically when the parameter has a different "original value" than the constant in the
        // indexable expression OR has no "original value" -- like a user-provided parameter to a
        // compiled statement.

        // D) math with a column and a constant ( 42 * T1.A ) for ( ? * T1.A ) w/ ? == 86
        AbstractExpression crossParamifiedD = exprD.clone();
        crossParamifiedD.setLeft(paramifiedNeitherConstant);
        result = crossParamifiedD.bindingToIndexedExpression(exprD);
        assertNull(result);

        // D) math with a column and a constant ( 42 * T1.A ) for ( ? * T1.A ) w/ ? == ???!
        AbstractExpression userParamifiedD = exprD.clone();
        userParamifiedD.setLeft(actualUserProvidedParameter);
        result = userParamifiedD.bindingToIndexedExpression(exprD);
        assertNull(result);

        // E) a function of a column and constants, ( anotherFunctionName( T1.A, 42, 44 ) )
        // for ( anotherFunctionName( T1.A, ?, ? ) ) where ?, ? = 42, 86
        FunctionExpression crossParamifiedE = (FunctionExpression) exprE.clone();
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        arguments.add(paramifiedConstant);
        arguments.add(paramifiedNeitherConstant);
        crossParamifiedE.setArgs(arguments);
        result = crossParamifiedE.bindingToIndexedExpression(exprE);
        assertNull(result);

        // for ( anotherFunctionName( T1.A, ?, ? ) ) where ?, ? = ???, ??? ...
        FunctionExpression userParamifiedE = (FunctionExpression) exprE.clone();
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        arguments.add(actualUserProvidedParameter);
        arguments.add(otherUserProvidedParameter);
        userParamifiedE.setArgs(arguments);
        result = userParamifiedE.bindingToIndexedExpression(exprE);
        assertNull(result);

        // Each of A through E should fail to match other one-offs of themselves
        // (i.e. wrong column, wrong constant, wrong function, wrong math op).

        // A) wrong column
        TupleValueExpression notTheColumn = new TupleValueExpression();
        notTheColumn.setTableName("T1");
        notTheColumn.setColumnName("notA");

        result = notTheColumn.bindingToIndexedExpression(exprA);
        assertNull(result);

        // B) wrong math with columns
        OperatorExpression notTheOperator = new OperatorExpression(ExpressionType.OPERATOR_DIVIDE, extraColumn, exprA);

        result = notTheOperator.bindingToIndexedExpression(exprB);
        assertNull(result);

        // C) wrong function of a column
        FunctionExpression neitherFunction = new FunctionExpression();
        neitherFunction.setAttributes("notTheFunctionName", "noNotTheFunctionName", "noOptionalArg", 86); // 86 is neither 42 nor 44.
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        neitherFunction.setArgs(arguments);

        result = neitherFunction.bindingToIndexedExpression(exprC);
        assertNull(result);

        // D) right math op with a wrong column and a right constant
        notTheOperator = new OperatorExpression(ExpressionType.OPERATOR_MULTIPLY, constant, extraColumn);

        result = notTheOperator.bindingToIndexedExpression(exprD);
        assertNull(result);

        // E) a right function of a column but not enough arguments.
        FunctionExpression notTheArgs = (FunctionExpression) exprE.clone();
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        arguments.add(constant);
        notTheArgs.setArgs(arguments);

        result = notTheArgs.bindingToIndexedExpression(exprE);
        assertNull(result);

        // E) or too many arguments.
        arguments = new ArrayList<AbstractExpression>();
        arguments.add(exprA);
        arguments.add(constant);
        arguments.add(exprA);
        arguments.add(constant);
        notTheArgs.setArgs(arguments);
        result = notTheArgs.bindingToIndexedExpression(exprE);
        assertNull(result);
    }

    // Test various expressions for NULL-rejection.
    public void testIsNullRejectingExpression() throws Exception
    {
        {
            // Test "IS NULL (T.C)" not NULL-rejecting
            TupleValueExpression tve = new TupleValueExpression();
            tve.setTableName("T");
            tve.setColumnName("C");
            OperatorExpression expr = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, tve, null);
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T"));
        }

        {
            // Test "IS NOT NULL (T.C)" is NULL-rejecting
            TupleValueExpression tve = new TupleValueExpression();
            tve.setTableName("T");
            tve.setColumnName("C");
            OperatorExpression subexpr = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, tve, null);
            OperatorExpression expr = new OperatorExpression(ExpressionType.OPERATOR_NOT, subexpr, null);
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr, "T"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "TT"));
        }

        {
            // Test "T1.C > T2.C" is NULL-rejecting
            TupleValueExpression tve1 = new TupleValueExpression();
            tve1.setTableName("T1");
            tve1.setColumnName("C");
            TupleValueExpression tve2 = new TupleValueExpression();
            tve2.setTableName("T2");
            tve2.setColumnName("C");
            OperatorExpression expr = new OperatorExpression(ExpressionType.COMPARE_GREATERTHAN, tve1, tve2);
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr, "T1"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T"));
        }

        {
            // Test "T1.C IN (1,2)" is NULL-rejecting
            TupleValueExpression tve1 = new TupleValueExpression();
            tve1.setTableName("T1");
            tve1.setColumnName("C");
            VectorValueExpression vve = new VectorValueExpression();

            InComparisonExpression ine = new InComparisonExpression();
            ine.m_left = tve1;
            ine.m_right = vve;
            assertTrue(ExpressionUtil.isNullRejectingExpression(ine, "T1"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(ine, "T"));
        }

        {
            // Test AND expressions
            // Test "T1.C > T2.C AND T2.B IS NULL " is NULL-rejecting
            TupleValueExpression tve1 = new TupleValueExpression();
            tve1.setTableName("T1");
            tve1.setColumnName("C");
            TupleValueExpression tve2 = new TupleValueExpression();
            tve2.setTableName("T2");
            tve2.setColumnName("C");
            OperatorExpression expr1 = new OperatorExpression(ExpressionType.COMPARE_GREATERTHAN, tve1, tve2);
            TupleValueExpression tve3 = new TupleValueExpression();
            tve2.setTableName("T2");
            tve2.setColumnName("B");
            OperatorExpression expr2 = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, tve3, null);
            OperatorExpression expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expr1, expr2);
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr, "T2"));
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr, "T1"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T"));

            // Test "T1.C IS NULL AND T2.B IS NULL " is NULL-rejecting
            OperatorExpression expr3 = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, tve1, null);
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expr3, expr2);
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T2"));
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T1"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T"));
        }

        {
            // Test OR expressions
            // Test "T1.C > T2.C OR T2.B IS NULL " is not NULL-rejecting
            TupleValueExpression tve1 = new TupleValueExpression();
            tve1.setTableName("T1");
            tve1.setColumnName("C");
            TupleValueExpression tve2 = new TupleValueExpression();
            tve2.setTableName("T2");
            tve2.setColumnName("C");
            OperatorExpression expr1 = new OperatorExpression(ExpressionType.COMPARE_GREATERTHAN, tve1, tve2);
            TupleValueExpression tve3 = new TupleValueExpression();
            tve3.setTableName("T2");
            tve3.setColumnName("B");
            OperatorExpression expr2 = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, tve3, null);
            OperatorExpression expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, expr1, expr2);
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T2"));
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T1"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T"));

            // Test "T1.C > T2.C OR T2.B > T1.C " is NULL-rejecting
            OperatorExpression expr3 = new OperatorExpression(ExpressionType.COMPARE_GREATERTHAN, tve3, tve1);
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, expr1, expr3);
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr, "T2"));
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr, "T1"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T"));
        }

        {
            // Test "ABS(T1.C) > 5" is NULL-rejecting
            TupleValueExpression tve = new TupleValueExpression();
            tve.setTableName("T1");
            tve.setColumnName("C");
            ArrayList<AbstractExpression> args = new ArrayList<AbstractExpression>();
            args.add(tve);
            FunctionExpression abs = new FunctionExpression();
            abs.setArgs(args);
            ConstantValueExpression cve = new ConstantValueExpression();
            cve.setValue("5");
            OperatorExpression expr = new OperatorExpression(ExpressionType.COMPARE_GREATERTHAN, abs, cve);
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr, "T1"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr, "T"));
        }

        {
            // Test negation
            // Test "!(T1.C > T2.C)" is NULL-rejecting
            TupleValueExpression tve1 = new TupleValueExpression();
            tve1.setTableName("T1");
            tve1.setColumnName("C");
            TupleValueExpression tve2 = new TupleValueExpression();
            tve2.setTableName("T2");
            tve2.setColumnName("C");
            OperatorExpression expr1 = new OperatorExpression(ExpressionType.COMPARE_GREATERTHAN, tve1, tve2);
            OperatorExpression expr2 = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr1, null);
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr2, "T1"));
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr2, "T2"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr2, "T"));

            // Test "!(P AND Q)" is equivalent to "!P OR !Q"
            // !(T1.C IS NULL AND T2.C IS NULL) is not NULL-rejecting
            OperatorExpression expr3 = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, tve1, null);
            OperatorExpression expr4 = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, tve2, null);
            OperatorExpression expr5 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expr3, expr4);
            OperatorExpression expr6 = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr5, null);
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr6, "T1"));
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr6, "T2"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr6, "T"));

            // !(T1.C > T2.C AND T2.C IS NULL) is NULL-rejecting for T2 only
            OperatorExpression expr7 = new OperatorExpression(ExpressionType.COMPARE_GREATERTHAN, tve1, tve2);
            OperatorExpression expr8 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expr7, expr4);
            OperatorExpression expr9 = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr8, null);
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr9, "T1"));
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr9, "T2"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr9, "T"));

            // Test "!(P OR Q)" is equivalent to "!P AND !Q"
            // !(T1.C IS NULL OR T2.C IS NULL) is NULL-rejecting for T1 and T2
            OperatorExpression expr10 = new OperatorExpression(ExpressionType.CONJUNCTION_OR, expr3, expr4);
            OperatorExpression expr11 = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr10, null);
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr11, "T1"));
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr11, "T2"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr11, "T"));

            // !(T1.C > T2.C OR T2.C IS NULL) is NULL-rejecting for T1 and T2
            OperatorExpression expr12 = new OperatorExpression(ExpressionType.CONJUNCTION_OR, expr7, expr4);
            OperatorExpression expr13 = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr12, null);
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr13, "T1"));
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr13, "T2"));
            // Wrong table
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr13, "T"));
        }

        {
            // Test double negation "NOT T.C IS NOT NULL"
            TupleValueExpression tve = new TupleValueExpression();
            tve.setTableName("T");
            tve.setColumnName("C");
            OperatorExpression expr1 = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, tve, null);
            OperatorExpression expr2 = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr1, null);
            OperatorExpression expr3 = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr2, null);
            assertTrue(!ExpressionUtil.isNullRejectingExpression(expr3, "T"));

            // NOT (P AND NOT Q) --> (NOT P) OR NOT NOT Q --> (NOT P) OR Q
            // NOT (T.C IS NULL AND NOT T.C > T.A) --> ( NOT T.C IS NULL) OR T.C
            // > T.A
            TupleValueExpression tve1 = new TupleValueExpression();
            tve.setTableName("T");
            tve.setColumnName("A");
            OperatorExpression expr4 = new OperatorExpression(ExpressionType.COMPARE_GREATERTHAN, tve, tve1);
            OperatorExpression expr5 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expr1, expr4);
            OperatorExpression expr6 = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr5, null);
            assertTrue(ExpressionUtil.isNullRejectingExpression(expr6, "T"));

            // NOT (T.C IS NOT NULL AND T.C > T.A) --> T.C IS NULL OR NOT( T.C >
            // T.A)
            OperatorExpression expr7 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expr2, expr4);
            OperatorExpression expr8 = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr7, null);
            assertFalse(ExpressionUtil.isNullRejectingExpression(expr8, "T"));
        }
        {
            // COALESCE expression
            TupleValueExpression tve1 = new TupleValueExpression("T1", "T1", "C", "C", 1);
            TupleValueExpression tve2 = new TupleValueExpression("T2", "T2", "C", "C", 1);
            OperatorExpression isnull = new OperatorExpression(ExpressionType.OPERATOR_IS_NULL, tve1, null);
            OperatorExpression alternative = new OperatorExpression(ExpressionType.OPERATOR_ALTERNATIVE, tve2, tve1);
            OperatorExpression colalesce = new OperatorExpression(ExpressionType.OPERATOR_CASE_WHEN, isnull, alternative);

            assertFalse(ExpressionUtil.isNullRejectingExpression(colalesce, "T1"));
            assertFalse(ExpressionUtil.isNullRejectingExpression(colalesce, "T2"));
            assertFalse(ExpressionUtil.isNullRejectingExpression(colalesce, "T"));

            TupleValueExpression tve3 = new TupleValueExpression("T3", "T3", "C", "C", 1);
            OperatorExpression compExpr = new OperatorExpression(
                    ExpressionType.COMPARE_GREATERTHAN, tve3, colalesce);
            assertFalse(ExpressionUtil.isNullRejectingExpression(compExpr, "T1"));
            assertFalse(ExpressionUtil.isNullRejectingExpression(compExpr, "T2"));
            assertTrue(ExpressionUtil.isNullRejectingExpression(compExpr, "T3"));
        }
    }

    // Test compiler time expression evaluation.
    public void testEvaluateExpression() throws Exception {
        TupleValueExpression tve = new TupleValueExpression();
        AbstractExpression comp = new OperatorExpression(ExpressionType.COMPARE_GREATERTHAN, tve, tve);
        AbstractExpression notcomp = new OperatorExpression(ExpressionType.OPERATOR_NOT, comp, null);
        ConstantValueExpression trueCVE = ConstantValueExpression.getTrue();
        ConstantValueExpression falseCVE = ConstantValueExpression.getFalse();

        AbstractExpression expr;
        {
            // TRUE and expr => expr
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, trueCVE, comp);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(comp, expr);

            // expr and TRUE => expr
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, comp, trueCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(comp, expr);

            // FALSE and expr => FASLE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, falseCVE, comp);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(falseCVE, expr);

            // expr and FALSE => FALSE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, comp, falseCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(falseCVE, expr);

            // TRUE and TRUE => TRUE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, trueCVE, trueCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(trueCVE, expr);

            // TRUE and FALSE => FALSE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, trueCVE, falseCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(falseCVE, expr);

            // FALSE and TRUE => FALSE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, falseCVE, trueCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(falseCVE, expr);

            // FALSE and FALSE => FALSE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, falseCVE, falseCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(falseCVE, expr);
        }
        {
             // TRUE or expr => TRUE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, trueCVE, comp);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(trueCVE, expr);

            // expr or TRUE => TRUE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, comp, trueCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(trueCVE, expr);

            // FALSE or expr => expr
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, falseCVE, comp);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(comp, expr);

            // expr or FALSE => expr
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, comp, falseCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(comp, expr);

            // expr or expr => no change
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, comp, comp);
            AbstractExpression origExpr = expr.clone();
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(origExpr, expr);

            // TRUE or TRUE => TRUE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, trueCVE, trueCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(trueCVE, expr);

            // TRUE or FALSE => FALSE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, trueCVE, falseCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(trueCVE, expr);

            // FALSE or TRUE => FALSE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, falseCVE, trueCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(trueCVE, expr);

            // FALSE or FALSE => FALSE
            expr = new OperatorExpression(ExpressionType.CONJUNCTION_OR, falseCVE, falseCVE);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(falseCVE, expr);
        }
        {
            AbstractExpression expr1, expr2;
            // expr AND expr AND TRUE => expr AND expr
            expr1 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, trueCVE, comp);
            expr2 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expr1, comp);
            expr = ExpressionUtil.evaluateExpression(expr2);
            AbstractExpression finalExpr = new OperatorExpression(ExpressionType.CONJUNCTION_AND, comp, comp);
            assertEquals(finalExpr, expr);

            // expr AND expr AND FALSE => FALSE
            expr1 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, falseCVE, comp);
            expr2 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expr1, comp);
            expr = ExpressionUtil.evaluateExpression(expr2);
            assertEquals(falseCVE, expr);

            // NOT(TRUE) => FASLE
            expr = new OperatorExpression(ExpressionType.OPERATOR_NOT, trueCVE, null);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(falseCVE, expr);

            // NOT(FALSE) => TRUE
            expr = new OperatorExpression(ExpressionType.OPERATOR_NOT, falseCVE, null);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(trueCVE, expr);

            // NOT (FALSE OR NOT expr) => expr
            expr1 = new OperatorExpression(ExpressionType.OPERATOR_NOT, comp, null);
            expr2 = new OperatorExpression(ExpressionType.CONJUNCTION_OR, falseCVE, expr1);
            expr = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr2, null);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(comp, expr);

            // NOT( .. OR .. OR ..) => (.. AND .. AND..)
            expr1 = new OperatorExpression(ExpressionType.CONJUNCTION_OR, comp, comp);
            expr2 = new OperatorExpression(ExpressionType.CONJUNCTION_OR, expr1, comp);
            expr = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr2, null);
            expr = ExpressionUtil.evaluateExpression(expr);
            AbstractExpression expectedExpr1 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, notcomp, notcomp);
            AbstractExpression expectedExpr2 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expectedExpr1, notcomp);
            assertEquals(expectedExpr2, expr);


            // NOT (FALSE AND expr) => TRUE OR NOT expr => TRUE
            expr1 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, falseCVE, comp);
            expr = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr1, null);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(trueCVE, expr);


            // NOT (FALSE AND expr) => TRUE OR NOT expr => TRUE
            expr1 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, falseCVE, comp);
            expr = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr1, null);
            expr = ExpressionUtil.evaluateExpression(expr);
            assertEquals(trueCVE, expr);

            // NOT( .. AND .. AND ..) not equal to (NOT.. OR NOT.. OR..)
            // special case not handled on purpose for short circuit reason.
            expr1 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, comp, comp);
            expr2 = new OperatorExpression(ExpressionType.CONJUNCTION_AND, expr1, comp);
            expr = new OperatorExpression(ExpressionType.OPERATOR_NOT, expr2, null);
            assertEquals(expr, ExpressionUtil.evaluateExpression(expr));
        }
    }
}
