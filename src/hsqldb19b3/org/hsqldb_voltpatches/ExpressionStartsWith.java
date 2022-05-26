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

package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.types.Type;

import java.util.Objects;

/**
 * A VoltDB extension, implementation of STARTS WITH operation
 *
 * @author Xin Jin
 */
public class ExpressionStartsWith extends ExpressionLogical {

    private StartsWith             startsWithObject;
    private boolean noOptimization;

    /**
     * Create a STARTS WITH expression
     */
    ExpressionStartsWith(Expression left, Expression right, boolean noOptimization) {

        super(OpTypes.STARTS_WITH);

        nodes               = new Expression[BINARY];
        nodes[LEFT]         = left;
        nodes[RIGHT]        = right;
        startsWithObject    = new StartsWith();
        this.noOptimization = noOptimization;
    }

    @Override
    public HsqlList resolveColumnReferences(RangeVariable[] rangeVarArray,
            int rangeCount, HsqlList unresolvedSet, boolean acceptsSequences) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                unresolvedSet = nodes[i].resolveColumnReferences(rangeVarArray,
                        rangeCount, unresolvedSet, acceptsSequences);
            }
        }

        return unresolvedSet;
    }

    @Override
    public Object getValue(Session session) {

        if (opType != OpTypes.STARTS_WITH) {
            return super.getValue(session);
        }

        Object leftValue   = nodes[LEFT].getValue(session);
        Object rightValue  = nodes[RIGHT].getValue(session);

        if (startsWithObject.isVariable) {
            startsWithObject.setPattern(session, rightValue, nodes);
        }

        return startsWithObject.compare(session, leftValue);
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes[LEFT].isParam) {
            nodes[LEFT].dataType = nodes[RIGHT].dataType;
        } else if (nodes[RIGHT].isParam) {
            nodes[RIGHT].dataType = nodes[LEFT].dataType;
        }

        if (nodes[LEFT].dataType == null || nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (!(nodes[LEFT].dataType.isBooleanType()
                && nodes[RIGHT].dataType.isBooleanType())
                && dataType.isBooleanType()) {
            // If both argument nodes are boolean we have resolved
            // this before.  So, this is ok.  Otherwise, this is not
            // properly typed.
            if (!(nodes[LEFT].dataType.isCharacterType() && nodes[RIGHT].dataType.isCharacterType())) {
                throw Error.error(ErrorCode.X_42565);
            }
        }

        if (startsWithObject != null) {
            startsWithObject.dataType = nodes[LEFT].dataType;
        }

        boolean isRightArgFixedConstant = nodes[RIGHT].opType == OpTypes.VALUE;

        if (isRightArgFixedConstant && nodes[LEFT].opType == OpTypes.VALUE) {
            setAsConstantValue(session);
            return;
        }

        if (isRightArgFixedConstant) {
            if (startsWithObject != null) {
                startsWithObject.isVariable = false;
            }
        } else {
            return;
        }

        Object pattern = isRightArgFixedConstant
                         ? nodes[RIGHT].getConstantValue(session)
                         : null;

        startsWithObject.setPattern(session, pattern, nodes);

        if (noOptimization) {
            return;
        }

        // User parameters should not arrive here.
        assert(!nodes[RIGHT].isParam);

        if (startsWithObject.isEquivalentToCastNullPredicate()) {
            // ENG-14266 solve 'col STARTS WITH CAST(NULL AS VARCHAR)' problem
            // If it is this case, we are already set.
            // EE can handle this (left expression is a ExpressionColumn, right expression is a null VALUE).
            startsWithObject = null;
            return;
        } else if (startsWithObject.isEquivalentToUnknownPredicate()) {
            this.setAsConstantValue(null);
            startsWithObject = null;
        } else if (startsWithObject.isEquivalentToCharPredicate()) {    // handling plain prefix
            Expression leftOld = nodes[LEFT];

            nodes = new Expression[BINARY];

            Expression leftBound =
                    new ExpressionValue(startsWithObject.getRangeLow(),
                                        Type.SQL_VARCHAR);
                Expression rightBound =
                    new ExpressionValue(startsWithObject.getRangeHigh(session),
                                        Type.SQL_VARCHAR);
            nodes[LEFT] = new ExpressionLogical(OpTypes.GREATER_EQUAL,
                                                leftOld, leftBound);
            nodes[RIGHT] = new ExpressionLogical(OpTypes.SMALLER_EQUAL,
                                                 leftOld, rightBound);
            opType     = OpTypes.AND;
            startsWithObject = null;
        } else if (startsWithObject.isEquivalentToNotNullPredicate()) {
            Expression notNull = new ExpressionLogical(OpTypes.IS_NULL,
                nodes[LEFT]);
            opType      = OpTypes.NOT;
            nodes       = new Expression[UNARY];
            nodes[LEFT] = notNull;
            startsWithObject = null;
        } else {
            if (nodes[LEFT].opType != OpTypes.COLUMN) {
                return;
            }
            if (!nodes[LEFT].dataType.isCharacterType()) {
                return;
            }
        }
    }

    @Override
    public String getSQL() {

        String       left  = getContextSQL(nodes[LEFT]);
        String       right = getContextSQL(nodes[RIGHT]);
        StringBuffer sb    = new StringBuffer();

        sb.append(left).append(' ').append(Tokens.T_STARTS).append(' ').append(Tokens.T_WITH).append(' ');
        sb.append(right);

        return sb.toString();
    }

    @Override
    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer();

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append("STARTS WITH ");
        sb.append(startsWithObject.describe(session));

        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }

        if (other instanceof ExpressionStartsWith) {
            if (noOptimization == ((ExpressionStartsWith)other).noOptimization) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + Objects.hashCode(noOptimization);
    }
}
