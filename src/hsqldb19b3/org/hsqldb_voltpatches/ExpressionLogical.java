/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.types.DTIType;
import org.hsqldb_voltpatches.types.DateTimeType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;

/**
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ExpressionLogical extends Expression {

    boolean noOptimisation;

    /**
     * For LIKE
     */
    ExpressionLogical(int type) {

        super(type);

        dataType = Type.SQL_BOOLEAN;
    }

    /**
     * For boolean constants
     */
    ExpressionLogical(boolean b) {

        super(OpTypes.VALUE);

        dataType  = Type.SQL_BOOLEAN;
        valueData = b ? Boolean.TRUE
                      : Boolean.FALSE;
    }

    /*
     * Create an equality expressions using existing columns and
     * range variables. The expression is fully resolved in constructor.
     */
    ExpressionLogical(RangeVariable leftRangeVar, ColumnSchema left,
                      RangeVariable rightRangeVar, ColumnSchema right) {

        super(OpTypes.EQUAL);

        ExpressionColumn leftExpression = new ExpressionColumn(leftRangeVar,
            left);
        ExpressionColumn rightExpression = new ExpressionColumn(rightRangeVar,
            right);

        nodes        = new Expression[BINARY];
        dataType     = Type.SQL_BOOLEAN;
        nodes[LEFT]  = leftExpression;
        nodes[RIGHT] = rightExpression;
    }

    /**
     * Creates an equality expression
     */
    ExpressionLogical(Expression left, Expression right) {

        super(OpTypes.EQUAL);

        nodes        = new Expression[BINARY];
        nodes[LEFT]  = left;
        nodes[RIGHT] = right;

        if (left.opType == OpTypes.COLUMN && right.opType == OpTypes.COLUMN) {
            isColumnEqual = true;
        }

        dataType = Type.SQL_BOOLEAN;
    }

    /**
     * Creates a binary operation expression
     */
    ExpressionLogical(int type, Expression left, Expression right) {

        super(type);

        nodes        = new Expression[BINARY];
        nodes[LEFT]  = left;
        nodes[RIGHT] = right;

        switch (opType) {

            case OpTypes.EQUAL :
                if (left.opType == OpTypes.COLUMN
                        && right.opType == OpTypes.COLUMN) {
                    isColumnEqual = true;
                }

            // $FALL-THROUGH$
            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER :
            case OpTypes.SMALLER :
            case OpTypes.SMALLER_EQUAL :
            case OpTypes.NOT_EQUAL :
            case OpTypes.OVERLAPS :
            case OpTypes.NOT_DISTINCT :
            case OpTypes.IN :
            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL :
            case OpTypes.AND :
            case OpTypes.OR :
                dataType = Type.SQL_BOOLEAN;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    /**
     * Creates a unary operation expression
     */
    ExpressionLogical(int type, Expression e) {

        super(type);

        nodes       = new Expression[UNARY];
        nodes[LEFT] = e;

        switch (opType) {

            case OpTypes.UNIQUE :
            case OpTypes.EXISTS :
            case OpTypes.IS_NULL :
            case OpTypes.NOT :
                dataType = Type.SQL_BOOLEAN;
                break;

            default :
                Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    /**
     * Creates a column not null expression
     */
    ExpressionLogical(ColumnSchema column) {

        super(OpTypes.NOT);

        nodes    = new Expression[UNARY];
        dataType = Type.SQL_BOOLEAN;

        Expression e = new ExpressionColumn(column);

        e           = new ExpressionLogical(OpTypes.IS_NULL, e);
        nodes[LEFT] = e;
    }

    // logical ops
    static Expression andExpressions(Expression e1, Expression e2) {

        if (e1 == null) {
            return e2;
        }

        if (e2 == null) {
            return e1;
        }

        return new ExpressionLogical(OpTypes.AND, e1, e2);
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer(64);

        if (opType == OpTypes.VALUE) {
            return super.getSQL();
        }

        String left  = getContextSQL(nodes[LEFT]);
        String right = getContextSQL(nodes.length > 1 ? nodes[RIGHT]
                                                      : null);

        switch (opType) {

            case OpTypes.NOT :
                if (nodes[LEFT].opType == OpTypes.IS_NULL) {
                    sb.append(getContextSQL(nodes[LEFT].nodes[LEFT])).append(
                        ' ').append(Tokens.T_IS).append(' ').append(
                        Tokens.T_NOT).append(' ').append(Tokens.T_NULL);

                    return sb.toString();
                }

                if (nodes[LEFT].opType == OpTypes.NOT_DISTINCT) {
                    sb.append(getContextSQL(nodes[LEFT].nodes[LEFT])).append(
                        ' ').append(Tokens.T_IS).append(' ').append(
                        Tokens.T_DISTINCT).append(' ').append(
                        Tokens.T_FROM).append(' ').append(
                        getContextSQL(nodes[LEFT].nodes[RIGHT]));

                    return sb.toString();
                }

                sb.append(Tokens.T_NOT).append(' ').append(left);

                return sb.toString();

            case OpTypes.NOT_DISTINCT :
                sb.append(Tokens.T_NOT).append(' ').append(
                    getContextSQL(nodes[LEFT].nodes[LEFT])).append(' ').append(
                    Tokens.T_IS).append(' ').append(Tokens.T_DISTINCT).append(
                    ' ').append(Tokens.T_FROM).append(' ').append(
                    getContextSQL(nodes[LEFT].nodes[RIGHT]));

                return sb.toString();

            case OpTypes.IS_NULL :
                sb.append(left).append(' ').append(Tokens.T_IS).append(
                    ' ').append(Tokens.T_NULL);

                return sb.toString();

            case OpTypes.UNIQUE :
                sb.append(' ').append(Tokens.T_UNIQUE).append(' ');
                break;

            case OpTypes.EXISTS :
                sb.append(' ').append(Tokens.T_EXISTS).append(' ');
                break;

            case OpTypes.EQUAL :
                sb.append(left).append('=').append(right);

                return sb.toString();

            case OpTypes.GREATER_EQUAL :
                sb.append(left).append(">=").append(right);

                return sb.toString();

            case OpTypes.GREATER :
                sb.append(left).append('>').append(right);

                return sb.toString();

            case OpTypes.SMALLER :
                sb.append(left).append('<').append(right);

                return sb.toString();

            case OpTypes.SMALLER_EQUAL :
                sb.append(left).append("<=").append(right);

                return sb.toString();

            case OpTypes.NOT_EQUAL :
                if (Tokens.T_NULL.equals(right)) {
                    sb.append(left).append(" IS NOT ").append(right);
                } else {
                    sb.append(left).append("!=").append(right);
                }

                return sb.toString();

            case OpTypes.AND :
                sb.append(left).append(' ').append(Tokens.T_AND).append(
                    ' ').append(right);

                return sb.toString();

            case OpTypes.OR :
                sb.append(left).append(' ').append(Tokens.T_OR).append(
                    ' ').append(right);

                return sb.toString();

            case OpTypes.IN :
                sb.append(left).append(' ').append(Tokens.T_IN).append(
                    ' ').append(right);

                return sb.toString();

            case OpTypes.MATCH_SIMPLE :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(right);

                return sb.toString();

            case OpTypes.MATCH_PARTIAL :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.PARTIAL).append(right);

                return sb.toString();

            case OpTypes.MATCH_FULL :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.FULL).append(right);

                return sb.toString();

            case OpTypes.MATCH_UNIQUE_SIMPLE :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.UNIQUE).append(right);

                return sb.toString();

            case OpTypes.MATCH_UNIQUE_PARTIAL :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.UNIQUE).append(' ').append(
                    Tokens.PARTIAL).append(right);

                return sb.toString();

            case OpTypes.MATCH_UNIQUE_FULL :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.UNIQUE).append(' ').append(
                    Tokens.FULL).append(right);

                return sb.toString();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }

        return sb.toString();
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        switch (opType) {

            case OpTypes.VALUE :
                sb.append("VALUE = ").append(valueData);
                sb.append(", TYPE = ").append(dataType.getNameString());

                return sb.toString();

            case OpTypes.NOT :
                if (nodes[LEFT].opType == OpTypes.NOT_DISTINCT) {
                    sb.append(Tokens.T_DISTINCT);

                    return sb.toString();
                }

                sb.append("NOT ");
                break;

            case OpTypes.NOT_DISTINCT :
                sb.append("NOT ");
                sb.append("DISTINCT ");
                break;

            case OpTypes.EQUAL :
                sb.append("EQUAL ");
                break;

            case OpTypes.GREATER_EQUAL :
                sb.append("GREATER_EQUAL ");
                break;

            case OpTypes.GREATER :
                sb.append("GREATER ");
                break;

            case OpTypes.SMALLER :
                sb.append("SMALLER ");
                break;

            case OpTypes.SMALLER_EQUAL :
                sb.append("SMALLER_EQUAL ");
                break;

            case OpTypes.NOT_EQUAL :
                sb.append("NOT_EQUAL ");
                break;

            case OpTypes.AND :
                sb.append("AND ");
                break;

            case OpTypes.OR :
                sb.append("OR ");
                break;

            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL :
                sb.append("MATCH ");
                break;

            case OpTypes.IS_NULL :
                sb.append("IS_NULL ");
                break;

            case OpTypes.UNIQUE :
                sb.append("UNIQUE ");
                break;

            case OpTypes.EXISTS :
                sb.append("EXISTS ");
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }

        if (nodes[LEFT] != null) {
            sb.append(" arg1=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }

        if (nodes[RIGHT] != null) {
            sb.append(" arg2=[");
            sb.append(nodes[RIGHT].describe(session, blanks + 1));
            sb.append(']');
        }

        return sb.toString();
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        switch (opType) {

            case OpTypes.VALUE :
                break;

            case OpTypes.NOT_DISTINCT :
            case OpTypes.EQUAL :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER :
            case OpTypes.SMALLER :
            case OpTypes.SMALLER_EQUAL :
            case OpTypes.NOT_EQUAL :
                resolveTypesForComparison(session, parent);
                break;

            case OpTypes.AND : {
                resolveTypesForLogicalOp();

                if (nodes[LEFT].opType == OpTypes.VALUE) {
                    if (nodes[RIGHT].opType == OpTypes.VALUE) {
                        setAsConstantValue(session);
                    } else {
                        Object value = nodes[LEFT].getValue(session);

                        if (value == null || Boolean.FALSE.equals(value)) {
                            setAsConstantValue(Boolean.FALSE);
                        }
                    }
                } else if (nodes[RIGHT].opType == OpTypes.VALUE) {
                    Object value = nodes[RIGHT].getValue(session);

                    if (value == null || Boolean.FALSE.equals(value)) {
                        setAsConstantValue(Boolean.FALSE);
                    }
                }

                break;
            }
            case OpTypes.OR : {
                resolveTypesForLogicalOp();

                if (nodes[LEFT].opType == OpTypes.VALUE) {
                    if (nodes[RIGHT].opType == OpTypes.VALUE) {
                        setAsConstantValue(session);
                    } else {
                        Object value = nodes[LEFT].getValue(session);

                        if (Boolean.TRUE.equals(value)) {
                            setAsConstantValue(Boolean.TRUE);
                        }
                    }
                } else if (nodes[RIGHT].opType == OpTypes.VALUE) {
                    Object value = nodes[RIGHT].getValue(session);

                    if (Boolean.TRUE.equals(value)) {
                        setAsConstantValue(Boolean.TRUE);
                    }
                }

                break;
            }
            case OpTypes.IS_NULL :
                if (nodes[LEFT].isParam) {
                    throw Error.error(ErrorCode.X_42565);
                }

                if (nodes[LEFT].opType == OpTypes.VALUE) {
                    setAsConstantValue(session);
                }
                break;

            case OpTypes.NOT :
                if (nodes[LEFT].isParam) {
                    nodes[LEFT].dataType = Type.SQL_BOOLEAN;

                    break;
                }

                if (nodes[LEFT].opType == OpTypes.VALUE) {
                    if (nodes[LEFT].dataType.isBooleanType()) {
                        setAsConstantValue(session);

                        break;
                    } else {
                        throw Error.error(ErrorCode.X_42565);
                    }
                }

                if (nodes[LEFT].dataType == null
                        || !nodes[LEFT].dataType.isBooleanType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                dataType = Type.SQL_BOOLEAN;
                break;

            case OpTypes.OVERLAPS :
                resolveTypesForOverlaps();
                break;

            case OpTypes.IN :
                resolveTypesForIn(session);
                break;

            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL :
                resolveTypesForAllAny(session);
                break;

            case OpTypes.UNIQUE :
            case OpTypes.EXISTS :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "Expression.resolveTypes()");
        }
    }

    private void resolveTypesForLogicalOp() {

        if (nodes[LEFT].isParam) {
            nodes[LEFT].dataType = Type.SQL_BOOLEAN;
        }

        if (nodes[RIGHT].isParam) {
            nodes[RIGHT].dataType = Type.SQL_BOOLEAN;
        }

        if (nodes[LEFT].dataType == null || nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42571);
        }

        if (nodes[LEFT].opType == OpTypes.ROW
                || nodes[RIGHT].opType == OpTypes.ROW
                || Type.SQL_BOOLEAN != nodes[LEFT].dataType
                || Type.SQL_BOOLEAN != nodes[RIGHT].dataType) {
            throw Error.error(ErrorCode.X_42564);
        }
    }

    private void resolveTypesForComparison(Session session,
                                           Expression parent) {

        if (exprSubType == OpTypes.ALL_QUANTIFIED
                || exprSubType == OpTypes.ANY_QUANTIFIED) {
            resolveTypesForAllAny(session);

            return;
        }

        if (nodes[LEFT].opType == OpTypes.ROW
                || nodes[RIGHT].opType == OpTypes.ROW) {
            if (nodes[LEFT].opType != OpTypes.ROW
                    || nodes[RIGHT].opType != OpTypes.ROW
                    || nodes[LEFT].nodes.length != nodes[RIGHT].nodes.length) {
                throw Error.error(ErrorCode.X_42564);
            }

            resolveRowTypes();
        } else {
            if (nodes[LEFT].isParam) {
                nodes[LEFT].dataType = nodes[RIGHT].dataType;
            } else if (nodes[RIGHT].isParam) {
                nodes[RIGHT].dataType = nodes[LEFT].dataType;
            }

            if (nodes[LEFT].dataType == null
                    || nodes[RIGHT].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }

            if (nodes[LEFT].dataType.typeComparisonGroup
                    != nodes[RIGHT].dataType.typeComparisonGroup) {
                if (convertDateTimeLiteral(session, nodes[LEFT],
                                           nodes[RIGHT])) {

                    // compatibility for scalars only
                } else {
                    throw Error.error(ErrorCode.X_42562);
                }
            } else if (nodes[LEFT].dataType.isDateTimeType()) {
                if (nodes[LEFT].dataType.isDateTimeTypeWithZone()
                        ^ nodes[RIGHT].dataType.isDateTimeTypeWithZone()) {
                    nodes[LEFT] = new ExpressionOp(nodes[LEFT]);
                }
            }
        }

        if (nodes[LEFT].opType == OpTypes.VALUE
                && nodes[RIGHT].opType == OpTypes.VALUE) {
            setAsConstantValue(session);
        }
    }

    private void resolveRowTypes() {

        for (int i = 0; i < nodes[LEFT].nodeDataTypes.length; i++) {
            Type leftType  = nodes[LEFT].nodeDataTypes[i];
            Type rightType = nodes[RIGHT].nodeDataTypes[i];

            if (leftType.typeComparisonGroup
                    != rightType.typeComparisonGroup) {
                throw Error.error(ErrorCode.X_42562);
            } else if (leftType.isDateTimeType()) {
                if (leftType.isDateTimeTypeWithZone()
                        ^ rightType.isDateTimeTypeWithZone()) {
                    nodes[LEFT].nodes[i] =
                        new ExpressionOp(nodes[LEFT].nodes[i]);
                    nodes[LEFT].nodeDataTypes[i] =
                        nodes[LEFT].nodes[i].dataType;
                }
            }
        }
    }

    /**
     * for compatibility, convert a datetime character string to a datetime
     * value for comparison
     */
    private boolean convertDateTimeLiteral(Session session, Expression a,
                                           Expression b) {

        if (a.dataType.isDateTimeType()) {}
        else if (b.dataType.isDateTimeType()) {
            Expression c = a;

            a = b;
            b = c;
        } else {
            return false;
        }

        if (a.dataType.isDateTimeTypeWithZone()) {
            return false;
        }

        if (b.opType == OpTypes.VALUE && b.dataType.isCharacterType()) {
            b.valueData = a.dataType.castToType(session, b.valueData,
                                                b.dataType);
            b.dataType = a.dataType;

            return true;
        }

        return false;
    }

    void resolveTypesForOverlaps() {

        if (nodes[LEFT].nodes[0].isParam) {
            nodes[LEFT].nodes[0].dataType = nodes[RIGHT].nodes[0].dataType;
        }

        if (nodes[RIGHT].nodes[0].isParam) {
            nodes[RIGHT].nodes[0].dataType = nodes[LEFT].nodes[0].dataType;
        }

        if (nodes[LEFT].nodes[0].dataType == null) {
            nodes[LEFT].nodes[0].dataType = nodes[RIGHT].nodes[0].dataType =
                Type.SQL_TIMESTAMP;
        }

        if (nodes[LEFT].nodes[1].isParam) {
            nodes[LEFT].nodes[1].dataType = nodes[RIGHT].nodes[0].dataType;
        }

        if (nodes[RIGHT].nodes[1].isParam) {
            nodes[RIGHT].nodes[1].dataType = nodes[LEFT].nodes[0].dataType;
        }

        if (!DTIType
                .isValidDatetimeRange(nodes[LEFT].nodes[0]
                    .dataType, nodes[LEFT].nodes[1].dataType) || !DTIType
                        .isValidDatetimeRange(nodes[RIGHT].nodes[0]
                            .dataType, nodes[RIGHT].nodes[1].dataType)) {
            throw Error.error(ErrorCode.X_42565);
        }

        if (!DTIType.isValidDatetimeRange(nodes[LEFT].nodes[0].dataType,
                                          nodes[LEFT].nodes[1].dataType)) {
            throw Error.error(ErrorCode.X_42563);
        }

        nodes[LEFT].nodeDataTypes[0]  = nodes[LEFT].nodes[0].dataType;
        nodes[LEFT].nodeDataTypes[1]  = nodes[LEFT].nodes[1].dataType;
        nodes[RIGHT].nodeDataTypes[0] = nodes[RIGHT].nodes[0].dataType;
        nodes[RIGHT].nodeDataTypes[1] = nodes[RIGHT].nodes[1].dataType;
    }

    void resolveTypesForAllAny(Session session) {

        int degree = nodes[LEFT].getDegree();

        if (degree == 1 && nodes[LEFT].opType != OpTypes.ROW) {
            nodes[LEFT] = new Expression(OpTypes.ROW,
                                         new Expression[]{ nodes[LEFT] });
        }

        if (nodes[RIGHT].opType == OpTypes.TABLE) {
            nodes[RIGHT].prepareTable(session, nodes[LEFT], degree);
            nodes[RIGHT].subQuery.prepareTable(session);

            if (nodes[RIGHT].isCorrelated) {
                nodes[RIGHT].subQuery.setCorrelated();
            }
        }

        if (degree != nodes[RIGHT].nodeDataTypes.length) {
            throw Error.error(ErrorCode.X_42564);
        }

        if (nodes[RIGHT].opType == OpTypes.TABLE) {}

        nodes[LEFT].nodeDataTypes = new Type[nodes[LEFT].nodes.length];

        for (int i = 0; i < nodes[LEFT].nodeDataTypes.length; i++) {
            Type type = nodes[LEFT].nodes[i].dataType;

            if (type == null) {
                type = nodes[RIGHT].nodeDataTypes[i];
            }
            // A VoltDB extension to support "IN ?"
            else if (i == 0 && degree == 1 &&
                    nodes[RIGHT].opType == OpTypes.DYNAMIC_PARAM &&
                    nodes[RIGHT].nodeDataTypes != null &&
                    nodes[RIGHT].nodeDataTypes.length == 1 &&
                    nodes[RIGHT].nodeDataTypes[0] == null) {
                if (type.isIntegralType()) {
                    // promote parameter type to vector of BIGINT regardless of exact LHS integer scale.
                    nodes[RIGHT].nodeDataTypes[0] = Type.SQL_BIGINT;
                }
                else {
                    nodes[RIGHT].nodeDataTypes[0] = type;
                }
            }
            // End of VoltDB extension to support "IN ?"

            if (type == null) {
                throw Error.error(ErrorCode.X_42567);
            }

            nodes[LEFT].nodeDataTypes[i] = type;
        }
    }

    void resolveTypesForIn(Session session) {
        resolveTypesForAllAny(session);
    }

    public Object getValue(Session session) {

        switch (opType) {

            case OpTypes.VALUE :
                return valueData;

            case OpTypes.SIMPLE_COLUMN : {
                Object[] data =
                    (Object[]) session.sessionContext
                        .rangeIterators[rangePosition].getCurrent();

                return data[columnIndex];
            }
            case OpTypes.NEGATE :
                return ((NumberType) dataType).negate(
                    nodes[LEFT].getValue(session, nodes[LEFT].dataType));

            case OpTypes.IS_NULL :
                return nodes[LEFT].getValue(session) == null ? Boolean.TRUE
                                                             : Boolean.FALSE;

            case OpTypes.OVERLAPS : {
                Object[] left  = nodes[LEFT].getRowValue(session);
                Object[] right = nodes[RIGHT].getRowValue(session);

                return DateTimeType.overlaps(session, left,
                                             nodes[LEFT].nodeDataTypes, right,
                                             nodes[RIGHT].nodeDataTypes);
            }
            case OpTypes.IN : {
                return testInCondition(session,
                                       nodes[LEFT].getRowValue(session));
            }
            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL : {
                return testMatchCondition(session,
                                          nodes[LEFT].getRowValue(session));
            }
            case OpTypes.UNIQUE : {
                nodes[LEFT].subQuery.materialiseCorrelated(session);

                return nodes[LEFT].subQuery.hasUniqueNotNullRows(session)
                       ? Boolean.TRUE
                       : Boolean.FALSE;
            }
            case OpTypes.EXISTS : {
                return testExistsCondition(session);
            }
            case OpTypes.NOT : {
                Boolean result = (Boolean) nodes[LEFT].getValue(session);

                return result == null ? null
                                      : result.booleanValue() ? Boolean.FALSE
                                                              : Boolean.TRUE;
            }
            case OpTypes.AND : {
                Boolean r1 = (Boolean) nodes[LEFT].getValue(session);

                if (Boolean.FALSE.equals(r1)) {
                    return Boolean.FALSE;
                }

                Boolean r2 = (Boolean) nodes[RIGHT].getValue(session);

                if (Boolean.FALSE.equals(r2)) {
                    return Boolean.FALSE;
                }

                if (r1 == null || r2 == null) {
                    return null;
                }

                return Boolean.TRUE;
            }
            case OpTypes.OR : {
                Boolean r1 = (Boolean) nodes[LEFT].getValue(session);

                if (Boolean.TRUE.equals(r1)) {
                    return Boolean.TRUE;
                }

                Boolean r2 = (Boolean) nodes[RIGHT].getValue(session);

                if (Boolean.TRUE.equals(r2)) {
                    return Boolean.TRUE;
                }

                if (r1 == null || r2 == null) {
                    return null;
                }

                return Boolean.FALSE;
            }
            case OpTypes.NOT_DISTINCT :
            case OpTypes.EQUAL :
            case OpTypes.GREATER :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.SMALLER_EQUAL :
            case OpTypes.SMALLER :
            case OpTypes.NOT_EQUAL : {
                if (exprSubType == OpTypes.ANY_QUANTIFIED
                        || exprSubType == OpTypes.ALL_QUANTIFIED) {
                    return testAllAnyCondition(
                        session, (Object[]) nodes[LEFT].getRowValue(session));
                }

                Object o1 = nodes[LEFT].getValue(session);
                Object o2 = nodes[RIGHT].getValue(session);

                if (o1 instanceof Object[]) {
                    return compareValues(session, (Object[]) o1,
                                         (Object[]) o2);
                } else {
                    return compareValues(session, o1, o2);
                }
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    /**
     * For MATCH SIMPLE and FULL expressions, nulls in left are handled
     * prior to calling this method
     */
    private Boolean compareValues(Session session, Object left, Object right) {

        int result = 0;

        if (left == null || right == null) {
            return null;
        }

        result = nodes[LEFT].dataType.compare(left, right);

        switch (opType) {

            case OpTypes.EQUAL :
                return result == 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.NOT_EQUAL :
                return result != 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.GREATER :
                return result > 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            case OpTypes.GREATER_EQUAL :
                return result >= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.SMALLER_EQUAL :
                return result <= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.SMALLER :
                return result < 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    /**
     * For MATCH SIMPLE and FULL expressions, nulls in left are handled
     * prior to calling this method
     */
    private Boolean compareValues(Session session, Object[] left,
                                  Object[] right) {

        int     result  = 0;
        boolean hasNull = false;

        if (left == null || right == null) {
            return null;
        }

        Object[] leftList  = (Object[]) left;
        Object[] rightList = (Object[]) right;

        for (int i = 0; i < nodes[LEFT].nodes.length; i++) {
            if (leftList[i] == null) {
                if (opType == OpTypes.MATCH_PARTIAL
                        || opType == OpTypes.MATCH_UNIQUE_PARTIAL) {
                    continue;
                }

                hasNull = true;
            }

            if (rightList[i] == null) {
                hasNull = true;
            }

            Object leftValue  = leftList[i];
            Object rightValue = rightList[i];
            Type[] types      = nodes[LEFT].nodeDataTypes;

            result = types[i].compare(leftValue, rightValue);

            if (result != 0) {
                break;
            }
        }

        switch (opType) {

            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_FULL :
            case OpTypes.NOT_DISTINCT :
                return result == 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.IN :
            case OpTypes.EQUAL :
                if (hasNull) {
                    return null;
                }

                return result == 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.NOT_EQUAL :
                if (hasNull) {
                    return null;
                }

                return result != 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.GREATER :
                if (hasNull) {
                    return null;
                }

                return result > 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            case OpTypes.GREATER_EQUAL :
                if (hasNull) {
                    return null;
                }

                return result >= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.SMALLER_EQUAL :
                if (hasNull) {
                    return null;
                }

                return result <= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.SMALLER :
                if (hasNull) {
                    return null;
                }

                return result < 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    /**
     * Returns the result of testing a VALUE_LIST expression
     */
    private Boolean testInCondition(Session session, Object[] data) {

        if (data == null) {
            return null;
        }

        if (Expression.countNulls(data) != 0) {
            return null;
        }

        if (nodes[RIGHT].opType == OpTypes.TABLE) {
            final int length = nodes[RIGHT].nodes.length;

            for (int i = 0; i < length; i++) {
                Object[] rowData = nodes[RIGHT].nodes[i].getRowValue(session);

                if (Boolean.TRUE.equals(compareValues(session, data,
                                                      rowData))) {
                    return Boolean.TRUE;
                }
            }

            return Boolean.FALSE;
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "");
    }

    private Boolean testMatchCondition(Session session, Object[] data) {

        int nulls;

        if (data == null) {
            return Boolean.TRUE;
        }

        nulls = countNulls(data);

        if (nulls != 0) {
            switch (opType) {

                case OpTypes.MATCH_SIMPLE :
                case OpTypes.MATCH_UNIQUE_SIMPLE :
                    return Boolean.TRUE;

                case OpTypes.MATCH_PARTIAL :
                case OpTypes.MATCH_UNIQUE_PARTIAL :
                    if (nulls == data.length) {
                        return Boolean.TRUE;
                    }
                    break;

                case OpTypes.MATCH_FULL :
                case OpTypes.MATCH_UNIQUE_FULL :
                    return nulls == data.length ? Boolean.TRUE
                                                : Boolean.FALSE;
            }
        }

        if (nodes[RIGHT].opType == OpTypes.TABLE) {
            final int length   = nodes[RIGHT].nodes.length;
            boolean   hasMatch = false;

            for (int i = 0; i < length; i++) {
                Object[] rowData = nodes[RIGHT].nodes[i].getRowValue(session);
                Boolean  result  = compareValues(session, data, rowData);

                if (result == null || !result.booleanValue()) {
                    continue;
                }

                switch (opType) {

                    case OpTypes.MATCH_SIMPLE :
                    case OpTypes.MATCH_PARTIAL :
                    case OpTypes.MATCH_FULL :
                        return Boolean.TRUE;

                    case OpTypes.MATCH_UNIQUE_SIMPLE :
                    case OpTypes.MATCH_UNIQUE_PARTIAL :
                    case OpTypes.MATCH_UNIQUE_FULL :
                        if (hasMatch) {
                            return Boolean.FALSE;
                        }

                        hasMatch = true;
                }
            }

            return hasMatch ? Boolean.TRUE
                            : Boolean.FALSE;
        } else if (nodes[RIGHT].opType == OpTypes.TABLE_SUBQUERY) {
            PersistentStore store = session.sessionData.getRowStore(
                nodes[RIGHT].subQuery.getTable());

            nodes[RIGHT].subQuery.materialiseCorrelated(session);
            convertToType(session, data, nodes[LEFT].nodeDataTypes,
                          nodes[RIGHT].nodeDataTypes);

            if (nulls != 0
                    && (opType == OpTypes.MATCH_PARTIAL
                        || opType == OpTypes.MATCH_UNIQUE_PARTIAL)) {
                boolean hasMatch = false;
                RowIterator it =
                    nodes[RIGHT].subQuery.getTable().rowIterator(session);

                while (it.hasNext()) {
                    Object[] rowData = it.getNextRow().getData();
                    Boolean  result  = compareValues(session, data, rowData);

                    if (result == null) {
                        continue;
                    }

                    if (result.booleanValue()) {
                        if (opType == OpTypes.MATCH_PARTIAL) {
                            return Boolean.TRUE;
                        }

                        if (hasMatch) {
                            return Boolean.FALSE;
                        }

                        hasMatch = true;
                    }
                }

                return hasMatch ? Boolean.TRUE
                                : Boolean.FALSE;
            }

            RowIterator it =
                nodes[RIGHT].subQuery.getTable().getPrimaryIndex()
                    .findFirstRow(session, store, data);
            boolean result = it.hasNext();

            if (!result) {
                return Boolean.FALSE;
            }

            switch (opType) {

                case OpTypes.MATCH_SIMPLE :
                case OpTypes.MATCH_PARTIAL :
                case OpTypes.MATCH_FULL :
                    return Boolean.TRUE;
            }

            it.getNextRow();

            result = it.hasNext();

            if (!result) {
                return Boolean.TRUE;
            }

            Object[] rowData = it.getNextRow().getData();
            Boolean returnValue =
                Boolean.TRUE.equals(compareValues(session, data, rowData))
                ? Boolean.FALSE
                : Boolean.TRUE;

            return returnValue;
        }

        throw Error.error(ErrorCode.X_42564);
    }

    private Boolean testExistsCondition(Session session) {

        SubQuery subQuery = nodes[LEFT].subQuery;

        if (subQuery.isCorrelated()) {
            subQuery.materialiseCorrelated(session);
            /*
                    Result r = subQuery.queryExpression.getResult(session, 1);    // 1 is already enough

                    return r.getNavigator().isEmpty() ? Boolean.FALSE
                                                      : Boolean.TRUE;
        */
        }

        return subQuery.getTable().isEmpty(session) ? Boolean.FALSE
                                                    : Boolean.TRUE;
    }

    private Boolean testAllAnyCondition(Session session, Object[] o) {

        SubQuery subquery = nodes[RIGHT].subQuery;

        subquery.materialiseCorrelated(session);

        Boolean result = getAllAnyValue(session, o, subquery);

        return result;
    }

    /** @todo - null value in rows */
    private Boolean getAllAnyValue(Session session, Object[] data,
                                   SubQuery subquery) {

        Table           table = subquery.getTable();
        boolean         empty = table.isEmpty(session);
        Index           index = table.getFullIndex();
        RowIterator     it;
        Row             firstrow;
        PersistentStore store   = session.sessionData.getRowStore(table);
        Row             lastrow = index.lastRow(session, store);
        Object[]        lastdata;
        Object[]        firstdata;

        switch (exprSubType) {

            case OpTypes.ANY_QUANTIFIED : {
                if (empty) {
                    return Boolean.FALSE;
                }

                if (countNulls(data) == data.length) {
                    return null;
                }

                lastdata = lastrow.getData();

                if (countNulls(lastdata) == data.length) {
                    return null;
                }

                convertToType(session, data, nodes[LEFT].nodeDataTypes,
                              nodes[RIGHT].nodeDataTypes);

                if (opType == OpTypes.EQUAL) {
                    it = index.findFirstRow(session, store, data);

                    return it.hasNext() ? Boolean.TRUE
                                        : Boolean.FALSE;
                }

                it        = index.findFirstRowNotNull(session, store);
                firstrow  = it.getNextRow();
                firstdata = firstrow.getData();

                Boolean comparefirst = compareValues(session, data, firstdata);
                Boolean comparelast  = compareValues(session, data, lastdata);

                switch (opType) {

                    case OpTypes.NOT_EQUAL :
                        return Boolean.TRUE.equals(comparefirst)
                               || Boolean.TRUE.equals(
                                   comparelast) ? Boolean.TRUE
                                                : Boolean.FALSE;

                    case OpTypes.GREATER :
                        return comparefirst;

                    case OpTypes.GREATER_EQUAL :
                        return comparefirst;

                    case OpTypes.SMALLER :
                        return comparelast;

                    case OpTypes.SMALLER_EQUAL :
                        return comparelast;
                }

                break;
            }
            case OpTypes.ALL_QUANTIFIED : {
                if (empty) {
                    return Boolean.TRUE;
                }

                if (countNulls(data) == data.length) {
                    return null;
                }

                it        = index.firstRow(session, store);
                firstrow  = it.getNextRow();
                firstdata = firstrow.getData();

                if (countNulls(firstdata) == data.length) {
                    return null;
                }

                convertToType(session, data, nodes[LEFT].nodeDataTypes,
                              nodes[RIGHT].nodeDataTypes);

                it = index.findFirstRow(session, store, data);

                if (opType == OpTypes.EQUAL) {
                    if (it.hasNext()) {
                        return subquery.getTable().getRowCount(store) == 1
                               ? Boolean.TRUE
                               : Boolean.FALSE;
                    } else {
                        return Boolean.FALSE;
                    }
                }

                if (opType == OpTypes.NOT_EQUAL) {
                    return it.hasNext() ? Boolean.FALSE
                                        : Boolean.TRUE;
                }

                lastdata = lastrow.getData();

                Boolean comparefirst = compareValues(session, data, firstdata);
                Boolean comparelast  = compareValues(session, data, lastdata);

                switch (opType) {

                    case OpTypes.GREATER :
                        return comparelast;

                    case OpTypes.GREATER_EQUAL :
                        return comparelast;

                    case OpTypes.SMALLER :
                        return comparefirst;

                    case OpTypes.SMALLER_EQUAL :
                        return comparefirst;
                }

                break;
            }
        }

        return null;
    }

    /**
     * Converts an OR containing an AND to an AND
     */
    void distributeOr() {

        if (opType != OpTypes.OR) {
            return;
        }

        if (nodes[LEFT].opType == OpTypes.AND) {
            opType = OpTypes.AND;

            Expression temp = new ExpressionLogical(OpTypes.OR,
                nodes[LEFT].nodes[RIGHT], nodes[RIGHT]);

            nodes[LEFT].opType       = OpTypes.OR;
            nodes[LEFT].nodes[RIGHT] = nodes[RIGHT];
            nodes[RIGHT]             = temp;
        } else if (nodes[RIGHT].opType == OpTypes.AND) {
            Expression temp = nodes[LEFT];

            nodes[LEFT]  = nodes[RIGHT];
            nodes[RIGHT] = temp;

            distributeOr();

            return;
        }

        ((ExpressionLogical) nodes[LEFT]).distributeOr();
        ((ExpressionLogical) nodes[RIGHT]).distributeOr();
    }

    Expression getIndexableExpression(RangeVariable rangeVar) {

        switch (opType) {

            case OpTypes.IS_NULL :
                return nodes[LEFT].opType == OpTypes.COLUMN
                       && nodes[LEFT].getRangeVariable() == rangeVar ? this
                                                                     : null;

            case OpTypes.NOT :
                return nodes[LEFT].opType == OpTypes.IS_NULL
                       && nodes[LEFT].nodes[LEFT].opType == OpTypes.COLUMN
                       && nodes[LEFT].nodes[LEFT].getRangeVariable()
                          == rangeVar ? this
                                      : null;

            case OpTypes.EQUAL :
                if (exprSubType == OpTypes.ANY_QUANTIFIED) {
                    if (nodes[RIGHT].isCorrelated) {
                        return null;
                    }

                    return nodes[LEFT].nodes[0].opType == OpTypes.COLUMN
                           && nodes[LEFT].nodes[0].getRangeVariable()
                              == rangeVar ? this
                                          : null;
                }

            // $FALL-THROUGH$
            case OpTypes.GREATER :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.SMALLER :
            case OpTypes.SMALLER_EQUAL :
                if (exprSubType != 0) {
                    return null;
                }

//                reorderComparison();
                if (nodes[LEFT].opType == OpTypes.COLUMN
                        && nodes[LEFT].getRangeVariable() == rangeVar) {
                    if (nodes[RIGHT].hasReference(rangeVar)) {
                        return null;
                    }

                    return this;
                }

                if (nodes[RIGHT].opType == OpTypes.COLUMN
                        && nodes[RIGHT].getRangeVariable() == rangeVar) {
                    swapCondition();

                    if (nodes[RIGHT].hasReference(rangeVar)) {
                        return null;
                    }

                    return this;
                }

            // $FALL-THROUGH$
            default :
                return null;
        }
    }

    /**
     * Called only on comparison expressions after reordering which have
     * a COLUMN left leaf
     */
    boolean isSimpleBound() {

        if (opType == OpTypes.IS_NULL) {
            return true;
        }

        if (nodes[RIGHT] != null) {
            if (nodes[RIGHT].opType == OpTypes.VALUE) {

                // also true for all parameters
                return true;
            }

            if (nodes[RIGHT].opType == OpTypes.SQL_FUNCTION) {
                if (((FunctionSQL) nodes[RIGHT]).isValueFunction()) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Swap the condition with its complement
     */
    void swapCondition() {

        int i = OpTypes.EQUAL;

        switch (opType) {

            case OpTypes.GREATER_EQUAL :
                i = OpTypes.SMALLER_EQUAL;
                break;

            case OpTypes.SMALLER_EQUAL :
                i = OpTypes.GREATER_EQUAL;
                break;

            case OpTypes.SMALLER :
                i = OpTypes.GREATER;
                break;

            case OpTypes.GREATER :
                i = OpTypes.SMALLER;
                break;

            case OpTypes.NOT_DISTINCT :
                i = OpTypes.NOT_DISTINCT;
                break;

            case OpTypes.EQUAL :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "Expression.swapCondition");
        }

        opType = i;

        Expression e = nodes[LEFT];

        nodes[LEFT]  = nodes[RIGHT];
        nodes[RIGHT] = e;
    }

    boolean reorderComparison(Session session) {

        Expression colExpression    = null;
        Expression nonColExpression = null;
        boolean    left             = false;
        boolean    replaceColumn    = false;
        int        operation        = 0;

        if (nodes[LEFT].opType == OpTypes.ADD) {
            operation = OpTypes.SUBTRACT;
            left      = true;
        } else if (nodes[LEFT].opType == OpTypes.SUBTRACT) {
            operation = OpTypes.ADD;
            left      = true;
        } else if (nodes[RIGHT].opType == OpTypes.ADD) {
            operation = OpTypes.SUBTRACT;
        } else if (nodes[RIGHT].opType == OpTypes.SUBTRACT) {
            operation = OpTypes.ADD;
        }

        if (operation == 0) {
            return false;
        }

        if (left) {
            if (nodes[LEFT].nodes[LEFT].opType == OpTypes.COLUMN) {
                colExpression    = nodes[LEFT].nodes[LEFT];
                nonColExpression = nodes[LEFT].nodes[RIGHT];
            } else if (nodes[LEFT].nodes[RIGHT].opType == OpTypes.COLUMN) {
                replaceColumn    = operation == OpTypes.ADD;
                colExpression    = nodes[LEFT].nodes[RIGHT];
                nonColExpression = nodes[LEFT].nodes[LEFT];
            }
        } else {
            if (nodes[RIGHT].nodes[LEFT].opType == OpTypes.COLUMN) {
                colExpression    = nodes[RIGHT].nodes[LEFT];
                nonColExpression = nodes[RIGHT].nodes[RIGHT];
            } else if (nodes[RIGHT].nodes[RIGHT].opType == OpTypes.COLUMN) {
                replaceColumn    = operation == OpTypes.ADD;
                colExpression    = nodes[RIGHT].nodes[RIGHT];
                nonColExpression = nodes[RIGHT].nodes[LEFT];
            }
        }

        if (colExpression == null) {
            return false;
        }

        Expression           otherExpression = left ? nodes[RIGHT]
                                                    : nodes[LEFT];
        ExpressionArithmetic newArg          = null;

        if (!replaceColumn) {
            newArg = new ExpressionArithmetic(operation, otherExpression,
                                              nonColExpression);

            newArg.resolveTypesForArithmetic(session);
        }

        if (left) {
            if (replaceColumn) {
                nodes[RIGHT]             = colExpression;
                nodes[LEFT].nodes[RIGHT] = otherExpression;

                ((ExpressionArithmetic) nodes[LEFT]).resolveTypesForArithmetic(
                    session);
            } else {
                nodes[LEFT]  = colExpression;
                nodes[RIGHT] = newArg;
            }
        } else {
            if (replaceColumn) {
                nodes[LEFT]               = colExpression;
                nodes[RIGHT].nodes[RIGHT] = otherExpression;

                ((ExpressionArithmetic) nodes[RIGHT])
                    .resolveTypesForArithmetic(session);
            } else {
                nodes[RIGHT] = colExpression;
                nodes[LEFT]  = newArg;
            }
        }

        return true;
    }
}
