/* Copyright (c) 2001-2011, The HSQL Development Group
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

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.BinaryType;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.DateTimeType;
import org.hsqldb_voltpatches.types.IntervalType;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * Implementation of CAST, CASE, LIMIT and ZONE operations.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class ExpressionOp extends Expression {

    static final ExpressionOp limitOneExpression = new ExpressionOp(
        OpTypes.LIMIT,
        new ExpressionValue(ValuePool.INTEGER_0, Type.SQL_INTEGER),
        new ExpressionValue(ValuePool.INTEGER_1, Type.SQL_INTEGER));

    /**
     * Creates a multiple arg operation expression
     */
    ExpressionOp(int type, Expression[] exprArray) {

        super(type);

        switch (opType) {

            case OpTypes.CONCAT_WS :
                nodes = exprArray;

                return;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionOp");
        }
    }

    /**
     * Creates a special binary operation expression
     */
    ExpressionOp(int type, Expression left, Expression right) {

        super(type);

        nodes        = new Expression[BINARY];
        nodes[LEFT]  = left;
        nodes[RIGHT] = right;

        switch (opType) {

            case OpTypes.LIKE_ARG :
            case OpTypes.ALTERNATIVE :
            case OpTypes.CASEWHEN :
            case OpTypes.LIMIT :
            case OpTypes.ZONE_MODIFIER :
                return;

            case OpTypes.PREFIX :
                dataType = left.dataType;

                return;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionOp");
        }
    }

    /**
     * creates a CAST expression
     */
    ExpressionOp(Expression e, Type dataType) {

        super(OpTypes.CAST);

        nodes         = new Expression[UNARY];
        nodes[LEFT]   = e;
        this.dataType = dataType;
        this.alias    = e.alias;
    }

    /**
     * creates a special conversion for time / timestamp comparison
     */
    ExpressionOp(Expression e) {

        super(e.dataType.isDateTimeTypeWithZone() ? OpTypes.CAST
                                                  : OpTypes.ZONE_MODIFIER);

        switch (e.dataType.typeCode) {

            case Types.SQL_TIME_WITH_TIME_ZONE :
                nodes                = new Expression[UNARY];
                nodes[LEFT] = new ExpressionOp(OpTypes.ZONE_MODIFIER, e, null);
                nodes[LEFT].dataType = e.dataType;
                dataType = DateTimeType.getDateTimeType(Types.SQL_TIME,
                        e.dataType.scale);
                break;

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                nodes                = new Expression[UNARY];
                nodes[LEFT] = new ExpressionOp(OpTypes.ZONE_MODIFIER, e, null);
                nodes[LEFT].dataType = e.dataType;
                dataType = DateTimeType.getDateTimeType(Types.SQL_TIMESTAMP,
                        e.dataType.scale);
                break;

            case Types.SQL_TIME :
                nodes                = new Expression[BINARY];
                nodes[LEFT]          = e;
                nodes[LEFT].dataType = e.dataType;
                dataType =
                    DateTimeType.getDateTimeType(Types.SQL_TIME_WITH_TIME_ZONE,
                                                 e.dataType.scale);
                break;

            case Types.SQL_TIMESTAMP :
                nodes                = new Expression[BINARY];
                nodes[LEFT]          = e;
                nodes[LEFT].dataType = e.dataType;
                dataType = DateTimeType.getDateTimeType(
                    Types.SQL_TIMESTAMP_WITH_TIME_ZONE, e.dataType.scale);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionOp");
        }

        this.alias = e.alias;
    }

    public static Expression getCastExpression(Session session, Expression e,
            Type dataType) {

        if (e.getType() == OpTypes.VALUE) {
            Object value = dataType.castToType(session, e.getValue(session),
                                               e.getDataType());

            return new ExpressionValue(value, dataType);
        }

        return new ExpressionOp(e, dataType);
    }

    public String getSQL() {

        StringBuffer sb    = new StringBuffer(64);
        String       left  = getContextSQL(nodes.length > 0 ? nodes[LEFT]
                                                            : null);
        String       right = getContextSQL(nodes.length > 1 ? nodes[RIGHT]
                                                            : null);

        switch (opType) {

            case OpTypes.VALUE :
                if (valueData == null) {
                    return Tokens.T_NULL;
                }

                if (dataType == null) {
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "ExpressionOp");
                }

                return dataType.convertToSQLString(valueData);

            case OpTypes.LIKE_ARG :
                sb.append(' ').append(Tokens.T_LIKE).append(' ');
                sb.append(left).append(' ').append(right).append(' ');
            case OpTypes.CAST :
                sb.append(' ').append(Tokens.T_CAST).append('(');
                sb.append(left).append(' ').append(Tokens.T_AS).append(' ');
                sb.append(dataType.getTypeDefinition());
                sb.append(')');

                return sb.toString();

            case OpTypes.CASEWHEN :
                sb.append(' ').append(Tokens.T_CASEWHEN).append('(');
                sb.append(left).append(',').append(right).append(')');

                return sb.toString();

            case OpTypes.ALTERNATIVE :
                sb.append(left).append(',').append(right);

                return sb.toString();

            case OpTypes.LIMIT :
                if (left != null) {
                    sb.append(' ').append(Tokens.T_OFFSET).append(' ');
                    sb.append(left).append(' ');
                }

                if (right != null) {
                    sb.append(' ').append(Tokens.T_FETCH).append(' ');
                    sb.append(Tokens.T_FIRST);
                    sb.append(right).append(' ').append(right).append(' ');
                    sb.append(Tokens.T_ROWS).append(' ').append(Tokens.T_ONLY);
                    sb.append(' ');
                }
                break;

            case OpTypes.ZONE_MODIFIER :
                sb.append(left).append(' ').append(Tokens.T_AT).append(' ');

                if (nodes[RIGHT] == null) {
                    sb.append(Tokens.T_LOCAL).append(' ');

                    break;
                }

                sb.append(Tokens.T_TIME).append(' ').append(Tokens.T_ZONE);
                sb.append(' ');
                sb.append(right);
                break;

            case OpTypes.CONCAT_WS :
                sb.append(Tokens.T_CONCAT_WS).append(Tokens.OPENBRACKET);
                sb.append(left);

                for (int i = 0; i < nodes.length; i++) {
                    sb.append(',').append(nodes[i].getSQL());
                }

                sb.append(Tokens.CLOSEBRACKET);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionOp");
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
                sb.append("VALUE = ").append(
                    dataType.convertToSQLString(valueData));
                sb.append(", TYPE = ").append(dataType.getNameString());

                return sb.toString();

            case OpTypes.LIKE_ARG :
                sb.append(Tokens.T_LIKE).append(' ').append("ARG ");
                sb.append(dataType.getTypeDefinition());
                sb.append(' ');
                break;

            case OpTypes.VALUELIST :
                sb.append(Tokens.T_VALUE).append(' ').append("LIST ");

                for (int i = 0; i < nodes.length; i++) {
                    sb.append(nodes[i].describe(session, blanks + 1));
                    sb.append(' ');
                }

                return sb.toString();

            case OpTypes.CAST :
                sb.append(Tokens.T_CAST).append(' ');
                sb.append(dataType.getTypeDefinition());
                sb.append(' ');
                break;

            case OpTypes.CASEWHEN :
                sb.append(Tokens.T_CASEWHEN).append(' ');
                break;

            case OpTypes.CONCAT_WS :
                sb.append(Tokens.T_CONCAT_WS).append(' ');
                break;
        }

        if (getLeftNode() != null) {
            sb.append(" arg_left=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }

        if (getRightNode() != null) {
            sb.append(" arg_right=[");
            sb.append(nodes[RIGHT].describe(session, blanks + 1));
            sb.append(']');
        }

        return sb.toString();
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeGroup rangeGroup, int rangeCount, RangeGroup[] rangeGroups,
            HsqlList unresolvedSet, boolean acceptsSequences) {

        if (opType == OpTypes.VALUE) {
            return unresolvedSet;
        }

        switch (opType) {

            case OpTypes.CASEWHEN :
                acceptsSequences = false;
                break;
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            unresolvedSet = nodes[i].resolveColumnReferences(session,
                    rangeGroup, rangeCount, rangeGroups, unresolvedSet,
                    acceptsSequences);
        }

        return unresolvedSet;
    }

    public void resolveTypes(Session session, Expression parent) {

        switch (opType) {

            case OpTypes.CASEWHEN :
                break;

            default :
                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i] != null) {
                        nodes[i].resolveTypes(session, this);
                    }
                }
        }

        switch (opType) {

            case OpTypes.VALUE :
                break;

            case OpTypes.LIKE_ARG : {
                dataType = nodes[LEFT].dataType;

                if (nodes[LEFT].opType == OpTypes.VALUE
                        && (nodes[RIGHT] == null
                            || nodes[RIGHT].opType == OpTypes.VALUE)) {
                    setAsConstantValue(session, parent);

                    break;
                }

                break;
            }
            case OpTypes.CAST : {
                Expression node     = nodes[LEFT];
                Type       nodeType = node.dataType;

                if (nodeType != null && !dataType.canConvertFrom(nodeType)) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (node.opType == OpTypes.VALUE) {
                    setAsConstantValue(session, parent);

                    node.dataType  = dataType;
                    node.valueData = valueData;

                    if (parent != null) {
                        parent.replaceNode(this, node);
                    }
                } else if (nodes[LEFT].opType == OpTypes.DYNAMIC_PARAM) {
                    node.dataType = dataType;
                }

                break;
            }
            case OpTypes.ZONE_MODIFIER :
                if (nodes[LEFT].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (nodes[RIGHT] != null) {
                    if (nodes[RIGHT].dataType == null) {
                        nodes[RIGHT].dataType =
                            Type.SQL_INTERVAL_HOUR_TO_MINUTE;
                    }

                    if (nodes[RIGHT].dataType.typeCode
                            != Types.SQL_INTERVAL_HOUR_TO_MINUTE) {
                        if (nodes[RIGHT].opType == OpTypes.VALUE) {
                            nodes[RIGHT].valueData =
                                Type.SQL_INTERVAL_HOUR_TO_MINUTE.castToType(
                                    session, nodes[RIGHT].valueData,
                                    nodes[RIGHT].dataType);
                            nodes[RIGHT].dataType =
                                Type.SQL_INTERVAL_HOUR_TO_MINUTE;
                        } else {
                            throw Error.error(ErrorCode.X_42563);
                        }
                    }
                }

                switch (nodes[LEFT].dataType.typeCode) {

                    case Types.SQL_TIME :
                        dataType = DateTimeType.getDateTimeType(
                            Types.SQL_TIME_WITH_TIME_ZONE,
                            nodes[LEFT].dataType.scale);
                        break;

                    case Types.SQL_TIMESTAMP :
                        dataType = DateTimeType.getDateTimeType(
                            Types.SQL_TIMESTAMP_WITH_TIME_ZONE,
                            nodes[LEFT].dataType.scale);
                        break;

                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                    case Types.SQL_TIME_WITH_TIME_ZONE :
                        dataType = nodes[LEFT].dataType;
                        break;

                    default :
                        throw Error.error(ErrorCode.X_42563);
                }

                // no constant optimisation as value dependent on session zone
                break;

            case OpTypes.CASEWHEN :

                // We use CASEWHEN as parent type.
                // In the parent, left node is the condition, and right node is
                // the leaf, tagged as type ALTERNATIVE; its left node is
                // case 1 (how to get the value when the condition in
                // the parent evaluates to true), while its right node is case 2
                // (how to get the value when the condition in
                // the parent evaluates to false).
                resolveTypesForCaseWhen(session);
                break;

            case OpTypes.CONCAT_WS :
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                dataType = Type.SQL_VARCHAR_DEFAULT;
                break;

            case OpTypes.ALTERNATIVE :
                break;

            case OpTypes.LIMIT :
                if (nodes[LEFT] != null) {
                    if (nodes[LEFT].dataType == null) {
                        throw Error.error(ErrorCode.X_42567);
                    }

                    if (!nodes[LEFT].dataType.isIntegralType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }

                if (nodes[RIGHT] != null) {
                    if (nodes[RIGHT].dataType == null) {
                        throw Error.error(ErrorCode.X_42567);
                    }

                    if (!nodes[RIGHT].dataType.isIntegralType()) {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }
                break;

            case OpTypes.PREFIX :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionOp");
        }
    }

    /**
     * For CASE WHEN and its special cases section 9.3 of the SQL standard
     * on type aggregation is implemented.
     */
    void resolveTypesForCaseWhen(Session session) {

        if (dataType != null) {
            return;
        }

        Expression expr = this;

        while (expr.opType == OpTypes.CASEWHEN) {
            expr.nodes[LEFT].resolveTypes(session, expr);

            if (expr.nodes[LEFT].isUnresolvedParam()) {
                expr.nodes[LEFT].dataType = Type.SQL_BOOLEAN;
            }

            expr.nodes[RIGHT].nodes[LEFT].resolveTypes(session,
                    expr.nodes[RIGHT]);

            if (expr.nodes[RIGHT].nodes[RIGHT].opType != OpTypes.CASEWHEN) {
                expr.nodes[RIGHT].nodes[RIGHT].resolveTypes(session,
                        expr.nodes[RIGHT]);
            }

            expr = expr.nodes[RIGHT].nodes[RIGHT];
        }

        if (exprSubType == OpTypes.CAST) {
            if (nodes[RIGHT].nodes[RIGHT].dataType != null
                    && nodes[RIGHT].nodes[RIGHT].dataType
                       != nodes[RIGHT].nodes[LEFT].dataType) {
                Type castType = nodes[RIGHT].nodes[RIGHT].dataType;

                if (castType.isCharacterType()) {
                    castType = Type.SQL_VARCHAR_DEFAULT;
                }

                nodes[RIGHT].nodes[LEFT] =
                    new ExpressionOp(nodes[RIGHT].nodes[LEFT], castType);
            }
        }

        expr = this;

        while (expr.opType == OpTypes.CASEWHEN) {
            dataType =
                Type.getAggregateType(expr.nodes[RIGHT].nodes[LEFT].dataType,
                                      dataType);
            dataType =
                Type.getAggregateType(expr.nodes[RIGHT].nodes[RIGHT].dataType,
                                      dataType);
            expr = expr.nodes[RIGHT].nodes[RIGHT];
        }

        expr = this;

        while (expr.opType == OpTypes.CASEWHEN) {
            if (expr.nodes[RIGHT].nodes[LEFT].dataType == null) {
                expr.nodes[RIGHT].nodes[LEFT].dataType = dataType;
            }

            if (expr.nodes[RIGHT].nodes[RIGHT].dataType == null) {
                expr.nodes[RIGHT].nodes[RIGHT].dataType = dataType;
            }

            if (expr.nodes[RIGHT].dataType == null) {
                expr.nodes[RIGHT].dataType = dataType;
            }

            expr = expr.nodes[RIGHT].nodes[RIGHT];
        }

        if (dataType == null || dataType.typeCode == Types.SQL_ALL_TYPES) {
            throw Error.error(ErrorCode.X_42567);
        }
    }

    public Object getValue(Session session) {

        switch (opType) {

            case OpTypes.VALUE :
                return valueData;

            case OpTypes.LIKE_ARG : {
                boolean hasEscape  = nodes[RIGHT] != null;
                int     escapeChar = Integer.MAX_VALUE;

                if (dataType.isBinaryType()) {
                    BinaryData left =
                        (BinaryData) nodes[LEFT].getValue(session);

                    if (left == null) {
                        return null;
                    }

                    if (hasEscape) {
                        BinaryData right =
                            (BinaryData) nodes[RIGHT].getValue(session);

                        if (right == null) {
                            return null;
                        }

                        if (right.length(session) != 1) {
                            throw Error.error(ErrorCode.X_2200D);
                        }

                        escapeChar = right.getBytes()[0];
                    }

                    byte[]  array       = left.getBytes();
                    byte[]  newArray    = new byte[array.length];
                    boolean wasEscape   = false;
                    int     escapeCount = 0;
                    int     i           = 0;
                    int     j           = 0;

                    for (; i < array.length; i++) {
                        if (array[i] == escapeChar) {
                            if (wasEscape) {
                                escapeCount++;

                                newArray[j++] = array[i];
                                wasEscape     = false;

                                continue;
                            }

                            wasEscape = true;

                            if (i == array.length - 1) {
                                throw Error.error(ErrorCode.X_22025);
                            }

                            continue;
                        }

                        if (array[i] == '_' || array[i] == '%') {
                            if (wasEscape) {
                                escapeCount++;

                                newArray[j++] = array[i];
                                wasEscape     = false;

                                continue;
                            }

                            break;
                        }

                        if (wasEscape) {
                            throw Error.error(ErrorCode.X_22025);
                        }

                        newArray[j++] = array[i];
                    }

                    newArray =
                        (byte[]) ArrayUtil.resizeArrayIfDifferent(newArray, j);

                    return new BinaryData(newArray, false);
                } else {
                    String left =
                        (String) Type.SQL_VARCHAR.convertToType(session,
                            nodes[LEFT].getValue(session),
                            nodes[LEFT].getDataType());

                    if (left == null) {
                        return null;
                    }

                    if (hasEscape) {
                        String right =
                            (String) Type.SQL_VARCHAR.convertToType(session,
                                nodes[RIGHT].getValue(session),
                                nodes[RIGHT].getDataType());

                        if (right == null) {
                            return null;
                        }

                        if (right.length() != 1) {
                            throw Error.error(ErrorCode.X_22019);
                        }

                        escapeChar = right.getBytes()[0];
                    }

                    char[]  array       = left.toCharArray();
                    char[]  newArray    = new char[array.length];
                    boolean wasEscape   = false;
                    int     escapeCount = 0;
                    int     i           = 0;
                    int     j           = 0;

                    for (; i < array.length; i++) {
                        if (array[i] == escapeChar) {
                            if (wasEscape) {
                                escapeCount++;

                                newArray[j++] = array[i];
                                wasEscape     = false;

                                continue;
                            }

                            wasEscape = true;

                            if (i == array.length - 1) {
                                throw Error.error(ErrorCode.X_22025);
                            }

                            continue;
                        }

                        if (array[i] == '_' || array[i] == '%') {
                            if (wasEscape) {
                                escapeCount++;

                                newArray[j++] = array[i];
                                wasEscape     = false;

                                continue;
                            }

                            break;
                        }

                        if (wasEscape) {
                            throw Error.error(ErrorCode.X_22025);
                        }

                        newArray[j++] = array[i];
                    }

                    return new String(newArray, 0, j);
                }
            }
            case OpTypes.SIMPLE_COLUMN : {
                Object value =
                    session.sessionContext.rangeIterators[rangePosition]
                        .getCurrent(columnIndex);

                return value;
            }
            case OpTypes.ORDER_BY :
                return nodes[LEFT].getValue(session);

            case OpTypes.PREFIX : {
                if (nodes[LEFT].dataType.isCharacterType()) {
                    Object value = nodes[RIGHT].getValue(session);

                    if (value == null) {
                        return null;
                    }

                    CharacterType type = (CharacterType) nodes[RIGHT].dataType;
                    long length =
                        ((CharacterType) nodes[RIGHT].dataType).size(session,
                            value);

                    type  = (CharacterType) nodes[LEFT].dataType;
                    value = nodes[LEFT].getValue(session);

                    if (value == null) {
                        return null;
                    }

                    return type.substring(session, value, 0, length, true,
                                          false);
                } else {
                    BinaryData value =
                        (BinaryData) nodes[RIGHT].getValue(session);

                    if (value == null) {
                        return null;
                    }

                    long       length = value.length(session);
                    BinaryType type   = (BinaryType) nodes[LEFT].dataType;

                    value = (BinaryData) nodes[LEFT].getValue(session);

                    if (value == null) {
                        return null;
                    }

                    return type.substring(session, value, 0, length, true);
                }
            }
            case OpTypes.CAST : {
                Object value =
                    dataType.castToType(session,
                                        nodes[LEFT].getValue(session),
                                        nodes[LEFT].dataType);

                if (dataType.userTypeModifier != null) {
                    Constraint[] constraints =
                        dataType.userTypeModifier.getConstraints();

                    for (int i = 0; i < constraints.length; i++) {
                        constraints[i].checkCheckConstraint(session, null,
                                                            null, value);
                    }
                }

                return value;
            }
            case OpTypes.CASEWHEN : {
                Boolean result = (Boolean) nodes[LEFT].getValue(session);

                if (Boolean.TRUE.equals(result)) {
                    return nodes[RIGHT].nodes[LEFT].getValue(session,
                            dataType);
                } else {
                    return nodes[RIGHT].nodes[RIGHT].getValue(session,
                            dataType);
                }
            }
            case OpTypes.CONCAT_WS : {
                String sep = (String) nodes[LEFT].getValue(session);

                if (sep == null) {
                    return null;
                }

                StringBuffer sb       = new StringBuffer("");
                boolean      hasValue = false;

                for (int i = 1; i < nodes.length; i++) {
                    String value = (String) nodes[i].getValue(session);

                    if (value == null) {
                        continue;
                    }

                    if (hasValue) {
                        sb.append(sep);
                    }

                    sb.append(value);

                    hasValue = true;
                }

                return sb.toString();
            }
            case OpTypes.ZONE_MODIFIER : {
                Object leftValue  = nodes[LEFT].getValue(session);
                Object rightValue = nodes[RIGHT] == null ? null
                                                         : nodes[RIGHT]
                                                             .getValue(
                                                                 session);

                if (leftValue == null) {
                    return null;
                }

                if (nodes[RIGHT] != null && rightValue == null) {
                    return null;
                }

                long zoneSeconds = nodes[RIGHT] == null
                                   ? session.getZoneSeconds()
                                   : ((IntervalType) nodes[RIGHT].dataType)
                                       .getSeconds(rightValue);

                return ((DateTimeType) dataType).changeZone(leftValue,
                        nodes[LEFT].dataType, (int) zoneSeconds,
                        session.getZoneSeconds());
            }
            case OpTypes.LIMIT :

            // fall through
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionOp");
        }
    }
}
