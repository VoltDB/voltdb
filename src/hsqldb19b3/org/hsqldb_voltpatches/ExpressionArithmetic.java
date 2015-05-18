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
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * Implementation of arithmetic and concatenation operations
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class ExpressionArithmetic extends Expression {

    ExpressionArithmetic(int type, Expression left, Expression right) {

        super(type);

        nodes        = new Expression[BINARY];
        nodes[LEFT]  = left;
        nodes[RIGHT] = right;

        switch (opType) {

            case OpTypes.ADD :
            case OpTypes.SUBTRACT :
            case OpTypes.MULTIPLY :
            case OpTypes.DIVIDE :
            case OpTypes.CONCAT :
                return;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    ExpressionArithmetic(int type, Expression e) {

        super(type);

        nodes       = new Expression[UNARY];
        nodes[LEFT] = e;

        switch (opType) {

            case OpTypes.NEGATE :
                return;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer(64);

        switch (opType) {

            case OpTypes.VALUE :
                if (valueData == null) {
                    return Tokens.T_NULL;
                }

                if (dataType == null) {
                    throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
                }

                return dataType.convertToSQLString(valueData);
        }

        String left  = getContextSQL(nodes.length > 0 ? nodes[LEFT]
                                                      : null);
        String right = getContextSQL(nodes.length > 1 ? nodes[RIGHT]
                                                      : null);

        switch (opType) {

            case OpTypes.CAST :
                sb.append(' ').append(Tokens.T_CAST).append('(');
                sb.append(left).append(' ').append(Tokens.T_AS).append(' ');
                sb.append(dataType.getTypeDefinition());
                sb.append(')');
                break;

            case OpTypes.NEGATE :
                sb.append('-').append(left);
                break;

            case OpTypes.ADD :
                sb.append(left).append('+').append(right);
                break;

            case OpTypes.SUBTRACT :
                sb.append(left).append('-').append(right);
                break;

            case OpTypes.MULTIPLY :
                sb.append(left).append('*').append(right);
                break;

            case OpTypes.DIVIDE :
                sb.append(left).append('/').append(right);
                break;

            case OpTypes.CONCAT :
                sb.append(left).append("||").append(right);
                break;

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
                sb.append("VALUE = ").append(
                    dataType.convertToSQLString(valueData));
                sb.append(", TYPE = ").append(dataType.getNameString());

                return sb.toString();

            case OpTypes.ROW :

            //
            case OpTypes.VALUELIST :
                sb.append("VALUELIST ");
                sb.append(" TYPE = ").append(dataType.getNameString());

                for (int i = 0; i < nodes.length; i++) {
                    sb.append(nodes[i].describe(session, blanks + blanks));
                    sb.append(' ');
                }
                break;

            case OpTypes.NEGATE :
                sb.append("NEGATE ");
                break;

            case OpTypes.ADD :
                sb.append("ADD ");
                break;

            case OpTypes.SUBTRACT :
                sb.append("SUBTRACT ");
                break;

            case OpTypes.MULTIPLY :
                sb.append("MULTIPLY ");
                break;

            case OpTypes.DIVIDE :
                sb.append("DIVIDE ");
                break;

            case OpTypes.CONCAT :
                sb.append("CONCAT ");
                break;

            case OpTypes.CAST :
                sb.append("CAST ");
                sb.append(dataType.getTypeDefinition());
                sb.append(' ');
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

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        switch (opType) {

            case OpTypes.VALUE :
                break;

            case OpTypes.NEGATE :
                if (nodes[LEFT].isUnresolvedParam()
                        || nodes[LEFT].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                // A VoltDB extension to use X'..' as a numeric literal
                voltConvertBinaryLiteralOperandsToBigint();
                // End VoltDB extension
                dataType = nodes[LEFT].dataType;

                if (!dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (nodes[LEFT].opType == OpTypes.VALUE) {
                    setAsConstantValue(session, parent);
                }
                break;

            case OpTypes.ADD :

                // special case for concat using +
                if ((nodes[LEFT].dataType != null && nodes[LEFT].dataType
                        .isCharacterType()) || (nodes[RIGHT].dataType != null
                                                && nodes[RIGHT].dataType
                                                    .isCharacterType())) {
                    opType = OpTypes.CONCAT;

                    resolveTypesForConcat(session, parent);

                    break;
                }

            // fall through
            case OpTypes.SUBTRACT :
            case OpTypes.MULTIPLY :
            case OpTypes.DIVIDE :
                resolveTypesForArithmetic(session, parent);
                break;

            case OpTypes.CONCAT :
                resolveTypesForConcat(session, parent);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    void resolveTypesForArithmetic(Session session, Expression parent) {

        if (nodes[LEFT].isUnresolvedParam()
                && nodes[RIGHT].isUnresolvedParam()) {
            nodes[LEFT].dataType  = Type.SQL_INTEGER;
            nodes[RIGHT].dataType = Type.SQL_INTEGER;
        }

        if (nodes[LEFT].dataType == null && nodes[RIGHT].dataType == null) {
            nodes[LEFT].dataType  = Type.SQL_INTEGER;
            nodes[RIGHT].dataType = Type.SQL_INTEGER;
        }

        // A VoltDB extension to use X'..' as a numeric literal
        voltConvertBinaryLiteralOperandsToBigint();
        // End VoltDB extension
        if (nodes[LEFT].isUnresolvedParam()) {
            if (nodes[RIGHT].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }

            if (nodes[RIGHT].dataType.isIntervalType()) {
                if (parent != null) {
                    switch (parent.opType) {

                        case OpTypes.EQUAL :
                        case OpTypes.GREATER_EQUAL :
                        case OpTypes.SMALLER_EQUAL :
                        case OpTypes.SMALLER :
                        case OpTypes.GREATER :
                            for (int i = 0; i < parent.nodes.length; i++) {
                                if (parent.nodes[i] != this) {
                                    if (parent.nodes[i].dataType != null
                                            && parent.nodes[i].dataType
                                                .isDateTimeType()) {
                                        nodes[LEFT].dataType =
                                            parent.nodes[i].dataType;
                                    }

                                    break;
                                }
                            }
                            break;

                        default :
                    }
                }
            }

            if (nodes[LEFT].dataType == null) {
                switch (opType) {

                    case OpTypes.SUBTRACT :
                        if (nodes[RIGHT].dataType.isIntervalType()) {
                            nodes[LEFT].dataType =
                                Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                        }
                        break;

                    case OpTypes.ADD :
                        if (nodes[RIGHT].dataType.isDateTimeType()) {
                            if (nodes[RIGHT].dataType.typeComparisonGroup
                                    == Types.SQL_DATE) {
                                nodes[LEFT].dataType =
                                    Type
                                    .SQL_INTERVAL_YEAR_TO_MONTH_MAX_PRECISION;
                            } else {
                                nodes[LEFT].dataType =
                                    Type
                                    .SQL_INTERVAL_DAY_TO_SECOND_MAX_PRECISION;
                            }
                        } else if (nodes[RIGHT].dataType.isIntervalType()) {
                            nodes[LEFT].dataType =
                                Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                        }
                        break;
                }
            }

            if (nodes[LEFT].dataType == null) {
                nodes[LEFT].dataType = nodes[RIGHT].dataType;
            }
        } else if (nodes[RIGHT].isUnresolvedParam()) {
            if (nodes[LEFT].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }

            switch (opType) {

                case OpTypes.MULTIPLY :
                case OpTypes.DIVIDE :
                    if (nodes[LEFT].dataType.isIntervalType()) {
                        nodes[RIGHT].dataType = Type.SQL_DECIMAL;
                    } else {
                        nodes[RIGHT].dataType = nodes[LEFT].dataType;
                    }
                    break;

                case OpTypes.SUBTRACT :
                case OpTypes.ADD :
                    if (nodes[LEFT].dataType.isDateTimeType()) {

                        // datetime subtract - type predetermined
                        if (dataType != null && dataType.isIntervalType()) {
                            nodes[RIGHT].dataType = nodes[LEFT].dataType;
                        } else if (nodes[LEFT].dataType.typeComparisonGroup
                                   == Types.SQL_DATE) {
                            nodes[RIGHT].dataType =
                                Type.SQL_INTERVAL_YEAR_TO_MONTH_MAX_PRECISION;
                        } else {
                            nodes[RIGHT].dataType =
                                Type.SQL_INTERVAL_DAY_TO_SECOND_MAX_PRECISION;
                        }
                    } else {
                        nodes[RIGHT].dataType = nodes[LEFT].dataType;
                    }
                    break;
            }
        }

        if (nodes[LEFT].dataType == null || nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }

        // datetime subtract - type predetermined
        if (dataType != null && dataType.isIntervalType()) {
            if (nodes[LEFT].dataType.isDateTimeType()
                    && nodes[RIGHT].dataType.isDateTimeType()) {
                if (nodes[LEFT].dataType.typeComparisonGroup
                        != nodes[RIGHT].dataType.typeComparisonGroup) {
                    throw Error.error(ErrorCode.X_42562);
                }
            } else {
                Type type = nodes[LEFT].dataType.getCombinedType(session,
                    nodes[RIGHT].dataType, opType);

                if (type == null) {
                    throw Error.error(ErrorCode.X_42562);
                }

                if (type.isIntervalType()) {
                    if (type.typeCode != dataType.typeCode) {
                        throw Error.error(ErrorCode.X_42562);
                    }
                } else if (type.isNumberType()) {
                    nodes[LEFT]  = new ExpressionOp(nodes[LEFT], dataType);
                    nodes[RIGHT] = new ExpressionOp(nodes[RIGHT], dataType);

                    nodes[LEFT].resolveTypes(session, this);
                    nodes[RIGHT].resolveTypes(session, this);
                } else {
                    throw Error.error(ErrorCode.X_42562);
                }
            }
        } else {
            dataType = nodes[LEFT].dataType.getCombinedType(session,
                    nodes[RIGHT].dataType, opType);

            if (dataType.isDateTimeType()) {
                if (nodes[LEFT].dataType.isIntervalType()) {
                    if (opType != OpTypes.ADD) {
                        throw Error.error(ErrorCode.X_42563);
                    }

                    Expression temp = nodes[LEFT];

                    nodes[LEFT]  = nodes[RIGHT];
                    nodes[RIGHT] = temp;
                } else if (nodes[RIGHT].dataType.isNumberType()) {
                    if (!session.database.sqlSyntaxOra) {
                        throw Error.error(ErrorCode.X_42562);
                    }
                }
            }
        }

        if (nodes[LEFT].opType == OpTypes.VALUE
                && nodes[RIGHT].opType == OpTypes.VALUE) {
            setAsConstantValue(session, parent);
        }
    }

    void resolveTypesForConcat(Session session, Expression parent) {

        if (dataType != null) {
            return;
        }

        if (nodes[LEFT].isUnresolvedParam()) {
            nodes[LEFT].dataType = getParameterType(nodes[RIGHT].dataType);
        } else if (nodes[RIGHT].isUnresolvedParam()) {
            nodes[RIGHT].dataType = getParameterType(nodes[LEFT].dataType);
        }

        if (nodes[LEFT].dataType == null) {
            nodes[LEFT].dataType = Type.SQL_VARCHAR_DEFAULT;
        }

        if (nodes[RIGHT].dataType == null) {
            nodes[RIGHT].dataType = Type.SQL_VARCHAR_DEFAULT;
        }

        if (nodes[LEFT].dataType.isBinaryType()
                ^ nodes[RIGHT].dataType.isBinaryType()) {
            throw Error.error(ErrorCode.X_42563);
        }

        if (nodes[LEFT].dataType.isArrayType()) {
            Expression e = nodes[RIGHT];

            if (e.opType == OpTypes.ARRAY_ACCESS) {
                if (parent == null) {
                    throw Error.error(ErrorCode.X_42563);
                }

                nodes[RIGHT]  = e.getLeftNode();
                e.nodes[LEFT] = this;

                parent.replaceNode(this, e);
            }
        }

        if (nodes[LEFT].dataType.isArrayType()
                ^ nodes[RIGHT].dataType.isArrayType()) {
            throw Error.error(ErrorCode.X_42563);
        }

        // conversion of right argument to character for backward compatibility
        if (nodes[LEFT].dataType.isCharacterType()
                && !nodes[RIGHT].dataType.isCharacterType()) {
            if (session.database.sqlEnforceTypes) {
                throw Error.error(ErrorCode.X_42562);
            }

            Type newType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                nodes[RIGHT].dataType.displaySize(),
                nodes[LEFT].dataType.getCollation());

            nodes[RIGHT] = ExpressionOp.getCastExpression(session,
                    nodes[RIGHT], newType);
        }

        if (nodes[RIGHT].dataType.isCharacterType()
                && !nodes[LEFT].dataType.isCharacterType()) {
            if (session.database.sqlEnforceTypes) {
                throw Error.error(ErrorCode.X_42562);
            }

            Type newType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                nodes[LEFT].dataType.displaySize(),
                nodes[RIGHT].dataType.getCollation());

            nodes[LEFT] = ExpressionOp.getCastExpression(session, nodes[LEFT],
                    newType);
        }

        dataType = nodes[LEFT].dataType.getCombinedType(session,
                nodes[RIGHT].dataType, OpTypes.CONCAT);

        if (nodes[LEFT].opType == OpTypes.VALUE
                && nodes[RIGHT].opType == OpTypes.VALUE) {
            setAsConstantValue(session, parent);
        }
    }

    private Type getParameterType(Type type) {

        if (type == null) {
            return null;
        }

        switch (type.typeCode) {

            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
                return Type.SQL_VARCHAR_DEFAULT;

            case Types.SQL_CLOB :
                return Type.SQL_CLOB;

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                return Type.SQL_VARBINARY_DEFAULT;

            case Types.SQL_BLOB :
                return Type.SQL_BLOB;

            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                return Type.SQL_BIT_VARYING_MAX_LENGTH;

            case Types.SQL_ARRAY :
                return type;

            default :
                return null;
        }
    }

    public Object getValue(Session session) {

        switch (opType) {

            case OpTypes.VALUE :
                return valueData;

            case OpTypes.SIMPLE_COLUMN : {
                Object value =
                    session.sessionContext.rangeIterators[rangePosition]
                        .getCurrent(columnIndex);

                return value;
            }
            case OpTypes.NEGATE :
                return ((NumberType) dataType).negate(
                    nodes[LEFT].getValue(session, nodes[LEFT].dataType));
        }

        Object a = nodes[LEFT].getValue(session);
        Object b = nodes[RIGHT].getValue(session);

        switch (opType) {

            case OpTypes.ADD :
                return dataType.add(session, a, b, nodes[RIGHT].dataType);

            case OpTypes.SUBTRACT :
                return dataType.subtract(session, a, b, nodes[RIGHT].dataType);

            case OpTypes.MULTIPLY :
                return dataType.multiply(a, b);

            case OpTypes.DIVIDE :
                return dataType.divide(session, a, b);

            case OpTypes.CONCAT :
                if (!session.database.sqlConcatNulls
                        && nodes[LEFT].dataType.isCharacterType()) {
                    if (a == null && b != null) {
                        a = "";
                    } else if (a != null && b == null) {
                        b = "";
                    }
                }

                return dataType.concat(session, a, b);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }
    // A VoltDB extension to use X'..' as a numeric value
    private void voltConvertBinaryLiteralOperandsToBigint() {
        // Strange that CONCAT is an arithmetic operator.
        // You could imagine using it for VARBINARY, so
        // definitely don't convert its operands to BIGINT!
        assert(opType != OpTypes.CONCAT);

        for (int i = 0; i < nodes.length; ++i) {
            Expression e = nodes[i];
            ExpressionValue.voltMutateToBigintType(e, this, i);
        }
    }
    // End VoltDB extension
}
