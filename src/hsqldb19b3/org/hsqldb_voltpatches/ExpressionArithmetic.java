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

import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of arithmetic and concatenation operations
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
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
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "Expression.getSQL()");
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
                sb.append("VALUE = ").append(valueData);
                sb.append(", TYPE = ").append(dataType.getNameString());

                return sb.toString();

            case OpTypes.ROW :

            //
            case OpTypes.TABLE :
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

    public HsqlList resolveColumnReferences(RangeVariable[] rangeVarArray,
            int rangeCount, HsqlList unresolvedSet, boolean acceptsSequences) {

        if (opType == OpTypes.VALUE) {
            return unresolvedSet;
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            unresolvedSet = nodes[i].resolveColumnReferences(rangeVarArray,
                    rangeCount, unresolvedSet, acceptsSequences);
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
                if (nodes[LEFT].isParam || nodes[LEFT].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                dataType = nodes[LEFT].dataType;

                if (!dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                if (nodes[LEFT].opType == OpTypes.VALUE) {
                    setAsConstantValue(session);
                }
                break;

            case OpTypes.ADD :

                // special case for concat using +
                if ((nodes[LEFT].dataType != null && nodes[LEFT].dataType
                        .isCharacterType()) || (nodes[RIGHT].dataType != null
                                                && nodes[RIGHT].dataType
                                                    .isCharacterType())) {
                    opType = OpTypes.CONCAT;

                    resolveTypesForConcat(session);

                    break;
                }

            // $FALL-THROUGH$
            case OpTypes.SUBTRACT :
            case OpTypes.MULTIPLY :
            case OpTypes.DIVIDE :
                resolveTypesForArithmetic(session);
                break;

            case OpTypes.CONCAT :
                resolveTypesForConcat(session);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "Expression.resolveTypes()");
        }
    }

    void resolveTypesForArithmetic(Session session) {

        if (nodes[LEFT].isParam && nodes[RIGHT].isParam) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (nodes[LEFT].isParam) {
            nodes[LEFT].dataType = nodes[RIGHT].dataType;
        } else if (nodes[RIGHT].isParam) {
            nodes[RIGHT].dataType = nodes[LEFT].dataType;
        }

        if (nodes[LEFT].dataType == null || nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }

        // datetime subtract - type predetermined
        if (nodes[LEFT].dataType.isDateTimeType()
                && nodes[RIGHT].dataType.isDateTimeType()) {
            if (dataType == null) {
                throw Error.error(ErrorCode.X_42566);
            } else if (!dataType.isIntervalType()
                       || nodes[LEFT].dataType.typeCode
                          != nodes[RIGHT].dataType.typeCode) {
                throw Error.error(ErrorCode.X_42562);
            }
        } else {
            dataType =
                nodes[LEFT].dataType.getCombinedType(nodes[RIGHT].dataType,
                    opType);

            if (dataType.isDateTimeType()) {
                if (nodes[LEFT].dataType.isIntervalType()) {
                    if (opType != OpTypes.ADD) {
                        throw Error.error(ErrorCode.X_42565);
                    }

                    Expression temp = nodes[LEFT];

                    nodes[LEFT]  = nodes[RIGHT];
                    nodes[RIGHT] = temp;
                }
            }
        }

        if (nodes[LEFT].opType == OpTypes.VALUE
                && nodes[RIGHT].opType == OpTypes.VALUE) {
            setAsConstantValue(session);
        }
    }

    void resolveTypesForConcat(Session session) {

        if (dataType != null) {
            return;
        }

        if (nodes[LEFT].isParam) {
            nodes[LEFT].dataType = nodes[RIGHT].dataType;
        } else if (nodes[RIGHT].isParam) {
            nodes[RIGHT].dataType = nodes[LEFT].dataType;
        }

        if (nodes[LEFT].dataType == null || nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (nodes[LEFT].dataType.isBinaryType()
                ^ nodes[RIGHT].dataType.isBinaryType()) {
            throw Error.error(ErrorCode.X_42565);
        }

        // conversion of right argument to character for backward compatibility
        if (nodes[LEFT].dataType.isCharacterType()
                && !nodes[RIGHT].dataType.isCharacterType()) {
            Type newType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                nodes[RIGHT].dataType.displaySize());

            nodes[RIGHT] = new ExpressionOp(nodes[RIGHT], newType);
        }

        dataType = nodes[LEFT].dataType.getCombinedType(nodes[RIGHT].dataType,
                OpTypes.CONCAT);

        if (nodes[LEFT].opType == OpTypes.VALUE
                && nodes[RIGHT].opType == OpTypes.VALUE) {
            setAsConstantValue(session);
        }
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
        }

        Object a = nodes[LEFT].getValue(session);
        Object b = nodes[RIGHT].getValue(session);

        switch (opType) {

            case OpTypes.ADD :
                return dataType.add(a, b, nodes[RIGHT].dataType);

            case OpTypes.SUBTRACT :
                return dataType.subtract(a, b, nodes[RIGHT].dataType);

            case OpTypes.MULTIPLY :
                return dataType.multiply(a, b);

            case OpTypes.DIVIDE :
                return dataType.divide(a, b);

            case OpTypes.CONCAT :
                return dataType.concat(session, a, b);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }
}
