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
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * Implementation of LIKE operations
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @since 1.9.0
 */
public final class ExpressionLike extends ExpressionLogical {

    private static final int ESCAPE = 2;
    private Like             likeObject;

    /**
     * Creates a LIKE expression
     */
    ExpressionLike(Expression left, Expression right, Expression escape,
                   boolean noOptimisation) {

        super(OpTypes.LIKE);

        nodes               = new Expression[TERNARY];
        nodes[LEFT]         = left;
        nodes[RIGHT]        = right;
        nodes[ESCAPE]       = escape;
        likeObject          = new Like();
        this.noOptimisation = noOptimisation;
    }

    private ExpressionLike(ExpressionLike other) {

        super(OpTypes.LIKE);

        this.nodes      = other.nodes;
        this.likeObject = other.likeObject;
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeGroup rangeGroup, int rangeCount, RangeGroup[] rangeGroups,
            HsqlList unresolvedSet, boolean acceptsSequences) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                unresolvedSet = nodes[i].resolveColumnReferences(session,
                        rangeGroup, rangeCount, rangeGroups, unresolvedSet,
                        acceptsSequences);
            }
        }

        return unresolvedSet;
    }

    public Object getValue(Session session) {

        if (opType != OpTypes.LIKE) {
            return super.getValue(session);
        }

        Object leftValue   = nodes[LEFT].getValue(session);
        Object rightValue  = nodes[RIGHT].getValue(session);
        Object escapeValue = nodes[ESCAPE] == null ? null
                                                   : nodes[ESCAPE].getValue(
                                                       session);

        if (likeObject.isVariable) {
            synchronized (likeObject) {
                likeObject.setPattern(session, rightValue, escapeValue,
                                      nodes[ESCAPE] != null);

                return likeObject.compare(session, leftValue);
            }
        }

        return likeObject.compare(session, leftValue);
    }

    public void resolveTypes(Session session, Expression parent) {

        if (opType != OpTypes.LIKE) {
            return;
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes[LEFT].isUnresolvedParam()
                && nodes[RIGHT].isUnresolvedParam()) {
            nodes[LEFT].dataType = Type.SQL_VARCHAR_DEFAULT;
        }

        if (nodes[LEFT].dataType == null && nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (nodes[LEFT].isUnresolvedParam()) {
            nodes[LEFT].dataType = nodes[RIGHT].dataType.isBinaryType()
                                   ? Type.SQL_VARBINARY_DEFAULT
                                   : Type.SQL_VARCHAR_DEFAULT;
        } else if (nodes[RIGHT].isUnresolvedParam()) {
            nodes[RIGHT].dataType = nodes[LEFT].dataType.isBinaryType()
                                    ? Type.SQL_VARBINARY_DEFAULT
                                    : Type.SQL_VARCHAR_DEFAULT;
        }

        if (nodes[LEFT].dataType == null || nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }

        int group = nodes[LEFT].dataType.typeComparisonGroup;

        if (group == Types.SQL_VARCHAR) {

            //
        } else if (group == Types.SQL_VARBINARY) {
            likeObject.isBinary = true;
        } else {
            if (session.database.sqlEnforceTypes) {
                throw Error.error(ErrorCode.X_42562);
            }

            if (group == Types.OTHER) {
                throw Error.error(ErrorCode.X_42563);
            }

            nodes[LEFT] = ExpressionOp.getCastExpression(session, nodes[LEFT],
                    Type.SQL_VARCHAR_DEFAULT);
            group = Types.SQL_VARCHAR;
        }

        if (nodes[RIGHT].dataType.typeComparisonGroup != group
                || (nodes[ESCAPE] != null
                    && nodes[ESCAPE].dataType.typeComparisonGroup != group)) {
            throw Error.error(ErrorCode.X_42563);
        }

        if (group == Types.SQL_VARCHAR) {
            boolean ignoreCase =
                !nodes[LEFT].dataType.getCollation().isCaseSensitive()
                || !nodes[RIGHT].dataType.getCollation().isCaseSensitive();

            likeObject.setIgnoreCase(ignoreCase);
        }

        likeObject.dataType = nodes[LEFT].dataType;

        boolean isEscapeFixedConstant = true;

        if (nodes[ESCAPE] != null) {
// A VoltDB extension to disable LIKE pattern escape characters
             // Can guarantee this won't work with an escape in the EE
            throw new RuntimeException("Like with an escape is not supported");
/* disable 44 lines ...
            if (nodes[ESCAPE].isUnresolvedParam()) {
                nodes[ESCAPE].dataType = likeObject.isBinary
                                         ? Type.SQL_VARBINARY
                                         : Type.SQL_VARCHAR;
            }

            nodes[ESCAPE].resolveTypes(session, this);

            isEscapeFixedConstant = nodes[ESCAPE].opType == OpTypes.VALUE;

            if (isEscapeFixedConstant) {
                nodes[ESCAPE].setAsConstantValue(session, parent);

                if (nodes[ESCAPE].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (nodes[ESCAPE].valueData != null) {
                    long length;

                    switch (nodes[ESCAPE].dataType.typeCode) {

                        case Types.SQL_CHAR :
                        case Types.SQL_VARCHAR :
                            length =
                                ((String) nodes[ESCAPE].valueData).length();
                            break;

                        case Types.SQL_BINARY :
                        case Types.SQL_VARBINARY :
                            length =
                                ((BinaryData) nodes[ESCAPE].valueData).length(
                                    session);
                            break;

                        default :
                            throw Error.error(ErrorCode.X_42563);
                    }

                    if (length != 1) {
                        throw Error.error(ErrorCode.X_22019);
                    }
                }
            }
... disabled 44 lines */
// End of VoltDB extension
        }

        boolean isRightArgFixedConstant = nodes[RIGHT].opType == OpTypes.VALUE;

        if (isRightArgFixedConstant && isEscapeFixedConstant) {
            if (nodes[LEFT].opType == OpTypes.VALUE) {
                setAsConstantValue(session, parent);

                likeObject = null;

                return;
            }

            likeObject.isVariable = false;
        }

        // always optimise with logical conditions
        Object pattern = isRightArgFixedConstant
                         ? nodes[RIGHT].getValue(session)
                         : null;
        boolean constantEscape = isEscapeFixedConstant
                                 && nodes[ESCAPE] != null;
        Object escape = constantEscape ? nodes[ESCAPE].getValue(session)
                                       : null;

        likeObject.setPattern(session, pattern, escape, nodes[ESCAPE] != null);

        if (noOptimisation) {
            return;
        }

        if (likeObject.isEquivalentToUnknownPredicate()) {
            this.setAsConstantValue(session, parent);

            likeObject = null;

            return;
        }

        if (likeObject.isEquivalentToEqualsPredicate()) {
            opType = OpTypes.EQUAL;
            nodes[RIGHT] = new ExpressionValue(likeObject.getRangeLow(),
                                               Type.SQL_VARCHAR);
            likeObject = null;

            setEqualityMode();

            return;
        }

        if (likeObject.isEquivalentToNotNullPredicate()) {
            Expression notNull = new ExpressionLogical(OpTypes.IS_NULL,
                nodes[LEFT]);

            opType      = OpTypes.NOT;
            nodes       = new Expression[UNARY];
            nodes[LEFT] = notNull;
            likeObject  = null;

            return;
        }

        if (nodes[LEFT].opType == OpTypes.COLUMN) {
            ExpressionLike newLike = new ExpressionLike(this);
            Expression prefix = new ExpressionOp(OpTypes.LIKE_ARG,
                                                 nodes[RIGHT], nodes[ESCAPE]);

            prefix.resolveTypes(session, null);

            Expression cast = new ExpressionOp(OpTypes.PREFIX, nodes[LEFT],
                                               prefix);
            Expression equ = new ExpressionLogical(OpTypes.EQUAL, cast,
                                                   prefix);

            equ = new ExpressionLogical(OpTypes.GREATER_EQUAL_PRE,
                                        nodes[LEFT], prefix, equ);
            nodes        = new Expression[BINARY];
            likeObject   = null;
            nodes[LEFT]  = equ;
            nodes[RIGHT] = newLike;
            opType       = OpTypes.AND;
        }
    }

    public String getSQL() {

        if (likeObject == null) {
            return super.getSQL();
        }

        String       left  = getContextSQL(nodes[LEFT]);
        String       right = getContextSQL(nodes[RIGHT]);
        StringBuffer sb    = new StringBuffer();

        sb.append(left).append(' ').append(Tokens.T_LIKE).append(' ');
        sb.append(right);

        /** @todo fredt - scripting of non-ascii escapes needs changes to general script logging */
        if (nodes[ESCAPE] != null) {
            sb.append(' ').append(Tokens.T_ESCAPE).append(' ');
            sb.append(nodes[ESCAPE].getSQL());
            sb.append(' ');
        }

        return sb.toString();
    }

    protected String describe(Session session, int blanks) {

        if (likeObject == null) {
            return super.describe(session, blanks);
        }

        StringBuffer sb = new StringBuffer();

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append("LIKE ");
        sb.append(likeObject.describe(session));

        return sb.toString();
    }

    public Expression duplicate() {

        ExpressionLike e = (ExpressionLike) super.duplicate();

        if (likeObject != null) {
            e.likeObject = likeObject.duplicate();
        }

        return e;
    }
}
