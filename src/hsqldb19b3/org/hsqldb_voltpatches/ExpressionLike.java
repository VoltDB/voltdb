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
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of LIKE operations
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class ExpressionLike extends ExpressionLogical {

    private final static int ESCAPE  = 2;
    private final static int TERNARY = 3;
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

    void collectObjectNames(Set set) {
        super.collectObjectNames(set);
    }

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

    public Object getValue(Session session) {

        if (opType != OpTypes.LIKE) {
            return super.getValue(session);
        }

        Object leftValue   = nodes[LEFT].getValue(session);
        Object rightValue  = nodes[RIGHT].getValue(session);
// A VoltDB extension to disable LIKE pattern escape characters
        Object escapeValue = (nodes.length < TERNARY || nodes[ESCAPE] == null) ? null
/* disable 1 line ...
        Object escapeValue = nodes[ESCAPE] == null ? null
... disabled 1 line */
// End of VoltDB extension
                                                   : nodes[ESCAPE].getValue(
                                                       session);

        if (likeObject.isVariable) {
            synchronized (likeObject) {
                likeObject.setPattern(session, rightValue, escapeValue,
// A VoltDB extension to disable LIKE pattern escape characters
                        (nodes.length == TERNARY) && (nodes[ESCAPE] != null));
/* disable 1 line ...
                                      nodes[ESCAPE] != null);
... disabled 1 line */
// End of VoltDB extension

                return likeObject.compare(session, leftValue);
            }
        }

        return likeObject.compare(session, leftValue);
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        boolean isEscapeFixedConstant = true;

        if (nodes[ESCAPE] != null) {
            if (nodes[ESCAPE].isParam) {
                throw Error.error(ErrorCode.X_42567);
            }

            nodes[ESCAPE].resolveTypes(session, this);

            isEscapeFixedConstant = nodes[ESCAPE].opType == OpTypes.VALUE;

            if (isEscapeFixedConstant) {
                nodes[ESCAPE].setAsConstantValue(session);

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
                            throw Error.error(ErrorCode.X_42565);
                    }

                    if (length != 1) {
                        throw Error.error(ErrorCode.X_22019);
                    }
                }
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

        if (nodes[LEFT].dataType.isCharacterType()
                && nodes[RIGHT].dataType.isCharacterType()
                && (nodes[ESCAPE] == null
                    || nodes[ESCAPE].dataType.isCharacterType())) {
            boolean ignoreCase =
                nodes[LEFT].dataType.typeCode == Types.VARCHAR_IGNORECASE
                || nodes[RIGHT].dataType.typeCode == Types.VARCHAR_IGNORECASE;

            likeObject.setIgnoreCase(ignoreCase);
        } else if (nodes[LEFT].dataType.isBinaryType()
                   && nodes[RIGHT].dataType.isBinaryType()
                   && (nodes[ESCAPE] == null
                       || nodes[ESCAPE].dataType.isBinaryType())) {
            likeObject.isBinary = true;
        } else {
            throw Error.error(ErrorCode.X_42565);
        }

// A VoltDB extension to disable LIKE pattern escape characters
        /*
         * Remove the unused escape node
         */
        if (nodes[ESCAPE] == null) {
            Expression oldNodes[] = nodes;
            nodes = new Expression[BINARY];
            nodes[LEFT] = oldNodes[LEFT];
            nodes[RIGHT] = oldNodes[RIGHT];
        }
// End of VoltDB extension
        likeObject.dataType = nodes[LEFT].dataType;

        boolean isRightArgFixedConstant = nodes[RIGHT].opType == OpTypes.VALUE;

        if (isRightArgFixedConstant && isEscapeFixedConstant
                && nodes[LEFT].opType == OpTypes.VALUE) {
            setAsConstantValue(session);

            likeObject = null;

            return;
        }

        if (isRightArgFixedConstant && isEscapeFixedConstant) {
            likeObject.isVariable = false;
        } else {
// A VoltDB extension to disable LIKE pattern escape characters
            /*
             * Can guarantee this won't work with an escape in the EE
             */
            if (nodes.length > 2) {
                throw new RuntimeException("Like with an escape is not supported in parameterized queries");
            }
// End of VoltDB extension
            return;
        }

        Object pattern = isRightArgFixedConstant
                         ? nodes[RIGHT].getConstantValue(session)
                         : null;
        boolean constantEscape = isEscapeFixedConstant
// A VoltDB extension to disable LIKE pattern escape characters
                                 && (nodes.length > 2);
/* disable 1 line ...
                                 && nodes[ESCAPE] != null;
... disabled 1 line */
// End of VoltDB extension
        Object escape = constantEscape
                        ? nodes[ESCAPE].getConstantValue(session)
                        : null;

// A VoltDB extension to disable LIKE pattern escape characters
        likeObject.setPattern(session, pattern, escape, (nodes.length > 2));
/* disable 1 line ...
        likeObject.setPattern(session, pattern, escape, nodes[ESCAPE] != null);
... disabled 1 line */
// End of VoltDB extension

        if (noOptimisation) {
            return;
        }

        if (likeObject.isEquivalentToUnknownPredicate()) {
            this.setAsConstantValue(null);

            likeObject = null;
        } else if (likeObject.isEquivalentToEqualsPredicate()) {
            opType = OpTypes.EQUAL;
            nodes[RIGHT] = new ExpressionValue(likeObject.getRangeLow(),
                                               Type.SQL_VARCHAR);
            likeObject = null;
        } else if (likeObject.isEquivalentToNotNullPredicate()) {
            Expression notNull = new ExpressionLogical(OpTypes.IS_NULL,
                nodes[LEFT]);

            opType      = OpTypes.NOT;
            nodes       = new Expression[UNARY];
            nodes[LEFT] = notNull;
            likeObject  = null;
        } else {
            if (nodes[LEFT].opType != OpTypes.COLUMN) {
                return;
            }

            if (!nodes[LEFT].dataType.isCharacterType()) {
                return;
            }

            boolean between = false;
            boolean like    = false;
            boolean larger  = false;

            if (likeObject.isEquivalentToBetweenPredicate()) {

                // X LIKE 'abc%' <=> X >= 'abc' AND X <= 'abc' || max_collation_char
                larger  = likeObject.hasCollation;
                between = !larger;
                like    = larger;
            } else if (likeObject
                    .isEquivalentToBetweenPredicateAugmentedWithLike()) {

                // X LIKE 'abc%...' <=> X >= 'abc' AND X <= 'abc' || max_collation_char AND X LIKE 'abc%...'
                larger  = likeObject.hasCollation;
                between = !larger;
                like    = true;
            }

            if (!between && !larger) {
// A VoltDB extension to disable LIKE pattern escape characters
                /*
                 * Escape is not supported in the EE yet
                 */
                if (nodes.length > 2) {
                    throw new RuntimeException("Like with an escape is not supported unless it is prefix like");
                }
// End of VoltDB extension
                return;
            }

            Expression leftBound =
                new ExpressionValue(likeObject.getRangeLow(),
                                    Type.SQL_VARCHAR);
            Expression rightBound =
                new ExpressionValue(likeObject.getRangeHigh(session),
                                    Type.SQL_VARCHAR);

            if (between && !like) {
                Expression leftOld = nodes[LEFT];

                nodes = new Expression[BINARY];
                nodes[LEFT] = new ExpressionLogical(OpTypes.GREATER_EQUAL,
                                                    leftOld, leftBound);
                nodes[RIGHT] = new ExpressionLogical(OpTypes.SMALLER_EQUAL,
                                                     leftOld, rightBound);
                opType     = OpTypes.AND;
                likeObject = null;
            } else if (between && like) {
                Expression gte = new ExpressionLogical(OpTypes.GREATER_EQUAL,
                                                       nodes[LEFT], leftBound);
                Expression lte = new ExpressionLogical(OpTypes.SMALLER_EQUAL,
                                                       nodes[LEFT],
                                                       rightBound);
                ExpressionLike newLike = new ExpressionLike(this);

// A VoltDB extension to disable LIKE pattern escape characters
                /*
                 * Escape is not supported in the EE yet
                 */
                if (nodes.length > 2) {
                    throw new RuntimeException("Like with an escape is not supported unless it is prefix like");
                }
// End of VoltDB extension
                nodes        = new Expression[BINARY];
                likeObject   = null;
                nodes[LEFT]  = new ExpressionLogical(OpTypes.AND, gte, lte);
                nodes[RIGHT] = newLike;
                opType       = OpTypes.AND;
            } else if (larger) {
                Expression gte = new ExpressionLogical(OpTypes.GREATER_EQUAL,
                                                       nodes[LEFT], leftBound);
                ExpressionLike newLike = new ExpressionLike(this);

                nodes        = new Expression[BINARY];
                likeObject   = null;
                nodes[LEFT]  = gte;
                nodes[RIGHT] = newLike;
                opType       = OpTypes.AND;
            }
// A VoltDB extension to disable LIKE pattern escape characters
            else {
                /*
                 * Escape is not supported in the EE yet
                 */
                if (nodes.length > 2) {
                    throw new RuntimeException("Like with an escape is not supported unless it is prefix like");
                }
            }
// End of VoltDB extension
        }
    }

    public String getSQL() {

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

        StringBuffer sb = new StringBuffer();

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append("LIKE ");
        sb.append(likeObject.describe(session));

        return sb.toString();
    }
}
