package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.Type;

import java.util.Objects;

/**
 * A VoltDB extension, implementation of STARTS WITH operation
 *
 * @author Xin Jin
 */
public final class ExpressionStartsWith extends ExpressionLogical {

    private final static int ESCAPE  = 2;
    private final static int TERNARY = 3;
    private StartsWith             startsWithObject;

    /**
     * Create a STARTS WITH expression
     */
    ExpressionStartsWith(Expression left, Expression right, Expression escape, boolean noOptimisation) {

        super(OpTypes.STARTS_WITH);

        nodes               = new Expression[TERNARY];
        nodes[LEFT]         = left;
        nodes[RIGHT]        = right;
        nodes[ESCAPE]       = escape;
        startsWithObject          = new StartsWith();
        this.noOptimisation = noOptimisation;
    }

    private ExpressionStartsWith(ExpressionStartsWith other) {

        super(OpTypes.STARTS_WITH);

        this.nodes      = other.nodes;
        this.startsWithObject = other.startsWithObject;
    }

    @Override
    void collectObjectNames(Set set) {
        super.collectObjectNames(set);
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
        Object escapeValue = (nodes.length < TERNARY || nodes[ESCAPE] == null) ? null
                                                   : nodes[ESCAPE].getValue(session);

        if (startsWithObject.isVariable) {
            synchronized (startsWithObject) {
               startsWithObject.setPattern(session, rightValue, escapeValue,
                        (nodes.length == TERNARY) && (nodes[ESCAPE] != null));

                return startsWithObject.compare(session, leftValue);
            }
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

        boolean isEscapeFixedConstant = true;

        if (TERNARY <= nodes.length && nodes[ESCAPE] != null) {
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
                && (nodes.length <= TERNARY || nodes[ESCAPE] == null
                    || nodes[ESCAPE].dataType.isCharacterType())) {
            boolean ignoreCase =
                nodes[LEFT].dataType.typeCode == Types.VARCHAR_IGNORECASE
                || nodes[RIGHT].dataType.typeCode == Types.VARCHAR_IGNORECASE;
            startsWithObject.setIgnoreCase(ignoreCase);
        } else if (nodes[LEFT].dataType.isBinaryType()
                   && nodes[RIGHT].dataType.isBinaryType()
                   && (nodes[ESCAPE] == null
                       || nodes[ESCAPE].dataType.isBinaryType())) {
            startsWithObject.isBinary = true;
        } else if (false == (nodes[LEFT].dataType.isBooleanType()
                              && nodes[RIGHT].dataType.isBooleanType())
                              && dataType.isBooleanType()) {
            // If both argument nodes are boolean we have resolved
            // this before.  So, this is ok.  Otherwise, this is not
            // properly typed.          throw Error.error(ErrorCode.X_42565);
        }

// A VoltDB extension to disable STARTS WITH pattern escape characters
        /*
         * Remove the unused escape node
         */
        if (TERNARY <= nodes.length && nodes[ESCAPE] == null) {
            Expression oldNodes[] = nodes;
            nodes = new Expression[BINARY];
            nodes[LEFT] = oldNodes[LEFT];
            nodes[RIGHT] = oldNodes[RIGHT];
        }
// End of VoltDB extension
        if (startsWithObject != null) {
            startsWithObject.dataType = nodes[LEFT].dataType;
        }

        boolean isRightArgFixedConstant = nodes[RIGHT].opType == OpTypes.VALUE;

        if (isRightArgFixedConstant && isEscapeFixedConstant
                && nodes[LEFT].opType == OpTypes.VALUE) {
            setAsConstantValue(session);

            startsWithObject = null;

            return;
        }

        if (isRightArgFixedConstant && isEscapeFixedConstant) {
            if (startsWithObject != null) {
                startsWithObject.isVariable = false;
            }
        } else {
            if (nodes.length > 2) {
                throw new RuntimeException("Starts with operation with an escape is not supported in parameterized queries");
            }
        }

        // In this case, pattern will always be not null 
        Object pattern = isRightArgFixedConstant
                         ? nodes[RIGHT].getConstantValue(session)
                         : null;
        boolean constantEscape = isEscapeFixedConstant && (nodes.length > 2);
        Object escape = constantEscape ? nodes[ESCAPE].getConstantValue(session) : null;

        startsWithObject.setPattern(session, pattern, escape, (nodes.length > 2));

        if (noOptimisation) {
            return;
        }

        if (nodes[RIGHT].isParam) {   // Handle the Dynamic Parameter
            return;
        }
        else if (startsWithObject.isEquivalentToUnknownPredicate()) {
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
            startsWithObject  = null;
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

        sb.append(left).append(' ').append(Tokens.T_STARTS).append(Tokens.T_WITH).append(' ');
        sb.append(right);

        /** @todo fredt - scripting of non-ascii escapes needs changes to general script logging */
        if (nodes[ESCAPE] != null) {
            sb.append(' ').append(Tokens.T_ESCAPE).append(' ');
            sb.append(nodes[ESCAPE].getSQL());
            sb.append(' ');
        }

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
            if (noOptimisation == ((ExpressionStartsWith)other).noOptimisation) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + Objects.hashCode(noOptimisation);
    }
}
