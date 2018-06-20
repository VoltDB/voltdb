package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.types.Type;

import java.util.Objects;

/**
 * A VoltDB extension, implementation of STARTS WITH operation
 *
 * @author Xin Jin
 */
public final class ExpressionStartsWith extends ExpressionLogical {

    private final static int BINARY  = 2;
    private StartsWith             startsWithObject;

    /**
     * Create a STARTS WITH expression
     */
    ExpressionStartsWith(Expression left, Expression right, boolean noOptimisation) {

        super(OpTypes.STARTS_WITH);

        nodes               = new Expression[BINARY];
        nodes[LEFT]         = left;
        nodes[RIGHT]        = right;
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

        if (startsWithObject.isVariable) {
            synchronized (startsWithObject) {
               startsWithObject.setPattern(session, rightValue);
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

        if (nodes[LEFT].isParam) {
            nodes[LEFT].dataType = nodes[RIGHT].dataType;
        } else if (nodes[RIGHT].isParam) {
            nodes[RIGHT].dataType = nodes[LEFT].dataType;
        }

        if (nodes[LEFT].dataType == null || nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (nodes[LEFT].dataType.isCharacterType()
                && nodes[RIGHT].dataType.isCharacterType()) {
            boolean ignoreCase =
                nodes[LEFT].dataType.typeCode == Types.VARCHAR_IGNORECASE
                || nodes[RIGHT].dataType.typeCode == Types.VARCHAR_IGNORECASE;
            startsWithObject.setIgnoreCase(ignoreCase);
        } else if (nodes[LEFT].dataType.isBinaryType()
                   && nodes[RIGHT].dataType.isBinaryType()) {
            startsWithObject.isBinary = true;
        } else if (false == (nodes[LEFT].dataType.isBooleanType()
                              && nodes[RIGHT].dataType.isBooleanType())
                              && dataType.isBooleanType()) {
            // If both argument nodes are boolean we have resolved
            // this before.  So, this is ok.  Otherwise, this is not
            // properly typed.          throw Error.error(ErrorCode.X_42565);
        }

        if (startsWithObject != null) {
            startsWithObject.dataType = nodes[LEFT].dataType;
        }

        boolean isRightArgFixedConstant = nodes[RIGHT].opType == OpTypes.VALUE;

        if (isRightArgFixedConstant && nodes[LEFT].opType == OpTypes.VALUE) {
            setAsConstantValue(session);

            startsWithObject = null;

            return;
        }

        if (isRightArgFixedConstant) {
            if (startsWithObject != null) {
                startsWithObject.isVariable = false;
            }
        } else {
            return;
        }

        // In this case, pattern will always be not null 
        Object pattern = isRightArgFixedConstant
                         ? nodes[RIGHT].getConstantValue(session)
                         : null;

        startsWithObject.setPattern(session, pattern);

        if (noOptimisation) {
            return;
        }

        if (nodes[RIGHT].isParam) {   // Shouldn't arrive here
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
