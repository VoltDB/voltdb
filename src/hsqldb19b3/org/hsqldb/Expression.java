/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb;

import java.util.List;
import java.util.Vector;

import org.hsqldb.HSQLInterface.HSQLParseException;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.lib.ArrayListIdentity;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.NullType;
import org.hsqldb.types.Type;

/**
 * Expression class.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class Expression {

    public static final int LEFT   = 0;
    public static final int RIGHT  = 1;
    public static final int UNARY  = 1;
    public static final int BINARY = 2;

    //
    //
    static final Expression[] emptyExpressionArray = new Expression[]{};

    //
    static final Expression EXPR_TRUE  = new ExpressionLogical(true);
    static final Expression EXPR_FALSE = new ExpressionLogical(false);

    //
    static final OrderedIntHashSet aggregateFunctionSet =
        new OrderedIntHashSet();

    static {
        aggregateFunctionSet.add(OpTypes.COUNT);
        aggregateFunctionSet.add(OpTypes.SUM);
        aggregateFunctionSet.add(OpTypes.MIN);
        aggregateFunctionSet.add(OpTypes.MAX);
        aggregateFunctionSet.add(OpTypes.AVG);
        aggregateFunctionSet.add(OpTypes.EVERY);
        aggregateFunctionSet.add(OpTypes.SOME);
        aggregateFunctionSet.add(OpTypes.STDDEV_POP);
        aggregateFunctionSet.add(OpTypes.STDDEV_SAMP);
        aggregateFunctionSet.add(OpTypes.VAR_POP);
        aggregateFunctionSet.add(OpTypes.VAR_SAMP);
    }

    static final OrderedIntHashSet columnExpressionSet =
        new OrderedIntHashSet();

    static {
        columnExpressionSet.add(OpTypes.COLUMN);
    }

    static final OrderedIntHashSet subqueryExpressionSet =
        new OrderedIntHashSet();

    static {
        subqueryExpressionSet.add(OpTypes.ROW_SUBQUERY);
        subqueryExpressionSet.add(OpTypes.TABLE_SUBQUERY);
    }

    static final OrderedIntHashSet subqueryAggregateExpressionSet =
        new OrderedIntHashSet();

    static {
        subqueryAggregateExpressionSet.add(OpTypes.COUNT);
        subqueryAggregateExpressionSet.add(OpTypes.SUM);
        subqueryAggregateExpressionSet.add(OpTypes.MIN);
        subqueryAggregateExpressionSet.add(OpTypes.MAX);
        subqueryAggregateExpressionSet.add(OpTypes.AVG);
        subqueryAggregateExpressionSet.add(OpTypes.EVERY);
        subqueryAggregateExpressionSet.add(OpTypes.SOME);
        subqueryAggregateExpressionSet.add(OpTypes.STDDEV_POP);
        subqueryAggregateExpressionSet.add(OpTypes.STDDEV_SAMP);
        subqueryAggregateExpressionSet.add(OpTypes.VAR_POP);
        subqueryAggregateExpressionSet.add(OpTypes.VAR_SAMP);

        //
        subqueryAggregateExpressionSet.add(OpTypes.TABLE_SUBQUERY);
        subqueryAggregateExpressionSet.add(OpTypes.ROW_SUBQUERY);
    }

    static final OrderedIntHashSet emptyExpressionSet =
        new OrderedIntHashSet();

    // type
    protected int opType;

    // type qualifier
    protected int exprSubType;

    //
    SimpleName alias;

    // aggregate
    protected boolean isAggregate;

    // VALUE
    protected Object       valueData;
    protected Expression[] nodes;
    Type[]                 nodeDataTypes;

    // QUERY - in single value selects, IN, EXISTS etc.
    SubQuery subQuery;

    // for query and value lists, etc
    boolean isCorrelated;

    // for COLUMN
    int columnIndex = -1;

    // data type
    protected Type dataType;

    //
    int     queryTableColumnIndex = -1;    // >= 0 when it is used for order by
    boolean isParam;

    // index of a session-dependent field
    int parameterIndex = -1;

    //
    int rangePosition = -1;

    //
    boolean isColumnEqual;

    Expression(int type) {
        opType = type;
        nodes  = emptyExpressionArray;
    }

    // IN condition optimisation

    /**
     * Creates a SCALAR SUBQUERY expression. Is called as ROW_SUBQUERY
     */
    Expression(int exprType, SubQuery sq) {

        this(OpTypes.TABLE_SUBQUERY);

        subQuery = sq;
    }

    /**
     * ROW or VALUELIST
     */
    Expression(int type, Expression[] list) {

        this(type);

        this.nodes = list;
    }

    public String describe(Session session) {
        return describe(session, 0);
    }

    static String getContextSQL(Expression expression) {

        if (expression == null) {
            return null;
        }

        String ddl = expression.getSQL();

        switch (expression.opType) {

            case OpTypes.VALUE :
            case OpTypes.COLUMN :
            case OpTypes.ROW :
            case OpTypes.FUNCTION :
            case OpTypes.SQL_FUNCTION :
            case OpTypes.ALTERNATIVE :
            case OpTypes.CASEWHEN :
            case OpTypes.CAST :
                return ddl;
        }

        StringBuffer sb = new StringBuffer();

        ddl = sb.append('(').append(ddl).append(')').toString();

        return ddl;
    }

    /**
     * For use with CHECK constraints. Under development.
     *
     * Currently supports a subset of expressions and is suitable for CHECK
     * search conditions that refer only to the inserted/updated row.
     *
     * For full DDL reporting of VIEW select queries and CHECK search
     * conditions, future improvements here are dependent upon improvements to
     * SELECT query parsing, so that it is performed in a number of passes.
     * An early pass should result in the query turned into an Expression tree
     * that contains the information in the original SQL without any
     * alterations, and with tables and columns all resolved. This Expression
     * can then be preserved for future use. Table and column names that
     * are not user-defined aliases should be kept as the HsqlName structures
     * so that table or column renaming is reflected in the precompiled
     * query.
     */
    public String getSQL() {

        StringBuffer sb = new StringBuffer(64);

        switch (opType) {

            case OpTypes.VALUE :
                if (valueData == null) {
                    return Tokens.T_NULL;
                }

                return dataType.convertToSQLString(valueData);

            case OpTypes.ROW :
                sb.append('(');

                for (int i = 0; i < nodes.length; i++) {
                    sb.append(nodes[i].getSQL());

                    if (i < nodes.length - 1) {
                        sb.append(',');
                    }
                }

                sb.append(')');

                return sb.toString();

            //
            case OpTypes.TABLE :
                for (int i = 0; i < nodes.length; i++) {
                    sb.append(nodes[i].getSQL());

                    if (i < nodes.length - 1) {
                        sb.append(',');
                    }
                }

                return sb.toString();
        }

        switch (opType) {

            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY :
/*
                buf.append('(');
                buf.append(subSelect.getSQL());
                buf.append(')');
*/
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

            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY :
                sb.append("QUERY ");
                sb.append(subQuery.queryExpression.describe(session));

                return sb.toString();

            case OpTypes.ROW :
                sb.append("ROW = ");

                for (int i = 0; i < nodes.length; i++) {
                    sb.append(nodes[i].describe(session, blanks + 1));
                }

                sb.append("), TYPE = ").append(dataType.getNameString());
                break;

            case OpTypes.TABLE :
                sb.append("VALUELIST ");

                for (int i = 0; i < nodes.length; i++) {
                    sb.append(nodes[i].describe(session, blanks + 1));
                    sb.append(' ');
                }
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

    /**
     * Set the data type
     */
    void setDataType(Session session, Type type) {

        if (opType == OpTypes.VALUE) {
            valueData = type.convertToType(session, valueData, dataType);
        }

        dataType = type;
    }

    public boolean equals(Expression other) {

        if (other == this) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (opType != other.opType || exprSubType != other.exprSubType
                || !equals(dataType, other.dataType)) {
            return false;
        }

        switch (opType) {

            case OpTypes.SIMPLE_COLUMN :
                return this.columnIndex == other.columnIndex;

            case OpTypes.VALUE :
                return equals(valueData, other.valueData);

            default :
                return equals(nodes, other.nodes)
                       && equals(subQuery, other.subQuery);
        }
    }

    public boolean equals(Object other) {

        if (other == this) {
            return true;
        }

        if (other instanceof Expression) {
            return equals((Expression) other);
        }

        return false;
    }

    public int hashCode() {

        int val = opType + exprSubType;

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                val += nodes[i].hashCode();
            }
        }

        return val;
    }

    static boolean equals(Object o1, Object o2) {

        if (o1 == o2) {
            return true;
        }

        return (o1 == null) ? o2 == null
                            : o1.equals(o2);
    }

    static boolean equals(Expression[] row1, Expression[] row2) {

        if (row1 == row2) {
            return true;
        }

        if (row1.length != row2.length) {
            return false;
        }

        int len = row1.length;

        for (int i = 0; i < len; i++) {
            Expression e1     = row1[i];
            Expression e2     = row2[i];
            boolean    equals = (e1 == null) ? e2 == null
                                             : e1.equals(e2);

            if (!equals) {
                return false;
            }
        }

        return true;
    }

    boolean isComposedOf(Expression exprList[], int start, int end,
                         OrderedIntHashSet excludeSet) {

        if (opType == OpTypes.VALUE) {
            return true;
        }

        if (excludeSet.contains(opType)) {
            return true;
        }

        for (int i = start; i < end; i++) {
            if (equals(exprList[i])) {
                return true;
            }
        }

        switch (opType) {

            case OpTypes.LIKE :
            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL :
            case OpTypes.UNIQUE :
            case OpTypes.EXISTS :
            case OpTypes.TABLE_SUBQUERY :
            case OpTypes.ROW_SUBQUERY :

            //
            case OpTypes.COUNT :
            case OpTypes.SUM :
            case OpTypes.MIN :
            case OpTypes.MAX :
            case OpTypes.AVG :
            case OpTypes.EVERY :
            case OpTypes.SOME :
            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                return false;
        }

        if (nodes.length == 0) {
            return false;
        }

        boolean result = true;

        for (int i = 0; i < nodes.length; i++) {
            result &= (nodes[i] == null
                       || nodes[i].isComposedOf(exprList, start, end,
                                                excludeSet));
        }

        return result;
    }

    boolean isComposedOf(OrderedHashSet expressions,
                         OrderedIntHashSet excludeSet) {

        if (opType == OpTypes.VALUE) {
            return true;
        }

        if (excludeSet.contains(opType)) {
            return true;
        }

        for (int i = 0; i < expressions.size(); i++) {
            if (equals(expressions.get(i))) {
                return true;
            }
        }

        switch (opType) {

            case OpTypes.COUNT :
            case OpTypes.SUM :
            case OpTypes.MIN :
            case OpTypes.MAX :
            case OpTypes.AVG :
            case OpTypes.EVERY :
            case OpTypes.SOME :
            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                return false;
        }

/*
        case OpCodes.LIKE :
        case OpCodes.ALL :
        case OpCodes.ANY :
        case OpCodes.IN :
        case OpCodes.MATCH_SIMPLE :
        case OpCodes.MATCH_PARTIAL :
        case OpCodes.MATCH_FULL :
        case OpCodes.MATCH_UNIQUE_SIMPLE :
        case OpCodes.MATCH_UNIQUE_PARTIAL :
        case OpCodes.MATCH_UNIQUE_FULL :
        case OpCodes.UNIQUE :
        case OpCodes.EXISTS :
        case OpCodes.TABLE_SUBQUERY :
        case OpCodes.ROW_SUBQUERY :
*/
        if (nodes.length == 0) {
            return false;
        }

        boolean result = true;

        for (int i = 0; i < nodes.length; i++) {
            result &= (nodes[i] == null
                       || nodes[i].isComposedOf(expressions, excludeSet));
        }

        return result;
    }

    Expression replace(OrderedHashSet expressions,
                       OrderedHashSet replacements) {

        if (opType == OpTypes.VALUE) {
            return this;
        }

        int index = expressions.getIndex(this);

        if (index != -1) {
            return (Expression) replacements.get(index);
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            nodes[i] = nodes[i].replace(expressions, replacements);
        }

        return this;
    }

    Expression replaceColumnReferences(RangeVariable range,
                                       Expression[] list) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            nodes[i] = nodes[i].replaceColumnReferences(range, list);
        }

        return this;
    }

    void convertToSimpleColumn(OrderedHashSet expressions,
                               OrderedHashSet replacements) {

        if (opType == OpTypes.VALUE) {
            return;
        }

        int index = expressions.getIndex(this);

        if (index != -1) {
            Expression e = (Expression) replacements.get(index);

            nodes         = emptyExpressionArray;
            opType        = OpTypes.SIMPLE_COLUMN;
            columnIndex   = e.columnIndex;
            rangePosition = e.rangePosition;

            return;
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            nodes[i].convertToSimpleColumn(expressions, replacements);
        }
    }

    boolean isSelfAggregate() {
        return false;
    }

    /**
     * Set the column alias
     */
    void setAlias(SimpleName name) {
        alias = name;
    }

    /**
     * Get the column alias
     */
    String getAlias() {

        if (alias != null) {
            return alias.name;
        }

        return "";
    }

    SimpleName getSimpleName() {

        if (alias != null) {
            return alias;
        }

        return null;
    }

    /**
     * Returns the type of expression
     */
    public int getType() {
        return opType;
    }

    /**
     * Returns the left node
     */
    Expression getLeftNode() {
        return nodes.length > 0 ? nodes[LEFT]
                                : null;
    }

    /**
     * Returns the right node
     */
    Expression getRightNode() {
        return nodes.length > 1 ? nodes[RIGHT]
                                : null;
    }

    void setLeftNode(Expression e) {
        nodes[LEFT] = e;
    }

    void setRightNode(Expression e) {
        nodes[RIGHT] = e;
    }

    /**
     * Returns the range variable for a COLUMN expression
     */
    RangeVariable getRangeVariable() {
        return null;
    }

    /**
     * return the expression for an alias used in an ORDER BY clause
     */
    Expression replaceAliasInOrderBy(Expression[] columns, int length) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            nodes[i] = nodes[i].replaceAliasInOrderBy(columns, length);
        }

        return this;
    }

    /**
     * Find a range variable with the given table alias
     */
    int findMatchingRangeVariableIndex(RangeVariable[] rangeVarArray) {
        return -1;
    }

    /**
     * collects all range variables in expression tree
     */
    void collectRangeVariables(RangeVariable[] rangeVariables, Set set) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].collectRangeVariables(rangeVariables, set);
            }
        }

        if (subQuery != null && subQuery.queryExpression != null) {
            HsqlList unresolvedExpressions =
                subQuery.queryExpression.getUnresolvedExpressions();

            if (unresolvedExpressions != null) {
                for (int i = 0; i < unresolvedExpressions.size(); i++) {
                    Expression e = (Expression) unresolvedExpressions.get(i);

                    e.collectRangeVariables(rangeVariables, set);
                }
            }
        }
    }

    /**
     * collects all range variables in expression tree
     */
    void collectObjectNames(Set set) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].collectObjectNames(set);
            }
        }

        if (subQuery != null) {
            if (subQuery.queryExpression != null) {
                subQuery.queryExpression.collectObjectNames(set);
            }
        }
    }

    /**
     * return true if given RangeVariable is used in expression tree
     */
    boolean hasReference(RangeVariable range) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                if (nodes[i].hasReference(range)) {
                    return true;
                }
            }
        }

        if (subQuery != null && subQuery.queryExpression != null) {
            if (subQuery.queryExpression.hasReference(range)) {
                return true;
            }
        }

        return false;
    }

    /**
     * resolve tables and collect unresolved column expressions
     */
    public HsqlList resolveColumnReferences(RangeVariable[] rangeVarArray,
            HsqlList unresolvedSet) {
        return resolveColumnReferences(rangeVarArray, rangeVarArray.length,
                                       unresolvedSet, true);
    }

    public HsqlList resolveColumnReferences(RangeVariable[] rangeVarArray,
            int rangeCount, HsqlList unresolvedSet, boolean acceptsSequences) {

        if (opType == OpTypes.VALUE) {
            return unresolvedSet;
        }

        switch (opType) {

            case OpTypes.CASEWHEN :
                acceptsSequences = false;
                break;

            case OpTypes.TABLE : {
                HsqlList localSet = null;

                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i] == null) {
                        continue;
                    }

                    localSet = nodes[i].resolveColumnReferences(
                        RangeVariable.emptyArray, localSet);
                }

                if (localSet != null) {
                    isCorrelated = true;

                    if (subQuery != null) {
                        subQuery.setCorrelated();
                    }

                    for (int i = 0; i < localSet.size(); i++) {
                        Expression e = (Expression) localSet.get(i);

                        unresolvedSet =
                            e.resolveColumnReferences(rangeVarArray,
                                                      unresolvedSet);
                    }

                    unresolvedSet = Expression.resolveColumnSet(rangeVarArray,
                            localSet, unresolvedSet);
                }

                return unresolvedSet;
            }
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            unresolvedSet = nodes[i].resolveColumnReferences(rangeVarArray,
                    rangeCount, unresolvedSet, acceptsSequences);
        }

        switch (opType) {

            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY : {
                QueryExpression queryExpression = subQuery.queryExpression;

                if (!queryExpression.areColumnsResolved()) {
                    isCorrelated = true;

                    subQuery.setCorrelated();

                    // take to enclosing context
                    if (unresolvedSet == null) {
                        unresolvedSet = new ArrayListIdentity();
                    }

                    unresolvedSet.addAll(
                        queryExpression.getUnresolvedExpressions());
                }

                break;
            }
            default :
        }

        return unresolvedSet;
    }

    public OrderedHashSet getUnkeyedColumns(OrderedHashSet unresolvedSet) {

        if (opType == OpTypes.VALUE) {
            return unresolvedSet;
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            unresolvedSet = nodes[i].getUnkeyedColumns(unresolvedSet);
        }

        switch (opType) {

            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY :
                if (subQuery != null) {
                    if (unresolvedSet == null) {
                        unresolvedSet = new OrderedHashSet();
                    }

                    unresolvedSet.add(this);
                }
                break;
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

            case OpTypes.TABLE :

                /** @todo - should it fall through */
                break;

            case OpTypes.ROW :
                nodeDataTypes = new Type[nodes.length];

                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i] != null) {
                        nodeDataTypes[i] = nodes[i].dataType;
                    }
                }
                break;

            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY : {
                QueryExpression queryExpression = subQuery.queryExpression;

                queryExpression.resolveTypes(session);
                subQuery.prepareTable(session);

                nodeDataTypes = queryExpression.getColumnTypes();
                dataType      = nodeDataTypes[0];

                break;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "Expression.resolveTypes()");
        }
    }

    void setAsConstantValue(Session session) {

        valueData = getConstantValue(session);
        opType    = OpTypes.VALUE;
        nodes     = emptyExpressionArray;
    }

    void setAsConstantValue(Object value) {

        valueData = value;
        opType    = OpTypes.VALUE;
        nodes     = emptyExpressionArray;
    }

    void prepareTable(Session session, Expression row, int degree) {

        if (nodeDataTypes != null) {
            return;
        }

        for (int i = 0; i < nodes.length; i++) {
            Expression e = nodes[i];

            if (e.opType == OpTypes.ROW) {
                if (degree != e.nodes.length) {
                    throw Error.error(ErrorCode.X_42564);
                }
            } else if (degree == 1) {
                nodes[i]       = new Expression(OpTypes.ROW);
                nodes[i].nodes = new Expression[]{ e };
            } else {
                throw Error.error(ErrorCode.X_42564);
            }
        }

        nodeDataTypes = new Type[degree];

        for (int j = 0; j < degree; j++) {
            Type type = row == null ? null
                                    : row.nodes[j].dataType;

            for (int i = 0; i < nodes.length; i++) {
                type = Type.getAggregateType(nodes[i].nodes[j].dataType, type);
            }

            if (type == null) {
                throw Error.error(ErrorCode.X_42567);
            }

            nodeDataTypes[j] = type;

            if (row != null && row.nodes[j].isParam) {
                row.nodes[j].dataType = type;
            }

            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i].nodes[j].isParam) {
                    nodes[i].nodes[j].dataType = nodeDataTypes[j];

                    continue;
                }

                if (nodes[i].nodes[j].opType == OpTypes.VALUE) {
                    if (nodes[i].nodes[j].valueData == null) {
                        nodes[i].nodes[j].dataType = nodeDataTypes[j];
                    }
                }
            }

            if (nodeDataTypes[j].isCharacterType()
                    && !((CharacterType) nodeDataTypes[j])
                        .isEqualIdentical()) {

                // collation issues
            }
        }
    }

    /**
     * Details of IN condition optimisation for 1.9.0
     * Predicates with SELECT are QUERY expressions
     *
     * Predicates with IN list
     *
     * Parser adds a SubQuery to the list for each predicate
     * At type resolution IN lists that are entirely fixed constant or parameter
     * values are selected for possible optimisation. The flags:
     *
     * IN expression right side isCorrelated == true if there are non-constant,
     * non-param expressions in the list (Expressions may have to be resolved
     * against the full set of columns of the query, so must be re-evaluated
     * for each row and evaluated after all the joins have been made)
     *
     * VALUELIST expression isFixedConstantValueList == true when all
     * expressions are fixed constant and none is a param. With this flag,
     * a single-column VALUELIST can be accessed as a HashMap.
     *
     * Predicates may be optimised as joins if isCorrelated == false
     *
     */
    void insertValuesIntoSubqueryTable(Session session,
                                       PersistentStore store) {

        TableDerived table = subQuery.getTable();

        for (int i = 0; i < nodes.length; i++) {
            Object[] data = nodes[i].getRowValue(session);

            for (int j = 0; j < nodeDataTypes.length; j++) {
                data[j] = nodeDataTypes[j].convertToType(session, data[j],
                        nodes[i].nodes[j].dataType);
            }

            Row row = (Row) store.getNewCachedObject(session, data);

            try {
                store.indexRow(session, row);
            } catch (HsqlException e) {}
        }
    }

    /**
     * Returns the name of a column as string
     *
     * @return column name
     */
    String getColumnName() {
        return getAlias();
    }

    ColumnSchema getColumn() {
        return null;
    }

    /**
     * Returns the column index in the table
     */
    int getColumnIndex() {
        return columnIndex;
    }

    /**
     * Returns the data type
     */
    Type getDataType() {
        return dataType;
    }

    int getDegree() {
        return opType == OpTypes.ROW ? nodes.length
                                     : 1;
    }

    public Object[] getRowValue(Session session) {

        switch (opType) {

            case OpTypes.ROW : {
                Object[] data = new Object[nodes.length];

                for (int i = 0; i < nodes.length; i++) {
                    data[i] = nodes[i].getValue(session);
                }

                return data;
            }
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY : {
                return subQuery.queryExpression.getValues(session);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    Object getValue(Session session, Type type) {

        Object o = getValue(session);

        if (o == null || dataType == type) {
            return o;
        }

        return type.convertToType(session, o, dataType);
    }

    public Object getConstantValue(Session session) {
        return getValue(session);
    }

    public Object getConstantValueNoCheck(Session session) {

        try {
            return getValue(session);
        } catch (HsqlException e) {
            return null;
        }
    }

    public Object getValue(Session session) {

        switch (opType) {

            case OpTypes.VALUE :
                return valueData;

            case OpTypes.SIMPLE_COLUMN : {
                Object[] data =
                    session.sessionContext.rangeIterators[rangePosition]
                        .getCurrent();

                return data[columnIndex];
            }
            case OpTypes.ROW : {
                if (nodes.length == 1) {
                    return nodes[0].getValue(session);
                }

                Object[] row = new Object[nodes.length];

                for (int i = 0; i < nodes.length; i++) {
                    row[i] = nodes[i].getValue(session);
                }

                return row;
            }
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY : {
                subQuery.materialiseCorrelated(session);

                Object value = subQuery.getValue(session);

                return value;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    boolean testCondition(Session session) {
        return Boolean.TRUE.equals(getValue(session));
    }

    static int countNulls(Object[] a) {

        int nulls = 0;

        for (int i = 0; i < a.length; i++) {
            if (a[i] == null) {
                nulls++;
            }
        }

        return nulls;
    }

    static void convertToType(Session session, Object[] data, Type[] dataType,
                              Type[] newType) {

        for (int i = 0; i < data.length; i++) {
            data[i] = newType[i].convertToType(session, data[i], dataType[i]);
        }
    }

    /**
     * Returns a Select object that can be used for checking the contents
     * of an existing table against the given CHECK search condition.
     */
    static QuerySpecification getCheckSelect(Session session, Table t,
            Expression e) {

        CompileContext     compileContext = new CompileContext(session);
        QuerySpecification s = new QuerySpecification(compileContext);

        s.exprColumns    = new Expression[1];
        s.exprColumns[0] = EXPR_TRUE;

        RangeVariable range = new RangeVariable(t, null, null, null,
            compileContext);

        s.rangeVariables = new RangeVariable[]{ range };

        HsqlList unresolved = e.resolveColumnReferences(s.rangeVariables,
            null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        e.resolveTypes(session, null);

        if (Type.SQL_BOOLEAN != e.getDataType()) {
            throw Error.error(ErrorCode.X_42568);
        }

        Expression condition = new ExpressionLogical(OpTypes.NOT, e);

        s.queryCondition = condition;

        s.resolveReferences(session);
        s.resolveTypes(session);

        return s;
    }

    boolean isParam() {
        return isParam;
    }

    void setAttributesAsColumn(ColumnSchema column, boolean isWritable) {
        throw Error.runtimeError(ErrorCode.U_S0500,
                                 "Expression.setAttributesAsColumn");
    }

    String getValueClassName() {

        Type type = dataType == null ? NullType.getNullType()
                                     : dataType;

        return type.getJDBCClassName();
    }

    public void collectAllFunctionExpressions(HsqlList set) {

        Expression.collectAllExpressions(set, this,
                                         Expression.emptyExpressionSet,
                                         Expression.emptyExpressionSet);
    }

    /**
     * collect all extrassions of a set of expression types appearing anywhere
     * in a select statement and its subselects, etc.
     */
    static void collectAllExpressions(HsqlList set, Expression e,
                                      OrderedIntHashSet typeSet,
                                      OrderedIntHashSet stopAtTypeSet) {

        if (e == null) {
            return;
        }

        if (stopAtTypeSet.contains(e.opType)) {
            return;
        }

        for (int i = 0; i < e.nodes.length; i++) {
            collectAllExpressions(set, e.nodes[i], typeSet, stopAtTypeSet);
        }

        if (typeSet.contains(e.opType)) {
            set.add(e);
        }

        if (e.subQuery != null && e.subQuery.queryExpression != null) {
            e.subQuery.queryExpression.collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }
    }

    /**
     * isCorrelated
     */
    public boolean isCorrelated() {

        if (opType == OpTypes.TABLE_SUBQUERY && subQuery != null
                && subQuery.isCorrelated()) {
            return true;
        }

        return false;
    }

    /**
     * checkValidCheckConstraint
     */
    public void checkValidCheckConstraint() {

        HsqlArrayList set = new HsqlArrayList();

        Expression.collectAllExpressions(set, this, subqueryExpressionSet,
                                         emptyExpressionSet);

        if (!set.isEmpty()) {
            throw Error.error(ErrorCode.X_0A000,
                              "subquery in check constraint");
        }
    }

    static HsqlList resolveColumnSet(RangeVariable[] rangeVars,
                                     HsqlList sourceSet, HsqlList targetSet) {

        if (sourceSet == null) {
            return targetSet;
        }

        for (int i = 0; i < sourceSet.size(); i++) {
            Expression e = (Expression) sourceSet.get(i);

            targetSet = e.resolveColumnReferences(rangeVars, targetSet);
        }

        return targetSet;
    }

    Expression getIndexableExpression(RangeVariable rangeVar) {
        return null;
    }

    /*************** VOLTDB *********************/

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @param indent A string of whitespace to be prepended to every line
     * in the resulting XML.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    String voltGetXML(Session session, String indent) throws HSQLParseException
    {
        StringBuffer sb = new StringBuffer();

        //
        // We want to keep track of which expressions are the same in the XML output
        //
        String include = "id=\"" + this.getUniqueId() + "\" ";

        // LEAF TYPES
        if (getType() == OpTypes.VALUE) {
            sb.append(indent).append("<value ").append(include);
            sb.append("type=\"").append(Types.getTypeName(dataType.typeCode)).append("\" ");

            if (isParam) {
                sb.append("isparam=\"true\" ");
            } else {
                String value = "NULL";
                if (valueData != null)
                    value = valueData.toString();
                sb.append("value=\"").append(value).append("\" ");
            }

            sb.append("/>");
        }
        else if (getType() == OpTypes.COLUMN) {
            // XXX Can bad SQL get us here or does HSQL barf before that?
            assert(false);
        }
        else if (getType() == OpTypes.ASTERISK) {
            sb.append(indent).append("<asterisk/>");
        }
        // catch unexpected types
        // XXX Should this throw HSQLParseException instead?
        else {
            System.err.println("UNSUPPORTED EXPR TYPE: " + String.valueOf(getType()));
            sb.append("unknown");
        }

        return sb.toString();
    }

    protected String cached_id = null;

    /**
     * Get the hex address of this Expression Object in memory,
     * to be used as a unique identifier.
     * @return The hex address of the pointer to this object.
     */
    protected String getUniqueId() {
        //
        // Calculated an new Id
        //
        if (this.cached_id == null) {
            // this line ripped from the "describe" method
            // seems to help with some types like "equal"
            this.cached_id = new String();
            int hashCode = 0;
            //
            // If object is a leaf node, then we'll use John's original code...
            //
            if (getType() == OpTypes.VALUE || getType() == OpTypes.COLUMN) {
                hashCode = super.hashCode();
            //
            // Otherwise we need to generate and Id based on what our children are
            //
            } else {
                //
                // Horribly inefficient, but it works for now...
                //
                final List<String> id_list = new Vector<String>();
                new Object() {
                    public void traverse(Expression exp) {
                        for (Expression expr : exp.nodes) {
                            if (expr != null)
                                id_list.add(expr.getUniqueId());
                        }
                    }
                }.traverse(this);

                if (id_list.size() > 0) {
                    // Flatten the id list, intern it, and then do the same trick from above
                    for (String temp : id_list)
                        this.cached_id += temp;
                    hashCode = this.cached_id.intern().hashCode();
                }
                else
                    hashCode = super.hashCode();
            }
            this.cached_id = Integer.toString(hashCode);
        }
        return (this.cached_id);
    }
}
