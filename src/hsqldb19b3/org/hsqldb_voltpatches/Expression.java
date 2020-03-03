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


package org.hsqldb_voltpatches;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.lib.ArrayListIdentity;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.NullType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;

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
        // A VoltDB extension APPROX_COUNT_DISTINCT
        aggregateFunctionSet.add(OpTypes.APPROX_COUNT_DISTINCT);
        // End of VoltDB extension
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
        // A VoltDB extension APPROX_COUNT_DISTINCT
        subqueryAggregateExpressionSet.add(OpTypes.APPROX_COUNT_DISTINCT);
        // End of VoltDB extension
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
        nodes = list;
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
            break;

        case OpTypes.ROW_SUBQUERY :
        case OpTypes.TABLE_SUBQUERY :
            sb.append("QUERY ");
            sb.append(subQuery.queryExpression.describe(session));
            break;

        case OpTypes.ROW :
            sb.append("ROW = (");
            for (Expression node : nodes) {
                sb.append(node.describe(session, blanks + 1));
            }
            sb.append("), TYPE = ")
            .append(dataType == null ? "null" : dataType.getNameString());
            break;

        case OpTypes.TABLE :
            sb.append("VALUELIST ");
            for (Expression node : nodes) {
                sb.append(node.describe(session, blanks + 1));
                sb.append(' ');
            }
            break;

        default:
            if (nodes.length > LEFT && nodes[LEFT] != null) {
                sb.append(" arg1=[");
                sb.append(nodes[LEFT].describe(session, blanks + 1));
                sb.append(']');
            }

            if (nodes.length > RIGHT && nodes[RIGHT] != null) {
                sb.append(" arg2=[");
                sb.append(nodes[RIGHT].describe(session, blanks + 1));
                sb.append(']');
            }
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
                return columnIndex == other.columnIndex;

            case OpTypes.VALUE :
                return equals(valueData, other.valueData);

            default :
                return equals(nodes, other.nodes)
                        && equals(subQuery, other.subQuery);
        }
    }

    public boolean queryTableColumnIndexEquals(Expression other) {
        return queryTableColumnIndex == other.queryTableColumnIndex;
    }

    @Override
    public boolean equals(Object other) {

        if (other == this) {
            return true;
        }

        if (other instanceof Expression) {
            return equals((Expression) other);
        }

        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + opType;
        result = prime * result + exprSubType;
        result = prime * result
                + ((dataType == null) ? 0 : dataType.hashCode());
        switch (opType) {

            case OpTypes.SIMPLE_COLUMN :
                result = prime * result + columnIndex;

            case OpTypes.VALUE:
                result = prime * result
                         + ((valueData == null) ? 0 : valueData.hashCode());

            default:
                result = prime * result + Arrays.hashCode(nodes);
                result = prime * result
                        + ((subQuery == null) ? 0 : subQuery.hashCode());
        }
        return result;
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
            // For ENG-18549
            // treat ? as constant value, allow ? in aggregate functions
            // TODO: this fix only lift the restriction for cast function
            // generalize to other functions in the future if needed
            case OpTypes.CAST:
                assert(1 == nodes.length);
                if (nodes[0].opType == OpTypes.DYNAMIC_PARAM) {
                    return true;
                }
                break;
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
            // A VoltDB extension APPROX_COUNT_DISTINCT
            case OpTypes.APPROX_COUNT_DISTINCT:
            // End of VoltDB extension
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

        // BEGIN Cherry-picked code change from hsqldb-2.3.2
        if (opType == OpTypes.VALUE || opType == OpTypes.DYNAMIC_PARAM
                || opType == OpTypes.PARAMETER || opType == OpTypes.VARIABLE) {
            return true;
        }
        // END Cherry-picked code change from hsqldb-2.3.2

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
            // A VoltDB extension APPROX_COUNT_DISTINCT
            case OpTypes.APPROX_COUNT_DISTINCT:
            // End of VoltDB extension
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

        int index = expressions.getIndexByQueryTableColumnIndex(this);

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
        for (int i = 0; i < nodes.length; i++) {
            Object[] data = nodes[i].getRowValue(session);

            for (int j = 0; j < nodeDataTypes.length; j++) {
                data[j] = nodeDataTypes[j].convertToType(session, data[j],
                        nodes[i].nodes[j].dataType);
            }

            Row row = (Row) store.getNewCachedObject(session, data);

            try {
                store.indexRow(session, row);
            }
            //XXX: what conditions are being casually ignored here?
            catch (HsqlException e) {}
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
        }
        catch (HsqlException e) {
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

        if (e.opType == OpTypes.USER_DEFINED_AGGREGATE) {
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

    /************************* Volt DB Extensions *************************/

    // A VoltDB extension to support indexed expressions
    public void collectAllColumnExpressions(HsqlList set) {

        Expression.collectAllExpressions(set, this,
                                         Expression.columnExpressionSet,
                                         Expression.emptyExpressionSet);
    }

    static Map<Integer, VoltXMLElement> prototypes = new HashMap<>();

    static {
        prototypes.put(OpTypes.VALUE,         new VoltXMLElement("value")); // constant value
        prototypes.put(OpTypes.COLUMN,        new VoltXMLElement("columnref")); // reference
        prototypes.put(OpTypes.COALESCE,      (new VoltXMLElement("operation")).withValue("optype", "operator_case_when"));
        prototypes.put(OpTypes.DEFAULT,       new VoltXMLElement("columnref")); // uninteresting!? ExpressionColumn
        prototypes.put(OpTypes.SIMPLE_COLUMN, (new VoltXMLElement("simplecolumn")));

        prototypes.put(OpTypes.VARIABLE,      null); // Some kind of HSQL session parameter? --paul
        prototypes.put(OpTypes.PARAMETER,     null); // Some kind of HSQL session parameter? --paul
        prototypes.put(OpTypes.DYNAMIC_PARAM, (new VoltXMLElement("value")).withValue("isparam", "true")); // param
        prototypes.put(OpTypes.ASTERISK,      new VoltXMLElement("asterisk"));
        prototypes.put(OpTypes.SEQUENCE,      null); // not yet supported sequence type
        prototypes.put(OpTypes.SCALAR_SUBQUERY,null); // not yet supported subquery feature, query based row/table
        prototypes.put(OpTypes.ROW_SUBQUERY,  null); // not yet supported subquery feature
        prototypes.put(OpTypes.TABLE_SUBQUERY,new VoltXMLElement("tablesubquery"));
        prototypes.put(OpTypes.ROW,           new VoltXMLElement("row")); // rows
        prototypes.put(OpTypes.TABLE,         new VoltXMLElement("table")); // not yet supported subquery feature, but needed for "in"
        prototypes.put(OpTypes.FUNCTION,      null); // not used (HSQL user-defined functions).
        prototypes.put(OpTypes.SQL_FUNCTION,  new VoltXMLElement("function"));
        prototypes.put(OpTypes.ROUTINE_FUNCTION, null); // not used

        //arithmetic operations
        prototypes.put(OpTypes.NEGATE,        (new VoltXMLElement("operation")).withValue("optype", "negate"));

        prototypes.put(OpTypes.ADD,           (new VoltXMLElement("operation")).withValue("optype", "add"));
        prototypes.put(OpTypes.SUBTRACT,      (new VoltXMLElement("operation")).withValue("optype", "subtract"));
        prototypes.put(OpTypes.MULTIPLY,      (new VoltXMLElement("operation")).withValue("optype", "multiply"));
        prototypes.put(OpTypes.DIVIDE,        (new VoltXMLElement("operation")).withValue("optype", "divide"));

        prototypes.put(OpTypes.CONCAT,        (new VoltXMLElement("function")) // concatenation
                                               .withValue("function_id", FunctionCustom.FUNC_CONCAT_ID_STRING)
                                               .withValue("name", Tokens.T_CONCAT_WORD)
                                               .withValue("valuetype", Type.SQL_VARCHAR.getNameString()));

        // logicals - comparisons
        prototypes.put(OpTypes.EQUAL,         (new VoltXMLElement("operation")).withValue("optype", "equal"));
        prototypes.put(OpTypes.GREATER_EQUAL, (new VoltXMLElement("operation")).withValue("optype", "greaterthanorequalto"));
        prototypes.put(OpTypes.GREATER,       (new VoltXMLElement("operation")).withValue("optype", "greaterthan"));
        prototypes.put(OpTypes.SMALLER,       (new VoltXMLElement("operation")).withValue("optype", "lessthan"));
        prototypes.put(OpTypes.SMALLER_EQUAL, (new VoltXMLElement("operation")).withValue("optype", "lessthanorequalto"));
        prototypes.put(OpTypes.NOT_EQUAL,     (new VoltXMLElement("operation")).withValue("optype", "notequal"));
        prototypes.put(OpTypes.IS_NULL,       (new VoltXMLElement("operation")).withValue("optype", "is_null"));

        // logicals - operations
        prototypes.put(OpTypes.NOT,           (new VoltXMLElement("operation")).withValue("optype", "not"));
        prototypes.put(OpTypes.AND,           (new VoltXMLElement("operation")).withValue("optype", "and"));
        prototypes.put(OpTypes.OR,            (new VoltXMLElement("operation")).withValue("optype", "or"));

        // logicals - quantified comparison
        prototypes.put(OpTypes.ALL_QUANTIFIED,null); // not used -- an ExpressionLogical exprSubType value only
        prototypes.put(OpTypes.ANY_QUANTIFIED,null); // not used -- an ExpressionLogical exprSubType value only

        // logicals - other predicates
        prototypes.put(OpTypes.LIKE,          (new VoltXMLElement("operation")).withValue("optype", "like"));
        prototypes.put(OpTypes.STARTS_WITH,   (new VoltXMLElement("operation")).withValue("optype", "startswith"));
        prototypes.put(OpTypes.IN,            null); // not yet supported ExpressionLogical
        prototypes.put(OpTypes.EXISTS,        (new VoltXMLElement("operation")).withValue("optype", "exists"));
        prototypes.put(OpTypes.OVERLAPS,      null); // not yet supported ExpressionLogical
        prototypes.put(OpTypes.UNIQUE,        null); // not yet supported ExpressionLogical
        prototypes.put(OpTypes.NOT_DISTINCT,  (new VoltXMLElement("operation")).withValue("optype", "notdistinct"));
        prototypes.put(OpTypes.MATCH_SIMPLE,  null); // not yet supported ExpressionLogical
        prototypes.put(OpTypes.MATCH_PARTIAL, null); // not yet supported ExpressionLogical
        prototypes.put(OpTypes.MATCH_FULL,    null); // not yet supported ExpressionLogical
        prototypes.put(OpTypes.MATCH_UNIQUE_SIMPLE,  null); // not yet supported ExpressionLogical
        prototypes.put(OpTypes.MATCH_UNIQUE_PARTIAL, null); // not yet supported ExpressionLogical
        prototypes.put(OpTypes.MATCH_UNIQUE_FULL,    null); // not yet supported ExpressionLogical
        // unwindowed aggregate functions
        prototypes.put(OpTypes.COUNT,         (new VoltXMLElement("aggregation")).withValue("optype", "count"));
        prototypes.put(OpTypes.APPROX_COUNT_DISTINCT, (new VoltXMLElement("aggregation")).withValue("optype", "approx_count_distinct"));
        prototypes.put(OpTypes.SUM,           (new VoltXMLElement("aggregation")).withValue("optype", "sum"));
        prototypes.put(OpTypes.MIN,           (new VoltXMLElement("aggregation")).withValue("optype", "min"));
        prototypes.put(OpTypes.MAX,           (new VoltXMLElement("aggregation")).withValue("optype", "max"));
        prototypes.put(OpTypes.AVG,           (new VoltXMLElement("aggregation")).withValue("optype", "avg"));
        prototypes.put(OpTypes.EVERY,         (new VoltXMLElement("aggregation")).withValue("optype", "every"));
        prototypes.put(OpTypes.SOME,          (new VoltXMLElement("aggregation")).withValue("optype", "some"));
        prototypes.put(OpTypes.STDDEV_POP,    (new VoltXMLElement("aggregation")).withValue("optype", "stddevpop"));
        prototypes.put(OpTypes.STDDEV_SAMP,   (new VoltXMLElement("aggregation")).withValue("optype", "stddevsamp"));
        prototypes.put(OpTypes.VAR_POP,       (new VoltXMLElement("aggregation")).withValue("optype", "varpop"));
        prototypes.put(OpTypes.VAR_SAMP,      (new VoltXMLElement("aggregation")).withValue("optype", "varsamp"));
        // windowed aggregate functions
        prototypes.put(OpTypes.WINDOWED_RANK, (new VoltXMLElement("win_aggregation")).withValue("optype", "windowed_rank"));
        prototypes.put(OpTypes.WINDOWED_DENSE_RANK,  (new VoltXMLElement("win_aggregation")).withValue("optype", "windowed_dense_rank"));
        prototypes.put(OpTypes.WINDOWED_COUNT,(new VoltXMLElement("win_aggregation")).withValue("optype", "windowed_count"));
        prototypes.put(OpTypes.WINDOWED_MAX,  (new VoltXMLElement("win_aggregation")).withValue("optype", "windowed_max"));
        prototypes.put(OpTypes.WINDOWED_MIN,  (new VoltXMLElement("win_aggregation")).withValue("optype", "windowed_min"));
        prototypes.put(OpTypes.WINDOWED_SUM,  (new VoltXMLElement("win_aggregation")).withValue("optype", "windowed_sum"));
        prototypes.put(OpTypes.WINDOWED_ROW_NUMBER,  (new VoltXMLElement("win_aggregation")).withValue("optype", "windowed_row_number"));

        // other operations
        prototypes.put(OpTypes.CAST,          (new VoltXMLElement("operation")).withValue("optype", "cast"));
        prototypes.put(OpTypes.CASEWHEN,      (new VoltXMLElement("operation")).withValue("optype", "operator_case_when"));
        prototypes.put(OpTypes.ORDER_BY,      new VoltXMLElement("orderby"));
        prototypes.put(OpTypes.LIMIT,         new VoltXMLElement("limit"));
        prototypes.put(OpTypes.ALTERNATIVE,   (new VoltXMLElement("operation")).withValue("optype", "operator_alternative"));
        prototypes.put(OpTypes.ZONE_MODIFIER, null); // ???
        prototypes.put(OpTypes.MULTICOLUMN,   null); // an uninteresting!? ExpressionColumn case

        // user defined aggregate function
        prototypes.put(OpTypes.USER_DEFINED_AGGREGATE, (new VoltXMLElement("aggregation")).withValue("optype", "user_defined_aggregate"));
    }

    /**
     * A SimpleColumnContext encapsulates the current Session object and
     * (optionally) select statement display columns and related state that may
     * be needed to resolve some column expressions in a statement's expression
     * trees. It is also used to tag pseudo-display-columns that are used solely
     * to resolve column expressions but are NOT actually intended for "display"
     * in the query result set. A SimpleColumnContext is the main vehicle for
     * passing shared context into the recursive Expression.voltGetXML process.
     **/
    static class SimpleColumnContext {
        final Session m_session;
        final List<Expression> m_displayCols;
        final int m_indexLimitVisible;
        final java.util.Set<Integer> m_ignoredDisplayColIndexes = new java.util.HashSet<>();
        private int m_startKey = -1;

        SimpleColumnContext(Session session, List<Expression> displayCols, int indexLimitVisible) {
            m_session = session;
            m_displayCols = displayCols;
            m_indexLimitVisible = indexLimitVisible;
        }

        SimpleColumnContext withStartKey(int startKey) {
            m_startKey = startKey;
            return this;
        }

        private VoltXMLElement resolveSimpleColumn(Expression simpleCol)
                throws HSQLParseException {
            if (m_displayCols == null) {
                if (simpleCol instanceof ExpressionColumn == false) {
                    throw new HSQLParseException(
                            "VoltDB does not support this complex query currently.");
                }
            }
            else {
                // find the substitute from displayCols list
                for (int ii=m_startKey+1; ii < m_displayCols.size(); ++ii) {
                    Expression otherCol = m_displayCols.get(ii);
                    // This mechanism of finding the expression that a SIMPLE_COLUMN
                    // is referring to is inherently fragile---columnIndex is an
                    // offset into different things depending on context!
                    if (otherCol != null && (otherCol.opType != OpTypes.SIMPLE_COLUMN) &&
                             (otherCol.columnIndex == simpleCol.columnIndex)  &&
                             !(otherCol instanceof ExpressionColumn)) {
                        // Prepare to skip displayCols that are the referent of a SIMPLE_COLUMN.
                        m_ignoredDisplayColIndexes.add(ii);
                        // Serialize the column this simple column stands-in for.
                        return otherCol.voltGetXML(this, simpleCol.getAlias());
                    }
                }
            }
            return null;
        }

        public boolean disabledTheColumnForDisplay(int jj) {
            return jj >= m_indexLimitVisible || m_ignoredDisplayColIndexes.contains(jj);
        }


    }

    /**
     * @param session
     * @return
     * @throws HSQLParseException
     */
    VoltXMLElement voltGetXML(Session session) throws HSQLParseException {
        return voltGetXML(new SimpleColumnContext(session, null, 0), null);
    }

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param context The context encapsulates the current Session object and (optionally)
     * select statement display columns that may be needed to resolve some names.
     * @return A VoltXML tree structure.
     * @throws HSQLParseException
     */
    VoltXMLElement voltGetXML(SimpleColumnContext context, String realAlias)
        throws HSQLParseException {
        // The voltXML representations of expressions tends to be driven much more by the expression's opType
        // than its Expression class.
        int exprOp = getType();

        // The opType value of "SIMPLE_COLUMN" is a special case that spans Expression classes and seems to
        // need to use the Expression's exact class to be able to correctly determine its VoltXMLElement
        // representation.
        // Last minute "SIMPLE_COLUMN" substitutions can blast a new opType into an Expression of a class
        // other than ExpressionColumn as an optimization for duplicated expressions.
        // VoltDB currently uses "alias" matching to navigate to the correct (duplicate) expression structure
        // typically an ExpressionAggregate.
        // The prototypes dictionary is set up to handle a SIMPLE_COLUMN of any class EXCEPT ExpressionColumn.
        // A SIMPLE_COLUMN ExpressionColumn can be treated as a normal "COLUMN" ExpressionColumn.
        // That case gets explicitly enabled here by fudging the opType from SIMPLE_COLUMN to COLUMN.
        if (exprOp == OpTypes.SIMPLE_COLUMN) {
            VoltXMLElement asResolved = context.resolveSimpleColumn(this);
            if (asResolved != null) {
                return asResolved;
            }
            // ENG-10429 moved the following two lines here. See fix note https://issues.voltdb.com/browse/ENG-10429.
            // convert the SIMPLE_COLUMN into a COLUMN
            opType = OpTypes.COLUMN;
            exprOp = OpTypes.COLUMN;
        }

        // Use the opType to find a pre-initialized prototype VoltXMLElement with the correct
        // name and any required hard-coded values pre-set.
        VoltXMLElement exp = prototypes.get(exprOp);
        if (exp == null) {
            // Must have found an unsupported opType.
            throwForUnsupportedExpression(exprOp);
        }

        // Duplicate the prototype and add any expression particulars needed for the specific opType value,
        // as well as a unique identifier, a possible alias, and child nodes.
        exp = exp.duplicate();
        exp.attributes.put("id", getUniqueId(context.m_session));
        if (opType == OpTypes.USER_DEFINED_AGGREGATE) {
            if (this instanceof ExpressionAggregate) {
                ExpressionAggregate tempExpr = (ExpressionAggregate) this;
                exp.attributes.put("user_aggregate_id", Integer.toString(tempExpr.getUserAggregateId()));
                exp.attributes.put("name", tempExpr.getName());
            }
        }

        if (realAlias != null) {
            exp.attributes.put("alias", realAlias);
        }
        else if ((alias != null) && (getAlias().length() > 0)) {
            exp.attributes.put("alias", getAlias());
        }

        // Add expression sub type
        if (exprSubType == OpTypes.ANY_QUANTIFIED) {
            exp.attributes.put("opsubtype", "any");
        }
        else if (exprSubType == OpTypes.ALL_QUANTIFIED) {
            exp.attributes.put("opsubtype", "all");
        }

        for (Expression expr : nodes) {
            if (expr != null) {
                VoltXMLElement vxmle = expr.voltGetXML(context, null);
                exp.children.add(vxmle);
                assert(vxmle != null);
            }
        }

        // Few opTypes need additional special case detailing or special case error detection.
        // Very few need access to members defined on specific Expression classes, but they
        // can usually be accessed via down-casting.
        // Even fewer need private members, and they are accessed by delegation to a
        // class-specific voltAnnotate... member function that directly manipulates the
        // VoltXMLElement.
        switch (exprOp) {
        case OpTypes.VALUE:
            // Apparently at this stage, all valid non-NULL values must have a
            // type determined by HSQL.
            // (I'm not sure why this MUST be the case --paul.)
            if (valueData == null) {
                String valueType = (dataType == null) ? "NULL" :
                        Types.getTypeName(dataType.typeCode);
                exp.attributes.put("valuetype", valueType);
                return exp;
            }

            exp.attributes.put("valuetype", Types.getTypeName(dataType.typeCode));

            if (valueData instanceof TimestampData) {
                // When we get the default from the DDL,
                // it gets jammed into a TimestampData object.  If we
                // don't do this, we get a Java class/reference
                // string in the output schema for the DDL.
                // EL HACKO: I'm just adding in the timezone seconds
                // at the moment, hope this is right --izzy
                TimestampData time = (TimestampData) valueData;
                exp.attributes.put("value", Long.toString(Math.round((time.getSeconds() +
                                                                      time.getZone()) * 1e6) +
                                                          time.getNanos() / 1000));
                return exp;
            }

            // convert binary values to hex
            if (valueData instanceof BinaryData) {
                BinaryData bd = (BinaryData) valueData;
                exp.attributes.put("value", hexEncode(bd.getBytes()));
                return exp;
            }

            // Otherwise just string format the value.
            if (dataType instanceof NumberType && ! dataType.isIntegralType()) {
                // remove the scentific exponent notation
                exp.attributes.put("value", new BigDecimal(valueData.toString()).toPlainString());
                return exp;
            }
            exp.attributes.put("value", valueData.toString());
            return exp;

        case OpTypes.COLUMN:
            ExpressionColumn ec = (ExpressionColumn)this;
            return ec.voltAnnotateColumnXML(exp);

        case OpTypes.COALESCE:
            return convertUsingColumnrefToCoaleseExpression(context.m_session, exp, dataType);

        case OpTypes.SQL_FUNCTION:
            FunctionSQL fn = (FunctionSQL)this;
            return fn.voltAnnotateFunctionXML(exp);

        case OpTypes.USER_DEFINED_AGGREGATE:
            exp.attributes.put("valuetype", dataType.getNameString());
            if (((ExpressionAggregate)this).isDistinctAggregate) {
                throw Error.runtimeError(ErrorCode.X_UDAF01, "User-defined aggregate function does not support the 'distinct' keyword");
            }
            return exp;

        case OpTypes.COUNT:
        case OpTypes.SUM:
        case OpTypes.AVG:
            if (((ExpressionAggregate)this).isDistinctAggregate) {
                exp.attributes.put("distinct", "true");
            }
            return exp;

        case OpTypes.ORDER_BY:
            if (((ExpressionOrderBy)this).isDescending()) {
                exp.attributes.put("desc", "true");
            }
            return exp;

        case OpTypes.CAST:
            if (dataType == null) {
                throw new HSQLParseException(
                        "VoltDB could not determine the type in a CAST operation");
            }
            exp.attributes.put("valuetype", dataType.getNameString());
            return exp;

        case OpTypes.TABLE_SUBQUERY:
            if (subQuery == null || subQuery.queryExpression == null) {
                throw new HSQLParseException("VoltDB could not determine the subquery");
            }
            ExpressionColumn parameters[] = new ExpressionColumn[0];
            exp.children.add(StatementQuery.voltGetXMLExpression(subQuery.queryExpression,
                    parameters, context.m_session));
            return exp;

        case OpTypes.ALTERNATIVE:
            assert(nodes.length == 2);
            // If with ELSE clause, pad NULL with it.
            if (nodes[RIGHT] instanceof ExpressionValue) {
                ExpressionValue val = (ExpressionValue) nodes[RIGHT];
                if (val.valueData == null && val.dataType == Type.SQL_ALL_TYPES) {
                    exp.children.get(RIGHT).attributes.put("valuetype", dataType.getNameString());
                }
            }
        case OpTypes.CASEWHEN:
            // Hsql has check dataType can not be null.
            assert(dataType != null);
            exp.attributes.put("valuetype", dataType.getNameString());
            return exp;

        case OpTypes.WINDOWED_RANK:
        case OpTypes.WINDOWED_DENSE_RANK:
        case OpTypes.WINDOWED_ROW_NUMBER:
        case OpTypes.WINDOWED_COUNT:
        case OpTypes.WINDOWED_MIN:
        case OpTypes.WINDOWED_MAX:
        case OpTypes.WINDOWED_SUM:
            assert(dataType != null);
            assert(this instanceof ExpressionWindowed);
            exp.attributes.put("valuetype", dataType.getNameString());
            ExpressionWindowed erank = (ExpressionWindowed) this;
            return erank.voltAnnotateWindowedAggregateXML(exp, context);

        default:
            return exp;
        }
    }

    private static final int caseDiff = ('a' - 'A');
    /**
     *
     * @param data A binary array of bytes.
     * @return A hex-encoded string with double length.
     */
    public static String hexEncode(byte[] data) {
        if (data == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (byte b : data) {
            // hex encoding same way as java.net.URLEncoder.
            char ch = Character.forDigit((b >> 4) & 0xF, 16);
            // to uppercase
            if (Character.isLetter(ch)) {
                ch -= caseDiff;
            }
            sb.append(ch);
            ch = Character.forDigit(b & 0xF, 16);
            if (Character.isLetter(ch)) {
                ch -= caseDiff;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    private static void throwForUnsupportedExpression(int exprOp)
            throws HSQLParseException {
        String opAsString;
        switch (exprOp) {
        //case OpTypes.COALESCE:
        //    opAsString = "the COALESCE operator. Consider using DECODE."; break; //MAY require ExpressionColumn state

        case OpTypes.VARIABLE:
            opAsString = "HSQL session variables"; break; // Some kind of HSQL session parameter? --paul
        case OpTypes.PARAMETER:
            opAsString = "HSQL session parameters"; break; // Some kind of HSQL session parameter? --paul

        case OpTypes.SEQUENCE:
            opAsString = "sequence types"; break; // not yet supported sequence type

        case OpTypes.SCALAR_SUBQUERY:
        case OpTypes.ROW_SUBQUERY:
        case OpTypes.TABLE_SUBQUERY:
        case OpTypes.ROW:
        case OpTypes.TABLE:
        case OpTypes.EXISTS:
            throw new HSQLParseException("Unsupported subquery syntax within an expression. Consider using a join or multiple statements instead");

        case OpTypes.FUNCTION:             opAsString = "HSQL-style user-defined Java SQL functions"; break;

        case OpTypes.ROUTINE_FUNCTION:     opAsString = "HSQL routine functions"; break; // not used

        case OpTypes.ALL_QUANTIFIED:
        case OpTypes.ANY_QUANTIFIED:
            opAsString = "sequences or subqueries"; break; // not used -- an ExpressionLogical exprSubType value only

        case OpTypes.IN:
            opAsString = "the IN operator. Consider using an OR expression"; break; // not yet supported

        case OpTypes.OVERLAPS:
        case OpTypes.UNIQUE:
        case OpTypes.NOT_DISTINCT:
            opAsString = "sequences or subqueries"; break; // not yet supported ExpressionLogical

        case OpTypes.MATCH_SIMPLE:
        case OpTypes.MATCH_PARTIAL:
        case OpTypes.MATCH_FULL:
        case OpTypes.MATCH_UNIQUE_SIMPLE:
        case OpTypes.MATCH_UNIQUE_PARTIAL:
        case OpTypes.MATCH_UNIQUE_FULL:
            opAsString = "the MATCH operator"; break; // not yet supported ExpressionLogical

        case OpTypes.ZONE_MODIFIER:
            opAsString = "ZONE modifier operations"; break; // ???
        case OpTypes.MULTICOLUMN:
            opAsString = "a MULTICOLUMN operation"; break; // an uninteresting!? ExpressionColumn case

        default:
            opAsString = " the unknown operator with numeric code (" + String.valueOf(exprOp) + ")";
        }
        throw new HSQLParseException("VoltDB does not support " + opAsString);
    }

    /**
     * VoltDB added method to simplify an expression by eliminating identical subexpressions (same id)
     * The original expression must be a logical conjunction of form e1 AND e2 AND e3 AND e4.
     * If subexpression e1 is identical to the subexpression e2 the simplified expression would be
     * e1 AND e3 AND e4.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return simplified expression.
     */
    public Expression eliminateDuplicates(final Session session) {
        // First build the map of child expressions joined by the logical AND
        // The key is the expression id and the value is the expression itself
        Map<String, Expression> subExprMap = new HashMap<>();
        extractAndSubExpressions(session, this, subExprMap);
        // Reconstruct the expression
        if (!subExprMap.isEmpty()) {
            Iterator<Map.Entry<String, Expression>> itExpr = subExprMap.entrySet().iterator();
            Expression finalExpr = itExpr.next().getValue();
            while (itExpr.hasNext()) {
                finalExpr = new ExpressionLogical(OpTypes.AND, finalExpr, itExpr.next().getValue());
            }
            return finalExpr;
        }
        return this;
    }

    protected void extractAndSubExpressions(final Session session, Expression expr,
            Map<String, Expression> subExprMap) {
        // If it is a logical expression AND then traverse down the tree
        if (expr instanceof ExpressionLogical && ((ExpressionLogical) expr).opType == OpTypes.AND) {
            extractAndSubExpressions(session, expr.nodes[LEFT], subExprMap);
            extractAndSubExpressions(session, expr.nodes[RIGHT], subExprMap);
        }
        else {
            String id = expr.getUniqueId(session);
            subExprMap.put(id, expr);
       }
    }

    protected String cached_id = null;

    private void traverse(Expression exp, final Session session) {
        for (Expression expr : exp.nodes) {
            if (expr != null)
                expr.getUniqueId(session);
        }
    }

    /**
     * Get the hex address of this Expression Object in memory,
     * to be used as a unique identifier.
     * @return The hex address of the pointer to this object.
     */
    protected String getUniqueId(final Session session) {
        if (cached_id != null) {
            return cached_id;
        }

        //
        // Calculated an new Id
        //

        // this line ripped from the "describe" method
        // seems to help with some types like "equal"
        cached_id = new String();
        //
        // If object is a leaf node, then we'll use John's original code...
        // Otherwise we need to generate and Id based on what our children are
        //
        if (getType() != OpTypes.VALUE && getType() != OpTypes.COLUMN) {
            //
            // Horribly inefficient, but it works for now...
            //
            traverse(this, session);
        }

        long nodeId = session.getNodeIdForExpression(this);

        cached_id = Long.toString(nodeId);
        return cached_id;
    }

    // A VoltDB extension to support indexed expressions
    public VoltXMLElement voltGetExpressionXML(Session session, Table table)
            throws HSQLParseException {
        resolveTableColumns(table);
        Expression parent = null; // As far as I can tell, this argument just gets passed around but never used !?
        resolveTypes(session, parent);
        return voltGetXML(new SimpleColumnContext(session, null, 0), null);
    }

    // A VoltDB extension to support indexed expressions
    private void resolveTableColumns(Table table) {
        HsqlList set = new HsqlArrayList();
        collectAllColumnExpressions(set);
        for (int i = 0; i < set.size(); i++) {
            ExpressionColumn array_element = (ExpressionColumn)set.get(i);
            ColumnSchema column = table.getColumn(table.getColumnIndex(array_element.getAlias()));
            array_element.setAttributesAsColumn(column, false);

        }
    }

    // A VoltDB extension to convert columnref expression for a column that is part of the USING clause
    // into a COALESCE expression
    //      columnref                   operation operator_case_when
    //      columnref T1.C      ->      operation is_null
    //      columnref T2.C                  columnref T1.C
    //                                  operation operator_alternative
    //                                      columnref T2.C
    //                                      columnref T1.C
    private VoltXMLElement convertUsingColumnrefToCoaleseExpression(Session session, VoltXMLElement exp, Type dataType)
            throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException {
        // Hsql has check dataType can not be null.
        assert(dataType != null);
        exp.attributes.put("valuetype", dataType.getNameString());

        // Extract unique columnref
        HashSet<String> tables = new HashSet<>();
        ArrayDeque<VoltXMLElement> uniqueColumnrefs = new ArrayDeque<>();
        for (VoltXMLElement columnref : exp.children) {
            String table = columnref.attributes.get("table");
            String tableAlias = columnref.attributes.get("tablealias");
            assert (table != null);
            String tableOrAlias = (tableAlias == null) ? table : tableAlias;
            if (tables.contains(tableOrAlias)) {
                continue;
            }
            tables.add(tableOrAlias);
            uniqueColumnrefs.add(columnref);
        }
        // Delete original children
        exp.children.clear();

        // There should be at least 2 columnref expressions
        if (uniqueColumnrefs.size() < 2) {
            throw Error.error(ErrorCode.X_42581, "Cannot distinguish column reference between two tables. "
                    + "Use fully qualified names including the table name or alias to avoid ambiguous references");
        }
        VoltXMLElement lastAlternativeExpr = null;
        VoltXMLElement resultColaesceExpr = null;
        while (true) {
            VoltXMLElement next = uniqueColumnrefs.pop();
            if (uniqueColumnrefs.isEmpty()) {
                // Last columnref. Simply plug it in to the last THEN Expression
                assert(lastAlternativeExpr != null);
                // Add next as the first child
                lastAlternativeExpr.children.add(0, next);
                break;
            }
            // IS_NULL expression
            VoltXMLElement isnull_expr = prototypes.get(OpTypes.IS_NULL);
            if (isnull_expr == null) {
                throwForUnsupportedExpression(OpTypes.IS_NULL);
            }
            isnull_expr = isnull_expr.duplicate();
            isnull_expr.attributes.put("id", this.getUniqueId(session));
            isnull_expr.children.add(next);
            // Alternative expression
            VoltXMLElement alt_expr = prototypes.get(OpTypes.ALTERNATIVE);
            if (alt_expr == null) {
                throwForUnsupportedExpression(OpTypes.ALTERNATIVE);
            }
            alt_expr = alt_expr.duplicate();
            alt_expr.attributes.put("id", this.getUniqueId(session));
            alt_expr.attributes.put("valuetype", dataType.getNameString());
            // The next expression should be a second child
            // but for now we keep it as the first one
            alt_expr.children.add(next);

            // COALESCE expression
            VoltXMLElement coalesceExpr = exp.duplicate();
            coalesceExpr.attributes.put("alias", next.attributes.get("alias"));
            coalesceExpr.attributes.put("column", next.attributes.get("column"));

            // Add IS NULL and ALTERNATIVE expressions to the coalesceExpr
            coalesceExpr.children.add(isnull_expr);
            coalesceExpr.children.add(alt_expr);
            if (resultColaesceExpr == null) {
                resultColaesceExpr = coalesceExpr;
            } else {
                assert(lastAlternativeExpr != null);
                // Add coalesceExpr as the first child to the last alternative expression
                lastAlternativeExpr.children.add(0, coalesceExpr);
            }
            lastAlternativeExpr = alt_expr;
        }
        assert(resultColaesceExpr != null);
        return resultColaesceExpr;
    }

    /**
     * This ugly code is never called by HSQL or VoltDB
     * explicitly, but it does make debugging in eclipse
     * easier because it makes expressions display their
     * type when you mouse over them.
     */
    @Override
    public String toString() {
        String type = null;

        // iterate through all optypes, looking for
        // a match...
        // sadly do this with reflection
        Field[] fields = OpTypes.class.getFields();
        for (Field f : fields) {
            if (f.getType() != int.class) {
                continue;
            }
            int value = 0;
            try {
                value = f.getInt(null);
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            // found a match
            if (value == opType) {
                type = f.getName();
                break;
            }
        }
        assert(type != null);

        // return the original default impl + the type
        String str = super.toString() + " with opType " + type +
                ", isAggregate: " + isAggregate +
                ", columnIndex: " + columnIndex;
        if (this instanceof ExpressionOrderBy) {
            str += "\n  " + nodes[LEFT].toString();
        }
        return str;
    }
}
