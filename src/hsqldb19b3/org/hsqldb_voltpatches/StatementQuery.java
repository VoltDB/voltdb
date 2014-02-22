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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultMetaData;

/**
 * Implementation of Statement for query expressions.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementQuery extends StatementDMQL {

    StatementQuery(Session session, QueryExpression queryExpression,
                   CompileContext compileContext) {

        super(StatementTypes.SELECT_CURSOR, StatementTypes.X_SQL_DATA,
              session.currentSchema);

        this.queryExpression = queryExpression;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    StatementQuery(Session session, QueryExpression queryExpression,
                   CompileContext compileContext, HsqlName[] targets) {

        super(StatementTypes.SELECT_SINGLE, StatementTypes.X_SQL_DATA,
              session.currentSchema);

        this.queryExpression = queryExpression;

        setDatabseObjects(compileContext);
        checkAccessRights(session);
    }

    Result getResult(Session session) {

        Result result = queryExpression.getResult(session,
            session.getMaxRows());

        result.setStatement(this);

        return result;
    }

    public ResultMetaData getResultMetaData() {

        switch (type) {

            case StatementTypes.SELECT_CURSOR :
                return queryExpression.getMetaData();

            case StatementTypes.SELECT_SINGLE :
                return queryExpression.getMetaData();

            default :
                throw Error.runtimeError(
                    ErrorCode.U_S0500,
                    "CompiledStatement.getResultMetaData()");
        }
    }

    void getTableNamesForRead(OrderedHashSet set) {

        queryExpression.getBaseTableNames(set);

        for (int i = 0; i < subqueries.length; i++) {
            if (subqueries[i].queryExpression != null) {
                subqueries[i].queryExpression.getBaseTableNames(set);
            }
        }
    }

    void getTableNamesForWrite(OrderedHashSet set) {}

    /************************* Volt DB Extensions *************************/

    private static class Pair<T, U> {
        protected final T m_first;
        protected final U m_second;

        public Pair(T first, U second) {
            m_first = first;
            m_second = second;
        }

        /**
         * @return the first
         */
        public T getFirst() {
            return m_first;
        }

        /**
         * @return the second
         */
        public U getSecond() {
            return m_second;
        }

        /**
         * Convenience class method for constructing pairs using Java's generic type
         * inference.
         */
        public static <T extends Comparable<T>, U> Pair<T, U> of(T x, U y) {
            return new Pair<T, U>(x, y);
        }
    }
    /**
     * Returns true if the specified exprColumn index is in the list of column indices specified by groupIndex
     * @return true/false
     */
    boolean isGroupByColumn(QuerySpecification select, int index) {
        if (!select.isGrouped) {
            return false;
        }
        for (int ii = 0; ii < select.groupIndex.getColumnCount(); ii++) {
            if (index == select.groupIndex.getColumns()[ii]) {
                return true;
            }
        }
        return false;
    }

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
     */
    @Override
    VoltXMLElement voltGetStatementXML(Session session)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        return voltGetXMLExpression(queryExpression, session);
    }

    VoltXMLElement voltGetXMLExpression(QueryExpression queryExpr, Session session)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        // "select" statements/clauses are always represented by a QueryExpression of type QuerySpecification.
        // The only other instances of QueryExpression are direct QueryExpression instances instantiated in XreadSetOperation
        // to represent UNION, etc.
        int exprType = queryExpr.getUnionType();
        if (exprType == QueryExpression.NOUNION) {
            // "select" statements/clauses are always represented by a QueryExpression of type QuerySpecification.
            if (! (queryExpr instanceof QuerySpecification)) {
                throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException(
                        queryExpr.operatorName() + " is not supported.");
            }
            QuerySpecification select = (QuerySpecification) queryExpr;
            return voltGetXMLSpecification(select, session);
        } else if (exprType == QueryExpression.UNION || exprType == QueryExpression.UNION_ALL ||
                   exprType == QueryExpression.EXCEPT || exprType == QueryExpression.EXCEPT_ALL ||
                   exprType == QueryExpression.INTERSECT || exprType == QueryExpression.INTERSECT_ALL){
            VoltXMLElement unionExpr = new VoltXMLElement("union");
            unionExpr.attributes.put("uniontype", queryExpr.operatorName());

            VoltXMLElement leftExpr = voltGetXMLExpression(
                    queryExpr.getLeftQueryExpression(), session);
            VoltXMLElement rightExpr = voltGetXMLExpression(
                    queryExpr.getRightQueryExpression(), session);
            /**
             * Try to merge parent and the child nodes for UNION and INTERSECT (ALL) set operation.
             * In case of EXCEPT(ALL) operation only the left child can be merged with the parent in order to preserve
             * associativity - (Select1 EXCEPT Select2) EXCEPT Select3 vs. Select1 EXCEPT (Select2 EXCEPT Select3)
             */
            if ("union".equalsIgnoreCase(leftExpr.name) &&
                    queryExpr.operatorName().equalsIgnoreCase(leftExpr.attributes.get("uniontype"))) {
                unionExpr.children.addAll(leftExpr.children);
            } else {
                unionExpr.children.add(leftExpr);
            }
            if (exprType != QueryExpression.EXCEPT && exprType != QueryExpression.EXCEPT_ALL &&
                "union".equalsIgnoreCase(rightExpr.name) &&
                queryExpr.operatorName().equalsIgnoreCase(rightExpr.attributes.get("uniontype"))) {
                unionExpr.children.addAll(rightExpr.children);
            } else {
                unionExpr.children.add(rightExpr);
            }
            return unionExpr;
        } else {
            throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException(
                    queryExpr.operatorName() + "  tuple set operator is not supported.");
        }
    }

    VoltXMLElement voltGetXMLSpecification(QuerySpecification select, Session session)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException {

        // select
        VoltXMLElement query = new VoltXMLElement("select");
        if (select.isDistinctSelect)
            query.attributes.put("distinct", "true");

        // limit
        if ((select.sortAndSlice != null) && (select.sortAndSlice.limitCondition != null)) {
            Expression limitCondition = select.sortAndSlice.limitCondition;
            if (limitCondition.nodes.length != 2) {
                throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException(
                    "Parser did not create limit and offset expression for LIMIT.");
            }
            try {
                // read offset. it may be a parameter token.
                VoltXMLElement offset = new VoltXMLElement("offset");
                if (limitCondition.nodes[0].isParam == false) {
                    Integer offsetValue = (Integer)limitCondition.nodes[0].getValue(session);
                    if (offsetValue > 0) {
                        Expression expr = new ExpressionValue(offsetValue,
                                org.hsqldb_voltpatches.types.Type.SQL_BIGINT);
                        offset.children.add(expr.voltGetXML(session));
                        offset.attributes.put("offset", offsetValue.toString());
                    }
                } else {
                    offset.attributes.put("offset_paramid", limitCondition.nodes[0].getUniqueId(session));
                }
                query.children.add(offset);

                // read limit. it may be a parameter token.
                VoltXMLElement limit = new VoltXMLElement("limit");
                if (limitCondition.nodes[1].isParam == false) {
                    Integer limitValue = (Integer)limitCondition.nodes[1].getValue(session);
                    Expression expr = new ExpressionValue(limitValue,
                            org.hsqldb_voltpatches.types.Type.SQL_BIGINT);
                    limit.children.add(expr.voltGetXML(session));
                    limit.attributes.put("limit", limitValue.toString());
                } else {
                    limit.attributes.put("limit_paramid", limitCondition.nodes[1].getUniqueId(session));
                }
                query.children.add(limit);

            } catch (HsqlException ex) {
                // XXX really?
                ex.printStackTrace();
            }
        }

        // Just gather a mish-mash of every possible relevant expression
        // and uniq them later
        org.hsqldb_voltpatches.lib.HsqlList col_list = new org.hsqldb_voltpatches.lib.HsqlArrayList();
        select.collectAllExpressions(col_list, Expression.columnExpressionSet, Expression.emptyExpressionSet);
        if (select.queryCondition != null)
        {
            Expression.collectAllExpressions(col_list, select.queryCondition,
                                             Expression.columnExpressionSet,
                                             Expression.emptyExpressionSet);
        }
        for (int i = 0; i < select.exprColumns.length; i++) {
            Expression.collectAllExpressions(col_list, select.exprColumns[i],
                                             Expression.columnExpressionSet,
                                             Expression.emptyExpressionSet);
        }
        for (RangeVariable rv : select.rangeVariables)
        {
            if (rv.indexCondition != null)
            {
                Expression.collectAllExpressions(col_list, rv.indexCondition,
                                                 Expression.columnExpressionSet,
                                                 Expression.emptyExpressionSet);

            }
            if (rv.indexEndCondition != null)
            {
                Expression.collectAllExpressions(col_list, rv.indexEndCondition,
                                                 Expression.columnExpressionSet,
                                                 Expression.emptyExpressionSet);

            }
            if (rv.nonIndexJoinCondition != null)
            {
                Expression.collectAllExpressions(col_list, rv.nonIndexJoinCondition,
                                                 Expression.columnExpressionSet,
                                                 Expression.emptyExpressionSet);

            }
        }

        // columns
        VoltXMLElement cols = new VoltXMLElement("columns");
        query.children.add(cols);

        java.util.ArrayList<Expression> orderByCols = new java.util.ArrayList<Expression>();
        java.util.ArrayList<Expression> groupByCols = new java.util.ArrayList<Expression>();
        java.util.ArrayList<Expression> displayCols = new java.util.ArrayList<Expression>();
        java.util.ArrayList<Pair<Integer, HsqlNameManager.SimpleName>> aliases =
                new java.util.ArrayList<Pair<Integer, HsqlNameManager.SimpleName>>();

        /*
         * select.exprColumn stores all of the columns needed by HSQL to
         * calculate the query's result set. It contains more than just the
         * columns in the output; for example, it contains columns representing
         * aliases, columns for groups, etc.
         *
         * Volt uses multiple collections to organize these columns.
         *
         * Observing this loop in a debugger, the following seems true:
         *
         * 1. Columns in exprColumns that appear in the output schema, appear in
         * exprColumns in the same order that they occur in the output schema.
         *
         * 2. expr.columnIndex is an index back in to the select.exprColumns
         * array. This allows multiple exprColumn entries to refer to each
         * other; for example, an OpType.SIMPLE_COLUMN type storing an alias
         * will have its columnIndex set to the offset of the expr it aliases.
         */
        for (int i = 0; i < select.exprColumns.length; i++) {
            final Expression expr = select.exprColumns[i];

            if (expr.alias != null) {
                /*
                 * Remember how aliases relate to columns. Will iterate again later
                 * and mutate the exprColumn entries setting the alias string on the aliased
                 * column entry.
                 */
                if (expr instanceof ExpressionColumn) {
                    ExpressionColumn exprColumn = (ExpressionColumn)expr;
                    if (exprColumn.alias != null && exprColumn.columnName == null) {
                        aliases.add(Pair.of(expr.columnIndex, expr.alias));
                    }
                } else if (expr.columnIndex > -1) {
                    /*
                     * Only add it to the list of aliases that need to be
                     * propagated to columns if the column index is valid.
                     * ExpressionArithmetic will have an alias but not
                     * necessarily a column index.
                     */
                    aliases.add(Pair.of(expr.columnIndex, expr.alias));
                }
            }

            // If the column doesn't refer to another exprColumn entry, set its
            // column index to itself. If all columns have a valid column index,
            // it's easier to patch up display column ordering later.
            if (expr.columnIndex == -1) {
                expr.columnIndex = i;
            }

            if (isGroupByColumn(select, i)) {
                groupByCols.add(expr);
            } else if (expr.opType == OpTypes.ORDER_BY) {
                orderByCols.add(expr);
            } else if (expr.equals(select.getHavingCondition())) {
                // Having
                if( !(expr instanceof ExpressionLogical && expr.isAggregate) ) {
                    throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException(
                            "VoltDB does not support HAVING clause without aggregation. " +
                            "Consider using WHERE clause if possible");
                }

            } else if (expr.opType != OpTypes.SIMPLE_COLUMN || (expr.isAggregate && expr.alias != null)) {
                // Add aggregate aliases to the display columns to maintain
                // the output schema column ordering.
                displayCols.add(expr);
            }
            // else, other simple columns are ignored. If others exist, maybe
            // volt infers a display column from another column collection?
        }

        for (Pair<Integer, HsqlNameManager.SimpleName> alias : aliases) {
            // set the alias data into the expression being aliased.
            select.exprColumns[alias.getFirst()].alias = alias.getSecond();
        }

        /*
         * The columns chosen above as display columns aren't always the same
         * expr objects HSQL would use as display columns - some data were
         * unified (namely, SIMPLE_COLUMN aliases were pushed into COLUMNS).
         *
         * However, the correct output schema ordering was correct in exprColumns.
         * This order was maintained by adding SIMPLE_COLUMNs to displayCols.
         *
         * Now need to serialize the displayCols, serializing the non-simple-columns
         * corresponding to simple_columns for any simple_columns that woodchucks
         * could chuck.
         *
         * Serialize the display columns in the exprColumn order.
         */
        java.util.Set<Integer> ignoredColsIndexes = new java.util.HashSet<Integer>();
        // having
        Expression havingCondition = select.getHavingCondition();
        if (havingCondition != null) {
            VoltXMLElement having = new VoltXMLElement("having");
            query.children.add(having);
            VoltXMLElement havingExpr = havingCondition.voltGetXML(session, displayCols, ignoredColsIndexes, 0);
            having.children.add(havingExpr);
        }

        for (int jj=0; jj < displayCols.size(); ++jj) {
            Expression expr = displayCols.get(jj);
            if (ignoredColsIndexes.contains(jj)) {
                continue;
            }
            VoltXMLElement xml = expr.voltGetXML(session, displayCols, ignoredColsIndexes, jj);
            cols.children.add(xml);
            assert(xml != null);
        }

        // parameters
        voltAppendParameters(session, query);

        // scans
        VoltXMLElement scans = new VoltXMLElement("tablescans");
        query.children.add(scans);
        assert(scans != null);

        for (RangeVariable rangeVariable : select.rangeVariables) {
            scans.children.add(rangeVariable.voltGetRangeVariableXML(session));
        }

        // groupby
        if (select.isGrouped) {
            VoltXMLElement groupCols = new VoltXMLElement("groupcolumns");
            query.children.add(groupCols);

            for (int jj=0; jj < groupByCols.size(); ++jj) {
                Expression expr = groupByCols.get(jj);
                VoltXMLElement xml = expr.voltGetXML(session, displayCols, ignoredColsIndexes, jj);
                groupCols.children.add(xml);
            }
        }

        // orderby
        if (orderByCols.size() > 0) {
            VoltXMLElement orderCols = new VoltXMLElement("ordercolumns");
            query.children.add(orderCols);
            for (int jj=0; jj < orderByCols.size(); ++jj) {
                Expression expr = orderByCols.get(jj);
                VoltXMLElement xml = expr.voltGetXML(session, displayCols, ignoredColsIndexes, jj);
                orderCols.children.add(xml);
            }
        }

        // Columns from USING expression in join are not qualified.
        // if join is INNER then the column from USING expression can be from any table
        // participating in join. In case of OUTER join, it must be the outer column
        java.util.List<VoltXMLElement> exprCols = new java.util.ArrayList<VoltXMLElement>();
        extractColumnReferences(query, exprCols);
        resolveUsingColumns(exprCols, select.rangeVariables);

        return query;
    }

    /**
     * Extract columnref elements from the input element.
     * @param element
     * @param cols - output collection containing the column references
     */
    protected void extractColumnReferences(VoltXMLElement element, java.util.List<VoltXMLElement> cols) {
        if ("columnref".equalsIgnoreCase(element.name)) {
            cols.add(element);
        } else {
            for (VoltXMLElement child : element.children) {
                extractColumnReferences(child, cols);
            }
        }
    }

    /**
     * Columns from USING expression are unqualified. In case of INNER join, it doesn't matter
     * we can pick the first table which contains the input column. In case of OUTER joins, we must
     * the OUTER table - if it's a null-able column the outer join must return them.
     * @param columns list of columns to resolve
     * @return rvs list of range variables
     */
    protected void resolveUsingColumns(java.util.List<VoltXMLElement> columns, RangeVariable[] rvs)
            throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException {
        // Only one OUTER join for a whole select is supported so far
        for (VoltXMLElement columnElmt : columns) {
            String table = null;
            String tableAlias = null;
            if (columnElmt.attributes.get("table") == null) {
                columnElmt.attributes.put("using", "true");
                for (RangeVariable rv : rvs) {
                    if (!rv.getTable().columnList.containsKey(columnElmt.attributes.get("column"))) {
                        // The column is not from this table. Skip it
                        continue;
                    }

                    // If there is an OUTER join we need to pick the outer table
                    if (rv.isRightJoin == true) {
                        // this is the outer table. no need to search further.
                        table = rv.getTable().getName().name;
                        if (rv.tableAlias != null) {
                            tableAlias = rv.tableAlias.name;
                        } else {
                            tableAlias = null;
                        }
                        break;
                    } else if (rv.isLeftJoin == false) {
                        // it's the inner join. we found the table but still need to iterate
                        // just in case there is an outer table we haven't seen yet.
                        table = rv.getTable().getName().name;
                        if (rv.tableAlias != null) {
                            tableAlias = rv.tableAlias.name;
                        } else {
                            tableAlias = null;
                        }
                    }
                }
                if (table != null) {
                    columnElmt.attributes.put("table", table);
                }
                if (tableAlias != null) {
                    columnElmt.attributes.put("tablealias", tableAlias);
                }
            }
        }
    }
    /*********************************************************************/
}
