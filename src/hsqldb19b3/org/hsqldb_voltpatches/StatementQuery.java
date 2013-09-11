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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlList;
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

    @Override
    Result getResult(Session session) {

        Result result = queryExpression.getResult(session,
            session.getMaxRows());

        result.setStatement(this);

        return result;
    }

    @Override
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

    @Override
    void getTableNamesForRead(OrderedHashSet set) {

        queryExpression.getBaseTableNames(set);

        for (SubQuery subquerie : subqueries) {
            if (subquerie.queryExpression != null) {
                subquerie.queryExpression.getBaseTableNames(set);
            }
        }
    }

    @Override
    void getTableNamesForWrite(OrderedHashSet set) {}

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

    /*************** VOLTDB *********************/

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    @Override
    VoltXMLElement voltGetStatementXML(Session session)
    throws HSQLParseException
    {
        return voltGetXMLExpression(queryExpression, session);
    }

    VoltXMLElement voltGetXMLExpression(QueryExpression queryExpr, Session session)
    throws HSQLParseException
    {
        // "select" statements/clauses are always represented by a QueryExpression of type QuerySpecification.
        // The only other instances of QueryExpression are direct QueryExpression instances instantiated in XreadSetOperation
        // to represent UNION, etc.
        int exprType = queryExpr.getUnionType();
        if (exprType == QueryExpression.NOUNION) {
            // "select" statements/clauses are always represented by a QueryExpression of type QuerySpecification.
            if (! (queryExpr instanceof QuerySpecification)) {
                throw new HSQLParseException(queryExpr.operatorName() + " is not supported.");
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
            throw new HSQLParseException(queryExpression.operatorName() + "  tuple set operator is not supported.");
        }
    }

    VoltXMLElement voltGetXMLSpecification(QuerySpecification select, Session session)
    throws HSQLParseException {

        // select
        VoltXMLElement query = new VoltXMLElement("select");
        if (select.isDistinctSelect)
            query.attributes.put("distinct", "true");

        // limit
        if ((select.sortAndSlice != null) && (select.sortAndSlice.limitCondition != null)) {
            Expression limitCondition = select.sortAndSlice.limitCondition;
            if (limitCondition.nodes.length != 2) {
                throw new HSQLParseException("Parser did not create limit and offset expression for LIMIT.");
            }
            try {
                // read offset. it may be a parameter token.
                if (limitCondition.nodes[0].isParam == false) {
                    Integer offset = (Integer)limitCondition.nodes[0].getValue(session);
                    if (offset > 0) {
                        query.attributes.put("offset", offset.toString());
                    }
                }
                else {
                    query.attributes.put("offset_paramid", limitCondition.nodes[0].getUniqueId(session));
                }

                // read limit. it may be a parameter token.
                if (limitCondition.nodes[1].isParam == false) {
                    Integer limit = (Integer)limitCondition.nodes[1].getValue(session);
                    query.attributes.put("limit", limit.toString());
                }
                else {
                    query.attributes.put("limit_paramid", limitCondition.nodes[1].getUniqueId(session));
                }
            } catch (HsqlException ex) {
                // XXX really?
                ex.printStackTrace();
            }
        }

        // columns that need to be output by the scans
        VoltXMLElement scanCols = new VoltXMLElement("scan_columns");
        query.children.add(scanCols);
        assert(scanCols != null);

        // Just gather a mish-mash of every possible relevant expression
        // and uniq them later
        HsqlList col_list = new HsqlArrayList();
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
        HsqlList uniq_col_list = new HsqlArrayList();
        for (int i = 0; i < col_list.size(); i++)
        {
            Expression orig = (Expression)col_list.get(i);
            if (!uniq_col_list.contains(orig))
            {
                uniq_col_list.add(orig);
            }
        }
        for (int i = 0; i < uniq_col_list.size(); i++)
        {
            VoltXMLElement xml = ((Expression)uniq_col_list.get(i)).voltGetXML(session);
            scanCols.children.add(xml);
            assert(xml != null);
        }

        // columns
        VoltXMLElement cols = new VoltXMLElement("columns");
        query.children.add(cols);

        ArrayList<Expression> orderByCols = new ArrayList<Expression>();
        ArrayList<Expression> groupByCols = new ArrayList<Expression>();
        ArrayList<Expression> displayCols = new ArrayList<Expression>();
        ArrayList<Pair<Integer, SimpleName>> aliases = new ArrayList<Pair<Integer, SimpleName>>();

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
            } else if (expr.opType != OpTypes.SIMPLE_COLUMN || (expr.isAggregate && expr.alias != null)) {
                // Add aggregate aliases to the display columns to maintain
                // the output schema column ordering.
                displayCols.add(expr);
            }
            // else, other simple columns are ignored. If others exist, maybe
            // volt infers a display column from another column collection?
        }

        for (Pair<Integer, SimpleName> alias : aliases) {
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
        Set<Integer> ignoredColsIndexes = new HashSet<Integer>();
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

        // Columns from USING expression in join are not qualified.
        // if join is INNER then the column from USING expression can be from any table
        // participating in join. In case of OUTER join, it must be the outer column
        resolveUsingColumns(cols, select.rangeVariables);

        // having
        if (select.havingCondition != null) {
            throw new HSQLParseException("VoltDB does not support the HAVING clause");
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

        return query;
    }

    /**
     * Columns from USING expression are unqualified. In case of INNER join, it doesn't matter
     * we can pick the first table which contains the input column. In case of OUTER joins, we must
     * the OUTER table - if it's a null-able column the outer join must return them.
     * @param columnName
     * @return table name this column belongs to
     */
    protected void resolveUsingColumns(VoltXMLElement columns, RangeVariable[] rvs) throws HSQLParseException {
        // Only one OUTER join for a whole select is supported so far
        for (VoltXMLElement columnElmt : columns.children) {
            boolean innerJoin = true;
            String table = null;
            if (columnElmt.attributes.get("table") == null) {
                for (RangeVariable rv : rvs) {
                    if (rv.isLeftJoin || rv.isRightJoin) {
                        if (innerJoin == false) {
                            throw new HSQLParseException("VoltDB does not support outer joins with more than two tables involved");
                        }
                        innerJoin = false;
                    }

                    if (!rv.getTable().columnList.containsKey(columnElmt.attributes.get("column"))) {
                        // The column is not from this table. Skip it
                        continue;
                    }

                    // If there is an OUTER join we need to pick the outer table
                    if (rv.isRightJoin == true) {
                        // this is the outer table. no need to search further.
                        table = rv.getTable().getName().name;
                        break;
                    } else if (rv.isLeftJoin == false) {
                        // it's the inner join. we found the table but still need to iterate
                        // just in case there is an outer table we haven't seen yet.
                        table = rv.getTable().getName().name;
                    }
                }
                if (table != null) {
                    columnElmt.attributes.put("table", table);
                }
            }
        }
    }

}
