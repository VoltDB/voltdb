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


package org.hsqldb;

import java.util.ArrayList;
import org.hsqldb.HSQLInterface.HSQLParseException;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;

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
     * @param indent A string of whitespace to be prepended to every line
     * in the resulting XML.
     * @param params The parameters (if any) to this compiled SELECT
     * statement. These are not available to this object, so they are
     * hackily passed to this method here.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    @Override
    String voltGetXML(Session session, String orig_indent)
    throws HSQLParseException
    {
        QuerySpecification select = (QuerySpecification) queryExpression;

        try {
            getResult(session);
        }
        catch (HsqlException e)
        {
            throw new HSQLParseException(e.getMessage());
        }
        catch (Exception e)
        {
            // XXX coward.
        }

        StringBuffer sb = new StringBuffer();
        String indent = orig_indent + HSQLInterface.XML_INDENT;

        // select
        sb.append(orig_indent).append("<select");
        if (select.isDistinctSelect)
            sb.append(" distinct=\"true\"");
        if (select.isGrouped)
            sb.append(" grouped=\"true\"");
        if (select.isAggregated)
            sb.append(" aggregated=\"true\"");

        // limit
        if ((select.sortAndSlice != null) && (select.sortAndSlice.limitCondition != null)) {
            Expression limitCondition = select.sortAndSlice.limitCondition;
            if (limitCondition.nodes.length != 2) {
                throw new HSQLParseException("Parser did not create limit and offset expression for LIMIT.");
            }
            try {
                // read offset. it may be a parameter token.
                if (limitCondition.nodes[0].isParam() == false) {
                    Integer offset = (Integer)limitCondition.nodes[0].getValue(session);
                    if (offset > 0) {
                        sb.append(" offset=\"" + offset + " \"");
                    }
                }
                else {
                    sb.append(" offset_paramid=\"" + limitCondition.nodes[0].getUniqueId() + "\"");
                }

                // read limit. it may be a parameter token.
                if (limitCondition.nodes[1].isParam() == false) {
                    Integer limit = (Integer)limitCondition.nodes[1].getValue(session);
                    sb.append(" limit=\"" + limit + "\"");
                }
                else {
                    sb.append(" limit_paramid=\"" + limitCondition.nodes[1].getUniqueId() + "\"");
                }
            } catch (HsqlException ex) {
                // XXX really?
                ex.printStackTrace();
            }
        }

        sb.append(">\n");

        // columns
        sb.append(indent + "<columns>\n");

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
            } else if (expr.opType == OpTypes.SIMPLE_COLUMN && expr.isAggregate && expr.alias != null) {
                // Add aggregate aliases to the display columns to maintain
                // the output schema column ordering.
                displayCols.add(expr);
            } else if (expr.opType == OpTypes.SIMPLE_COLUMN) {
                // Other simple columns are ignored. If others exist, maybe
                // volt infers a display column from another column collection?
            } else {
                displayCols.add(expr);
            }
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
        for (int jj=0; jj < displayCols.size(); ++jj) {
            Expression expr = displayCols.get(jj);
            if (expr == null) {
                continue;
            }
            else if (expr.opType == OpTypes.SIMPLE_COLUMN)
            {
                // simple columns are not serialized as display columns
                // but they are place holders for another column
                // in the output schema. Go find that corresponding column
                // and serialize it in this place.
                for (int ii=jj; ii < displayCols.size(); ++ii)
                {
                    Expression otherCol = displayCols.get(ii);
                    if (otherCol == null) {
                        continue;
                    }
                    else if ((otherCol.opType != OpTypes.SIMPLE_COLUMN) &&
                             (otherCol.columnIndex == expr.columnIndex))
                    {
                        // serialize the column this simple column stands-in for
                        sb.append(otherCol.voltGetXML(session, indent + HSQLInterface.XML_INDENT)).append("\n");
                        // null-out otherCol to not serialize it twice
                        displayCols.set(ii, null);
                        // quit seeking simple_column's replacement
                        break;
                    }
                }
            }
            else {
                sb.append(expr.voltGetXML(session, indent + HSQLInterface.XML_INDENT)).append("\n");
            }
        }

        sb.append(indent + "</columns>\n");

        // parameters
        sb.append(indent + "<parameters>\n");
        for (int i = 0; i < parameters.length; i++) {
            sb.append(indent + HSQLInterface.XML_INDENT + "<parameter index='").append(i).append("'");
            ExpressionColumn param = parameters[i];
            sb.append(" id='").append(param.getUniqueId()).append("'");
            sb.append(" type='").append(Types.getTypeName(param.getDataType().typeCode)).append("'");
            sb.append(" />\n");
        }
        sb.append(indent + "</parameters>\n");

        // scans
        sb.append(indent + "<tablescans>\n");
        for (RangeVariable rangeVariable : rangeVariables)
            sb.append(rangeVariable.voltGetXML(session, indent + HSQLInterface.XML_INDENT)).append("\n");
        sb.append(indent + "</tablescans>\n");

        // conditions
        if (select.queryCondition != null) {
            sb.append(indent).append("<querycondition>\n");
            sb.append(select.queryCondition.voltGetXML(session, indent + HSQLInterface.XML_INDENT)).append("\n");
            sb.append(indent).append("</querycondition>\n");
        }
        else {
            // look for inner joins expressed on range variables
            Expression cond = null;
            for (int rvi=0; rvi < select.rangeVariables.length; ++rvi) {
                RangeVariable rv = rangeVariables[rvi];
                // joins on non-indexed columns for inner join tokens created a range variable
                // and assigned this expression.
                if (rv.nonIndexJoinCondition != null) {
                    if (cond != null) {
                        cond = new ExpressionLogical(OpTypes.AND, cond, rv.nonIndexJoinCondition);
                    } else {
                        cond = rv.nonIndexJoinCondition;
                    }
                }
                // joins on indexed columns for inner join tokens created a range variable
                // and assigned an expression and set the flag isJoinIndex.
                else if (rv.isJoinIndex) {
                    if (rv.indexCondition != null) {
                        if (cond != null) {
                            cond = new ExpressionLogical(OpTypes.AND, cond, rv.indexCondition);
                        } else {
                            cond = rv.indexCondition;
                        }
                    }
                    if (rv.indexEndCondition != null) {
                        if (cond != null) {
                            cond = new ExpressionLogical(OpTypes.AND, cond, rv.indexCondition);
                        } else {
                            cond = rv.indexCondition;
                        }
                    }
                }
            }
            if (cond != null) {
                sb.append(indent).append("<querycondition>\n");
                sb.append(cond.voltGetXML(session, indent + HSQLInterface.XML_INDENT)).append("\n");
                sb.append(indent).append("</querycondition>\n");
            }
        }

        // having
        if (select.havingCondition != null) {
            sb.append(indent).append("<havingcondition>\n");
            sb.append(select.havingCondition.voltGetXML(session, indent + HSQLInterface.XML_INDENT)).append("\n");
            sb.append(indent).append("</havingcondition>\n");
        }

        // groupby
        if (select.isGrouped) {
            sb.append(indent + "<groupcolumns>\n");
            for (Expression groupByCol : groupByCols) {
                sb.append(groupByCol.voltGetXML(session, indent + HSQLInterface.XML_INDENT)).append("\n");
            }
            sb.append(indent + "</groupcolumns>\n");
        }
        // orderby
        if (orderByCols.size() > 0) {
            sb.append(indent + "<ordercolumns>\n");
            for (Expression orderByCol : orderByCols)
                sb.append(orderByCol.voltGetXML(session, indent + HSQLInterface.XML_INDENT)).append("\n");
            sb.append(indent + "</ordercolumns>\n");
        }

        sb.append(orig_indent).append("</select>");

        return sb.toString();
    }
}
