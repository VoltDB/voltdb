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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.lib.ArrayListIdentity;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of column, variable, parameter, etc. access operations.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ExpressionColumn extends Expression {
    public final static ExpressionColumn[] emptyArray =
        new ExpressionColumn[]{};

    //
    ColumnSchema  column;
    String        schema;
    String        tableName;
    String        columnName;
    RangeVariable rangeVariable;

    //
    NumberSequence sequence;
    boolean        isWritable;    // = false; true if column of writable table

    /**
     * Creates a OpCodes.COLUMN expression
     */
    ExpressionColumn(String schema, String table, String column) {

        super(OpTypes.COLUMN);

        this.schema = schema;
        tableName   = table;
        columnName  = column;
    }

    ExpressionColumn(ColumnSchema column) {

        super(OpTypes.COLUMN);

        columnName = column.getName().name;
    }

    ExpressionColumn(RangeVariable rangeVar, ColumnSchema column) {
        this(rangeVar, column, rangeVar.rangeTable.findColumn(column.getName().name));
    }

    ExpressionColumn(RangeVariable rangeVar, ColumnSchema column, int index) {
        super(OpTypes.COLUMN);
        setAttributesAsColumn(rangeVar, index);
    }

    /**
     * Creates a temporary OpCodes.COLUMN expression
     */
    ExpressionColumn(Expression e, int colIndex, int rangePosition) {

        this(OpTypes.SIMPLE_COLUMN);

        dataType           = e.dataType;
        columnIndex        = colIndex;
        alias              = e.alias;
        this.rangePosition = rangePosition;
    }

    ExpressionColumn() {
        super(OpTypes.ASTERISK);
    }

    ExpressionColumn(int type) {

        super(type);

        if (type == OpTypes.DYNAMIC_PARAM) {
            isParam = true;
        }
    }

    ExpressionColumn(Expression[] nodes, String name) {

        super(OpTypes.COALESCE);

        this.nodes      = nodes;
        this.columnName = name;
    }

    /**
     * Creates an OpCodes.ASTERISK expression
     */
    ExpressionColumn(String schema, String table) {

        super(OpTypes.MULTICOLUMN);

        this.schema = schema;
        tableName   = table;
    }

    /**
     * Creates a OpCodes.SEQUENCE expression
     */
    ExpressionColumn(NumberSequence sequence) {

        super(OpTypes.SEQUENCE);

        this.sequence = sequence;
        dataType      = sequence.getDataType();
    }

    private void setAttributesAsColumn(RangeVariable range, int i) {

        columnIndex   = i;
        column        = range.getColumn(i);
        dataType      = column.getDataType();
        rangeVariable = range;

        if (range.variables == null) {
            Table t = range.getTable();
            columnName  = column.getName().name;
            tableName   = t.getName().name;
            schema      = t.getSchemaName().name;
            if (alias == null && range.hasColumnAlias()) {
                alias = range.getColumnAliasName(i);
            }
            rangeVariable.addColumn(columnIndex);
        }
    }

    @Override
    void setAttributesAsColumn(ColumnSchema column, boolean isWritable) {

        this.column     = column;
        dataType        = column.getDataType();
        this.isWritable = isWritable;
    }

    @Override
    SimpleName getSimpleName() {

        if (alias != null) {
            return alias;
        }

        if (column != null) {
            return column.getName();
        }

        if (opType == OpTypes.COALESCE) {
            return nodes[LEFT].getSimpleName();
        }

        return null;
    }

    @Override
    String getAlias() {

        if (alias != null) {
            return alias.name;
        }

        if (opType == OpTypes.COLUMN) {
            return columnName;
        }

        if (opType == OpTypes.COALESCE) {
            return columnName;
        }

        return "";
    }

    public String getBaseColumnName() {

        if (opType == OpTypes.COLUMN && rangeVariable != null) {
            return rangeVariable.getTable().getColumn(
                columnIndex).getName().name;
        }

        return null;
    }

    public HsqlName getBaseColumnHsqlName() {
        return column.getName();
    }

    @Override
    void collectObjectNames(Set set) {

        // BEGIN Cherry-picked code change from hsqldb-2.3.2
        switch (opType) {

            case OpTypes.SEQUENCE :
                HsqlName name = sequence.getName();

                set.add(name);

                return;

            case OpTypes.MULTICOLUMN :
            case OpTypes.DYNAMIC_PARAM :
            case OpTypes.ASTERISK :
            case OpTypes.SIMPLE_COLUMN :
            case OpTypes.COALESCE :
                break;

            case OpTypes.PARAMETER :
            case OpTypes.VARIABLE :
                break;

            case OpTypes.COLUMN :
                set.add(column.getName());

                if (column.getName().parent != null) {
                    set.add(column.getName().parent);
                }

                return;
        }
        /* Disable 13 lines
        if (opType == OpTypes.SEQUENCE) {
            HsqlName name = ((NumberSequence) valueData).getName();

            set.add(name);

            return;
        }

        set.add(column.getName());

        if (column.getName().parent != null) {
            set.add(column.getName().parent);
        }
        ... disabled 13 lines */
        // END Cherry-picked code change from hsqldb-2.3.2
    }

    @Override
    String getColumnName() {

        if (opType == OpTypes.COLUMN && column != null) {
            return column.getName().name;
        }

        return getAlias();
    }

    @Override
    ColumnSchema getColumn() {
        return column;
    }

    String getSchemaName() {
        return schema;
    }

    @Override
    RangeVariable getRangeVariable() {
        return rangeVariable;
    }

    @Override
    public HsqlList resolveColumnReferences(RangeVariable[] rangeVarArray,
            int rangeCount, HsqlList unresolvedSet, boolean acceptsSequences) {
        switch (opType) {
        case OpTypes.SEQUENCE :
            if (!acceptsSequences) {
                throw Error.error(ErrorCode.X_42598);
            }
            break;

        case OpTypes.MULTICOLUMN :
        case OpTypes.DYNAMIC_PARAM :
        case OpTypes.ASTERISK :
        case OpTypes.SIMPLE_COLUMN :
        case OpTypes.COALESCE :
            break;

        case OpTypes.PARAMETER :
        case OpTypes.VARIABLE :
        case OpTypes.COLUMN :
            if (rangeVariable != null) {
                break;
            }
            // Look in all the range variables.  We may
            // find this column more than once, and that
            // would be an error See ENG-9367.
            unresolvedSet = resolveColumnViaRangeVariables(rangeVarArray, rangeCount, unresolvedSet);
        }
        return unresolvedSet;
    }

    private HsqlList resolveColumnViaRangeVariables(RangeVariable[] rangeVarArray, int rangeCount,
            HsqlList unresolvedSet) {
        //
        // Note that we can't actually commit to a resolution
        // until we have looked everywhere.  This means we need to
        // store up potential resolutions until we have looked at all
        // the range variables.  If we find just one, we finally
        // resolve it below.
        Map<String, ExpressionColumn> usingResolutions = new TreeMap<>();
        Map<String, RangeVariableColumnReferenceResolution>
            rangeVariableResolutions = new TreeMap<>();
        for (int i = 0; i < rangeCount; i++) {
            RangeVariable rangeVar = rangeVarArray[i];
            if (rangeVar == null) {
                continue;
            }
            resolveColumnReference(rangeVar, usingResolutions, rangeVariableResolutions);
        }
        if (usingResolutions.isEmpty()) {
            if (rangeVariableResolutions.isEmpty()) {
                // Didn't find anything.  So, add this expression to the unresolved set.
                if (unresolvedSet == null) {
                    unresolvedSet = new ArrayListIdentity();
                }
                unresolvedSet.add(this);
                return unresolvedSet;
            }
            if (rangeVariableResolutions.size() == 1) {
                for (RangeVariableColumnReferenceResolution exactlyOne :
                    rangeVariableResolutions.values()) {
                    finallyResolve(exactlyOne);
                }
                return unresolvedSet;
            }
        }
        else if (usingResolutions.size() == 1 &&
                rangeVariableResolutions.isEmpty()) {
            for (ExpressionColumn exactlyOne : usingResolutions.values()) {
                finallyResolve(exactlyOne);
            }
            return unresolvedSet;
        }
        // Found more than one resolution.
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("Column \"%s\" is ambiguous.  It's in tables: ", columnName));
        String sep = "";
        // The resolution maps are TreeMaps. List them in name order.
        if (! usingResolutions.isEmpty()) {
            sb.append("USING(");
            appendNameList(sb, usingResolutions.keySet(), "");
            sb.append(")");
            sep = ", ";
        }
        appendNameList(sb, rangeVariableResolutions.keySet(), sep);
        throw new HsqlException(sb.toString(), "", 0);
    }

    private void resolveColumnReference(RangeVariable rangeVar,
            Map<String, ExpressionColumn> usingResolutions,
            Map<String, RangeVariableColumnReferenceResolution> rangeVariableResolutions) {

        if (tableName == null) {
            ExpressionColumn expr = rangeVar.getColumnExpression(columnName);
            if (expr != null) {
                String id;
                if (expr.alias != null && expr.alias.name != null) {
                    id = expr.alias.name;
                }
                else {
                    id = expr.columnName;
                }
                assert id != null;
                if (id == null) {
                    // This should never happen.  We should always have an
                    // alias, or at least a column name.  After all, this will
                    // have been built with "USING(C)" where "C" is a column name.
                    id = "<UnknownColumnName>";
                }
                usingResolutions.put(id, expr);
                return;
            }
        }
        else if (!rangeVar.resolvesTableName(this)) {
            return;
        }

        int colIndex = rangeVar.findColumn(columnName);
        if (colIndex == -1) {
            return;
        }

        int replacementOpType = opType;
        if (tableName == null && rangeVar.variables != null) {
            ColumnSchema column = rangeVar.getColumn(colIndex);
            if (column.getParameterMode()
                    == SchemaObject.ParameterModes.PARAM_OUT) {
                return;
            }
            replacementOpType = rangeVar.isVariable ? OpTypes.VARIABLE
                                                    : OpTypes.PARAMETER;
        }
        RangeVariableColumnReferenceResolution rvRes =
                new RangeVariableColumnReferenceResolution(rangeVar, colIndex,
                                                           replacementOpType);
        rangeVariableResolutions.put(rvRes.toString(), rvRes);
    }

    /*
     * Append the names of all the elements in the set of resolutions to the
     * string buffer.  This is only used for error messages.
     */
    private <T> void appendNameList(StringBuffer sb,
                                    Collection<T> items,
                                    String sep) {
        for (T oneItem : items) {
            sb.append(sep).append(oneItem.toString());
            sep = ", ";
        }
    }

    private static class RangeVariableColumnReferenceResolution {
        final RangeVariable m_rangeVariable;
        final int m_colIndex;
        final int m_opType;

        RangeVariableColumnReferenceResolution(RangeVariable rangeVariable,
                int colIndex, int replacementOpType) {
            assert(rangeVariable != null && 0 <= colIndex);
            m_rangeVariable = rangeVariable;
            m_colIndex = colIndex;
            m_opType = replacementOpType;
        }

        @Override
        public String toString() {
            // Prefer to use an alias.  If there is none, use the table name.
            if (m_rangeVariable.tableAlias != null && m_rangeVariable.tableAlias.name != null) {
                return m_rangeVariable.tableAlias.name;
            }
            if (m_rangeVariable.getTable() != null
                    && m_rangeVariable.getTable().getName() != null
                    && m_rangeVariable.getTable().getName().name != null) {
                return m_rangeVariable.getTable().getName().name;
            }
            return "<UnknownTable>";
        }
    }

    private void finallyResolve(ExpressionColumn expr) {
        opType   = expr.opType;
        nodes    = expr.nodes;
        dataType = expr.dataType;
    }

    private void finallyResolve(RangeVariableColumnReferenceResolution res) {
        setAttributesAsColumn(res.m_rangeVariable, res.m_colIndex);
        opType = res.m_opType;
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {

        switch (opType) {

            case OpTypes.DEFAULT :
                if (parent != null && parent.opType != OpTypes.ROW) {
                    throw Error.error(ErrorCode.X_42544);
                }
                break;

            case OpTypes.COALESCE : {
                Type type = null;

                for (int i = 0; i < nodes.length; i++) {
                    type = Type.getAggregateType(nodes[i].dataType, type);
                }

                dataType = type;

                break;
            }
        }
    }

    @Override
    public Object getValue(Session session) {

        switch (opType) {

            case OpTypes.DEFAULT :
                return null;

            case OpTypes.VARIABLE : {
                return session.sessionContext.routineVariables[columnIndex];
            }
            case OpTypes.PARAMETER : {
                return session.sessionContext.routineArguments[columnIndex];
            }
            case OpTypes.COLUMN : {
                Object[] data =
                    session.sessionContext
                    .rangeIterators[rangeVariable.rangePosition]
                    .getCurrent();
                Object value   = data[columnIndex];
                Type   colType = column.getDataType();

                if (!dataType.equals(colType)) {
                    value = dataType.convertToType(session, value, colType);
                }

                return value;
            }
            case OpTypes.SIMPLE_COLUMN : {
                Object[] data =
                    session.sessionContext
                    .rangeIterators[rangePosition].getCurrent();

                return data[columnIndex];
            }
            case OpTypes.COALESCE : {
                Object value = null;

                for (int i = 0; i < nodes.length; i++) {
                    value = nodes[i].getValue(session, dataType);

                    if (value != null) {
                        return value;
                    }
                }

                return value;
            }
            case OpTypes.DYNAMIC_PARAM : {
                return session.sessionContext.dynamicArguments[parameterIndex];
            }
            case OpTypes.SEQUENCE : {
                return session.sessionData.getSequenceValue(sequence);
            }
            case OpTypes.ASTERISK :
            case OpTypes.MULTICOLUMN :
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    @Override
    public String getSQL() {

        switch (opType) {

            case OpTypes.DEFAULT :
                return Tokens.T_DEFAULT;

            case OpTypes.DYNAMIC_PARAM :
                return Tokens.T_QUESTION;

            case OpTypes.ASTERISK :
                return "*";

            case OpTypes.COALESCE :

//                return columnName;
                return alias.getStatementName();

            case OpTypes.VARIABLE :
            case OpTypes.PARAMETER :
            case OpTypes.COLUMN : {
                if (column == null) {
                    if (alias != null) {
                        return alias.getStatementName();
                    } else {
                        return columnName;
                    }
                }

                // A VoltDB extension to allow toSQL on expressions in DDL
                if (rangeVariable == null || rangeVariable.tableAlias == null) {
                /* disable 1 line ...
                if (rangeVariable.tableAlias == null) {
                ... disabled 1 line */
                // End of VoltDB extension
                    return column.getName().getSchemaQualifiedStatementName();
                } else {
                    StringBuffer sb = new StringBuffer();

                    sb.append(rangeVariable.tableAlias.getStatementName());
                    sb.append('.');
                    sb.append(column.getName().statementName);

                    return sb.toString();
                }
            }
            case OpTypes.MULTICOLUMN : {
                if (nodes.length == 0) {
                    return "*";
                }

                StringBuffer sb = new StringBuffer();

                for (int i = 0; i < nodes.length; i++) {
                    Expression e = nodes[i];

                    if (i > 0) {
                        sb.append(',');
                    }

                    String s = e.getSQL();

                    sb.append(s);
                }

                return sb.toString();
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    @Override
    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        switch (opType) {

            case OpTypes.DEFAULT :
                sb.append(Tokens.T_DEFAULT);
                break;

            case OpTypes.ASTERISK :
                sb.append("OpTypes.ASTERISK ");
                break;

            case OpTypes.VARIABLE :
                sb.append("VARIABLE: ");
                sb.append(column.getName().name);
                break;

            case OpTypes.PARAMETER :
                sb.append(Tokens.T_PARAMETER).append(": ");
                sb.append(column.getName().name);
                break;

            case OpTypes.COALESCE :
                sb.append(Tokens.T_COLUMN).append(": ");
                sb.append(columnName);

                if (alias != null) {
                    sb.append(" AS ").append(alias.name);
                }
                break;

            case OpTypes.COLUMN :
                sb.append(Tokens.T_COLUMN).append(": ");
                sb.append(column.getName().name);

                if (alias != null) {
                    sb.append(" AS ").append(alias.name);
                }

                sb.append(' ').append(Tokens.T_TABLE).append(": ").append(
                    tableName);
                break;

            case OpTypes.DYNAMIC_PARAM :
                sb.append("DYNAMIC PARAM: ");
                sb.append(", TYPE = ").append(dataType.getNameString());
                break;

            case OpTypes.SEQUENCE :
                sb.append(Tokens.T_SEQUENCE).append(": ");
                sb.append(sequence.getName().name);
                break;

            case OpTypes.MULTICOLUMN :

            // shouldn't get here
        }

        return sb.toString();
    }

    /**
     * Returns the table name for a column expression as a string
     *
     * @return table name
     */
    String getTableName() {

        if (opType == OpTypes.MULTICOLUMN) {
            return tableName;
        }

        if (opType == OpTypes.COLUMN) {
            if (rangeVariable == null) {
                return tableName;
            } else {
                return rangeVariable.getTable().getName().name;
            }
        }

        return "";
    }

    static void checkColumnsResolved(HsqlList set) {

        if (set != null && !set.isEmpty()) {
            ExpressionColumn e  = (ExpressionColumn) set.get(0);
            StringBuffer     sb = new StringBuffer();

            if (e.schema != null) {
                sb.append(e.schema + '.');
            }

            if (e.tableName != null) {
                sb.append(e.tableName + '.');
            }

            throw Error.error(ErrorCode.X_42501,
                              sb.toString() + e.getColumnName());
        }
    }

    @Override
    public OrderedHashSet getUnkeyedColumns(OrderedHashSet unresolvedSet) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            unresolvedSet = nodes[i].getUnkeyedColumns(unresolvedSet);
        }

        if (opType == OpTypes.COLUMN
                && !rangeVariable.hasKeyedColumnInGroupBy) {
            if (unresolvedSet == null) {
                unresolvedSet = new OrderedHashSet();
            }

            unresolvedSet.add(this);
        }

        return unresolvedSet;
    }

    /**
     * collects all range variables in expression tree
     */
    @Override
    void collectRangeVariables(RangeVariable[] rangeVariables, Set set) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].collectRangeVariables(rangeVariables, set);
            }
        }

        if (rangeVariable != null) {
            for (int i = 0; i < rangeVariables.length; i++) {
                if (rangeVariables[i] == rangeVariable) {
                    set.add(rangeVariables[i]);
                }
            }
        }
    }

    /*
     * Compare two SELECT list elements that are column references.
     * to determine whether they resolve to the same column.
     * Column names must be the same.
     * If table aliases are not the same, the columns need to be
     * resolved to the same range variable, possibly via a using list.
     */
    private static boolean isSameColumn(ExpressionColumn expressionColumn,
            ExpressionColumn other) {
        // They are both column references.  We want to return
        // 0 if they refer to the same table expression column.
        // This is to say, if they have the same column name and
        // the same table alias or else their column name is in
        // the using list for their range variables.  The columns
        // are assumed to have been already resolved to their range
        // variables.
        if (!expressionColumn.columnName.equals(other.columnName)) {
            return false;
        }
        String tableAlias = expressionColumn.getTableAlias();
        String otherTableAlias = other.getTableAlias();
        if (tableAlias == null) {
            if (otherTableAlias == null) {
                return true;
            }
        }
        else if (tableAlias.equals(other.getTableAlias())) {
            return true;
        }
        // The column expressions are syntactically different,
        // but they may still resolve to the same column.
        RangeVariable rv = expressionColumn.getRangeVariable();
        RangeVariable orv = other.getRangeVariable();
        if (rv == null) {
            // Find out if this column name is in the
            // using list of orv.  If it is, these both
            // denote the same column.
            if (orv != null && orv.getColumnExpression(expressionColumn.columnName) != null) {
                return true;
            }
            return false;
        }
        ExpressionColumn myEC = rv.getColumnExpression(expressionColumn.columnName);
        if (orv == null) {
            // If orv == null but the column names are equal, it
            // could be that orv is an instance of a using variable
            // and rv is the range variable for the column with
            // the same column name.  Remember, getColumnExpression()
            // just gets ExpressionColumns for USING column names.
            if (myEC != null) {
                return true;
            }
            return false;
        }
        ExpressionColumn oEC = orv.getColumnExpression(expressionColumn.columnName);
        // Are they the same in a using list?
        return (myEC != null && oEC != null && myEC == oEC);
    }

    /*
     * For an ExpressionColumn with no table name qualifier in an ORDRER BY
     * expression, look to see if it's an alias for something in the SELECT
     * list.  If it is, we replace the column expression with the SELECT
     * expression to which the alias refers.  Note that this creates a kind
     * of scoping.  If an ORDER BY column expression matches an alias or an
     * unaliased column name in the SELECT list, it is not ambiguous with
     * any other column in the table expression.
     *
     * For example,
     *   select r1.b, b from r1 order by b;
     *   -- b is not ambiguous.  Both select list possibilities,
     *   -- lr.b and b, refer to the same table.
     *
     *   select lr.b from r1 lr, r1 rr order by b;
     *   -- b is not ambiguous.  The b in the order by is
     *   -- replaced by lr.b from the select list.
     *
     *   select lr.b b, rr.b b from r1 lr, r2 rr order by b;
     *   -- b is ambiguous.  There are two aliases named b.
     *
     *   select lr.b b, b from r1 lr join r1 rr using (b) order by b;
     *   -- Mysql and postgresql differ in this case.  It may
     *   -- be because of different rules for USING.
     *   -- This is ambiguous with postgresql, but not
     *   -- with mysql.  A close reading of the standard might
     *   -- suggest that the first reference to lr.b is illegal,
     *   -- since the USING removes column "b" from the table expression
     *   -- columns, so perhaps both are wrong.  Ignoring that,
     *   -- it makes sense that this is unambiguous, since the
     *   -- "b" in the order by, the "b" in the select list and
     *   -- the "b" in "lr.b" are all references of the same
     *   -- column.
     *   select lr.b b, rr.b b from r1 lr join r1 rr using (b) order by b;
     *   -- It's interesting that this, which is almost exactly the
     *      same as the previous case, is ambiguous for both mysql and
     *      postgresql.  Apparently rr.b and ll.b are different to the
     *      mysql SQL compiler, even though the using(b) forces them to
     *      be identical.
     *
     * This function calculates the replacement expression.  We
     * look through the select list, whose expressions are in
     * the parameter columns[0:length-1].  This array may have
     * some other columns, but these are the only ones that represent
     * the SELECT list. For each such Expression, expr, if this column
     * reference matches the expression, put the expression
     * in the list of candidates. Also retain the first Expression added,
     * as this will often be the only once found.  At the
     * end, if there is only one candidate, return it.
     * Otherwise, craft an error message from the list of candidates.
     * If there are no candidates, return this ExpressionColumn unchanged,
     * and allow it to be resolved according to the table expressions in
     * the FROM clause.
     */
    @Override
    Expression replaceAliasInOrderBy(Expression[] columns, int length) {
        // Recurse into sub-expressions.  For example, if
        // this expression is e0 + e1, nodes[0] is e0 and
        // nodes[1] is e1.
        // Either or both of them may be select list aliases.
        //XXX: Yet, e0 + e1 would logically not be typed as an
        // ExpressionColumn, would it? Yet, there appear to be cases in which
        // an ExpressionColumn (though PERHAPS one never found in an ORDER BY
        // clause or never found referencing select list columns from an ORDER
        // BY clause) MAY have child expressions
        // -- see the OpTypes.COALESCE handling
        // throughout this class.
        // "Just in case", call on Expression to process the children here.
        super.replaceAliasInOrderBy(columns, length);

        // Now process the node itself.
        // If this reference has no table name qualifier, try to
        // resolve it as a select list alias. Otherwise, pass it by,
        // so it can be resolved against the FROM clause definitions.
        if (tableName != null) {
            return this;
        }
        // For VoltDB, schema will always be null.
        if (schema != null) {
            return this;
        }
        switch (opType) {
            case OpTypes.COALESCE :
            case OpTypes.COLUMN : {
                // Look through the columns in columns up to length. These are
                // the select list, and they may have aliases equal to
                // the column which we are trying to resolve.  In that
                // case we really want to use the expression in the
                // select list.  Note that we only look for aliases here.
                // Column references which name columns in tables in the
                // from clause are handled later on.
                List<ExpressionColumn> foundColumns = new ArrayList<>();
                List<Integer> foundOthers = new ArrayList<>();
                Expression firstFoundExpr = null;
                for (int i = 0; i < length; i++) {
                    SimpleName selectAliasName = columns[i].alias;
                    String selectAlias = selectAliasName == null ? null : selectAliasName.name;
                    if (selectAlias != null) {
                        if (!columnName.equals(selectAlias)) {
                            continue;
                        }
                    }
                    // Actual column expressions have different methods of
                    // matching columnName and of matching each other to
                    // resolve potential ambiguities.
                    if (columns[i] instanceof ExpressionColumn) {
                        ExpressionColumn ecol = (ExpressionColumn)columns[i];
                        // A column without an alias must match by
                        // its column name.
                        if (selectAlias == null
                            && !columnName.equals(ecol.columnName)) {
                            continue;
                        }
                        addColumnIfUnique(foundColumns, ecol);
                    }
                    else {
                        // A non-column without an alias will never match.
                        if (selectAlias == null) {
                            continue;
                        }
                        foundOthers.add(i);
                    }
                    if (firstFoundExpr == null) {
                        firstFoundExpr = columns[i];
                    }
                }
                // If we only got one answer, then we just return it.
                // If we got more than one, then we print an ambiguous
                // error message.  If we got no answer, we let HSQL
                // handle it in the usual way.
                if (firstFoundExpr != null) {
                    if (foundColumns.size() + foundOthers.size() == 1) {
                        return firstFoundExpr;
                    }
                    StringBuffer sb = new StringBuffer();
                    sb.append("The name \"").append(columnName)
                    .append("\" in an order by expression is ambiguous.  It could mean any of ");
                    appendNameList(sb, foundColumns, "");
                    if (!foundColumns.isEmpty()) {
                        if (!foundOthers.isEmpty()) {
                            sb.append("or ");
                        }
                        sb.append("the columns at indexes ");
                        appendNameList(sb, foundOthers, "");
                    }
                    sb.append(".");
                    throw new HsqlException(sb.toString(), "", 0);
                }
            }
            default :
        }

        return this;
    }

    private static void addColumnIfUnique(List<ExpressionColumn> foundColumns,
            ExpressionColumn ecol) {
        for (ExpressionColumn found : foundColumns) {
            if (isSameColumn(found, ecol)) {
                return;
            }
        }
        foundColumns.add(ecol);
    }

    private String getTableAlias() {
        if (getRangeVariable() == null
                || getRangeVariable().tableAlias == null) {
            return null;
        }
        return getRangeVariable().tableAlias.name;
    }

    @Override
    Expression replaceColumnReferences(RangeVariable range,
                                       Expression[] list) {

        if (opType == OpTypes.COLUMN && rangeVariable == range) {
            return list[columnIndex];
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            nodes[i] = nodes[i].replaceColumnReferences(range, list);
        }

        return this;
    }

    @Override
    int findMatchingRangeVariableIndex(RangeVariable[] rangeVarArray) {

        for (int i = 0; i < rangeVarArray.length; i++) {
            RangeVariable rangeVar = rangeVarArray[i];

            if (rangeVar.resolvesTableName(this)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * return true if given RangeVariable is used in expression tree
     */
    @Override
    boolean hasReference(RangeVariable range) {

        if (range == rangeVariable) {
            return true;
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                if (nodes[i].hasReference(range)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public boolean equals(Expression other) {

        if (other == this) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (opType != other.opType) {
            return false;
        }

        switch (opType) {

            case OpTypes.SIMPLE_COLUMN :
                return columnIndex == other.columnIndex;

            case OpTypes.COALESCE :
                return nodes == other.nodes;

            case OpTypes.COLUMN :
                return (other instanceof ExpressionColumn) &&
                        rangeVariable == ((ExpressionColumn)other).rangeVariable &&
                        column == ((ExpressionColumn)other).column;

            // A VoltDB extension
            case OpTypes.ASTERISK :
                return true;
            // End of VoltDB extension

            default :
                return false;
        }
    }

    /************************* Volt DB Extensions *************************/

    /**
     * VoltDB added method to provide detail for a non-catalog-dependent
     * representation of this HSQLDB object.
     * @return XML, correctly indented, representing this object.
     */
    VoltXMLElement voltAnnotateColumnXML(VoltXMLElement exp)
    {
        if (tableName != null) {
            if (rangeVariable != null && rangeVariable.rangeTable != null &&
                    rangeVariable.tableAlias != null &&
                    rangeVariable.rangeTable.tableType == TableBase.SYSTEM_SUBQUERY) {
                exp.attributes.put("table", rangeVariable.tableAlias.name.toUpperCase());
            } else {
                exp.attributes.put("table", tableName.toUpperCase());
            }
        }
        exp.attributes.put("column", columnName.toUpperCase());
        if ((alias == null) || (getAlias().length() == 0)) {
            exp.attributes.put("alias", columnName.toUpperCase());
        }
        if (rangeVariable != null && rangeVariable.tableAlias != null) {
            exp.attributes.put("tablealias",  rangeVariable.tableAlias.name.toUpperCase());
        }
        return exp;
    }
    /**********************************************************************/
}
