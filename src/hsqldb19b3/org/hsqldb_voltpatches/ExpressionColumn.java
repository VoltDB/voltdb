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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

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
    private static final ColumnComparator m_comparator = new ColumnComparator();
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
        super(OpTypes.COLUMN);
        setAttributesAsColumn(rangeVar,
                rangeVar.rangeTable.findColumn(column.getName().name));
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

    void setAttributesAsColumn(RangeVariable range, int index) {
        columnIndex   = index;
        column        = range.getColumn(index);
        dataType      = column.getDataType();
        rangeVariable = range;
        // Note: rangeVariable.variables is only non-null in the
        // special case of a RangeVariable that is rigged to contain
        // system settings.
        // There's a good chance that this special case is not
        // exercised by VoltDB.
        if (rangeVariable.variables != null) {
            return;
        }

        columnName  = column.getName().name;
        Table table = range.getTable();
        tableName   = table.getName().name;
        schema      = table.getSchemaName().name;
        if (alias == null && rangeVariable.hasColumnAliases()) {
            alias = rangeVariable.getColumnAliasName(index);
        }
        rangeVariable.addColumn(columnIndex);
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
                    return unresolvedSet;
                }
                // Look in all the range variables.  We may
                // find this column more than once, and that
                // would be an error See ENG-9367.
                //
                // Note that we can't actually commit to a resolution
                // until we have looked everywhere.  This means we need to
                // store up potential resolutions until we have looked at all
                // the range variables.  If we find just one, we finally
                // resolve it. below.
                java.util.Set<ColumnReferenceResolution> usingResolutions
                    = new java.util.TreeSet<>(m_comparator);
                java.util.Set<ColumnReferenceResolution> rangeVariableResolutions
                    = new java.util.TreeSet<>(m_comparator);
                ColumnReferenceResolution lastRes = null;
                int foundSize = 0;
                for (int i = 0; i < rangeCount; i++) {
                    RangeVariable rangeVar = rangeVarArray[i];

                    if (rangeVar == null) {
                        continue;
                    }

                    ColumnReferenceResolution resolution = resolveColumnReference(rangeVar);
                    if (resolution != null) {
                        if (resolution instanceof ExpressionColumnReferenceResolution) {
                            if (usingResolutions.add(resolution)) {
                                foundSize += 1;
                            }
                        }
                        else {
                            assert(resolution instanceof RangeVariableColumnReferenceResolution);
                            if (rangeVariableResolutions.add(resolution)) {
                                foundSize += 1;
                            }
                        }
                        // Cache this in case this is the only resolution.
                        lastRes = resolution;
                    }
                }
                if (foundSize == 1) {
                    lastRes.finallyResolve();
                    return unresolvedSet;
                }
                if (foundSize > 1) {
                    StringBuffer sb = new StringBuffer();
                    sb.append(String.format("Column \"%s\" is ambiguous.  It's in tables: ", columnName));
                    String sep = "";
                    // Note: The resolution sets are TreeSets.  So we can iterate over them
                    //       in name order.
                    if (usingResolutions.size() > 0) {
                        sb.append("USING(");
                        appendNameList(sb, usingResolutions, "");
                        sb.append(")");
                        sep = ", ";
                    }
                    appendNameList(sb, rangeVariableResolutions, sep);
                    throw new HsqlException(sb.toString(), "", 0);
                }
                // If we get here we didn't find anything.  So, add this expression
                // to the unresolved set.
                if (unresolvedSet == null) {
                    unresolvedSet = new ArrayListIdentity();
                }

                unresolvedSet.add(this);
        }
        // IF we got to here, return the set of unresolved columns.
        return unresolvedSet;
    }

    /*
     * Append the names of all the elements in the set of resolutions to the
     * string buffer.  This is only used for error messages.
     */
    private <T> void appendNameList(StringBuffer sb,
                                    java.util.Set<T> resolutions,
                                    String sep) {
        for (T oneRes : resolutions) {
            sb.append(sep).append(oneRes.toString());
            sep = ", ";
        }
    }

    /**
     * Return a sort of closure which is useful for resolving a column reference.
     * The column reference is either an expression or a column in a table, which
     * is named by a range variable.  We keep the expression or the
     * range variable/column index pair here.  We may have several resolutions
     * if the column reference is ambiguous.  We can't actually commit to one
     * until we have examined all of them.  So, we defer changing this object
     * until we are more sure of the reference.
     *
     * We store these in a java.util.TreeSet.  So we need to have our own
     * notion of equality.
     */
    private interface ColumnReferenceResolution {
        /**
         * This is the important operation for this interface.  This
         * member function calculates the final resolution.  We call this after we have
         * verified that there is only one possible resolution for this
         * column name.
         */
        public void finallyResolve();
    }

    private static class ColumnComparator implements Comparator<ColumnReferenceResolution> {

        @Override
        public int compare(ColumnReferenceResolution o1, ColumnReferenceResolution o2) {
            String n1 = o1.toString();
            String n2 = o2.toString();
            return n1.compareTo(n2);
        }

    }

    /**
     * This class implements the interface for expression columns.
     * An expression column is created for a "USING(C)" join condition.
     * In this case, the expression will be an ExpressionColumn referencing
     * the column "C", which presumably is a column common to two joined
     * tables.
     */
    private class ExpressionColumnReferenceResolution implements ColumnReferenceResolution {
        Expression    m_expr;
        private static final String m_unknownColumnName = "UnknownColumnName";
        public ExpressionColumnReferenceResolution(Expression expr) {
            assert(expr != null);
            assert(expr instanceof ExpressionColumn);
            m_expr = expr;
        }

        @Override
        public void finallyResolve() {
            opType   = m_expr.opType;
            nodes    = m_expr.nodes;
            dataType = m_expr.dataType;
        }

        @Override
        public String toString() {
            ExpressionColumn ec = (ExpressionColumn)m_expr;
            if (ec.alias != null && ec.alias.name != null) {
                return ec.alias.name;
            }
            if (ec.columnName != null) {
                return ec.columnName;
            }
            /*
             * This should never happen.  We should always have an
             * alias, or at least a column name.  After all, this will
             * have been built with "USING(C)" where "C" is a column
             * name.
             */
            return m_unknownColumnName;
        }

    }

    private class RangeVariableColumnReferenceResolution implements ColumnReferenceResolution {
        final RangeVariable m_rangeVariable;
        final int           m_colIndex;
        final int           m_replacementOpType;
        private final static String m_unknownTableName = "UnknownTable";
        public RangeVariableColumnReferenceResolution(RangeVariable rangeVariable,
                int colIndex, int replacementOpType) {
            assert(rangeVariable != null && 0 <= colIndex);
            m_rangeVariable = rangeVariable;
            m_colIndex = colIndex;
            m_replacementOpType = replacementOpType;
        }

        @Override
        public void finallyResolve() {
            setAttributesAsColumn(m_rangeVariable, m_colIndex);
            opType = m_replacementOpType;
        }
        @Override
        public String toString() {
            // We prefer to use aliases.  If we can't find an
            // alias, we use the table name.
            if (m_rangeVariable.tableAlias != null && m_rangeVariable.tableAlias.name != null) {
                return m_rangeVariable.tableAlias.name;
            }
            if (m_rangeVariable.getTable() != null
                       && m_rangeVariable.getTable().getName() != null
                       && m_rangeVariable.getTable().getName().name != null) {
                return m_rangeVariable.getTable().getName().name;
            }
            return m_unknownTableName;
        }

    }

    public ColumnReferenceResolution resolveColumnReference(RangeVariable rangeVar) {

        if (tableName == null) {
            Expression e = rangeVar.getColumnExpression(columnName);

            if (e != null) {
                return new ExpressionColumnReferenceResolution(e);
            }

            if (rangeVar.variables != null) {
                int colIndex = rangeVar.findColumn(tableName, columnName);

                if (colIndex == -1) {
                    return null;
                }

                ColumnSchema column = rangeVar.getColumn(colIndex);

                if (column.getParameterMode()
                        == SchemaObject.ParameterModes.PARAM_OUT) {
                    return null;
                }
                int replacementOpType = rangeVar.isVariable ? OpTypes.VARIABLE
                        : OpTypes.PARAMETER;
                return new RangeVariableColumnReferenceResolution(
                        rangeVar, colIndex, replacementOpType);
            }
        }

        if (!rangeVar.resolvesTableName(this)) {
            return null;
        }

        int colIndex = rangeVar.findColumn(tableName, columnName);
        if (colIndex == -1) {
            return null;
        }
        return new RangeVariableColumnReferenceResolution(rangeVar, colIndex, opType);
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
                if (alias != null) {
                    return alias.getStatementName();
                }
                return columnName;

            case OpTypes.VARIABLE :
            case OpTypes.PARAMETER :
            case OpTypes.COLUMN : {
                if (column == null) {
                    if (alias != null) {
                        return alias.getStatementName();
                    }
                    return columnName;
                }

                if (rangeVariable == null || rangeVariable.tableAlias == null) {
                    return column.getName().getSchemaQualifiedStatementName();
                }
                StringBuffer sb = new StringBuffer();
                sb.append(rangeVariable.tableAlias.getStatementName());
                sb.append('.');
                sb.append(column.getName().statementName);
                return sb.toString();
            }

            case OpTypes.MULTICOLUMN : {
                if (nodes.length == 0) {
                    return "*";
                }

                StringBuffer sb = new StringBuffer();
                String prefix = "";
                for (Expression e : nodes) {
                    String s = e.getSQL();
                    sb.append(prefix).append(s);
                    prefix = ",";
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
                sb.append(", TYPE = ").append((dataType != null) ? dataType.getNameString() : "null");
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
     * @return table name
     */
    String getTableName() {
        if (opType == OpTypes.MULTICOLUMN) {
            return tableName;
        }

        if (opType == OpTypes.COLUMN) {
            if (rangeVariable == null) {
                return tableName;
            }
            return rangeVariable.getTable().getName().name;
        }
        return "";
    }

    static void checkColumnsResolved(HsqlList set) {

        if (set != null && !set.isEmpty()) {
            Object obj = set.get(0);
            if (obj instanceof ExpressionColumn) {
                ExpressionColumn e  = (ExpressionColumn) obj;
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
            else {
                assert(obj instanceof ExpressionAggregate);
                throw Error.error(ErrorCode.X_47000);
            }
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
     * This class holds the name of an expression to which a column
     * reference in an order by resolves.  This is actually for error messages, not debugging.
     * The indices are useful only if tableName != null.
     *
     * The T.C case occurs when one references a select list
     * element T.C in a column reference. For example:
     *
     *       select T.C, T.C from T where T.C > 0;
     *
     * In this case, T.C is repeated twice, but it's always the same column in the
     * same table. So, the indices, which is to say the order in the select list,
     * would be irrelevant. That's why the compareTo method only cares about
     * names if there is a table name. If we see this SQL:
     *
     *     select C, C from T where C > 0;
     *
     * We know C is not an alias, and so we look up C first, and then we
     * look up C's table name. So we will be in the non-null table name case.
     *
     * If there is not a table name, that is to say if tableName == null,
     * then we are looking at an alias. This would be the case for this SQL:
     *
     *   select T.C as CC, T.E, T.D as CC from T where CC > 0;
     *
     * In this case the aliases CC are equal and we care about the indices.
     * We want to give an error message which says
     *        "CC occurs in columns: CC(1), CC(3)".
     * The numbers in parentheses are the indices, and the order in which
     * the alias occurs in the select list.  Note that in this case,
     * with two select list expressions aliased as CC, the name CC is not
     * resolvable anywhere.
     */
    private static class SelectListAliasResolution implements Comparable<SelectListAliasResolution> {
        // This is the expression in the select list.
        private final Expression m_expression;
        // The table alias from the range variable.  This is different
        // from the table name in queries like "select .. from T as A..."
        // where A is the alias and T is the name.
        private final String m_tableAlias;
        // The column name.  This may be null for a general expression.
        private final String m_columnName;
        // The alias of the column from the select list.  If there
        // is no alias, this is the column name.
        private final String m_alias;
        // The zero-based index of the column.  This is the order
        // of this column in the select list.
        //
        // This is mostly used to disambiguate if the name is
        // not equal.
        private final int m_index;

        public SelectListAliasResolution(Expression expr, String selectTableAlias,
                String selectColumnName, String alias, int index) {
            m_expression = expr;
            m_tableAlias = selectTableAlias;
            m_columnName = selectColumnName;
            // Even if m_columnName is null, m_alias will be non-null.
            // This can happen in statements like:
            //     SELECT T.X * R.Y Z AS PROD FROM T, R ORDER BY Z
            // where there is no canonical table name or column name
            // for Z.
            assert alias != null;
            m_alias = alias;
            m_index = index;
        }

        public final ExpressionColumn getExpressionColumn() {
            if (m_expression instanceof ExpressionColumn) {
                return (ExpressionColumn)m_expression;
            }
            return null;
        }

        @Override
        public String toString() {
            if (!m_alias.equals(m_columnName)) {
                return m_alias + "(" + m_index + ")";
            }
            if (m_tableAlias == null) {
                return m_alias;
            }
            return m_tableAlias + "." + m_columnName;
        }

        // Note: not used by TreeSet. @See compareTo
        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (!(other instanceof SelectListAliasResolution)) {
                return false;
            }
            // The real equals test is here.  We defer to compareTo.
            SelectListAliasResolution aliasOther = (SelectListAliasResolution)other;
            int nc = compareTo(aliasOther);
            return nc == 0;
        }

        /*
         * We are comparing two select list elements.
         *   o The simple case is that they are neither column references.
         *     This can happen if they are more general expressions, such
         *     as "(x + 1) as a" compared to a column reference a.
         *     The m_expressionColumn will be null in the general expression
         *     case.  In this case we just compare the index.  Mostly
         *     these will not be equal, but we need a reliable order.
         *   o The more complicated case is if they are both column references.
         *     In this case, m_expressionColumn will be non-null, and this
         *     ExpressionColumn should have a range variable.  We mostly
         *     ignore any column aliases, since we want to know if these
         *     two column references are to the same column.  So we compare
         *     table aliases and column names, and we look up the column
         *     name in the using list.  These will be in the range variable.
         * Note that we don't care about aliases at all here.
         */
        @Override
        public int compareTo(SelectListAliasResolution o) {
            ExpressionColumn ecol = getExpressionColumn();
            ExpressionColumn oecol = o.getExpressionColumn();
            if (ecol == null || oecol == null) {
                // We only want to be equal if the other's index is
                // equal to ours.  This will always be false, I think.
                return m_index - o.m_index;
            }

            // They are both column references.  We want to return
            // 0 if they refer to the same table expression column.
            // This is to say, if they have the same column name and
            // the same table alias or else their column name is in
            // the using list for their range variables.  It has to
            // be in the using list for both range variables.  In
            // a query like this:
            //   select C from R as LR join R as CR using(C), R as RR order by C;
            // C is in the using list of the first join (LR and CR) but
            // not the third (RR).  So, this would be ambiguous.
            int nc = m_columnName.compareTo(o.m_columnName);
            if (nc != 0) {
                return nc;
            }
            RangeVariable rv = ecol.getRangeVariable();
            RangeVariable orv = oecol.getRangeVariable();
            if (rv == null) {
                if (orv == null) {
                    return m_index - o.m_index;
                }
                // Find out if this column name is in the
                // using list of orv.  If it is, these both
                // denote the same column.  We want to use the
                // alias here because we always have an alias
                // and we may not have a column name.
                if (orv.getColumnExpression(m_alias) != null) {
                    return 0;
                }
                return m_index - o.m_index;
            }
            if (orv == null) {
                // If orv == null but the column names are equal, it
                // could be that orv is an instance of a using variable
                // and rv is the range variable for the column with
                // the same column name.  Remember, getColumnExpression()
                // just gets ExpressionColumns for USING column names.
                if (rv.getColumnExpression(m_columnName) != null) {
                    return 0;
                }
                return m_index - o.m_index;
            }
            ExpressionColumn myEC = rv.getColumnExpression(m_columnName);
            ExpressionColumn oEC = orv.getColumnExpression(m_columnName);

            if (myEC != null && oEC != null && myEC == oEC) {
                // They are the same in a using list.
                return 0;
            }
            assert(m_tableAlias != null);
            return m_tableAlias.compareTo(o.m_tableAlias);
        }
    }

    /*
     * If we see an ExpressionColumn in an order by expression,
     * we we look to see if it's an alias for something in the
     * select list.  If it is, we replace the column expression
     * with the expression to which the alias refers.  Note that
     * this creates a kind of scoping.  If a column reference
     * name matches an alias or an unaliased column name in a
     * select list it has replaced, and is not ambiguous with
     * any other column in the table expression.
     *
     * For example,
     *   select r1.b, b from r1 order by b;
     *   -- b is not ambiguous.  Both select list possibilities,
     *   -- lr.b and b, are to the same table.
     *
     *   select lr.b from r1 lr, r1 rr order by b;
     *   -- b is not ambiguous.  The b in the order by is
     *   -- replaced by lr.b from the select list.
     *
     *   select lr.b b, rr.b b from r1 lr, r2 rr order by b;
     *   -- b is ambiguous.  There are two aliases named b.
     *   select lr.b b, b from r1 lr join r1 rr using (b) order by b;
     *   -- Mysql and postgresql differ in this case.  It may
     *   -- be because of different rules for using.
     *   -- This is ambiguous with postgresql, but not
     *   -- with mysql.  A close reading of the standard might
     *   -- suggest that the first reference to lr.b is illegal,
     *   -- since the USING removes column "b" from the table expression
     *   -- columns, so perhaps both are wrong.  If we ignore that,
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
     * some other columns, but these are the only ones we care
     * about.  For each such Expression, expr, if this column
     * reference matches the expression, then put the expression
     * in the list of candidates. Since we only want one, we
     * will remember the last Expression we added.  At the
     * end, if there is only one candidate, we return the last
     * expression we added.  Otherwise we craft an error message
     * from the list of candidates.  If there are no candidates
     * we just return this, and let HSQL return its cryptic not-found
     * message.
     *
     * Consider a variant of the first example above.
     *   select lr.a, lr.a a, a from r1 lr order by a;
     * Here all of these are references to the same table
     * expression column.  So, these are all unambiguous.
     * So, when we add each of these columns to the set,
     * of candidates we want only one to be added.  We
     * need to know when two such expressions refer to the
     * same table expression column.  We also want to know
     * if the column reference from the select list is
     * in a using list.
     */
    @Override
    Expression replaceAliasInOrderBy(Expression[] columns, int length) {

        // Recurse into sub-expressions.  For example, if
        // this expression is e0 + e1, nodes[0] is e0 and
        // nodes[1] is e1.
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }
            nodes[i] = nodes[i].replaceAliasInOrderBy(columns, length);
        }
        // Now process the node itself.
        switch (opType) {
            case OpTypes.COALESCE :
            case OpTypes.COLUMN : {
                // Look through all the columns in columns.  These are
                // the select list, and they may have aliases equal to
                // the column which we are trying to resolve.  In that
                // case we really want to use the expression in the
                // select list.  Note that we only look for aliases here.
                // Column references which name columns in tables in the
                // from clause are handled later on.
                java.util.Set<SelectListAliasResolution> foundNames = new java.util.TreeSet<>();
                Expression firstFoundExpr = null;
                for (int i = 0; i < length; i++) {
                    ExpressionColumn ecol = (columns[i] instanceof ExpressionColumn) ? (ExpressionColumn)columns[i] : null;
                    // Ferret out the table name, column name
                    // and alias name from this select column.
                    // If the alias name is null, then use the column name.
                    // This may be null as well.
                    String     selectTableName = null;
                    String     selectTableAlias = null;
                    String     selectColumnName = null;
                    if (ecol != null) {
                        selectTableName = ecol.getTableName();
                        selectTableAlias = ecol.getTableAlias();
                        if (selectTableAlias == null) {
                            selectTableAlias = selectTableName;
                        }
                        selectColumnName = ecol.columnName;
                    }
                    SimpleName selectAliasName = columns[i].alias;
                    String     selectAlias     = selectAliasName == null ? null : selectAliasName.name;
                    if (selectAlias == null) {
                        selectAlias = selectColumnName;
                    }
                    // For VoltDB, schema will always be null.
                    if (schema == null) {
                        // If this reference has no table name, then
                        // just compare the aliases.  If this reference
                        // has a table name this is handled
                        // by the usual lookup rules.
                        if (tableName == null) {
                            if (columnName.equals(selectAlias)) {
                                foundNames.add(new SelectListAliasResolution(columns[i],
                                                                             selectTableAlias,
                                                                             selectColumnName,
                                                                             selectAlias,
                                                                             i));
                                if (firstFoundExpr == null) {
                                    firstFoundExpr = columns[i];
                                }
                            }
                        }
                    }
                }
                // If we only got one answer, then we just return it.
                // If we got more than one, then we print an ambiguous
                // error message.  If we got no answer, we let HSQL
                // handle it in the usual way.
                if (foundNames.size() == 1) {
                    return firstFoundExpr;
                }
                if (foundNames.size() > 1) {
                    StringBuffer sb = new StringBuffer();
                    sb.append(String.format("The name \"%s\" in an order by expression is ambiguous.  It's in columns: ", columnName));
                    appendNameList(sb, foundNames, "");
                    sb.append(".");
                    throw new HsqlException(sb.toString(), "", 0);
                }
            }
            default :
        }

        return this;
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
    public int hashCode() {
        // A VoltDB extension
        int val = Objects.hashCode(opType);
        switch (opType) {
        case OpTypes.SIMPLE_COLUMN :
            return val + Objects.hashCode(columnIndex) ;

        case OpTypes.COALESCE :
            return val + Arrays.hashCode(nodes);

        case OpTypes.COLUMN :
            return val + Objects.hashCode(rangeVariable) + Objects.hashCode(column);

        case OpTypes.ASTERISK :
            return val;

        case OpTypes.DYNAMIC_PARAM :
            return val + Objects.hashCode(parameterIndex);
        // End of VoltDB extension

        default :
            return super.hashCode();
    }
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
            case OpTypes.DYNAMIC_PARAM :
                return parameterIndex == other.parameterIndex;
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
    VoltXMLElement voltAnnotateColumnXML(VoltXMLElement exp) {
        if (tableName != null) {
            if (rangeVariable != null && rangeVariable.rangeTable != null &&
                    rangeVariable.tableAlias != null &&
                    rangeVariable.rangeTable.tableType == TableBase.SYSTEM_SUBQUERY) {
                exp.attributes.put("table", rangeVariable.tableAlias.name.toUpperCase());
            }
            else {
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
        exp.attributes.put("index", Integer.toString(columnIndex));
        return exp;
    }
    /**********************************************************************/
}
