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

import java.util.Comparator;

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

        columnIndex = rangeVar.rangeTable.findColumn(column.getName().name);

        setAttributesAsColumn(rangeVar, columnIndex);
    }

    ExpressionColumn(RangeVariable rangeVar, ColumnSchema column, int index) {

        super(OpTypes.COLUMN);

        columnIndex = index;

        setAttributesAsColumn(rangeVar, columnIndex);
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

    void setAttributesAsColumn(RangeVariable range, int i) {

        if (range.variables != null) {
            columnIndex   = i;
            column        = range.getColumn(i);
            dataType      = column.getDataType();
            rangeVariable = range;
        } else {
            Table t = range.getTable();

            columnIndex = i;
            column      = range.getColumn(i);
            dataType    = column.getDataType();
            columnName  = column.getName().name;
            tableName   = t.getName().name;
            schema      = t.getSchemaName().name;

            if (alias == null && range.hasColumnAlias()) {
                alias = range.getColumnAliasName(i);
            }

            rangeVariable = range;

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
                    = new java.util.TreeSet<ColumnReferenceResolution>(m_comparator);
                java.util.Set<ColumnReferenceResolution> rangeVariableResolutions
                    = new java.util.TreeSet<ColumnReferenceResolution>(m_comparator);
                ColumnReferenceResolution lastRes = null;
                int foundSize = 0;
                for (int i = 0; i < rangeCount; i++) {
                    RangeVariable rangeVar = rangeVarArray[i];

                    if (rangeVar == null) {
                        continue;
                    }

                    ColumnReferenceResolution resolution = resolveColumnReference(rangeVar);
                    if (resolution != null) {
                        if (resolution.isExpression()) {
                            if (usingResolutions.add(resolution)) {
                                foundSize += 1;
                            }
                            // Cache this in case this is the only resolution.
                            lastRes = resolution;
                        } else if (resolution.isRangeVariable()) {
                            if (rangeVariableResolutions.add(resolution)) {
                                foundSize += 1;
                            }
                            // Cache this in case this is the only resolution.
                            lastRes = resolution;
                        } else {
                            assert(false);
                        }
                    }
                }
                if (foundSize == 1) {
                    lastRes.finallyResolve();
                    return unresolvedSet;
                } else if (foundSize > 1) {
                    StringBuffer sb = new StringBuffer();
                    sb.append(String.format("Column \"%s\" is ambiguous.  It's in tables: ", columnName));
                    String sep = "";
                    // Note: The resolution sets are TreeSets.  So we can iterate over them
                    //       in name order.
                    if (usingResolutions.size() > 0) {
                        sb.append("USING(");
                        for (ColumnReferenceResolution crr : usingResolutions) {
                            sb.append(sep).append(crr.getName());
                            sep = ", ";
                        }
                        sb.append(")");
                        sep = ", ";
                    }
                    for (ColumnReferenceResolution crr : rangeVariableResolutions) {
                        sb.append(sep).append(crr.getName());
                        sep = ", ";
                    }
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

    /**
     * Return a sort of closure which is useful for resolving a column reference.
     * The column reference is either an expression or a column in a table, which
     * is named by a range variable.  We keep the expression or the
     * range variable/column index pair here.  We may have several resolutions
     * if the column reference is ambiguous.  We can't actually commit to one
     * until we have examined all of them.  So, we defer changing this object
     * until we are more sure of the reference.
     *
     * We store these in a java.util.HashSet.  So we need to have our own
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
        /**
         * This method calculates the name of the column reference.  Two
         * column references are equal if they have the same name.  This can
         * be a column alias or a table name.
         * @return
         */
        public String getName();
        /**
         * This is used for creating error messages.  We need to know when a
         * potential resolution is in a USING or range variable.
         */
        boolean isExpression();
        boolean isRangeVariable();
    }

    /**
     * Return a {@link ColumnReferenceResolution} from an expression.
     * @param expr
     * @return
     */
    private ColumnReferenceResolution makeExpressionResolution(Expression expr) {
        return this.new ExpressionColumnReferenceResolution(expr);
    }

    /**
     * Return a {@link ColumnReferenceResolution} from a range variable and a
     * column index.
     * @param rangeVariable
     * @param colIndex
     * @return
     */
    private ColumnReferenceResolution makeRangeVariableResolution(RangeVariable rangeVariable, int colIndex) {
        return this.new RangeVariableColumnReferenceResolution(rangeVariable, colIndex);
    }

    private static class ColumnComparator implements Comparator<ColumnReferenceResolution> {

        @Override
        public int compare(ColumnReferenceResolution o1, ColumnReferenceResolution o2) {
            String n1 = o1.getName();
            String n2 = o2.getName();
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
            return getName();
        }

        @Override
        /**
         * We hash the names if there is a name.  Otherwise we
         * has the entire expression or range variable object.
         */
        public int hashCode() {
            return getName().hashCode();
        }

        @Override
        public String getName() {
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

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof ExpressionColumnReferenceResolution)) {
                return false;
            }
            // Test the names.
            ExpressionColumnReferenceResolution other = (ExpressionColumnReferenceResolution)obj;
            String name = getName();
            String otherName = other.getName();
            return (name != m_unknownColumnName)
                    && (otherName != m_unknownColumnName)
                    && name.equals(otherName);
        }

        private ExpressionColumn getOuterType() {
            return ExpressionColumn.this;
        }

        @Override
        public boolean isExpression() {
            return true;
        }

        @Override
        public boolean isRangeVariable() {
            return false;
        }
    }

    private class RangeVariableColumnReferenceResolution implements ColumnReferenceResolution {
        RangeVariable m_rangeVariable;
        int           m_colIndex;
        private final static String m_unknownTableName = "UnknownTable";
        public RangeVariableColumnReferenceResolution(RangeVariable rangeVariable, int colIndex) {
            assert(rangeVariable != null && 0 <= colIndex);
            m_rangeVariable = rangeVariable;
            m_colIndex = colIndex;
        }

        @Override
        public void finallyResolve() {
            setAttributesAsColumn(m_rangeVariable, m_colIndex);
        }
        @Override
        public String toString() {
            return getName();
        }

        /**
         * We hash the names if there is a name.  Otherwise we
         * has the entire expression or range variable object.
         */
        @Override
        public int hashCode() {
            return getName().hashCode();
        }

        @Override
        public boolean isExpression() {
            return false;
        }

        @Override
        public boolean isRangeVariable() {
            return true;
        }

        @Override
        public String getName() {
            // We prefer to aliases.  If we can't find an
            // alias, we use the table name.
            if (m_rangeVariable.tableAlias != null && m_rangeVariable.tableAlias.name != null) {
                return m_rangeVariable.tableAlias.name;
            } else if (m_rangeVariable.getTable() != null
                       && m_rangeVariable.getTable().getName() != null
                       && m_rangeVariable.getTable().getName().name != null) {
                return m_rangeVariable.getTable().getName().name;
            }
            return m_unknownTableName;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof RangeVariableColumnReferenceResolution)) {
                return false;
            }
            RangeVariableColumnReferenceResolution other = (RangeVariableColumnReferenceResolution) obj;
            String otherName = other.getName();
            String name = getName();
            // Test the names.
            return (name != m_unknownTableName)
                    && (otherName != m_unknownTableName)
                    && (name.equals(otherName));
        }
    }

    public ColumnReferenceResolution resolveColumnReference(RangeVariable rangeVar) {

        if (tableName == null) {
            Expression e = rangeVar.getColumnExpression(columnName);

            if (e != null) {
                return makeExpressionResolution(e);
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
                } else {
                    opType = rangeVar.isVariable ? OpTypes.VARIABLE
                                                 : OpTypes.PARAMETER;

                    return makeRangeVariableResolution(rangeVar, colIndex);
                }
            }
        }

        if (!rangeVar.resolvesTableName(this)) {
            return null;
        }

        int colIndex = rangeVar.findColumn(tableName, columnName);

        if (colIndex != -1) {
            return makeRangeVariableResolution(rangeVar, colIndex);
        }

        return null;
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

    @Override
    Expression replaceAliasInOrderBy(Expression[] columns, int length) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            nodes[i] = nodes[i].replaceAliasInOrderBy(columns, length);
        }

        switch (opType) {

            case OpTypes.COALESCE :
            case OpTypes.COLUMN : {
                for (int i = 0; i < length; i++) {
                    SimpleName aliasName = columns[i].alias;
                    String     alias     = aliasName == null ? null
                                                             : aliasName.name;

                    if (schema == null && tableName == null
                            && columnName.equals(alias)) {
                        return columns[i];
                    }
                }

                for (int i = 0; i < length; i++) {
                    if (columns[i] instanceof ExpressionColumn) {
                        if (this.equals(columns[i])) {
                            return columns[i];
                        }

                        if (tableName == null && schema == null
                                && columnName
                                    .equals(((ExpressionColumn) columns[i])
                                        .columnName)) {
                            return columns[i];
                        }
                    }
                }
            }
            default :
        }

        return this;
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
                return this.columnIndex == other.columnIndex;

            case OpTypes.COALESCE :
                return nodes == other.nodes;

            case OpTypes.COLUMN :
                return column == other.getColumn();

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
        //TODO: also indicate RangeVariable in case table is ambiguus (for self-joins).
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
