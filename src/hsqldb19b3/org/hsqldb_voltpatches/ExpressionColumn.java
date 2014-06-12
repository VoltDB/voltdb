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

    void setAttributesAsColumn(ColumnSchema column, boolean isWritable) {

        this.column     = column;
        dataType        = column.getDataType();
        this.isWritable = isWritable;
    }

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

    void collectObjectNames(Set set) {

        if (opType == OpTypes.SEQUENCE) {
            HsqlName name = ((NumberSequence) valueData).getName();

            set.add(name);

            return;
        }

        set.add(column.getName());

        if (column.getName().parent != null) {
            set.add(column.getName().parent);
        }
    }

    String getColumnName() {

        if (opType == OpTypes.COLUMN && column != null) {
            return column.getName().name;
        }

        return getAlias();
    }

    ColumnSchema getColumn() {
        return column;
    }

    String getSchemaName() {
        return schema;
    }

    RangeVariable getRangeVariable() {
        return rangeVariable;
    }

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

                for (int i = 0; i < rangeCount; i++) {
                    RangeVariable rangeVar = rangeVarArray[i];

                    if (rangeVar == null) {
                        continue;
                    }

                    if (resolveColumnReference(rangeVar)) {
                        return unresolvedSet;
                    }
                }

                if (unresolvedSet == null) {
                    unresolvedSet = new ArrayListIdentity();
                }

                unresolvedSet.add(this);
        }

        return unresolvedSet;
    }

    public boolean resolveColumnReference(RangeVariable rangeVar) {

        if (tableName == null) {
            Expression e = rangeVar.getColumnExpression(columnName);

            if (e != null) {
                opType   = e.opType;
                nodes    = e.nodes;
                dataType = e.dataType;

                return true;
            }

            if (rangeVar.variables != null) {
                int colIndex = rangeVar.findColumn(columnName);

                if (colIndex == -1) {
                    return false;
                }

                ColumnSchema column = rangeVar.getColumn(colIndex);

                if (column.getParameterMode()
                        == SchemaObject.ParameterModes.PARAM_OUT) {
                    return false;
                } else {
                    opType = rangeVar.isVariable ? OpTypes.VARIABLE
                                                 : OpTypes.PARAMETER;

                    setAttributesAsColumn(rangeVar, colIndex);

                    return true;
                }
            }
        }

        if (!rangeVar.resolvesTableName(this)) {
            return false;
        }

        int colIndex = rangeVar.findColumn(columnName);

        if (colIndex != -1) {
            setAttributesAsColumn(rangeVar, colIndex);

            return true;
        }

        return false;
    }

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
                    (Object[]) session.sessionContext
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
                    (Object[]) session.sessionContext
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
