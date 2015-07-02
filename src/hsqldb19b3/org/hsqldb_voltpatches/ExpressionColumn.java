/* Copyright (c) 2001-2014, The HSQL Development Group
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
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayListIdentity;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.navigator.RangeIterator;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of column, variable, parameter, etc. access operations.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class ExpressionColumn extends Expression {

    public static final ExpressionColumn[] emptyArray =
        new ExpressionColumn[]{};
    static final SimpleName rownumName =
        HsqlNameManager.getSimpleName("ROWNUM", false);

    //
    public final static HashMappedList diagnosticsList = new HashMappedList();
    final static String[] diagnosticsVariableTokens    = new String[] {
        Tokens.T_NUMBER, Tokens.T_MORE, Tokens.T_ROW_COUNT
    };
    public final static int            idx_number      = 0;
    public final static int            idx_more        = 1;
    public final static int            idx_row_count   = 2;

    static {
        for (int i = 0; i < diagnosticsVariableTokens.length; i++) {
            HsqlName name = HsqlNameManager.newSystemObjectName(
                diagnosticsVariableTokens[i], SchemaObject.VARIABLE);
            Type type = Type.SQL_INTEGER;

            if (diagnosticsVariableTokens[i] == Tokens.T_MORE) {
                type = Type.SQL_CHAR;
            }

            ColumnSchema col = new ColumnSchema(name, type, false, false,
                                                null);

            diagnosticsList.add(diagnosticsVariableTokens[i], col);
        }
    }

    //
    ColumnSchema  column;
    String        schema;
    String        tableName;
    String        columnName;
    RangeVariable rangeVariable;

    //
    NumberSequence sequence;
    boolean        isWritable;    // = false; true if column of writable table

    //
    boolean isParam;

    //

    /**
     * Creates a OpTypes.COLUMN expression
     */
    ExpressionColumn(String schema, String table, String column) {

        super(OpTypes.COLUMN);

        this.schema     = schema;
        this.tableName  = table;
        this.columnName = column;
    }

    ExpressionColumn(ColumnSchema column) {

        super(OpTypes.COLUMN);

        this.column   = column;
        this.dataType = column.getDataType();
        columnName    = column.getName().name;
    }

    ExpressionColumn(RangeVariable rangeVar, int index) {

        super(OpTypes.COLUMN);

        columnIndex = index;

        setAutoAttributesAsColumn(rangeVar, columnIndex);
    }

    /**
     * Creates a temporary OpTypes.COLUMN expression
     */
    ExpressionColumn(Expression e, int colIndex, int rangePosition) {

        super(OpTypes.SIMPLE_COLUMN);

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
        } else if (type == OpTypes.ROWNUM) {
            columnName = rownumName.name;
            dataType   = Type.SQL_INTEGER;
        }
    }

    /**
     * For diagnostics vars
     */
    ExpressionColumn(int type, int columnIndex) {

        super(type);

        this.column      = (ColumnSchema) diagnosticsList.get(columnIndex);
        this.columnIndex = columnIndex;
        this.dataType    = column.dataType;
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
     * Creates a OpTypes.SEQUENCE expression
     */
    ExpressionColumn(NumberSequence sequence, int opType) {

        super(opType);

        this.sequence = sequence;
        dataType      = sequence.getDataType();
    }

    void setAutoAttributesAsColumn(RangeVariable range, int i) {

        columnIndex   = i;
        column        = range.getColumn(i);
        dataType      = column.getDataType();
        columnName    = range.getColumnAlias(i).name;
        tableName     = range.getTableAlias().name;
        rangeVariable = range;

        rangeVariable.addColumn(columnIndex);
    }

    void setAttributesAsColumn(RangeVariable range, int i) {

        columnIndex   = i;
        column        = range.getColumn(i);
        dataType      = column.getDataType();
        rangeVariable = range;

        rangeVariable.addColumn(columnIndex);
    }

    public byte getNullability() {

        switch (opType) {

            case OpTypes.COLUMN :
                if (nullability == SchemaObject.Nullability.NULLABLE_UNKNOWN) {
                    return column.getNullability();
                }

                return nullability;

            case OpTypes.COALESCE :
            case OpTypes.SEQUENCE :
            case OpTypes.ROWNUM :
                return SchemaObject.Nullability.NO_NULLS;

            default :
                return SchemaObject.Nullability.NULLABLE_UNKNOWN;
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

        if (rangeVariable != null && rangeVariable.hasColumnAlias()) {
            return rangeVariable.getColumnAlias(columnIndex);
        }

        if (column != null) {
            return column.getName();
        }

        if (opType == OpTypes.COALESCE) {
            return nodes[LEFT].getSimpleName();
        } else if (opType == OpTypes.ROWNUM) {
            return rownumName;
        }

        return null;
    }

    String getAlias() {

        if (alias != null) {
            return alias.name;
        }

        switch (opType) {

            case OpTypes.COLUMN :
            case OpTypes.COALESCE :
            case OpTypes.ROWNUM :
                return columnName;
        }

        return "";
    }

    void collectObjectNames(Set set) {

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
    }

    String getColumnName() {

        switch (opType) {

            case OpTypes.COLUMN :
            case OpTypes.PARAMETER :
            case OpTypes.VARIABLE :
                if (column != null) {
                    return column.getName().name;
                }

                if (columnName != null) {
                    return columnName;
                }
        }

        return getAlias();
    }

    public ColumnSchema getColumn() {
        return column;
    }

    String getSchemaName() {
        return schema;
    }

    RangeVariable getRangeVariable() {
        return rangeVariable;
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeGroup rangeGroup, int rangeCount, RangeGroup[] rangeGroups,
            HsqlList unresolvedSet, boolean acceptsSequences) {

        switch (opType) {

            case OpTypes.SEQUENCE :
                if (!acceptsSequences) {
                    throw Error.error(ErrorCode.X_42598);
                }
                break;

            case OpTypes.ROWNUM :
            case OpTypes.MULTICOLUMN :
            case OpTypes.DYNAMIC_PARAM :
            case OpTypes.ASTERISK :
            case OpTypes.SIMPLE_COLUMN :
            case OpTypes.DIAGNOSTICS_VARIABLE :
                break;

            case OpTypes.COALESCE :
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i].resolveColumnReferences(session, rangeGroup,
                                                     rangeGroups,
                                                     unresolvedSet);
                }
                break;

            case OpTypes.COLUMN :
            case OpTypes.PARAMETER :
            case OpTypes.VARIABLE : {
                boolean resolved       = false;
                boolean tableQualified = tableName != null;

                if (rangeVariable != null) {
                    return unresolvedSet;
                }

                RangeVariable[] rangeVarArray = rangeGroup.getRangeVariables();

                for (int i = 0; i < rangeCount; i++) {
                    RangeVariable rangeVar = rangeVarArray[i];

                    if (rangeVar == null) {
                        continue;
                    }

                    if (resolved) {
                        if (session.database.sqlEnforceRefs) {
                            if (resolvesDuplicateColumnReference(rangeVar)) {
                                String message = getColumnName();

                                if (alias != null) {
                                    StringBuffer sb =
                                        new StringBuffer(message);

                                    sb.append(' ').append(Tokens.T_AS).append(
                                        ' ').append(alias.getStatementName());

                                    message = sb.toString();
                                }

                                throw Error.error(ErrorCode.X_42580, message);
                            }
                        }
                    } else {
                        if (resolveColumnReference(rangeVar, false)) {
                            resolved = true;

                            if (!session.database.sqlEnforceRefs) {
                                break;
                            }
                        }
                    }
                }

                if (resolved) {
                    return unresolvedSet;
                }

                if (session.database.sqlSyntaxOra
                        || session.database.sqlSyntaxDb2) {
                    if (acceptsSequences && tableName != null) {
                        if (Tokens.T_CURRVAL.equals(columnName)
                                || Tokens.T_PREVVAL.equals(columnName)) {
                            NumberSequence seq =
                                session.database.schemaManager.getSequence(
                                    tableName, session.getSchemaName(schema),
                                    false);

                            if (seq != null) {
                                opType     = OpTypes.SEQUENCE_CURRENT;
                                dataType   = seq.getDataType();
                                sequence   = seq;
                                schema     = null;
                                tableName  = null;
                                columnName = null;
                                resolved   = true;
                            }
                        } else if (Tokens.T_NEXTVAL.equals(columnName)) {
                            NumberSequence seq =
                                session.database.schemaManager.getSequence(
                                    tableName, session.getSchemaName(schema),
                                    false);

                            if (seq != null) {
                                opType     = OpTypes.SEQUENCE;
                                dataType   = seq.getDataType();
                                sequence   = seq;
                                schema     = null;
                                tableName  = null;
                                columnName = null;
                                resolved   = true;
                            }
                        }
                    }
                }

                if (resolved) {
                    return unresolvedSet;
                }

                for (int idx = rangeGroups.length - 1; idx >= 0; idx--) {
                    rangeVarArray = rangeGroups[idx].getRangeVariables();

                    for (int i = 0; i < rangeVarArray.length; i++) {
                        RangeVariable rangeVar = rangeVarArray[i];

                        if (rangeVar == null) {
                            continue;
                        }

                        if (resolveColumnReference(rangeVar, true)) {
                            if (opType == OpTypes.COLUMN) {
                                rangeGroup.setCorrelated();

                                for (int idxx = rangeGroups.length - 1;
                                        idxx > idx; idxx--) {
                                    rangeGroups[idxx].setCorrelated();
                                }
                            }

                            return unresolvedSet;
                        }
                    }
                }

                if (unresolvedSet == null) {
                    unresolvedSet = new ArrayListIdentity();
                }

                unresolvedSet.add(this);
            }
        }

        return unresolvedSet;
    }

    private boolean resolveColumnReference(RangeVariable rangeVar,
                                           boolean outer) {

        if (tableName == null) {
            Expression e = rangeVar.getColumnExpression(columnName);

            if (e != null) {
                opType   = e.opType;
                nodes    = e.nodes;
                dataType = e.dataType;

                return true;
            }
        }

        int colIndex = rangeVar.findColumn(schema, tableName, columnName);

        if (colIndex == -1) {
            return false;
        }

        switch (rangeVar.rangeType) {

            case RangeVariable.PARAMETER_RANGE :
            case RangeVariable.VARIALBE_RANGE : {
                if (tableName != null) {
                    return false;
                }

                ColumnSchema column = rangeVar.getColumn(colIndex);

                if (column.getParameterMode()
                        == SchemaObject.ParameterModes.PARAM_OUT) {
                    return false;
                } else {
                    opType = rangeVar.rangeType
                             == RangeVariable.VARIALBE_RANGE ? OpTypes.VARIABLE
                                                             : OpTypes
                                                             .PARAMETER;
                }

                break;
            }
            case RangeVariable.TRANSITION_RANGE : {
                if (tableName == null) {
                    return false;
                }

                if (schema != null) {
                    return false;
                }

                opType = OpTypes.TRANSITION_VARIABLE;

                break;
            }
            default : {
                break;
            }
        }

        setAttributesAsColumn(rangeVar, colIndex);

        return true;
    }

    boolean resolvesDuplicateColumnReference(RangeVariable rangeVar) {

        if (tableName == null) {
            Expression e = rangeVar.getColumnExpression(columnName);

            if (e != null) {
                return false;
            }
        }

        switch (rangeVar.rangeType) {

            case RangeVariable.PARAMETER_RANGE :
            case RangeVariable.VARIALBE_RANGE :
            case RangeVariable.TRANSITION_RANGE :
                return false;

            default :
                int colIndex = rangeVar.findColumn(schema, tableName,
                                                   columnName);

                return colIndex != -1;
        }
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

                nullability = SchemaObject.Nullability.NO_NULLS;

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

            case OpTypes.DIAGNOSTICS_VARIABLE : {
                return getDiagnosticsVariable(session);
            }
            case OpTypes.VARIABLE : {
                return session.sessionContext.routineVariables[columnIndex];
            }
            case OpTypes.PARAMETER : {
                return session.sessionContext.routineArguments[columnIndex];
            }
            case OpTypes.TRANSITION_VARIABLE : {
                return session.sessionContext
                    .triggerArguments[rangeVariable.rangePosition][columnIndex];
            }
            case OpTypes.COLUMN : {
                RangeIterator[] iterators =
                    session.sessionContext.rangeIterators;
                Object value =
                    iterators[rangeVariable.rangePosition].getCurrent(
                        columnIndex);

                if (dataType != column.dataType) {
                    value = dataType.convertToType(session, value,
                                                   column.dataType);
                }

                return value;
            }
            case OpTypes.SIMPLE_COLUMN : {
                Object value =
                    session.sessionContext.rangeIterators[rangePosition]
                        .getCurrent(columnIndex);

                return value;
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
            case OpTypes.SEQUENCE_CURRENT : {
                return session.sessionData.getSequenceCurrent(sequence);
            }
            case OpTypes.ROWNUM : {
                return ValuePool.getInt(session.sessionContext.rownum);
            }
            case OpTypes.ASTERISK :
            case OpTypes.MULTICOLUMN :
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionColumn");
        }
    }

    private Object getDiagnosticsVariable(Session session) {
        return session.sessionContext.diagnosticsVariables[columnIndex];
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
                return alias.getStatementName();

            case OpTypes.DIAGNOSTICS_VARIABLE :
            case OpTypes.VARIABLE :
            case OpTypes.PARAMETER :
                return column.getName().statementName;

            case OpTypes.ROWNUM : {
                StringBuffer sb = new StringBuffer(Tokens.T_ROWNUM);

                sb.append('(').append(')');

                return sb.toString();
            }
            case OpTypes.COLUMN : {
                if (column == null) {
                    if (alias != null) {
                        return alias.getStatementName();
                    } else {
                        if (tableName == null) {
                            return columnName;
                        }

                        StringBuffer sb = new StringBuffer();

                        sb.append(tableName);
                        sb.append('.');
                        sb.append(columnName);

                        return sb.toString();
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
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionColumn");
        }
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

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
                sb.append(column.getName().getSchemaQualifiedStatementName());

                if (alias != null) {
                    sb.append(" AS ").append(alias.name);
                }
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

        sb.append('\n');

        return sb.toString();
    }

    /**
     * Returns the table name used in query
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
            StringBuffer sb = new StringBuffer();
            Expression   e  = (Expression) set.get(0);

            if (e instanceof ExpressionColumn) {
                ExpressionColumn c = (ExpressionColumn) e;

                if (c.schema != null) {
                    sb.append(c.schema + '.');
                }

                if (c.tableName != null) {
                    sb.append(c.tableName + '.');
                }

                sb.append(c.getColumnName());

                throw Error.error(ErrorCode.X_42501, sb.toString());
            } else {
                OrderedHashSet newSet = new OrderedHashSet();

                e.collectAllExpressions(newSet,
                                        Expression.columnExpressionSet,
                                        Expression.emptyExpressionSet);

                // throw with column name
                checkColumnsResolved(newSet);

                // throw anyway if not found
                throw Error.error(ErrorCode.X_42501);
            }
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
    OrderedHashSet collectRangeVariables(OrderedHashSet set) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                set = nodes[i].collectRangeVariables(set);
            }
        }

        if (rangeVariable != null) {
            if (set == null) {
                set = new OrderedHashSet();
            }

            set.add(rangeVariable);
        }

        return set;
    }

    OrderedHashSet collectRangeVariables(RangeVariable[] rangeVariables,
                                         OrderedHashSet set) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                set = nodes[i].collectRangeVariables(rangeVariables, set);
            }
        }

        if (rangeVariable != null) {
            for (int i = 0; i < rangeVariables.length; i++) {
                if (rangeVariables[i] == rangeVariable) {
                    if (set == null) {
                        set = new OrderedHashSet();
                    }

                    set.add(rangeVariable);

                    break;
                }
            }
        }

        return set;
    }

    Expression replaceAliasInOrderBy(Session session, Expression[] columns,
                                     int length) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            nodes[i] = nodes[i].replaceAliasInOrderBy(session, columns,
                    length);
        }

        switch (opType) {

            case OpTypes.COALESCE :
            case OpTypes.COLUMN : {
                int matchIndex = -1;

                for (int i = 0; i < length; i++) {
                    SimpleName aliasName = columns[i].alias;
                    String     alias     = aliasName == null ? null
                                                             : aliasName.name;

                    if (schema == null && tableName == null
                            && columnName.equals(alias)) {
                        if (matchIndex < 0) {
                            matchIndex = i;
                        } else if (session.database.sqlEnforceRefs) {
                            String message = getColumnName();

                            throw Error.error(ErrorCode.X_42580, message);
                        }
                    }
                }

                if (matchIndex >= 0) {
                    return columns[matchIndex];
                }

                for (int i = 0; i < length; i++) {
                    if (columns[i] instanceof ExpressionColumn) {
                        if (this.equals(columns[i])) {
                            if (matchIndex < 0) {
                                matchIndex = i;
                            } else if (session.database.sqlEnforceRefs) {
                                String message = getColumnName();

                                throw Error.error(ErrorCode.X_42580, message);
                            }
                        }

                        if (tableName == null && schema == null
                                && columnName
                                    .equals(((ExpressionColumn) columns[i])
                                        .columnName)) {
                            if (matchIndex < 0) {
                                matchIndex = i;
                            } else if (session.database.sqlEnforceRefs) {
                                String message = getColumnName();

                                throw Error.error(ErrorCode.X_42580, message);
                            }
                        }
                    }
                }

                if (matchIndex >= 0) {
                    return columns[matchIndex];
                }

                break;
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

    /**
     * SIMPLE_COLUMN expressions can be of different Expression subclass types
     */
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

            case OpTypes.VARIABLE :
            case OpTypes.PARAMETER :
            case OpTypes.COLUMN :
                return column == other.getColumn()
                       && rangeVariable == other.getRangeVariable();

            // A VoltDB extension
            case OpTypes.ASTERISK :
                return true;
            // End of VoltDB extension
            default :
                return false;
        }
    }

    void replaceRangeVariables(RangeVariable[] ranges,
                               RangeVariable[] newRanges) {

        for (int i = 0; i < nodes.length; i++) {
            nodes[i].replaceRangeVariables(ranges, newRanges);
        }

        for (int i = 0; i < ranges.length; i++) {
            if (rangeVariable == ranges[i]) {
                rangeVariable = newRanges[i];

                break;
            }
        }
    }

    void resetColumnReferences() {
        rangeVariable = null;
        columnIndex   = -1;
    }

    public boolean isIndexable(RangeVariable range) {

        if (opType == OpTypes.COLUMN) {
            return rangeVariable == range;
        }

        return false;
    }

    public boolean isUnresolvedParam() {
        return isParam && dataType == null;
    }

    boolean isDynamicParam() {
        return isParam;
    }

    RangeVariable[] getJoinRangeVariables(RangeVariable[] ranges) {

        if (opType == OpTypes.COLUMN) {
            return new RangeVariable[]{ rangeVariable };
        }

        return RangeVariable.emptyArray;
    }

    /**
     * For normal tables only. We don't want to create an index on
     * each column that is checked.
     */
    double costFactor(Session session, RangeVariable range, int operation) {

        if (range.rangeTable instanceof TableDerived) {
            return 1024;
        }

        PersistentStore store = range.rangeTable.getRowStore(session);
        int indexType = range.rangeTable.indexTypeForColumn(session,
            columnIndex);
        double factor;

        switch (indexType) {

            case Index.INDEX_UNIQUE :
                if (operation == OpTypes.EQUAL) {
                    factor = 1;
                } else {
                    factor = store.elementCount() / 2.0;
                }
                break;

            case Index.INDEX_NON_UNIQUE :
                if (operation == OpTypes.EQUAL) {
                    factor = store.elementCount() / 8.0;

                    if (factor > 1024) {
                        factor = 1024;
                    }
                } else {
                    factor = store.elementCount() / 2.0;
                }
                break;

            case Index.INDEX_NONE :
            default :
                factor = store.elementCount();
                break;
        }

        return factor < Index.minimumSelectivity ? Index.minimumSelectivity
                                                 : factor;
    }

    public Expression duplicate() {

        if (opType == OpTypes.PARAMETER) {
            return this;
        }

        return super.duplicate();
    }
    // A VoltDB extension to export abstract parse trees

    /**
     * VoltDB added method to provide detail for a non-catalog-dependent
     * representation of this HSQLDB object.
     * @return XML, correctly indented, representing this object.
     */
    VoltXMLElement voltAnnotateColumnXML(VoltXMLElement exp)
    {
        if (rangeVariable != null && rangeVariable.rangeTable != null &&
                rangeVariable.tableAlias != null &&
                rangeVariable.rangeTable.tableType == TableBase.SYSTEM_SUBQUERY) {
            exp.attributes.put("table", rangeVariable.tableAlias.name.toUpperCase());
        } 
        else if (tableName != null) {
            exp.attributes.put("table", tableName.toUpperCase());
        }
        // This case was introduced to cover columns in the where clause of
        // an update statement.
        else if (rangeVariable != null && rangeVariable.rangeTable != null &&
                rangeVariable.rangeTable.tableName != null &&
                rangeVariable.rangeTable.tableName.getNameString() != null) {
            exp.attributes.put("table",
                    rangeVariable.rangeTable.tableName.getNameString().toUpperCase());
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
    // End of VoltDB extension
}
