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

import org.hsqldb.HSQLInterface.HSQLParseException;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultMetaData;

/**
 * Statement implementation for DML and base DQL statements.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */

// fredt@users 20040404 - patch 1.7.2 - fixed type resolution for parameters
// boucherb@users 200404xx - patch 1.7.2 - changed parameter naming scheme for SQLCI client usability/support
// fredt@users 20050609 - 1.8.0 - fixed EXPLAIN PLAN by implementing describe(Session)
// fredt@users - 1.9.0 - support for generated column reporting
// fredt@users - 1.9.0 - support for multi-row inserts
public abstract class StatementDMQL extends Statement {

    public static final String PCOL_PREFIX        = "@p";
    static final String        RETURN_COLUMN_NAME = "@p0";

    /** target table for INSERT_XXX, UPDATE and DELETE and MERGE */
    Table targetTable;
    Table baseTable;

    /** column map of query expression */
    int[]           baseColumnMap;
    RangeVariable[] targetRangeVariables;

    /** source table for MERGE */
    Table sourceTable;

    /** condition expression for UPDATE, MERGE and DELETE */
    Expression condition;

    /** for TRUNCATE variation of DELETE */
    boolean restartIdentity;

    /** column map for INSERT operation direct or via MERGE */
    int[] insertColumnMap;

    /** column map for UPDATE operation direct or via MERGE */
    int[] updateColumnMap;
    int[] baseUpdateColumnMap;

    /** Column value Expressions for UPDATE and MERGE. */
    Expression[] updateExpressions;

    /** Column value Expressions for MERGE */
    Expression[][] multiColumnValues;

    /** INSERT_VALUES */
    Expression insertExpression;

    /**
     * Flags indicating which columns' values will/will not be
     * explicitly set.
     */
    boolean[] insertCheckColumns;
    boolean[] updateCheckColumns;

    /**
     * Select to be evaluated when this is an INSERT_SELECT or
     * SELECT statement
     */
    QueryExpression queryExpression;

    /**
     * Parse-order array of Expression objects, all of iType == PARAM ,
     * involved in some way in any INSERT_XXX, UPDATE, DELETE, SELECT or
     * CALL CompiledStatement
     */
    ExpressionColumn[] parameters;

    /**
     * int[] contains column indexes for generated values
     */
    int[] generatedIndexes;

    /**
     * ResultMetaData for generated values
     */
    ResultMetaData generatedResultMetaData;

    /**
     * ResultMetaData for parameters
     */
    ResultMetaData parameterMetaData;

    /**
     * Subqueries inverse usage depth order
     */
    SubQuery[] subqueries;

    /**
     * The type of this CompiledStatement. <p>
     *
     * One of: <p>
     *
     * <ol>
     *  <li>UNKNOWN
     *  <li>INSERT_VALUES
     *  <li>INSERT_SELECT
     *  <li>UPDATE
     *  <li>DELETE
     *  <li>SELECT
     *  <li>CALL
     *  <li>MERGE
     *  <li>DDL
     * </ol>
     */

    /**
     * Total number of RangeIterator objects used
     */
    int rangeIteratorCount;

    /**
     * Database objects used
     */
    NumberSequence[] sequences;
    Routine[]        routines;
    RangeVariable[]  rangeVariables;

    StatementDMQL(int type, int group, HsqlName schemaName) {

        super(type, group);

        this.schemaName             = schemaName;
        this.isTransactionStatement = true;
    }

    void setBaseIndexColumnMap() {

        if (targetTable != baseTable) {
            baseColumnMap = targetTable.getBaseTableColumnMap();
        }
    }

    public Result execute(Session session) {

        Result result = getAccessRightsResult(session);

        if (result != null) {
            return result;
        }

        if (this.isExplain) {
            return Result.newSingleColumnStringResult("OPERATION",
                    describe(session));
        }

        if (session.sessionContext.dynamicArguments.length
                != parameters.length) {

//            return Result.newErrorResult(Error.error(ErrorCode.X_42575));
        }

        try {
            materializeSubQueries(session);

            result = getResult(session);
        } catch (Throwable t) {
            String commandString = sql;

            if (session.database.getProperties().getErrorLevel()
                    == HsqlDatabaseProperties.NO_MESSAGE) {
                commandString = null;
            }

            result = Result.newErrorResult(t, commandString);

            result.getException().setStatementType(group, type);


        }

        session.sessionContext.clearStructures(this);

        return result;
    }

    abstract Result getResult(Session session);

    /**
     * For the creation of the statement
     */
    public void setGeneratedColumnInfo(int generate, ResultMetaData meta) {

        // can support INSERT_SELECT also
        if (type != StatementTypes.INSERT) {
            return;
        }

        int colIndex = baseTable.getIdentityColumnIndex();

        if (colIndex == -1) {
            return;
        }

        switch (generate) {

            case ResultConstants.RETURN_NO_GENERATED_KEYS :
                return;

            case ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES :
                int[] columnIndexes = meta.getGeneratedColumnIndexes();

                if (columnIndexes.length != 1) {
                    return;
                }

                if (columnIndexes[0] != colIndex) {
                    return;
                }

            // fall through
            case ResultConstants.RETURN_GENERATED_KEYS :
                generatedIndexes = new int[]{ colIndex };
                break;

            case ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES :
                String[] columnNames = meta.getGeneratedColumnNames();

                if (columnNames.length != 1) {
                    return;
                }

                if (baseTable.findColumn(columnNames[0]) != colIndex) {
                    return;
                }

                generatedIndexes = new int[]{ colIndex };
                break;
        }

        generatedResultMetaData =
            ResultMetaData.newResultMetaData(generatedIndexes.length);

        for (int i = 0; i < generatedIndexes.length; i++) {
            ColumnSchema column = baseTable.getColumn(generatedIndexes[i]);

            generatedResultMetaData.columns[i] = column;
        }

        generatedResultMetaData.prepareData();
    }

    Object[] getGeneratedColumns(Object[] data) {

        if (generatedIndexes == null) {
            return null;
        }

        Object[] values = new Object[generatedIndexes.length];

        for (int i = 0; i < generatedIndexes.length; i++) {
            values[i] = data[generatedIndexes[i]];
        }

        return values;
    }

    public boolean hasGeneratedColumns() {
        return generatedIndexes != null;
    }

    boolean[] getInsertOrUpdateColumnCheckList() {

        switch (type) {

            case StatementTypes.INSERT :
                return insertCheckColumns;

            case StatementTypes.UPDATE_WHERE :
                return updateCheckColumns;

            case StatementTypes.MERGE :
                boolean[] check =
                    (boolean[]) ArrayUtil.duplicateArray(insertCheckColumns);

                ArrayUtil.orBooleanArray(updateCheckColumns, check);

                return check;
        }

        return null;
    }

    private void setParameters() {

        for (int i = 0; i < parameters.length; i++) {
            parameters[i].parameterIndex = i;
        }
    }

    void materializeSubQueries(Session session) {

        if (subqueries.length == 0) {
            return;
        }

        HashSet subqueryPopFlags = new HashSet();

        for (int i = 0; i < subqueries.length; i++) {
            SubQuery sq = subqueries[i];

            // VIEW working tables may be reused in a single query but they are filled only once
            if (!subqueryPopFlags.add(sq)) {
                continue;
            }

            if (!sq.isCorrelated()) {
                sq.materialise(session);
            }
        }
    }

    public void clearVariables() {

        isValid            = false;
        targetTable        = null;
        baseTable          = null;
        condition          = null;
        insertColumnMap    = null;
        updateColumnMap    = null;
        updateExpressions  = null;
        insertExpression   = null;
        insertCheckColumns = null;

//        expression         = null;
        parameters = null;
        subqueries = null;
    }

    void setDatabseObjects(CompileContext compileContext) {

        parameters = compileContext.getParameters();

        setParameters();
        setParameterMetaData();

        subqueries         = compileContext.getSubqueries();
        rangeIteratorCount = compileContext.getRangeVarCount();
        rangeVariables     = compileContext.getRangeVariables();
        sequences          = compileContext.getSequences();
        routines           = compileContext.getRoutines();

        OrderedHashSet set = new OrderedHashSet();

        getTableNamesForRead(set);

        for (int i = 0; i < routines.length; i++) {
            set.addAll(routines[i].getTableNamesForRead());
        }

        if (set.size() > 0) {
            readTableNames = new HsqlName[set.size()];

            set.toArray(readTableNames);
            set.clear();
        }

        getTableNamesForWrite(set);

        for (int i = 0; i < routines.length; i++) {
            set.addAll(routines[i].getTableNamesForWrite());
        }

        if (set.size() > 0) {
            writeTableNames = new HsqlName[set.size()];

            set.toArray(writeTableNames);
        }
    }

    /**
     * Determines if the authorizations are adequate
     * to execute the compiled object. Completion requires the list of
     * all database objects in a compiled statement.
     */
    void checkAccessRights(Session session) {

        if (targetTable != null && !targetTable.isTemp()) {
            targetTable.checkDataReadOnly();
            session.checkReadWrite();
        }

        if (session.isAdmin()) {
            return;
        }

        for (int i = 0; i < sequences.length; i++) {
            session.getGrantee().checkAccess(sequences[i]);
        }

        for (int i = 0; i < routines.length; i++) {
            if (routines[i].isLibraryRoutine()) {
                continue;
            }

            session.getGrantee().checkAccess(routines[i]);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable range = rangeVariables[i];

            if (range.rangeTable.getSchemaName()
                    == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            session.getGrantee().checkSelect(range.rangeTable,
                                             range.usedColumns);
        }

        switch (type) {

            case StatementTypes.CALL : {
                break;
            }
            case StatementTypes.INSERT : {
                session.getGrantee().checkInsert(targetTable,
                                                 insertCheckColumns);

                break;
            }
            case StatementTypes.SELECT_CURSOR :
                break;

            case StatementTypes.DELETE_WHERE : {
                session.getGrantee().checkDelete(targetTable);

                break;
            }
            case StatementTypes.UPDATE_WHERE : {
                session.getGrantee().checkUpdate(targetTable,
                                                 updateCheckColumns);

                break;
            }
            case StatementTypes.MERGE : {
                session.getGrantee().checkInsert(targetTable,
                                                 insertCheckColumns);
                session.getGrantee().checkUpdate(targetTable,
                                                 updateCheckColumns);

                break;
            }
        }
    }

    Result getAccessRightsResult(Session session) {

        try {
            checkAccessRights(session);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        return null;
    }

    /**
     * Returns the metadata, which is empty if the CompiledStatement does not
     * generate a Result.
     */
    public ResultMetaData getResultMetaData() {

        switch (type) {

            case StatementTypes.DELETE_WHERE :
            case StatementTypes.INSERT :
            case StatementTypes.UPDATE_WHERE :
                return ResultMetaData.emptyResultMetaData;

            default :
                throw Error.runtimeError(
                    ErrorCode.U_S0500,
                    "CompiledStatement.getResultMetaData()");
        }
    }

    /** @todo 1.9.0 - build the metadata only once and reuse */

    /**
     * Returns the metadata for the placeholder parameters.
     */
    public ResultMetaData getParametersMetaData() {
        return parameterMetaData;
    }

    void setParameterMetaData() {

        int     offset;
        int     idx;
        boolean hasReturnValue;

        offset = 0;

        if (parameters.length == 0) {
            parameterMetaData = ResultMetaData.emptyParamMetaData;

            return;
        }

// NO:  Not yet
//        hasReturnValue = (type == CALL && !expression.isProcedureCall());
//
//        if (hasReturnValue) {
//            outlen++;
//            offset = 1;
//        }
        parameterMetaData =
            ResultMetaData.newParameterMetaData(parameters.length);

// NO: Not yet
//        if (hasReturnValue) {
//            e = expression;
//            out.sName[0]       = DIProcedureInfo.RETURN_COLUMN_NAME;
//            out.sClassName[0]  = e.getValueClassName();
//            out.colType[0]     = e.getDataType();
//            out.colSize[0]     = e.getColumnSize();
//            out.colScale[0]    = e.getColumnScale();
//            out.nullability[0] = e.nullability;
//            out.isIdentity[0]  = false;
//            out.paramMode[0]   = expression.PARAM_OUT;
//        }
        for (int i = 0; i < parameters.length; i++) {
            idx = i + offset;

            // always i + 1.  We currently use the convention of @p0 to name the
            // return value OUT parameter
            parameterMetaData.columnLabels[idx] = StatementDMQL.PCOL_PREFIX
                                                  + (i + 1);
            parameterMetaData.columnTypes[idx] = parameters[i].dataType;

            byte parameterMode = SchemaObject.ParameterModes.PARAM_IN;

            if (parameters[i].column != null
                    && parameters[i].column.getParameterMode()
                       != SchemaObject.ParameterModes.PARAM_UNKNOWN) {
                parameterMode = parameters[i].column.getParameterMode();
            }

            parameterMetaData.paramModes[idx] = parameterMode;
            parameterMetaData.paramNullable[idx] =
                parameters[i].column == null
                ? SchemaObject.Nullability.NULLABLE
                : parameters[i].column.getNullability();
        }
    }

    /**
     * Retrieves a String representation of this object.
     */
    public String describe(Session session) {

        try {
            return describeImpl(session);
        } catch (Exception e) {
            e.printStackTrace();

            return e.toString();
        }
    }

    /**
     * Provides the toString() implementation.
     */
    private String describeImpl(Session session) throws Exception {

        StringBuffer sb;

        sb = new StringBuffer();

        switch (type) {

            case StatementTypes.SELECT_CURSOR : {
                sb.append(queryExpression.describe(session));
                appendParms(sb).append('\n');
                appendSubqueries(session, sb);

                return sb.toString();
            }
            case StatementTypes.INSERT : {
                if (queryExpression == null) {
                    sb.append("INSERT VALUES");
                    sb.append('[').append('\n');
                    appendMultiColumns(sb, insertColumnMap).append('\n');
                    appendTable(sb).append('\n');
                    appendParms(sb).append('\n');
                    appendSubqueries(session, sb).append(']');

                    return sb.toString();
                } else {
                    sb.append("INSERT SELECT");
                    sb.append('[').append('\n');
                    appendColumns(sb, insertColumnMap).append('\n');
                    appendTable(sb).append('\n');
                    sb.append(queryExpression.describe(session)).append('\n');
                    appendParms(sb).append('\n');
                    appendSubqueries(session, sb).append(']');

                    return sb.toString();
                }
            }
            case StatementTypes.UPDATE_WHERE : {
                sb.append("UPDATE");
                sb.append('[').append('\n');
                appendColumns(sb, updateColumnMap).append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);
                sb.append(targetRangeVariables[0].describe(session)).append(
                    '\n');
                sb.append(targetRangeVariables[1].describe(session)).append(
                    '\n');
                appendParms(sb).append('\n');
                appendSubqueries(session, sb).append(']');

                return sb.toString();
            }
            case StatementTypes.DELETE_WHERE : {
                sb.append("DELETE");
                sb.append('[').append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);
                sb.append(targetRangeVariables[0].describe(session)).append(
                    '\n');
                sb.append(targetRangeVariables[1].describe(session)).append(
                    '\n');
                appendParms(sb).append('\n');
                appendSubqueries(session, sb).append(']');

                return sb.toString();
            }
            case StatementTypes.CALL : {
                sb.append("CALL");
                sb.append('[').append(']');

                return sb.toString();
            }
            case StatementTypes.MERGE : {
                sb.append("MERGE");
                sb.append('[').append('\n');
                appendMultiColumns(sb, insertColumnMap).append('\n');
                appendColumns(sb, updateColumnMap).append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);
                sb.append(targetRangeVariables[0].describe(session)).append(
                    '\n');
                sb.append(targetRangeVariables[1].describe(session)).append(
                    '\n');
                sb.append(targetRangeVariables[2].describe(session)).append(
                    '\n');
                appendParms(sb).append('\n');
                appendSubqueries(session, sb).append(']');

                return sb.toString();
            }
            default : {
                return "UNKNOWN";
            }
        }
    }

    private StringBuffer appendSubqueries(Session session, StringBuffer sb) {

        sb.append("SUBQUERIES[");

        for (int i = 0; i < subqueries.length; i++) {
            sb.append("\n[level=").append(subqueries[i].level).append('\n');

            if (subqueries[i].queryExpression != null) {
                sb.append(subqueries[i].queryExpression.describe(session));
            }

            sb.append("]");
        }

        sb.append(']');

        return sb;
    }

    private StringBuffer appendTable(StringBuffer sb) {

        sb.append("TABLE[").append(targetTable.getName().name).append(']');

        return sb;
    }

    private StringBuffer appendSourceTable(StringBuffer sb) {

        sb.append("SOURCE TABLE[").append(sourceTable.getName().name).append(
            ']');

        return sb;
    }

    private StringBuffer appendColumns(StringBuffer sb, int[] columnMap) {

        if (columnMap == null || updateExpressions == null) {
            return sb;
        }

        sb.append("COLUMNS=[");

        for (int i = 0; i < columnMap.length; i++) {
            sb.append('\n').append(columnMap[i]).append(':').append(
                ' ').append(
                targetTable.getColumn(columnMap[i]).getNameString()).append(
                '[').append(updateExpressions[i]).append(']');
        }

        sb.append(']');

        return sb;
    }

    private StringBuffer appendMultiColumns(StringBuffer sb, int[] columnMap) {

        if (columnMap == null || multiColumnValues == null) {
            return sb;
        }

        sb.append("COLUMNS=[");

        for (int j = 0; j < multiColumnValues.length; j++) {
            for (int i = 0; i < columnMap.length; i++) {
                sb.append('\n').append(columnMap[i]).append(':').append(
                    ' ').append(
                    targetTable.getColumn(columnMap[i]).getName().name).append(
                    '[').append(multiColumnValues[j][i]).append(']');
            }
        }

        sb.append(']');

        return sb;
    }

    private StringBuffer appendParms(StringBuffer sb) {

        sb.append("PARAMETERS=[");

        for (int i = 0; i < parameters.length; i++) {
            sb.append('\n').append('@').append(i).append('[').append(
                parameters[i]).append(']');
        }

        sb.append(']');

        return sb;
    }

    private StringBuffer appendCondition(Session session, StringBuffer sb) {

        return condition == null ? sb.append("CONDITION[]\n")
                                 : sb.append("CONDITION[").append(
                                     condition.describe(session)).append(
                                     "]\n");
    }

    public void resolve() {}

    public RangeVariable[] getRangeVariables() {
        return rangeVariables;
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
     String voltGetXML(Session session, String orig_indent)
     throws HSQLParseException {
         // XXX this seems, how you say, dumb.  leaving it though until I track
         // down that nobody cares
         StringBuffer sb;
         sb = new StringBuffer();
         return sb.toString();
    }
}
