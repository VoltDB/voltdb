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
import java.util.List;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultConstants;
import org.hsqldb_voltpatches.result.ResultMetaData;

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

            // $FALL-THROUGH$
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
    static boolean isGroupByColumn(QuerySpecification select, int index) {
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
     * Extract columnref elements from the input element.
     * @param element
     * @param cols - output collection containing the column references
     */

    static protected void extractColumnReferences(VoltXMLElement element, java.util.List<VoltXMLElement> cols) {
        if ("columnref".equalsIgnoreCase(element.name)) {
            cols.add(element);
        } else {
            for (VoltXMLElement child : element.children) {
                extractColumnReferences(child, cols);
            }
        }
    }

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
            throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        // XXX this seems, how you say, dumb.  leaving it though until I track
        // down that nobody cares
        assert(false);
        return null;
    }

    static VoltXMLElement voltGetXMLExpression(QueryExpression queryExpr, ExpressionColumn parameters[], Session session)
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
            return voltGetXMLSpecification(select, parameters, session);
        } else if (exprType == QueryExpression.UNION || exprType == QueryExpression.UNION_ALL ||
                   exprType == QueryExpression.EXCEPT || exprType == QueryExpression.EXCEPT_ALL ||
                   exprType == QueryExpression.INTERSECT || exprType == QueryExpression.INTERSECT_ALL){
            VoltXMLElement unionExpr = new VoltXMLElement("union");
            unionExpr.attributes.put("uniontype", queryExpr.operatorName());

            VoltXMLElement leftExpr = voltGetXMLExpression(
                    queryExpr.getLeftQueryExpression(), parameters, session);
            VoltXMLElement rightExpr = voltGetXMLExpression(
                    queryExpr.getRightQueryExpression(), parameters, session);
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

    /** return a list of VoltXMLElements that need to be added to the statement XML for LIMIT and OFFSET */
    protected static List<VoltXMLElement> voltGetLimitOffsetXMLFromSortAndSlice(Session session, SortAndSlice sortAndSlice)
            throws HSQLParseException {
        List<VoltXMLElement> result = new ArrayList<>();

        if (sortAndSlice == null || sortAndSlice == SortAndSlice.noSort) {
            return result;
        }

        if (sortAndSlice.limitCondition != null) {
            Expression limitCondition = sortAndSlice.limitCondition;
            if (limitCondition.nodes.length != 2) {
                throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException(
                    "Parser did not create limit and offset expression for LIMIT.");
            }
            try {
                // read offset. it may be a parameter token.
                VoltXMLElement offset = new VoltXMLElement("offset");
                Expression offsetExpr = limitCondition.getLeftNode();
                if (offsetExpr.isParam == false) {
                    Integer offsetValue = (Integer)offsetExpr.getValue(session);
                    if (offsetValue > 0) {
                        Expression expr = new ExpressionValue(offsetValue,
                                org.hsqldb_voltpatches.types.Type.SQL_BIGINT);
                        offset.children.add(expr.voltGetXML(session));
                        offset.attributes.put("offset", offsetValue.toString());
                    }
                } else {
                    offset.attributes.put("offset_paramid", offsetExpr.getUniqueId(session));
                }
                result.add(offset);

                // Limit may be null (offset with no limit), or
                // it may be a parameter
                Expression limitExpr = limitCondition.getRightNode();
                if (limitExpr != null) {
                    VoltXMLElement limit = new VoltXMLElement("limit");
                    if (limitExpr.isParam == false) {
                        Integer limitValue = (Integer)limitExpr.getValue(session);
                        Expression expr = new ExpressionValue(limitValue,
                                org.hsqldb_voltpatches.types.Type.SQL_BIGINT);
                        limit.children.add(expr.voltGetXML(session));
                        limit.attributes.put("limit", limitValue.toString());
                    } else {
                        limit.attributes.put("limit_paramid", limitExpr.getUniqueId(session));
                    }
                    result.add(limit);
                }

            } catch (HsqlException ex) {
                // XXX really?
                ex.printStackTrace();
            }
        }

        return result;
    }

    private static VoltXMLElement voltGetXMLSpecification(QuerySpecification select, ExpressionColumn parameters[], Session session)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        // select
        VoltXMLElement query = new VoltXMLElement("select");
        if (select.isDistinctSelect)
            query.attributes.put("distinct", "true");

        List<VoltXMLElement> limitOffsetXml = voltGetLimitOffsetXMLFromSortAndSlice(session, select.sortAndSlice);
        for (VoltXMLElement elem : limitOffsetXml) {
            query.children.add(elem);
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
        voltAppendParameters(session, query, parameters);

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
     * Columns from USING expression are unqualified. In case of INNER join, it doesn't matter
     * we can pick the first table which contains the input column. In case of OUTER joins, we must
     * the OUTER table - if it's a null-able column the outer join must return them.
     * @param columns list of columns to resolve
     * @return rvs list of range variables
     */
    static protected void resolveUsingColumns(java.util.List<VoltXMLElement> columns, RangeVariable[] rvs)
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

    static protected void voltAppendParameters(Session session, VoltXMLElement parentXml,
                                               ExpressionColumn[] parameters)
    {
        VoltXMLElement parameterXML = new VoltXMLElement("parameters");
        parentXml.children.add(parameterXML);
        assert(parameterXML != null);
        int index = 0;
        for (Expression expr : parameters) {
            org.hsqldb_voltpatches.types.Type paramType = expr.getDataType();
            if (paramType == null) {
                // Parameters used with " IN ?" use a different method of recording their paramType
                // to avoid confusing the HSQL front end. Account for that here.
                if (expr.nodeDataTypes != null &&
                        expr.nodeDataTypes.length == 1 &&
                        expr.nodeDataTypes[0] != null) {
                    paramType = expr.nodeDataTypes[0];
                }
                else {
                    // This covers the case of parameters that were getting tentatively scanned
                    // by the parser but then lost in a "rewind" and later rescanned and added
                    // (again!) to this list but then actually processed, given a data type, etc.
                    // Somehow (?) the hsql executor manages to just ignore the originally scanned
                    // duplicate parameters left behind like this. So, so should VoltDB.
                    continue;
                }
            }
            VoltXMLElement parameter = new VoltXMLElement("parameter");
            parameterXML.children.add(parameter);
            parameter.attributes.put("index", String.valueOf(index));
            ++index;
            parameter.attributes.put("id", expr.getUniqueId(session));
            parameter.attributes.put("valuetype", Types.getTypeName(paramType.typeCode));
            // Use of non-null nodeDataTypes for a DYNAMIC_PARAM is a voltdb extension to signal
            // that values passed to parameters such as the one in "col in ?" must be vectors.
            // So, it can just be forwarded as a boolean.
            if (expr.nodeDataTypes != null) {
                parameter.attributes.put("isvector", "true");
            }
        }
    }

    static protected Expression voltCombineWithAnd(Expression... conditions)
    {
        Expression result = null;
        for(Expression child : conditions) {
            if (child != null) {
                if (result == null) {
                    result = child;
                    continue;
                }
                result = new ExpressionLogical(OpTypes.AND, result, child);
            }
        }
        return result;
    }
    /**********************************************************************/
}
