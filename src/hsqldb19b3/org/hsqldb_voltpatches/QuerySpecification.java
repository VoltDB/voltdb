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

import java.util.ArrayList;
import java.util.List;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.RangeGroup.RangeGroupSimple;
import org.hsqldb_voltpatches.RangeVariable.RangeIteratorRight;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayListIdentity;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.IntValueHashMap;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.navigator.RangeIterator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorData;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorDataTable;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.result.ResultProperties;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * Implementation of an SQL query specification, including SELECT.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 2.3.2
 * @since 1.9.0
 */
public class QuerySpecification extends QueryExpression {

    //
    public int            resultRangePosition;
    public boolean        isDistinctSelect;
    public boolean        isAggregated;
    public boolean        isGrouped;
    public boolean        isOrderSensitive;
    public boolean        isSimpleDistinct;
    RangeVariable[]       rangeVariables;
    private HsqlArrayList rangeVariableList;
    int                   startInnerRange = -1;
    int                   endInnerRange   = -1;
    Expression            queryCondition;
    Expression            checkQueryCondition;
    private Expression    havingCondition;
    Expression            rowExpression;
    Expression[]          exprColumns;
    HsqlArrayList         exprColumnList;
    public int            indexLimitVisible;
    private int           indexLimitRowId;
    private int           groupByColumnCount;    // columns in 'group by'
    private int           havingColumnCount;     // columns in 'having' (0 or 1)
    private int           indexStartHaving;
    public int            indexStartOrderBy;
    public int            indexStartAggregates;
    private int           indexLimitExpressions;
    public int            indexLimitData;
    private boolean       hasRowID;
    private boolean       isSimpleCount;
    private boolean       isSingleMemoryTable;

    //
    public boolean isUniqueResultRows;

    //
    Type[]                    resultColumnTypes;
    private ArrayListIdentity aggregateSet;

    //
    private ArrayListIdentity resolvedSubqueryExpressions = null;

    //
    //
    private boolean[] aggregateCheck;

    //
    private OrderedHashSet tempSet = new OrderedHashSet();

    //
    int[]                  columnMap;
    private Table          baseTable;
    private OrderedHashSet conditionTables;      // for view super-view references

    //
    public Index groupIndex;

    //
    private RangeGroup[] outerRanges;

    //
    QuerySpecification(Session session, Table table,
                       CompileContext compileContext, boolean isValueList) {

        this(compileContext);

        this.isValueList = isValueList;

        RangeVariable range = new RangeVariable(table, null, null, null,
            compileContext);

        range.addTableColumns(exprColumnList, 0, null);

        indexLimitVisible = exprColumnList.size();

        addRangeVariable(session, range);

        sortAndSlice    = SortAndSlice.noSort;
        isBaseMergeable = true;
        isMergeable     = true;
        isTable         = true;
    }

    QuerySpecification(CompileContext compileContext) {

        super(compileContext);

        resultRangePosition = compileContext.getNextRangeVarIndex();
        rangeVariableList   = new HsqlArrayList();
        exprColumnList      = new HsqlArrayList();
        sortAndSlice        = SortAndSlice.noSort;
        isBaseMergeable     = true;
        isMergeable         = true;
    }

    void addRangeVariable(Session session, RangeVariable rangeVar) {
        rangeVariableList.add(rangeVar);
    }

    void addRangeSeparator() {

        RangeVariable range =
            (RangeVariable) rangeVariableList.get(rangeVariableList.size()
                - 1);

        range.isBoundary = true;
    }

    public TableDerived getValueListTable() {

        if (isValueList) {
            RangeVariable range = null;

            if (rangeVariables == null) {
                if (rangeVariableList.size() == 1) {
                    range = (RangeVariable) rangeVariableList.get(0);
                }
            } else if (rangeVariables.length == 1) {
                range = rangeVariables[0];
            }

            if (range != null) {
                return (TableDerived) range.getTable();
            }
        }

        return null;
    }

    public RangeVariable[] getRangeVariables() {
        return rangeVariables;
    }

    // range variable sub queries are resolves fully
    private void resolveRangeVariables(Session session,
                                       RangeGroup[] rangeGroups) {

        if (rangeVariables == null
                || rangeVariables.length < rangeVariableList.size()) {
            rangeVariables = new RangeVariable[rangeVariableList.size()];

            rangeVariableList.toArray(rangeVariables);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            RangeGroup rangeGroup;

            if (rangeVariables[i].isLateral) {
                RangeVariable[] rangeVars =
                    (RangeVariable[]) ArrayUtil.resizeArray(rangeVariables, i);

                rangeGroup = new RangeGroupSimple(rangeVars, this);
            } else if (rangeGroups == RangeGroup.emptyArray) {
                rangeGroup = RangeGroup.emptyGroup;
            } else {
                rangeGroup = new RangeGroupSimple(RangeVariable.emptyArray,
                                                  this);
            }

            rangeVariables[i].resolveRangeTable(session, rangeGroup,
                                                rangeGroups);
        }
    }

    void addSelectColumnExpression(Expression e) {

        if (e.getType() == OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }

        if (indexLimitVisible > 0) {
            if (e.opType == OpTypes.MULTICOLUMN) {
                if (((ExpressionColumn) e).getTableName() == null) {
                    throw Error.error(ErrorCode.X_42578);
                }
            }

            Expression first = ((Expression) exprColumnList.get(0));

            if (first.opType == OpTypes.MULTICOLUMN
                    && ((ExpressionColumn) first).getTableName() == null) {
                throw Error.error(ErrorCode.X_42578);
            }
        }

        exprColumnList.add(e);

        indexLimitVisible++;
    }

    void addQueryCondition(Expression e) {
        queryCondition = e;
    }

    void addGroupByColumnExpression(Expression e) {

        if (e.getType() == OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }

        exprColumnList.add(e);

        isGrouped = true;

        groupByColumnCount++;
    }

    void addHavingExpression(Expression e) {

        exprColumnList.add(e);

        havingCondition   = e;
        havingColumnCount = 1;
    }

    void addSortAndSlice(SortAndSlice sortAndSlice) {
        this.sortAndSlice = sortAndSlice;
    }

    public void resolveReferences(Session session, RangeGroup[] rangeGroups) {

        if (isReferencesResolved) {
            return;
        }

        outerRanges = rangeGroups;

        resolveRangeVariables(session, rangeGroups);
        resolveColumnReferencesForAsterisk();
        finaliseColumns();
        resolveColumnReferences(session, rangeGroups);
        setReferenceableColumns();
        Expression.resolveColumnSet(session, RangeVariable.emptyArray,
                                    rangeGroups, unresolvedExpressions);

        unionColumnTypes     = new Type[indexLimitVisible];
        isReferencesResolved = true;
    }

    public boolean hasReference(RangeVariable range) {

        if (unresolvedExpressions == null) {
            return false;
        }

        for (int i = 0; i < unresolvedExpressions.size(); i++) {
            if (((Expression) unresolvedExpressions.get(i)).hasReference(
                    range)) {
                return true;
            }
        }

        return false;
    }

    public boolean areColumnsResolved() {
        return super.areColumnsResolved();
    }

    public void resolveTypes(Session session) {

        if (isResolved) {
            return;
        }

        resolveTypesPartOne(session);
        resolveTypesPartTwo(session);
        resolveTypesPartThree(session);
        ArrayUtil.copyArray(resultTable.colTypes, unionColumnTypes,
                            unionColumnTypes.length);
    }

    void resolveTypesPartOne(Session session) {

        if (isPartOneResolved) {
            return;
        }

        resolveExpressionTypes(session);
        resolveAggregates();

        for (int i = 0; i < unionColumnTypes.length; i++) {
            unionColumnTypes[i] = Type.getAggregateType(unionColumnTypes[i],
                    exprColumns[i].getDataType());
        }

        isPartOneResolved = true;
    }

    /**
     * additional resolution for union
     */
    void resolveTypesPartTwoRecursive(Session session) {

        for (int i = 0; i < unionColumnTypes.length; i++) {
            Type type = unionColumnTypes[i];

            exprColumns[i].setDataType(session, type);
        }

        setResultColumnTypes();
        createResultMetaData(session);
        createTable(session);
    }

    void resolveTypesPartTwo(Session session) {

        if (isPartTwoResolved) {
            return;
        }

        resolveGroups();

        for (int i = 0; i < unionColumnTypes.length; i++) {
            Type type = unionColumnTypes[i];

            if (type == null) {
                if (session.database.sqlEnforceTypes) {
                    throw Error.error(ErrorCode.X_42567);
                }

                type                = Type.SQL_VARCHAR_DEFAULT;
                unionColumnTypes[i] = type;
            }

            exprColumns[i].setDataType(session, type);

            if (exprColumns[i].dataType.isArrayType()
                    && exprColumns[i].dataType.collectionBaseType() == null) {
                throw Error.error(ErrorCode.X_42567);
            }
        }

        for (int i = indexLimitVisible; i < indexStartHaving; i++) {
            if (exprColumns[i].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }
        }

        checkLobUsage();
        setMergeability();
        setUpdatability();
        setResultColumnTypes();
        createResultMetaData(session);
        createTable(session);
        mergeQuery();

        isPartTwoResolved = true;
    }

    void resolveTypesPartThree(Session session) {

        if (isResolved) {
            return;
        }

        sortAndSlice.setSortIndex(this);
        setRangeVariableConditions(session);
        setDistinctConditions(session);
        setAggregateConditions(session);
        sortAndSlice.setSortRange(this);

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].resolveRangeTableTypes(session, rangeVariables);
        }

        setResultNullability();

        rangeVariableList = null;
        tempSet           = null;
        compileContext    = null;
        outerRanges       = null;
        isResolved        = true;
    }

    public void addExtraConditions(Expression e) {

        if (isAggregated || isGrouped) {
            return;
        }

        queryCondition = ExpressionLogical.andExpressions(queryCondition, e);
    }

    /**
     * Resolves all column expressions in the GROUP BY clause and beyond.
     * Replaces any alias column expression in the ORDER BY cluase
     * with the actual select column expression.
     */
    private void resolveColumnReferences(Session session,
                                         RangeGroup[] rangeGroups) {

        if (isDistinctSelect || isGrouped) {
            acceptsSequences = false;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            Expression e = rangeVariables[i].getJoinCondition();

            if (e == null) {
                continue;
            }

            resolveColumnReferencesAndAllocate(session, e, i + 1, rangeGroups,
                                               false);
        }

        resolveColumnReferencesAndAllocate(session, queryCondition,
                                           rangeVariables.length, rangeGroups,
                                           false);

        if (resolvedSubqueryExpressions != null) {

            // subqueries in conditions not to be converted to SIMPLE_COLUMN
            resolvedSubqueryExpressions.setSize(0);
        }

        for (int i = 0; i < indexLimitVisible; i++) {
            resolveColumnReferencesAndAllocate(session, exprColumns[i],
                                               rangeVariables.length,
                                               rangeGroups, acceptsSequences);

            if (!isGrouped && !isDistinctSelect) {
                HsqlList list = exprColumns[i].collectAllSubqueries(null);

                if (list != null) {
                    isMergeable = false;
                }

                list = exprColumns[i].collectAllExpressions(null,
                        Expression.sequenceExpressionSet,
                        Expression.subqueryAggregateExpressionSet);

                if (list != null) {
                    isOrderSensitive = true;
                    isMergeable      = false;
                    isBaseMergeable  = false;
                }
            }
        }

        for (int i = indexLimitVisible; i < indexStartHaving; i++) {
            exprColumns[i] = resolveColumnReferencesInGroupBy(session,
                    exprColumns[i]);
        }

        for (int i = indexStartHaving; i < indexStartOrderBy; i++) {
            resolveColumnReferencesAndAllocate(session, exprColumns[i],
                                               rangeVariables.length,
                                               rangeGroups, false);
        }

        // A VoltDB extension to support GROUP BY <column alias> (mysql-like feature)
        resolveColumnReferencesInGroupBy();
        // End of VoltDB extension
        resolveColumnRefernecesInOrderBy(session, rangeGroups, sortAndSlice);
    }
    // A VoltDB extension to support ???

    void resolveColumnReferencesInGroupBy() {
        if (! isAggregated) {
            return;
        }

        if (unresolvedExpressions == null || unresolvedExpressions.isEmpty()) {
            return;
        }

        /**
         * Hsql HashSet does not work properly to remove duplicates, I doubt the hash
         * function or equal function on expression work properly or something else
         * is wrong. So use list
         *
         */
        // resolve GROUP BY columns/expressions
        HsqlList newUnresolvedExpressions = new ArrayListIdentity();

        int size = unresolvedExpressions.size();
        for (int i = 0; i < size; i++) {
            Object obj = unresolvedExpressions.get(i);
            newUnresolvedExpressions.add(obj);
            if (i + 1 < size && obj == unresolvedExpressions.get(i+1)) {
                // unresolvedExpressions is a public member that can be accessed from anywhere and
                // I (xin) am 90% percent sure about the hsql adds the unresolved expression twice
                // together for our targeted GROUP BY alias case.
                // so we want to skip the repeated expression also.
                // For other unresolved expression, it may differs.
                i += 1;
            }
            if (obj instanceof ExpressionColumn == false) {
                continue;
            }
            ExpressionColumn element = (ExpressionColumn) obj;
            if (element.tableName != null) {
                // this alias does not belong to any table
                continue;
            }

            // group by alias which is thought as an column
            if (element.getType() != OpTypes.COLUMN) {
                continue;
            }

            // find the unsolved expression in the groupBy list
            int k = indexLimitVisible;
            int endGroupbyIndex = indexLimitVisible + groupByColumnCount;
            for (; k < endGroupbyIndex; k++) {
                if (element == exprColumns[k]) {
                    break;
                }
            }
            if (k == endGroupbyIndex) {
                // not found in selected list
                continue;
            }
            assert(exprColumns[k].getType() == OpTypes.COLUMN);

            ExpressionColumn exprCol = (ExpressionColumn) exprColumns[k];
            String alias = exprCol.getColumnName();
            if (alias == null) {
                // we should not handle this case (group by constants)
                continue;
            }

            // find it in the SELECT list
            for (int j = 0; j < indexLimitVisible; j++) {
                Expression selectCol = exprColumns[j];
                if (selectCol.isAggregate()) {
                    // Group by can not support aggregate expression
                    continue;
                }
                if (selectCol.alias == null) {
                    // columns referenced by their alias must have an alias
                    continue;
                }
                if (alias.equals(selectCol.alias.name)) {
                    exprColumns[k] = selectCol;
                    exprColumnList.set(k, selectCol);
                    // found it and get the next one

                    newUnresolvedExpressions.remove(element);
                    break;
                }
            }
        }
        unresolvedExpressions = newUnresolvedExpressions;
    }
    // End of VoltDB extension

    void resolveColumnRefernecesInOrderBy(Session session,
                                          RangeGroup[] rangeGroups,
                                          SortAndSlice sortAndSlice) {

        // replace the aliases with expressions
        // replace column names with expressions and resolve the table columns
        int orderCount = sortAndSlice.getOrderLength();

        for (int i = 0; i < orderCount; i++) {
            ExpressionOrderBy e =
                (ExpressionOrderBy) sortAndSlice.exprList.get(i);

            replaceColumnIndexInOrderBy(e);

            if (e.getLeftNode().queryTableColumnIndex != -1) {
                continue;
            }

            if (sortAndSlice.sortUnion) {
                if (e.getLeftNode().getType() != OpTypes.COLUMN) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }

            e.replaceAliasInOrderBy(session, exprColumns, indexLimitVisible);
            resolveColumnReferencesAndAllocate(session, e,
                                               rangeVariables.length,
                                               RangeGroup.emptyArray, false);

            if (isAggregated || isGrouped) {
                boolean check = e.getLeftNode().isComposedOf(exprColumns, 0,
                    indexLimitVisible + groupByColumnCount,
                    Expression.aggregateFunctionSet);

                if (!check) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }

        if (sortAndSlice.limitCondition != null) {
            unresolvedExpressions =
                sortAndSlice.limitCondition.resolveColumnReferences(session,
                    this, rangeGroups, unresolvedExpressions);
        }

        sortAndSlice.prepare(this);
    }

    private boolean resolveColumnReferences(Session session, Expression e,
            int rangeCount, boolean withSequences) {

        if (e == null) {
            return true;
        }

        int oldSize = unresolvedExpressions == null ? 0
                                                    : unresolvedExpressions
                                                        .size();

        unresolvedExpressions = e.resolveColumnReferences(session, this,
                rangeCount, RangeGroup.emptyArray, unresolvedExpressions,
                withSequences);

        int newSize = unresolvedExpressions == null ? 0
                                                    : unresolvedExpressions
                                                        .size();

        return oldSize == newSize;
    }

    private void resolveColumnReferencesForAsterisk() {

        for (int pos = 0; pos < indexLimitVisible; ) {
            Expression e = (Expression) (exprColumnList.get(pos));

            if (e.getType() == OpTypes.MULTICOLUMN) {
                exprColumnList.remove(pos);

                String tablename = ((ExpressionColumn) e).getTableName();

                if (tablename == null) {
                    addAllJoinedColumns(e);
                } else {
                    boolean resolved = false;

                    for (int i = 0; i < rangeVariables.length; i++) {
                        RangeVariable range =
                            rangeVariables[i].getRangeForTableName(tablename);

                        if (range != null) {
                            HashSet exclude = getAllNamedJoinColumns();

                            rangeVariables[i].addTableColumns(range, e,
                                                              exclude);

                            resolved = true;

                            break;
                        }
                    }

                    if (!resolved) {
                        throw Error.error(ErrorCode.X_42501, tablename);
                    }
                }

                for (int i = 0; i < e.nodes.length; i++) {
                    exprColumnList.add(pos, e.nodes[i]);

                    pos++;
                }

                indexLimitVisible += e.nodes.length - 1;
            } else {
                pos++;
            }
        }
    }

    private void resolveColumnReferencesAndAllocate(Session session,
            Expression expression, int count, RangeGroup[] rangeGroups,
            boolean withSequences) {

        if (expression == null) {
            return;
        }

        HsqlList list = expression.resolveColumnReferences(session, this,
            count, rangeGroups, null, withSequences);

        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                Expression e        = (Expression) list.get(i);
                boolean    resolved = true;

                if (e.isSelfAggregate()) {
                    for (int j = 0; j < e.nodes.length; j++) {
                        HsqlList colList = e.nodes[j].resolveColumnReferences(
                            session, this, count, RangeGroup.emptyArray, null,
                            false);

                        for (int k = 0; k < rangeGroups.length; k++) {
                            if (rangeGroups[k].isVariable()) {
                                colList = Expression.resolveColumnSet(
                                    session,
                                    rangeGroups[k].getRangeVariables(),
                                    RangeGroup.emptyArray, colList);
                            }
                        }

                        resolved &= colList == null;
                    }
                } else {
                    resolved = resolveColumnReferences(session, e, count,
                                                       withSequences);
                }

                if (resolved) {
                    if (e.isSelfAggregate()) {
                        if (aggregateSet == null) {
                            aggregateSet = new ArrayListIdentity();
                        }

                        aggregateSet.add(e);

                        isAggregated = true;

                        expression.setAggregate();
                    }

                    if (resolvedSubqueryExpressions == null) {
                        resolvedSubqueryExpressions = new ArrayListIdentity();
                    }

                    resolvedSubqueryExpressions.add(e);
                } else {
                    if (unresolvedExpressions == null) {
                        unresolvedExpressions = new ArrayListIdentity();
                    }

                    unresolvedExpressions.add(e);
                }
            }
        }
    }

    private Expression resolveColumnReferencesInGroupBy(Session session,
            Expression expression) {

        if (expression == null) {
            return null;
        }

        HsqlList list = expression.resolveColumnReferences(session, this,
            rangeVariables.length, RangeGroup.emptyArray, null, false);

        if (list != null) {

            // if not resolved, resolve as simple alias
            if (expression.getType() == OpTypes.COLUMN) {
                Expression resolved = expression.replaceAliasInOrderBy(session,
                    exprColumns, indexLimitVisible);

                if (resolved != expression) {
                    return resolved;
                }
            }

            // resolve and allocate to throw exception
            resolveColumnReferencesAndAllocate(session, expression,
                                               rangeVariables.length,
                                               RangeGroup.emptyArray, false);
        }

        return expression;
    }

    private HashSet getAllNamedJoinColumns() {

        HashSet set = null;

        for (int i = 0; i < rangeVariableList.size(); i++) {
            RangeVariable range = (RangeVariable) rangeVariableList.get(i);

            if (range.namedJoinColumns != null) {
                if (set == null) {
                    set = new HashSet();
                }

                set.addAll(range.namedJoinColumns);
            }
        }

        return set;
    }

    public Expression getEquiJoinExpressions(OrderedHashSet nameSet,
            RangeVariable rightRange, boolean fullList) {

        HashSet        set             = new HashSet();
        Expression     result          = null;
        OrderedHashSet joinColumnNames = new OrderedHashSet();

        for (int i = 0; i < rangeVariableList.size(); i++) {
            RangeVariable  range = (RangeVariable) rangeVariableList.get(i);
            HashMappedList columnList = range.rangeTable.columnList;

            for (int j = 0; j < columnList.size(); j++) {
                ColumnSchema column       = (ColumnSchema) columnList.get(j);
                String       name         = range.getColumnAlias(j).name;
                boolean      columnInList = nameSet.contains(name);
                boolean namedJoin = range.namedJoinColumns != null
                                    && range.namedJoinColumns.contains(name);
                boolean repeated = !namedJoin && !set.add(name);

                if (repeated && (!fullList || columnInList)) {
                    throw Error.error(ErrorCode.X_42578, name);
                }

                if (!columnInList) {
                    continue;
                }

                joinColumnNames.add(name);

                int leftPosition =
                    range.rangeTable.getColumnIndex(column.getNameString());
                int rightPosition = rightRange.rangeTable.getColumnIndex(name);
                Expression e = new ExpressionLogical(range, leftPosition,
                                                     rightRange,
                                                     rightPosition);

                result = ExpressionLogical.andExpressions(result, e);

                ExpressionColumn col = range.getColumnExpression(name);

                if (col == null) {
                    col = new ExpressionColumn(new Expression[] {
                        e.getLeftNode(), e.getRightNode()
                    }, name);

                    range.addNamedJoinColumnExpression(name, col);
                } else {
                    col.nodes = (Expression[]) ArrayUtil.resizeArray(col.nodes,
                            col.nodes.length + 1);
                    col.nodes[col.nodes.length - 1] = e.getRightNode();
                }

                rightRange.addNamedJoinColumnExpression(name, col);
            }
        }

        if (fullList && !joinColumnNames.containsAll(nameSet)) {
            throw Error.error(ErrorCode.X_42501);
        }

        rightRange.addNamedJoinColumns(joinColumnNames);

        return result;
    }

    private void addAllJoinedColumns(Expression e) {

        HsqlArrayList list = new HsqlArrayList();

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].addTableColumns(list);
        }

        Expression[] nodes = new Expression[list.size()];

        list.toArray(nodes);

        e.nodes = nodes;
    }

    private void finaliseColumns() {

        indexLimitRowId   = indexLimitVisible;
        indexStartHaving  = indexLimitRowId + groupByColumnCount;
        indexStartOrderBy = indexStartHaving + havingColumnCount;
        indexStartAggregates = indexStartOrderBy
                               + sortAndSlice.getOrderLength();
        indexLimitData = indexLimitExpressions = indexStartAggregates;
        exprColumns    = new Expression[indexLimitExpressions];

        exprColumnList.toArray(exprColumns);

        exprColumnList = null;

        for (int i = 0; i < indexLimitVisible; i++) {
            exprColumns[i].queryTableColumnIndex = i;
        }

        if (sortAndSlice.hasOrder()) {
            for (int i = 0; i < sortAndSlice.getOrderLength(); i++) {
                exprColumns[indexStartOrderBy + i] =
                    (Expression) sortAndSlice.exprList.get(i);
            }
        }

        rowExpression = new Expression(OpTypes.ROW, exprColumns);
    }

    private void replaceColumnIndexInOrderBy(Expression orderBy) {

        Expression e = orderBy.getLeftNode();

        if (e.getType() != OpTypes.VALUE) {
            return;
        }

        Type type = e.getDataType();

        if (type != null && type.typeCode == Types.SQL_INTEGER) {
            int i = ((Integer) e.getValue(null)).intValue();

            if (0 < i && i <= indexLimitVisible) {
                orderBy.setLeftNode(exprColumns[i - 1]);

                return;
            }
        }

        throw Error.error(ErrorCode.X_42576);
    }

    OrderedHashSet collectRangeVariables(RangeVariable[] rangeVars,
                                         OrderedHashSet set) {

        for (int i = 0; i < indexStartAggregates; i++) {
            set = exprColumns[i].collectRangeVariables(rangeVars, set);
        }

        if (queryCondition != null) {
            set = queryCondition.collectRangeVariables(rangeVars, set);
        }

        if (havingCondition != null) {
            set = havingCondition.collectRangeVariables(rangeVars, set);
        }

        return set;
    }

    OrderedHashSet collectRangeVariables(OrderedHashSet set) {

        for (int i = 0; i < indexStartAggregates; i++) {
            set = exprColumns[i].collectRangeVariables(set);
        }

        if (queryCondition != null) {
            set = queryCondition.collectRangeVariables(set);
        }

        if (havingCondition != null) {
            set = havingCondition.collectRangeVariables(set);
        }

        return set;
    }

    /**
     * Sets the types of all the expressions used in this SELECT list.
     */
    public void resolveExpressionTypes(Session session) {

        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];

            e.resolveTypes(session, rowExpression);

            if (e.getType() == OpTypes.ROW) {
                throw Error.error(ErrorCode.X_42565);
            }

            if (e.getType() == OpTypes.ROW_SUBQUERY && e.getDegree() > 1) {
                throw Error.error(ErrorCode.X_42565);
            }

            if (e.getDataType() != null
                    && e.getDataType().typeCode == Types.SQL_ROW) {
                throw Error.error(ErrorCode.X_42565);
            }
        }

        if (rangeVariables != null) {
            for (int i = 0; i < rangeVariables.length; i++) {
                Expression e = rangeVariables[i].getJoinCondition();

                if (e != null) {
                    e.resolveTypes(session, null);

                    if (e.getDataType() != Type.SQL_BOOLEAN) {
                        throw Error.error(ErrorCode.X_42568);
                    }
                }
            }
        }
        if (queryCondition != null) {
            queryCondition.resolveTypes(session, null);

            if (queryCondition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
            // A VoltDB extension to guard against abuse of aggregates in subqueries.
            // Make sure no aggregates in WHERE clause
            tempSet.clear();
            queryCondition.collectAllExpressions(
                    tempSet, Expression.aggregateFunctionSet,
                    Expression.subqueryExpressionSet);
            if (!tempSet.isEmpty()) {

                // A top level WHERE clause can't have aggregate expressions.
                // In theory, a subquery WHERE clause may have aggregate
                // expressions in some edge cases where they reference only
                // columns from parent query(s).
                // Even these should be restricted to cases where the subquery
                // is evaluated after the parent agg, such as from the HAVING
                // or SELECT clause of the parent query defining the columns.
                // TO be safe, VoltDB doesn't support ANY cases of aggs of
                // parent columns. All this code block does is choose between
                // two error messages for two different unsupported cases.
                if ( ! isTopLevel) {
                    OrderedHashSet columnSet = new OrderedHashSet();
                    org.hsqldb_voltpatches.lib.Iterator aggIt = tempSet.iterator();
                    while (aggIt.hasNext()) {
                        Expression nextAggr = (Expression) aggIt.next();
                        nextAggr.collectAllExpressions(columnSet,
                                Expression.columnExpressionSet, Expression.emptyExpressionSet);
                    }
                    org.hsqldb_voltpatches.lib.Iterator columnIt = columnSet.iterator();
                    while (columnIt.hasNext()) {
                        Expression nextColumn = (Expression) columnIt.next();
                        assert(nextColumn instanceof ExpressionColumn);
                        ExpressionColumn nextColumnEx = (ExpressionColumn) nextColumn;
                        String tableName = nextColumnEx.rangeVariable.rangeTable.tableName.name;
                        String tableAlias = (nextColumnEx.rangeVariable.tableAlias != null) ?
                                nextColumnEx.rangeVariable.tableAlias.name : null;
                        boolean resolved = false;
                        for (RangeVariable rv : rangeVariables) {
                            if (rv.rangeTable.tableName.name.equals(tableName)) {
                                if (rv.tableAlias == null && tableAlias == null) {
                                    resolved = true;
                                } else if (rv.tableAlias != null && tableAlias != null) {
                                    resolved = tableAlias.equals(rv.tableAlias.name);
                                }
                            }
                        }
                        if (!resolved) {
                            throw Error.error(ErrorCode.X_47001);
                        }
                    }
                }
                // If we get here it means that WHERE expression has an aggregate expression
                // with local columns
                throw Error.error(ErrorCode.X_47000);
            }
            // End of VoltDB extension
        }

        if (havingCondition != null) {
            havingCondition.resolveTypes(session, null);

            if (havingCondition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        if (sortAndSlice.limitCondition != null) {
            sortAndSlice.limitCondition.resolveTypes(session, null);
        }
    }

    private void resolveAggregates() {

        tempSet.clear();

        if (isAggregated) {
            aggregateCheck = new boolean[indexStartAggregates];

            tempSet.addAll(aggregateSet);

            indexLimitData = indexLimitExpressions = exprColumns.length
                    + tempSet.size();
            exprColumns = (Expression[]) ArrayUtil.resizeArray(exprColumns,
                    indexLimitExpressions);

            for (int i = indexStartAggregates, j = 0;
                    i < indexLimitExpressions; i++, j++) {
                Expression e = (Expression) tempSet.get(j);

                exprColumns[i]          = e.duplicate();
                exprColumns[i].nodes    = e.nodes;    // keep original nodes
                exprColumns[i].dataType = e.dataType;
            }

            tempSet.clear();
        }
    }

    private void setRangeVariableConditions(Session session) {

        RangeVariableResolver rangeResolver = new RangeVariableResolver(this);

        rangeResolver.processConditions(session);

        rangeVariables = rangeResolver.rangeVariables;
    }

    private void setDistinctConditions(Session session) {

        if (!isDistinctSelect && !isGrouped) {
            return;
        }

        if (isAggregated) {
            return;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].isLateral || rangeVariables[i].isLeftJoin
                    || rangeVariables[i].isRightJoin) {
                return;
            }
        }

        RangeVariable range = null;
        int[]         colMap;

        if (isGrouped) {
            colMap = new int[groupByColumnCount];

            for (int i = 0; i < groupByColumnCount; i++) {
                if (exprColumns[indexLimitRowId + i].getType()
                        != OpTypes.COLUMN) {
                    return;
                }

                if (range == null) {
                    range =
                        exprColumns[indexLimitRowId + i].getRangeVariable();
                } else {
                    if (range != exprColumns[indexLimitRowId + i]
                            .getRangeVariable()) {
                        return;
                    }
                }

                colMap[i] = exprColumns[i].columnIndex;
            }
        } else {
            colMap = new int[indexLimitVisible];
        }

        for (int i = 0; i < indexLimitVisible; i++) {
            if (exprColumns[i].getType() != OpTypes.COLUMN) {
                return;
            }

            if (range == null) {
                range = exprColumns[i].getRangeVariable();
            } else {
                if (range != exprColumns[i].getRangeVariable()) {
                    return;
                }
            }

            if (!isGrouped) {
                colMap[i] = exprColumns[i].columnIndex;
            }
        }

        boolean check = ArrayUtil.areAllIntIndexesAsBooleanArray(colMap,
            range.usedColumns);

        if (!check) {
            return;
        }

        if (!range.hasAnyIndexCondition()) {
            Index index = range.rangeTable.getIndexForColumns(colMap);

            if (index != null) {
                range.setSortIndex(index, false);
            }
        }

        isSimpleDistinct = range.setDistinctColumnsOnIndex(colMap);
    }

    private void setAggregateConditions(Session session) {

        if (!isAggregated) {
            return;
        }

        if (isGrouped) {
            setGroupedAggregateConditions(session);
        } else if (!sortAndSlice.hasOrder() && !sortAndSlice.hasLimit()
                   && aggregateSet.size() == 1 && indexLimitVisible == 1) {
            Expression e      = exprColumns[indexStartAggregates];
            int        opType = e.getType();

            switch (opType) {

                case OpTypes.MAX :
                case OpTypes.MIN : {
                    if (e.hasCondition()) {
                        break;
                    }

                    SortAndSlice slice = new SortAndSlice();

                    slice.isGenerated = true;

                    slice.addLimitCondition(ExpressionOp.limitOneExpression);

                    if (slice.prepareSpecial(session, this)) {
                        this.sortAndSlice = slice;
                    }

                    break;
                }
                case OpTypes.COUNT : {
                    if (e.hasCondition()) {
                        break;
                    }

                    if (rangeVariables.length == 1 && queryCondition == null) {
                        Expression expr = e.getLeftNode();

                        if (expr.getType() == OpTypes.ASTERISK) {
                            isSimpleCount = true;
                        } else if (expr.getNullability()
                                   == SchemaObject.Nullability.NO_NULLS) {
                            if (((ExpressionAggregate) e)
                                    .isDistinctAggregate) {
                                if (expr.opType == OpTypes.COLUMN) {
                                    Table t =
                                        expr.getRangeVariable().getTable();

                                    if (t.getPrimaryKey().length == 1) {
                                        if (t.getColumn(t.getPrimaryKey()[0])
                                                == expr.getColumn()) {
                                            isSimpleCount = true;
                                        }
                                    }
                                }
                            } else {
                                isSimpleCount = true;
                            }
                        }
                    }

                    break;
                }
                default :
            }
        }
    }

    private void setGroupedAggregateConditions(Session session) {

        //
    }

    void checkLobUsage() {}

    private void resolveGroups() {

        // - 1.9.0 is standard compliant but has more extended support for
        //   referencing columns
        // - check there is no direct aggregate expression in group by
        // - check each expression in select list can be
        //   decomposed into the expressions in group by or any aggregates
        //   this allows direct function of group by expressions, but
        //   doesn't allow indirect functions. e.g.
        //     select 2*abs(cola) , sum(colb) from t group by abs(cola) // ok
        //     select 2*(cola + 10) , sum(colb) from t group by cola + 10 // ok
        //     select abs(cola) , sum(colb) from t group by cola // ok
        //     select 2*cola + 20 , sum(colb) from t group by cola + 10 // not allowed although correct
        //     select cola , sum(colb) from t group by abs(cola) // not allowed because incorrect
        // - group by can introduce invisible, derived columns into the query table
        // - check the having expression can be decomposed into
        //   select list expresions plus group by expressions
        // - having cannot introduce additional, derived columns
        // - having cannot reference columns not in the select or group by list
        // - if there is any aggregate in select list but no group by, no
        //   non-aggregates is allowed
        // - check order by columns
        // - if distinct select, order by must be composed of the select list columns
        // - if grouped by, then order by should be decomposed into the
        //   select list plus group by list
        // - references to column aliases are allowed only in order by (Standard
        //   compliance) and take precendence over references to non-alias
        //   column names.
        // - references to table / correlation and column list in correlation
        //   names are handled according to the Standard
        //  fredt@users
        tempSet.clear();

        if (isGrouped) {
            for (int i = indexLimitVisible;
                    i < indexLimitVisible + groupByColumnCount; i++) {
                exprColumns[i].collectAllExpressions(
                    tempSet, Expression.aggregateFunctionSet,
                    Expression.subqueryExpressionSet);

                if (!tempSet.isEmpty()) {
                    throw Error.error(ErrorCode.X_42572,
                                      ((Expression) tempSet.get(0)).getSQL());
                }
            }

            for (int i = 0; i < indexLimitVisible; i++) {
                if (!exprColumns[i].isComposedOf(
                        exprColumns, indexLimitVisible,
                        indexLimitVisible + groupByColumnCount,
                        Expression.aggregateFunctionSet)) {
                    tempSet.add(exprColumns[i]);
                }
            }

            if (!tempSet.isEmpty() && !resolveForGroupBy(tempSet)) {
                throw Error.error(ErrorCode.X_42574,
                                  ((Expression) tempSet.get(0)).getSQL());
            }
        } else if (isAggregated) {
            for (int i = 0; i < indexLimitVisible; i++) {
                exprColumns[i].collectAllExpressions(
                    tempSet, Expression.columnExpressionSet,
                    Expression.aggregateFunctionSet);

                if (!tempSet.isEmpty()) {
                    throw Error.error(ErrorCode.X_42574,
                                      ((Expression) tempSet.get(0)).getSQL());
                }
            }
        }

        tempSet.clear();

        if (havingCondition != null) {
            if (unresolvedExpressions != null) {
                tempSet.addAll(unresolvedExpressions);
            }

            for (int i = indexLimitVisible;
                    i < indexLimitVisible + groupByColumnCount; i++) {
                tempSet.add(exprColumns[i]);
            }

            if (!havingCondition.isComposedOf(
                    tempSet, outerRanges,
                    Expression.subqueryAggregateExpressionSet)) {
                throw Error.error(ErrorCode.X_42573);
            }

            tempSet.clear();
        }

        if (isDistinctSelect) {
            int orderCount = sortAndSlice.getOrderLength();

            for (int i = 0; i < orderCount; i++) {
                Expression e = (Expression) sortAndSlice.exprList.get(i);

                if (e.queryTableColumnIndex != -1) {
                    continue;
                }

                if (!e.isComposedOf(exprColumns, 0, indexLimitVisible,
                                    Expression.emptyExpressionSet)) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }

        if (isGrouped) {
            int orderCount = sortAndSlice.getOrderLength();

            for (int i = 0; i < orderCount; i++) {
                Expression e = (Expression) sortAndSlice.exprList.get(i);

                if (e.queryTableColumnIndex != -1) {
                    continue;
                }

                if (!e.isAggregate()
                        && !e.isComposedOf(
                            exprColumns, 0,
                            indexLimitVisible + groupByColumnCount,
                            Expression.emptyExpressionSet)) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }

        if (!isAggregated) {
            return;
        }

        OrderedHashSet expressions       = new OrderedHashSet();
        OrderedHashSet columnExpressions = new OrderedHashSet();

        for (int i = indexStartAggregates; i < indexLimitExpressions; i++) {
            Expression e = exprColumns[i];
            Expression c = new ExpressionColumn(e, i, resultRangePosition);

            expressions.add(e);
            columnExpressions.add(c);
        }

        for (int i = 0; i < indexStartHaving; i++) {
            if (exprColumns[i].isAggregate()) {
                continue;
            }

            Expression e = exprColumns[i];

            if (expressions.add(e)) {
                Expression c = new ExpressionColumn(e, i, resultRangePosition);

                columnExpressions.add(c);
            }
        }

        // order by with aggregate
        int orderCount = sortAndSlice.getOrderLength();

        for (int i = 0; i < orderCount; i++) {
            Expression e = (Expression) sortAndSlice.exprList.get(i);

            if (e.getLeftNode().isAggregate()) {
                e.setAggregate();
            }
        }

        for (int i = indexStartOrderBy; i < indexStartAggregates; i++) {
            if (exprColumns[i].getLeftNode().isAggregate()) {
                exprColumns[i].setAggregate();
            }
        }

        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];

            if (!e.isAggregate() /* && !e.isCorrelated() */) {
                continue;
            }

            aggregateCheck[i] = true;

            if (e.isAggregate()) {
                e.convertToSimpleColumn(expressions, columnExpressions);
            }
        }

        for (int i = 0; i < aggregateSet.size(); i++) {
            Expression e = (Expression) aggregateSet.get(i);

            e.convertToSimpleColumn(expressions, columnExpressions);
        }

        if (resolvedSubqueryExpressions != null) {
            for (int i = 0; i < resolvedSubqueryExpressions.size(); i++) {
                Expression e = (Expression) resolvedSubqueryExpressions.get(i);

                e.convertToSimpleColumn(expressions, columnExpressions);
            }
        }
    }

    boolean resolveForGroupBy(HsqlList unresolvedSet) {

        for (int i = indexLimitVisible;
                i < indexLimitVisible + groupByColumnCount; i++) {
            Expression e = exprColumns[i];

            if (e.getType() == OpTypes.COLUMN) {
                RangeVariable range    = e.getRangeVariable();
                int           colIndex = e.getColumnIndex();

                range.columnsInGroupBy[colIndex] = true;
            }
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable range = rangeVariables[i];

            range.hasKeyedColumnInGroupBy =
                range.rangeTable.getUniqueNotNullColumnGroup(
                    range.columnsInGroupBy) != null;
        }

        OrderedHashSet set = null;

        for (int i = 0; i < unresolvedSet.size(); i++) {
            Expression e = (Expression) unresolvedSet.get(i);

            set = e.getUnkeyedColumns(set);
        }

        return set == null;
    }

    /**
     * Returns the result of executing this Select.
     *
     * @param maxrows may be 0 to indicate no limit on the number of rows.
     * Positive values limit the size of the result set.
     * @return the result of executing this Select
     */
    Result getResult(Session session, int maxrows) {

//todo single row
        Result r = getSingleResult(session, maxrows);

        r.getNavigator().reset();

        return r;
    }

    private Result getSingleResult(Session session, int maxRows) {

        int[] limits = sortAndSlice.getLimits(session, this, maxRows);
        Result              r         = buildResult(session, limits);
        RowSetNavigatorData navigator = (RowSetNavigatorData) r.getNavigator();

        if (isDistinctSelect) {
            navigator.removeDuplicates(session);
        }

        if (sortAndSlice.hasOrder()) {
            navigator.sortOrder(session);
        }

        if (limits != SortAndSlice.defaultLimits
                && !sortAndSlice.skipFullResult) {
            navigator.trim(limits[0], limits[1]);
        }

        return r;
    }

    private Result buildResult(Session session, int[] limits) {

        RowSetNavigatorData navigator = new RowSetNavigatorData(session,
            (QuerySpecification) this);
        Result  result        = Result.newResult(navigator);
        boolean resultGrouped = isGrouped && !isSimpleDistinct;

        result.metaData = resultMetaData;

        if (isUpdatable) {
            result.rsProperties = ResultProperties.updatablePropsValue;
        }

        int skipCount  = 0;
        int limitCount = limits[2];

        if (sortAndSlice.skipFullResult) {
            skipCount  = limits[0];
            limitCount = limits[1];
        }

        if (this.isSimpleCount) {
            Object[] data  = new Object[indexLimitData];
            Table    table = rangeVariables[0].getTable();

            table.materialise(session);

            PersistentStore store = table.getRowStore(session);
            long            count = store.elementCount(session);

            data[0] = data[indexStartAggregates] = ValuePool.getLong(count);

            navigator.add(data);

            return result;
        }

        int fullJoinIndex = 0;
        RangeIterator[] rangeIterators =
            new RangeIterator[rangeVariables.length];

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeIterators[i] = rangeVariables[i].getIterator(session);
        }

        session.sessionContext.rownum = 1;

        for (int currentIndex = 0; ; ) {
            if (currentIndex < fullJoinIndex) {

                // finished current span
                // or finished outer rows on right navigator
                boolean end = true;

                for (int i = fullJoinIndex + 1; i < rangeVariables.length;
                        i++) {
                    if (rangeVariables[i].isRightJoin) {
                        fullJoinIndex = i;
                        currentIndex  = i;
                        end           = false;

                        ((RangeIteratorRight) rangeIterators[i])
                            .setOnOuterRows();

                        break;
                    }
                }

                if (end) {
                    break;
                }
            }

            RangeIterator it = rangeIterators[currentIndex];

            if (it.next()) {
                if (currentIndex < rangeVariables.length - 1) {
                    currentIndex++;

                    continue;
                }
            } else {
                it.reset();

                currentIndex--;

                continue;
            }

            if (limitCount == 0) {
                break;
            }

            session.sessionData.startRowProcessing();

            Object[] data = new Object[indexLimitData];

            for (int i = 0; i < indexStartAggregates; i++) {
                if (isAggregated && aggregateCheck[i]) {
                    continue;
                } else {
                    data[i] = exprColumns[i].getValue(session);
                }
            }

            for (int i = indexLimitVisible; i < indexLimitRowId; i++) {
                if (i == indexLimitVisible) {
                    data[i] = it.getRowidObject();
                } else {
                    data[i] = it.getCurrentRow();
                }
            }

            session.sessionContext.rownum++;

            if (skipCount > 0) {
                skipCount--;

                continue;
            }

            Object[] groupData = null;

            if (isAggregated || resultGrouped) {
                groupData = navigator.getGroupData(data);

                if (groupData != null) {
                    data = groupData;
                }
            }

            for (int i = indexStartAggregates; i < indexLimitExpressions;
                    i++) {
                data[i] = exprColumns[i].updateAggregatingValue(session,
                        data[i]);
            }

            if (groupData == null) {
                navigator.add(data);
            } else if (isAggregated) {
                navigator.update(groupData, data);
            }

            int rowCount = navigator.getSize();

            if (rowCount == session.resultMaxMemoryRows && !isAggregated
                    && !isSingleMemoryTable) {
                navigator = new RowSetNavigatorDataTable(session, this,
                        navigator);

                result.setNavigator(navigator);
            }

            if (isAggregated || resultGrouped) {
                if (!sortAndSlice.isGenerated) {
                    continue;
                }
            }

            if (rowCount >= limitCount) {
                break;
            }
        }

        navigator.reset();

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeIterators[i].reset();
        }

        if (!resultGrouped && !isAggregated) {
            return result;
        }

        if (isAggregated) {
            if (!resultGrouped && navigator.getSize() == 0) {
                Object[] data = new Object[exprColumns.length];

                for (int i = 0; i < indexStartAggregates; i++) {
                    if (!aggregateCheck[i]) {
                        data[i] = exprColumns[i].getValue(session);
                    }
                }

                navigator.add(data);
            }

            navigator.reset();
            session.sessionContext.setRangeIterator(navigator);

            while (navigator.next()) {
                Object[] data = navigator.getCurrent();

                for (int i = indexStartAggregates; i < indexLimitExpressions;
                        i++) {
                    data[i] = exprColumns[i].getAggregatedValue(session,
                            data[i]);
                }

                for (int i = 0; i < indexStartAggregates; i++) {
                    if (aggregateCheck[i]) {
                        data[i] = exprColumns[i].getValue(session);
                    }
                }
            }

            session.sessionContext.unsetRangeIterator(navigator);
        }

        navigator.reset();

        if (havingCondition != null) {
            while (navigator.hasNext()) {
                Object[] data = (Object[]) navigator.getNext();

                if (!Boolean.TRUE.equals(
                        data[indexLimitVisible + groupByColumnCount])) {
                    navigator.removeCurrent();
                }
            }

            navigator.reset();
        }

        return result;
    }

    void setReferenceableColumns() {

        accessibleColumns = new boolean[indexLimitVisible];

        IntValueHashMap aliases = new IntValueHashMap();

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression expression = exprColumns[i];
            String     alias      = expression.getAlias();

            if (alias.length() == 0) {
                SimpleName name = HsqlNameManager.getAutoColumnName(i);

                expression.setAlias(name);

                continue;
            }

            int index = aliases.get(alias, -1);

            if (index == -1) {
                aliases.put(alias, i);

                accessibleColumns[i] = true;
            } else {
                accessibleColumns[index] = false;
            }
        }
    }

    void setColumnAliases(SimpleName[] names) {

        if (names.length != indexLimitVisible) {
            throw Error.error(ErrorCode.X_42593);
        }

        for (int i = 0; i < indexLimitVisible; i++) {
            exprColumns[i].setAlias(names[i]);
        }
    }

    private void createResultMetaData(Session session) {

        resultMetaData = ResultMetaData.newResultMetaData(resultColumnTypes,
                columnMap, indexLimitVisible, indexLimitRowId);

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression   e           = exprColumns[i];
            ColumnSchema tableColumn = null;
            ColumnBase   column;

            tableColumn                   = e.getColumn();
            resultMetaData.columnTypes[i] = e.getDataType();

            if (tableColumn == null) {
                column = new ColumnBase();
            } else {
                column = new ColumnBase(session.database.getCatalogName().name,
                                        tableColumn);
            }

            column.setType(e.getDataType());

            resultMetaData.columns[i]      = column;
            resultMetaData.columnLabels[i] = e.getAlias();
        }
    }

    private void setResultNullability() {

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e           = exprColumns[i];
            byte       nullability = e.getNullability();

            if (e.opType == OpTypes.COLUMN) {
                RangeVariable range = e.getRangeVariable();

                if (range != null) {
                    if (range.rangePositionInJoin >= startInnerRange
                            && range.rangePositionInJoin < endInnerRange) {

                        //
                    } else {
                        nullability = SchemaObject.Nullability.NULLABLE;
                    }
                }
            }

            resultMetaData.columns[i].setNullability(nullability);
        }
    }

    void createTable(Session session) {

        createResultTable(session);

        mainIndex = resultTable.getPrimaryIndex();

        if (sortAndSlice.hasOrder() && !sortAndSlice.skipSort) {
            orderIndex = sortAndSlice.getNewIndex(session, resultTable);
        }

        if (isDistinctSelect || isFullOrder) {
            createFullIndex(session);
        }

        if (isGrouped) {
            int[] groupCols = new int[groupByColumnCount];

            for (int i = 0; i < groupByColumnCount; i++) {
                groupCols[i] = indexLimitRowId + i;
            }

            groupIndex = resultTable.createAndAddIndexStructure(session, null,
                    groupCols, null, null, false, false, false);
        } else if (isAggregated) {
            groupIndex = mainIndex;
        }

        if (isUpdatable && view == null) {
            int[] idCols = new int[]{ indexLimitVisible };

            idIndex = resultTable.createAndAddIndexStructure(session, null,
                    idCols, null, null, false, false, false);
        }
    }

    private void createFullIndex(Session session) {

        int[] fullCols = new int[indexLimitVisible];

        ArrayUtil.fillSequence(fullCols);

        fullIndex = resultTable.createAndAddIndexStructure(session, null,
                fullCols, null, null, false, false, false);
        resultTable.fullIndex = fullIndex;
    }

    private void setResultColumnTypes() {

        resultColumnTypes = new Type[indexLimitData];

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e = exprColumns[i];

            resultColumnTypes[i] = e.getDataType();
        }

        for (int i = indexLimitVisible; i < indexLimitRowId; i++) {
            if (i == indexLimitVisible) {
                resultColumnTypes[i] = Type.SQL_BIGINT;
            } else {
                resultColumnTypes[i] = Type.SQL_ALL_TYPES;
            }
        }

        for (int i = indexLimitRowId; i < indexLimitData; i++) {
            Expression e    = exprColumns[i];
            Type       type = e.getDataType();

            if (type.getCollation() != e.collation && e.collation != null) {
                type = Type.getType(type, e.collation);
            }

            resultColumnTypes[i] = type;
        }
    }

    void createResultTable(Session session) {

        HsqlName tableName =
            session.database.nameManager.getSubqueryTableName();
        int tableType = persistenceScope == TableBase.SCOPE_STATEMENT
                        ? TableBase.SYSTEM_SUBQUERY
                        : TableBase.RESULT_TABLE;
        HashMappedList columnList = new HashMappedList();

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e          = exprColumns[i];
            SimpleName simpleName = e.getSimpleName();
            String     nameString = simpleName.name;
            HsqlName name =
                session.database.nameManager.newColumnSchemaHsqlName(tableName,
                    simpleName);

            if (!accessibleColumns[i]) {
                nameString = HsqlNameManager.getAutoNoNameColumnString(i);
            }

            ColumnSchema column = new ColumnSchema(name, e.dataType, true,
                                                   false, null);

            columnList.add(nameString, column);
        }

        try {
            resultTable = new TableDerived(session.database, tableName,
                                           tableType, resultColumnTypes,
                                           columnList,
                                           ValuePool.emptyIntArray);
        } catch (Exception e) {}
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();
        int          limit;

        sb.append(Tokens.T_SELECT).append(' ');

        limit = indexLimitVisible;

        for (int i = 0; i < limit; i++) {
            if (i > 0) {
                sb.append(',');
            }

            sb.append(exprColumns[i].getSQL());
        }

        sb.append(Tokens.T_FROM);

        limit = rangeVariables.length;

        for (int i = 0; i < limit; i++) {
            RangeVariable rangeVar = rangeVariables[i];

            if (i > 0) {
                if (rangeVar.isLeftJoin && rangeVar.isRightJoin) {
                    sb.append(Tokens.T_FULL).append(' ');
                } else if (rangeVar.isLeftJoin) {
                    sb.append(Tokens.T_LEFT).append(' ');
                } else if (rangeVar.isRightJoin) {
                    sb.append(Tokens.T_RIGHT).append(' ');
                }

                sb.append(Tokens.T_JOIN).append(' ');
            }

            sb.append(rangeVar.getTable().getName().statementName);
        }

        if (isGrouped) {
            sb.append(' ').append(Tokens.T_GROUP).append(' ').append(
                Tokens.T_BY);

            limit = indexLimitVisible + groupByColumnCount;

            for (int i = indexLimitVisible; i < limit; i++) {
                sb.append(exprColumns[i].getSQL());

                if (i < limit - 1) {
                    sb.append(',');
                }
            }
        }

        if (havingCondition != null) {
            sb.append(' ').append(Tokens.T_HAVING).append(' ');
            sb.append(havingCondition.getSQL());
        }

        if (sortAndSlice.hasOrder()) {
            limit = indexStartOrderBy + sortAndSlice.getOrderLength();

            sb.append(' ').append(Tokens.T_ORDER).append(Tokens.T_BY).append(
                ' ');

            for (int i = indexStartOrderBy; i < limit; i++) {
                sb.append(exprColumns[i].getSQL());

                if (i < limit - 1) {
                    sb.append(',');
                }
            }
        }

        if (sortAndSlice.hasLimit()) {
            sb.append(sortAndSlice.limitCondition.getLeftNode().getSQL());
        }

        return sb.toString();
    }

    public ResultMetaData getMetaData() {
        return resultMetaData;
    }

    public String describe(Session session, int blanks) {

        StringBuffer sb;
        String       temp;
        StringBuffer b = new StringBuffer(blanks);

        for (int i = 0; i < blanks; i++) {
            b.append(' ');
        }

        sb = new StringBuffer();

        sb.append(b).append("isDistinctSelect=[").append(
            isDistinctSelect).append("]\n");
        sb.append(b).append("isGrouped=[").append(isGrouped).append("]\n");
        sb.append(b).append("isAggregated=[").append(isAggregated).append(
            "]\n");
        sb.append(b).append("columns=[");

        for (int i = 0; i < indexLimitVisible; i++) {
            int index = i;

            if (exprColumns[i].getType() == OpTypes.SIMPLE_COLUMN) {
                index = exprColumns[i].columnIndex;
            }

            sb.append(b);

            temp = exprColumns[index].describe(session, 2);

            sb.append(temp.substring(0, temp.length() - 1));

            if (resultMetaData.columns[i].getNullability()
                    == SchemaObject.Nullability.NO_NULLS) {
                sb.append(" not nullable\n");
            } else {
                sb.append(" nullable\n");
            }
        }

        sb.append("\n");
        sb.append(b).append("]\n");

        for (int i = 0; i < rangeVariables.length; i++) {
            sb.append(b).append("[");
            sb.append("range variable ").append(i + 1).append("\n");
            sb.append(rangeVariables[i].describe(session, blanks + 2));
            sb.append(b).append("]");
        }

        sb.append(b).append("]\n");

        temp = queryCondition == null ? "null"
                                      : queryCondition.describe(session,
                                      blanks);

        if (isGrouped) {
            sb.append(b).append("groupColumns=[");

            for (int i = indexLimitRowId;
                    i < indexLimitRowId + groupByColumnCount; i++) {
                int index = i;

                if (exprColumns[i].getType() == OpTypes.SIMPLE_COLUMN) {
                    index = exprColumns[i].columnIndex;
                }

                sb.append(exprColumns[index].describe(session, blanks));
            }

            sb.append(b).append("]\n");
        }

        if (havingCondition != null) {
            temp = havingCondition.describe(session, blanks);

            sb.append(b).append("havingCondition=[").append(temp).append(
                "]\n");
        }

        if (sortAndSlice.hasOrder()) {
            sb.append(b).append("order by=[\n");

            for (int i = 0; i < sortAndSlice.exprList.size(); i++) {
                sb.append(b).append(
                    ((Expression) sortAndSlice.exprList.get(i)).describe(
                        session, blanks));
            }

            if (sortAndSlice.primaryTableIndex != null) {
                sb.append(b).append("uses index");
            }

            sb.append(b).append("]\n");
        }

        if (sortAndSlice.hasLimit()) {
            if (sortAndSlice.limitCondition.getLeftNode() != null) {
                sb.append(b).append("offset=[").append(
                    sortAndSlice.limitCondition.getLeftNode().describe(
                        session, b.length())).append("]\n");
            }

            if (sortAndSlice.limitCondition.getRightNode() != null) {
                sb.append(b).append("limit=[").append(
                    sortAndSlice.limitCondition.getRightNode().describe(
                        session, b.length())).append("]\n");
            }
        }

        return sb.toString();
    }

    void setMergeability() {

        isOrderSensitive |= sortAndSlice.hasLimit() || sortAndSlice.hasOrder();

        if (isOrderSensitive) {
            isMergeable = false;
        }

        if (isAggregated) {
            isMergeable = false;
        }

        if (isGrouped || isDistinctSelect) {
            isMergeable = false;
        }

        if (rangeVariables.length != 1) {
            isBaseMergeable = false;
            isMergeable     = false;

            return;
        }
    }

    void setUpdatability() {

        if (!isUpdatable) {
            return;
        }

        isUpdatable = false;

        if (isGrouped || isDistinctSelect || isAggregated) {
            return;
        }

        if (!isBaseMergeable) {
            return;
        }

        if (!isTopLevel) {
            return;
        }

        if (sortAndSlice.hasLimit() || sortAndSlice.hasOrder()) {
            return;
        }

        RangeVariable rangeVar  = rangeVariables[0];
        Table         table     = rangeVar.getTable();
        Table         baseTable = table.getBaseTable();

        if (baseTable == null) {
            return;
        }

        isInsertable = table.isInsertable();
        isUpdatable  = table.isUpdatable();

        if (!isInsertable && !isUpdatable) {
            return;
        }

        IntValueHashMap columns = new IntValueHashMap();
        boolean[]       checkList;
        int[]           baseColumnMap = table.getBaseTableColumnMap();
        int[]           columnMap     = new int[indexLimitVisible];

        if (queryCondition != null) {
            tempSet.clear();
            collectSubQueriesAndReferences(tempSet, queryCondition);

            if (tempSet.contains(table.getName())
                    || tempSet.contains(baseTable.getName())) {
                isUpdatable  = false;
                isInsertable = false;

                return;
            }
        }

        for (int i = 0; i < indexLimitVisible; i++) {
            Expression expression = exprColumns[i];

            if (expression.getType() == OpTypes.COLUMN) {
                String name = expression.getColumn().getName().name;

                if (columns.containsKey(name)) {
                    columns.put(name, 1);

                    continue;
                }

                columns.put(name, 0);
            } else {
                tempSet.clear();
                collectSubQueriesAndReferences(tempSet, expression);

                if (tempSet.contains(table.getName())) {
                    isUpdatable  = false;
                    isInsertable = false;

                    return;
                }
            }
        }

        isUpdatable = false;

        for (int i = 0; i < indexLimitVisible; i++) {
            if (accessibleColumns[i]) {
                Expression expression = exprColumns[i];

                if (expression.getType() == OpTypes.COLUMN) {
                    String name = expression.getColumn().getName().name;

                    if (columns.get(name) == 0) {
                        int index = table.findColumn(name);

                        columnMap[i] = baseColumnMap[index];

                        if (columnMap[i] != -1) {
                            isUpdatable = true;
                        }

                        continue;
                    }
                }
            }

            columnMap[i] = -1;
            isInsertable = false;
        }

        if (isInsertable) {
            checkList = baseTable.getColumnCheckList(columnMap);

            for (int i = 0; i < checkList.length; i++) {
                if (checkList[i]) {
                    continue;
                }

                ColumnSchema column = baseTable.getColumn(i);

                if (column.isIdentity() || column.isGenerated()
                        || column.hasDefault() || column.isNullable()) {}
                else {
                    isInsertable = false;

                    break;
                }
            }
        }

        if (!isUpdatable) {
            isInsertable = false;
        }

        if (isUpdatable) {
            this.columnMap = columnMap;
            this.baseTable = baseTable;

            if (view != null) {
                return;
            }

            indexLimitRowId++;

            hasRowID = true;

            if (!baseTable.isFileBased()) {
                indexLimitRowId++;

                isSingleMemoryTable = true;
            }

            indexLimitData = indexLimitRowId;
        }
    }

    /**
     * isBaseMergeable is simply a flag to allow merging the current query
     * isMergeable is a flag to allow this to act as base for a query
     *
     */
    void mergeQuery() {

        RangeVariable   rangeVar            = rangeVariables[0];
        Table           table               = rangeVar.getTable();
        Expression      localQueryCondition = queryCondition;
        QueryExpression baseQueryExpression = table.getQueryExpression();

        if (isBaseMergeable && baseQueryExpression != null
                && baseQueryExpression.isMergeable) {
            QuerySpecification baseSelect =
                baseQueryExpression.getMainSelect();

            rangeVariables[0] = baseSelect.rangeVariables[0];

            rangeVariables[0].resetConditions();

            for (int i = 0; i < indexLimitExpressions; i++) {
                Expression e = exprColumns[i];

                exprColumns[i] = e.replaceColumnReferences(rangeVar,
                        baseSelect.exprColumns);
            }

            if (localQueryCondition != null) {
                localQueryCondition =
                    localQueryCondition.replaceColumnReferences(rangeVar,
                        baseSelect.exprColumns);
            }

            Expression baseQueryCondition = baseSelect.queryCondition;

            checkQueryCondition = baseSelect.checkQueryCondition;
            queryCondition =
                ExpressionLogical.andExpressions(baseQueryCondition,
                                                 localQueryCondition);
        }

        if (view != null) {
            switch (view.getCheckOption()) {

                case SchemaObject.ViewCheckModes.CHECK_LOCAL :
                    if (!isUpdatable) {
                        throw Error.error(ErrorCode.X_42537);
                    }

                    checkQueryCondition = localQueryCondition;
                    break;

                case SchemaObject.ViewCheckModes.CHECK_CASCADE :
                    if (!isUpdatable) {
                        throw Error.error(ErrorCode.X_42537);
                    }

                    checkQueryCondition = queryCondition;
                    break;
            }
        }
    }

    static void collectSubQueriesAndReferences(OrderedHashSet set,
            Expression expression) {

        expression.collectAllExpressions(set,
                                         Expression.subqueryExpressionSet,
                                         Expression.emptyExpressionSet);

        int size = set.size();

        for (int i = 0; i < size; i++) {
            Expression e = (Expression) set.get(i);

            e.collectObjectNames(set);
        }
    }

    public OrderedHashSet getSubqueries() {

        OrderedHashSet set = null;

        for (int i = 0; i < indexLimitExpressions; i++) {
            set = exprColumns[i].collectAllSubqueries(set);
        }

        if (queryCondition != null) {
            set = queryCondition.collectAllSubqueries(set);
        }

        if (havingCondition != null) {
            set = havingCondition.collectAllSubqueries(set);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            OrderedHashSet temp = rangeVariables[i].getSubqueries();

            set = OrderedHashSet.addAll(set, temp);
        }

        return set;
    }

    public Table getBaseTable() {
        return baseTable;
    }

    public OrderedHashSet collectAllSubqueries(OrderedHashSet set) {
        return set;
    }

    public OrderedHashSet collectOuterColumnExpressions(OrderedHashSet set,
            OrderedHashSet exclude) {

        set = collectAllExpressions(set, Expression.columnExpressionSet,
                                    Expression.subqueryAggregateExpressionSet);

        if (set == null) {
            return null;
        }

        for (int i = set.size() - 1; i >= 0; i--) {
            Expression col = (Expression) set.get(i);

            if (ArrayUtil.find(rangeVariables, col.getRangeVariable()) >= 0) {
                set.remove(i);
            }

            if (exclude.contains(col)) {
                set.remove(i);
            }
        }

        if (set.isEmpty()) {
            set = null;
        }

        return set;
    }

    public OrderedHashSet collectAllExpressions(OrderedHashSet set,
            OrderedIntHashSet typeSet, OrderedIntHashSet stopAtTypeSet) {

        for (int i = 0; i < indexStartAggregates; i++) {
            set = exprColumns[i].collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }

        if (queryCondition != null) {
            set = queryCondition.collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }

        if (havingCondition != null) {
            set = havingCondition.collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }

        return set;
    }

    public void collectObjectNames(Set set) {

        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].collectObjectNames(set);
        }

        if (queryCondition != null) {
            queryCondition.collectObjectNames(set);
        }

        if (havingCondition != null) {
            havingCondition.collectObjectNames(set);
        }

        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            HsqlName name = rangeVariables[i].getTable().getName();

            set.add(name);
        }
    }

    public void replaceColumnReferences(RangeVariable range,
                                        Expression[] list) {

        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i] = exprColumns[i].replaceColumnReferences(range,
                    list);
        }

        if (queryCondition != null) {
            queryCondition = queryCondition.replaceColumnReferences(range,
                    list);
        }

        if (havingCondition != null) {
            havingCondition = havingCondition.replaceColumnReferences(range,
                    list);
        }

        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            rangeVariables[i].replaceColumnReferences(range, list);
        }
    }

    public void replaceRangeVariables(RangeVariable[] ranges,
                                      RangeVariable[] newRanges) {

        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].replaceRangeVariables(ranges, newRanges);
        }

        if (queryCondition != null) {
            queryCondition.replaceRangeVariables(ranges, newRanges);
        }

        if (havingCondition != null) {
            havingCondition.replaceRangeVariables(ranges, newRanges);
        }

        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            rangeVariables[i].getSubqueries();
        }
    }

    /**
     * Not for views. Only used on root node.
     */
    public void setReturningResult() {

        setReturningResultSet();

        acceptsSequences = true;
        isTopLevel       = true;
    }

    void setReturningResultSet() {
        persistenceScope = TableBase.SCOPE_SESSION;
    }

    public boolean isSingleColumn() {
        return indexLimitVisible == 1;
    }

    public String[] getColumnNames() {

        String[] names = new String[indexLimitVisible];

        for (int i = 0; i < indexLimitVisible; i++) {
            names[i] = exprColumns[i].getAlias();
        }

        return names;
    }

    public Type[] getColumnTypes() {

        if (resultColumnTypes.length == indexLimitVisible) {
            return resultColumnTypes;
        }

        Type[] types = new Type[indexLimitVisible];

        ArrayUtil.copyArray(resultColumnTypes, types, types.length);

        return types;
    }

    public int getColumnCount() {
        return indexLimitVisible;
    }

    public int[] getBaseTableColumnMap() {
        return columnMap;
    }

    public Expression getCheckCondition() {
        return queryCondition;
    }

    void getBaseTableNames(OrderedHashSet set) {

        for (int i = 0; i < rangeVariables.length; i++) {
            Table    rangeTable = rangeVariables[i].rangeTable;
            HsqlName name       = rangeTable.getName();

            if (rangeTable.isView()) {
                continue;
            }

            if (rangeTable.isDataReadOnly() || rangeTable.isTemp()) {
                continue;
            }

            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }

            set.add(name);
        }
    }

    /**
     * returns true if almost equivalent
     */
    boolean isEquivalent(QueryExpression other) {

        if (!(other instanceof QuerySpecification)) {
            return false;
        }

        QuerySpecification otherSpec = (QuerySpecification) other;

        if (!Expression.equals(exprColumns, otherSpec.exprColumns)) {
            return false;
        }

        if (!Expression.equals(queryCondition, otherSpec.queryCondition)) {
            return false;
        }

        if (rangeVariables.length != otherSpec.rangeVariables.length) {
            return false;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].getTable()
                    != otherSpec.rangeVariables[i].getTable()) {
                return false;
            }
        }

        return true;
    }
    // A VoltDB extension to export abstract parse trees

    Expression getHavingCondition() { return havingCondition; }

    // Display columns expressions
    List<Expression> displayCols = new ArrayList<Expression>();

    /**
     * Dumps the exprColumns list for this query specification.
     * Writes to stdout.
     *
     * This method is useful to understand how the HSQL parser
     * transforms its data structures during parsing.  For example,
     * place call to this method at the beginning and end of
     * resolveGroups() to see what it does.
     *
     * @param header    A string to be prepended to output
     */
    private void dumpExprColumns(String header){
        System.out.println("\n\n---------------------------------------------");
        System.out.println(header);
        try {
            System.out.println(getSQL());
        } catch (Exception e) {
        }
        for (int i = 0; i < exprColumns.length; ++i) {
            if (i == 0)
                System.out.println("Visible columns:");
            if (i == indexStartOrderBy)
                System.out.println("start order by:");
            if (i == indexStartAggregates)
                System.out.println("start aggregates:");
            if (i == indexLimitVisible)
                System.out.println("After limit of visible columns:");

            System.out.println(i + ": " + exprColumns[i]);
        }

        System.out.println("\n\n");
    }

    public String voltDescribe(Session session, int blanks) {

        StringBuffer sb;
        String       temp;
        sb = new StringBuffer();

        String tableName = null;
        if ((resultTable != null) && (resultTable instanceof Table)) {
            tableName = ((Table)resultTable).getName().getSchemaQualifiedStatementName();
        }
        if (tableName == null) {
            sb.append("Anonymous Table");
        } else {
            sb.append("Table named: " + tableName);
        }
        sb.append(": JVM System ID = ")
          .append(System.identityHashCode(this));
        blanks += 2;
        sb.append(Expression.voltIndentStr(blanks, true, false))
          .append("isDistinctSelect = [")
          .append(isDistinctSelect)
          .append("]");
        sb.append(Expression.voltIndentStr(blanks, true, false))
          .append("isGrouped = [").append(isGrouped).append("]");
        sb.append(Expression.voltIndentStr(blanks, true, false))
          .append("isAggregated = [").append(isAggregated).append("]");
        sb.append(Expression.voltIndentStr(blanks, true, false))
          .append("columns = [");
        if (exprColumns == null) {
            sb.append("<NONE>");
        } else {
            for (int i = 0; i < exprColumns.length; i++) {
                boolean visible = i < indexLimitVisible;
                int index = i;

                if (exprColumns[i].getType() == OpTypes.SIMPLE_COLUMN) {
                    index = exprColumns[i].columnIndex;
                }

                sb.append(Expression.voltIndentStr(blanks + 2, true, false));
                Expression exprCol = exprColumns[i];
                if (exprCol.getType() == OpTypes.SIMPLE_COLUMN) {
                    temp = String.format("SIMPLE_COLUMN(%d->%d)", i, index);
                } else {
                    temp = exprCol.voltDescribe(session, blanks + 2);
                }
                String label = (visible ? "visible" : "(invisible)");
                sb.append(i)
                  .append(".) ")
                  .append(temp)
                  .append(String.format(" -> %s: index: %d, opType: %d, columnIndex: %d%s",
                          label, index, exprCol.opType, exprCol.columnIndex,
                          (exprCol.alias != null) ? String.format(", alias: %s", exprCol.alias.getNameString()) : ""));

                if (visible) {
                    if (resultMetaData.columns[i].getNullability()
                            == SchemaObject.Nullability.NO_NULLS) {
                        sb.append(" not nullable");
                    } else {
                        sb.append(" nullable");
                    }
                }
            }
        }
        sb.append(Expression.voltIndentStr(blanks, true, false)).append("]");
        sb.append(Expression.voltIndentStr(blanks, true, false)).append("Range Variables: [");
        if (rangeVariables == null) {
            sb.append("<NONE>");
        } else {
            for (int i = 0; i < rangeVariables.length; i++) {
                sb.append(Expression.voltIndentStr(blanks + 2, true, false))
                  .append("range variable ").append(i + 1).append(" [")
                  .append(Expression.voltIndentStr(blanks + 4, true, false))
                  .append(rangeVariables[i].voltDescribe(session, blanks + 4))
                  .append(Expression.voltIndentStr(blanks + 2, true, false))
                  .append("]");
            }
        }
        sb.append(Expression.voltIndentStr(blanks, true, false))
          .append("]");

        temp = queryCondition == null ? "null"
                                      : queryCondition.voltDescribe(session, blanks);

        if (isGrouped) {
            sb.append(Expression.voltIndentStr(blanks, true, false))
              .append("groupColumns = [");
            if (exprColumns == null) {

            } else {
                for (int i = indexLimitRowId;
                        i < indexLimitRowId + groupByColumnCount; i++) {
                    int index = i;

                    if (exprColumns[i].getType() == OpTypes.SIMPLE_COLUMN) {
                        index = exprColumns[i].columnIndex;
                    }

                    sb.append(Expression.voltIndentStr(blanks + 2, true, false))
                      .append(exprColumns[index].voltDescribe(session, blanks + 2));
                }
            }
            sb.append(Expression.voltIndentStr(blanks + 2, true, false)).append("]");
        }

        if (havingCondition != null) {
            temp = havingCondition.voltDescribe(session, blanks + 2);

            sb.append(Expression.voltIndentStr(blanks + 2, true, false))
              .append("havingCondition = [")
              .append(Expression.voltIndentStr(blanks + 2, true, false))
              .append(temp)
              .append(Expression.voltIndentStr(blanks + 2, true, false))
              .append("]");
        }

        if (sortAndSlice.hasOrder()) {
            sb.append(Expression.voltIndentStr(blanks + 2, true, false))
              .append("order by = [");
            for (int i = 0; i < sortAndSlice.exprList.size(); i++) {
                sb.append(Expression.voltIndentStr(blanks + 2, true, false))
                  .append("  ")
                  .append(((Expression) sortAndSlice.exprList.get(i)).voltDescribe(session, blanks + 2));
            }
            if (sortAndSlice.primaryTableIndex != null) {
                sb.append(Expression.voltIndentStr(blanks + 2, true, false))
                  .append("uses index");
            }

            sb.append(Expression.voltIndentStr(blanks + 2, true, false)).append("]\n");
        }

        if (sortAndSlice.hasLimit()) {
            if (sortAndSlice.limitCondition.getLeftNode() != null) {
                sb.append(Expression.voltIndentStr(blanks + 2, true, false))
                  .append("offset = [")
                  .append(sortAndSlice.limitCondition.getLeftNode().voltDescribe(session, blanks + 2))
                  .append(Expression.voltIndentStr(blanks + 2, true, false))
                  .append("]");
            }
            if (sortAndSlice.limitCondition.getRightNode() != null) {
                sb.append(Expression.voltIndentStr(blanks + 2, true, false))
                  .append("limit = [")
                  .append(sortAndSlice.limitCondition.getRightNode().voltDescribe(session, blanks + 2))
                  .append(Expression.voltIndentStr(blanks + 2, true, false))
                  .append("]");
            }
        }
        return sb.toString();
    }
    // End of VoltDB extension
}
