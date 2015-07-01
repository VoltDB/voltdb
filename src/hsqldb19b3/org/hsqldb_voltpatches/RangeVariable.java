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

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.navigator.RangeIterator;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.Type;

/**
 * Metadata for range variables, including conditions.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
final class RangeVariable {

    static final RangeVariable[] emptyArray = new RangeVariable[]{};

    //
    final Table            rangeTable;
    final SimpleName       tableAlias;
    private OrderedHashSet columnAliases;
    private SimpleName[]   columnAliasNames;
    private OrderedHashSet columnNames;
    OrderedHashSet         namedJoinColumns;
    HashMap                namedJoinColumnExpressions;
    Index                  rangeIndex;
    private final Object[] emptyData;
    final boolean[]        columnsInGroupBy;
    boolean                hasKeyedColumnInGroupBy;
    final boolean[]        usedColumns;
    boolean[]              updatedColumns;

    // index conditions
    Expression indexCondition;
    Expression indexEndCondition;
    boolean    isJoinIndex;

    // non-index consitions
    Expression nonIndexJoinCondition;
    Expression nonIndexWhereCondition;

    //
    boolean              isLeftJoin;              // table joined with LEFT / FULL OUTER JOIN
    boolean              isRightJoin;             // table joined with RIGHT / FULL OUTER JOIN
    boolean              isMultiFindFirst;        // findFirst() uses multi-column index
    private Expression[] findFirstExpressions;    // expressions for column values
    private int          multiColumnCount;
    int                  level;

    //
    int rangePosition;

    // for variable and argument lists
    HashMappedList variables;

    // variable v.s. argument
    boolean isVariable;

    RangeVariable(HashMappedList variables, boolean isVariable) {

        this.variables   = variables;
        this.isVariable  = isVariable;
        rangeTable       = null;
        tableAlias       = null;
        emptyData        = null;
        columnsInGroupBy = null;
        usedColumns      = null;
    }

    RangeVariable(Table table, SimpleName alias, OrderedHashSet columnList,
                  SimpleName[] columnNameList, CompileContext compileContext) {

        rangeTable       = table;
        tableAlias       = alias;
        columnAliases    = columnList;
        columnAliasNames = columnNameList;
        emptyData        = rangeTable.getEmptyRowData();
        columnsInGroupBy = rangeTable.getNewColumnCheckList();
        usedColumns      = rangeTable.getNewColumnCheckList();
        rangeIndex       = rangeTable.getPrimaryIndex();

        compileContext.registerRangeVariable(this);
    }

/*
    RangeVariable(Table table, String alias, OrderedHashSet columnList,
                  Index index, CompileContext compileContext) {

        rangeTable       = table;
        tableAlias       = alias;
        columnAliases    = columnList;
        emptyData        = rangeTable.getEmptyRowData();
        columnsInGroupBy = rangeTable.getNewColumnCheckList();
        usedColumns      = rangeTable.getNewColumnCheckList();
        rangeIndex       = index;

        compileContext.registerRangeVariable(this);
    }
*/
    RangeVariable(RangeVariable range) {

        rangeTable       = range.rangeTable;
        tableAlias       = null;
        emptyData        = rangeTable.getEmptyRowData();
        columnsInGroupBy = rangeTable.getNewColumnCheckList();
        usedColumns      = rangeTable.getNewColumnCheckList();
        rangeIndex       = rangeTable.getPrimaryIndex();
        rangePosition    = range.rangePosition;
        level            = range.level;
    }

    void setJoinType(boolean isLeft, boolean isRight) {
        isLeftJoin  = isLeft;
        isRightJoin = isRight;
    }

    public void addNamedJoinColumns(OrderedHashSet columns) {
        namedJoinColumns = columns;
    }

    public void addColumn(int columnIndex) {
        usedColumns[columnIndex] = true;
    }

    void addNamedJoinColumnExpression(String name, Expression e) {

        if (namedJoinColumnExpressions == null) {
            namedJoinColumnExpressions = new HashMap();
        }

        namedJoinColumnExpressions.put(name, e);
    }

    ExpressionColumn getColumnExpression(String name) {

        return namedJoinColumnExpressions == null ? null
                                                  : (ExpressionColumn) namedJoinColumnExpressions
                                                  .get(name);
    }

    Table getTable() {
        return rangeTable;
    }

    public OrderedHashSet getColumnNames() {

        if (columnNames == null) {
            columnNames = new OrderedHashSet();

            rangeTable.getColumnNames(this.usedColumns, columnNames);
        }

        return columnNames;
    }

    public OrderedHashSet getUniqueColumnNameSet() {

        OrderedHashSet set = new OrderedHashSet();

        if (columnAliases != null) {
            set.addAll(columnAliases);

            return set;
        }

        for (int i = 0; i < rangeTable.columnList.size(); i++) {
            String  name  = rangeTable.getColumn(i).getName().name;
            boolean added = set.add(name);

            if (!added) {
                throw Error.error(ErrorCode.X_42578, name);
            }
        }

        return set;
    }

    /**
     * Retruns index for column
     *
     * @param columnName name of column
     * @return int index or -1 if not found
     */
    public int findColumn(String columnName) {

        if (namedJoinColumnExpressions != null
                && namedJoinColumnExpressions.containsKey(columnName)) {
            return -1;
        }

        if (variables != null) {
            return variables.getIndex(columnName);
        } else if (columnAliases != null) {
            return columnAliases.getIndex(columnName);
        } else {
            return rangeTable.findColumn(columnName);
        }
    }

    ColumnSchema getColumn(String columnName) {

        int index = findColumn(columnName);

        return index < 0 ? null
                         : rangeTable.getColumn(index);
    }

    ColumnSchema getColumn(int i) {

        if (variables != null) {
            return (ColumnSchema) variables.get(i);
        } else {
            return rangeTable.getColumn(i);
        }
    }

    String getColumnAlias(int i) {

        SimpleName name = getColumnAliasName(i);

        return name.name;
    }

    public SimpleName getColumnAliasName(int i) {

        if (columnAliases != null) {
            return columnAliasNames[i];
        } else {
            return rangeTable.getColumn(i).getName();
        }
    }

    boolean hasColumnAlias() {
        return columnAliases != null;
    }

    boolean resolvesTableName(ExpressionColumn e) {

        if (e.tableName == null) {
            return true;
        }

        if (e.schema == null) {
            if (tableAlias == null) {
                if (e.tableName.equals(rangeTable.tableName.name)) {
                    return true;
                }
            } else if (e.tableName.equals(tableAlias.name)) {
                return true;
            }
        } else {
            if (e.tableName.equals(rangeTable.tableName.name)
                    && e.schema.equals(rangeTable.tableName.schema.name)) {
                return true;
            }
        }

        return false;
    }

    public boolean resolvesTableName(String name) {

        if (name == null) {
            return true;
        }

        if (tableAlias == null) {
            if (name.equals(rangeTable.tableName.name)) {
                return true;
            }
        } else if (name.equals(tableAlias.name)) {
            return true;
        }

        return false;
    }

    boolean resolvesSchemaName(String name) {

        if (name == null) {
            return true;
        }

        if (tableAlias != null) {
            return false;
        }

        return name.equals(rangeTable.tableName.schema.name);
    }

    /**
     * Add all columns to a list of expressions
     */
    void addTableColumns(HsqlArrayList exprList) {

        if (namedJoinColumns != null) {
            int count    = exprList.size();
            int position = 0;

            for (int i = 0; i < count; i++) {
                Expression e          = (Expression) exprList.get(i);
                String     columnName = e.getColumnName();

                if (namedJoinColumns.contains(columnName)) {
                    if (position != i) {
                        exprList.remove(i);
                        exprList.add(position, e);
                    }

                    e = getColumnExpression(columnName);

                    exprList.set(position, e);

                    position++;
                }
            }
        }

        addTableColumns(exprList, exprList.size(), namedJoinColumns);
    }

    /**
     * Add all columns to a list of expressions
     */
    int addTableColumns(HsqlArrayList expList, int position, HashSet exclude) {

        Table table = getTable();
        int   count = table.getColumnCount();

        for (int i = 0; i < count; i++) {
            ColumnSchema column = table.getColumn(i);
            String columnName = columnAliases == null ? column.getName().name
                                                      : (String) columnAliases
                                                          .get(i);

            if (exclude != null && exclude.contains(columnName)) {
                continue;
            }

            Expression e = new ExpressionColumn(this, column, i);

            expList.add(position++, e);
        }

        return position;
    }

    void addTableColumns(Expression expression, HashSet exclude) {

        HsqlArrayList list  = new HsqlArrayList();
        Table         table = getTable();
        int           count = table.getColumnCount();

        for (int i = 0; i < count; i++) {
            ColumnSchema column = table.getColumn(i);
            String columnName = columnAliases == null ? column.getName().name
                                                      : (String) columnAliases
                                                          .get(i);

            if (exclude != null && exclude.contains(columnName)) {
                continue;
            }

            Expression e = new ExpressionColumn(this, column, i);

            list.add(e);
        }

        Expression[] nodes = new Expression[list.size()];

        list.toArray(nodes);

        expression.nodes = nodes;
    }

    /**
     * Removes reference to Index to avoid possible memory leaks after alter
     * table or drop index
     */
    void setForCheckConstraint() {
        rangeIndex = null;
    }

    /**
     *
     * @param e condition
     * @param index Index object
     * @param isJoin whether a join or not
     */
    void addIndexCondition(Expression e, Index index, boolean isJoin) {

        rangeIndex  = index;
        isJoinIndex = isJoin;

        switch (e.getType()) {

            case OpTypes.NOT :
                indexCondition = e;
                break;

            case OpTypes.IS_NULL :
                indexEndCondition = e;
                break;

            case OpTypes.EQUAL :
                indexCondition    = e;
                indexEndCondition = indexCondition;
                break;

            case OpTypes.GREATER :
            case OpTypes.GREATER_EQUAL :
                indexCondition = e;
                break;

            case OpTypes.SMALLER :
            case OpTypes.SMALLER_EQUAL :
                indexEndCondition = e;
                break;

            default :
                Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    /**
     *
     * @param e a join condition
     */
    void addJoinCondition(Expression e) {
        nonIndexJoinCondition =
            ExpressionLogical.andExpressions(nonIndexJoinCondition, e);
    }

    /**
     *
     * @param e a where condition
     */
    void addWhereCondition(Expression e) {
        nonIndexWhereCondition =
            ExpressionLogical.andExpressions(nonIndexWhereCondition, e);
    }

    void addCondition(Expression e, boolean isJoin) {

        if (isJoin) {
            addJoinCondition(e);
        } else {
            addWhereCondition(e);
        }
    }

    /**
     * Only multiple EQUAL conditions are used
     *
     * @param exprList list of expressions
     * @param index Index to use
     * @param isJoin whether a join or not
     */
    void addIndexCondition(Expression[] exprList, Index index, int colCount,
                           boolean isJoin) {
// VoltDB extension
        if (rangeIndex == index && isJoinIndex && (!isJoin) &&
                (multiColumnCount > 0) && (colCount == 0)) {
            // This is one particular set of conditions which broke the classification of
            // ON and WHERE clauses.
            return;
        }
// End of VoltDB extension
        rangeIndex  = index;
        isJoinIndex = isJoin;

        for (int i = 0; i < colCount; i++) {
            Expression e = exprList[i];

            indexEndCondition =
                ExpressionLogical.andExpressions(indexEndCondition, e);
        }

        if (colCount == 1) {
            indexCondition = exprList[0];
        } else {
            findFirstExpressions = exprList;
            isMultiFindFirst     = true;
            multiColumnCount     = colCount;
        }
    }

    boolean hasIndexCondition() {
        return indexCondition != null;
    }

    /**
     * Retreives a String representation of this obejct. <p>
     *
     * The returned String describes this object's table, alias
     * access mode, index, join mode, Start, End and And conditions.
     *
     * @return a String representation of this object
     */
    public String describe(Session session) {

        StringBuffer sb;
        String       temp;
        Index        index;
        Index        primaryIndex;
        int[]        primaryKey;
        boolean      hidden;
        boolean      fullScan;

        sb           = new StringBuffer();
        index        = rangeIndex;
        primaryIndex = rangeTable.getPrimaryIndex();
        primaryKey   = rangeTable.getPrimaryKey();
        hidden       = false;
        fullScan     = (indexCondition == null && indexEndCondition == null);

        if (index == null) {
            index = primaryIndex;
        }

        if (index == primaryIndex && primaryKey.length == 0) {
            hidden   = true;
            fullScan = true;
        }

        sb.append(super.toString()).append('\n');
        sb.append("table=[").append(rangeTable.getName().name).append("]\n");

        if (tableAlias != null) {
            sb.append("alias=[").append(tableAlias.name).append("]\n");
        }

        sb.append("access=[").append(fullScan ? "FULL SCAN"
                                              : "INDEX PRED").append("]\n");
        sb.append("index=[");
        sb.append(index == null ? "NONE"
                                : index.getName() == null ? "UNNAMED"
                                                          : index.getName()
                                                          .name);
        sb.append(hidden ? "[HIDDEN]]\n"
                         : "]\n");

        temp = "INNER";

        if (isLeftJoin) {
            temp = "LEFT OUTER";

            if (isRightJoin) {
                temp = "FULL";
            }
        } else if (isRightJoin) {
            temp = "RIGHT OUTER";
        }

        sb.append("joinType=[").append(temp).append("]\n");

        temp = indexCondition == null ? "null"
                                      : indexCondition.describe(session);

        if (findFirstExpressions != null) {
            StringBuffer sbt = new StringBuffer();

            for (int i = 0; i < multiColumnCount; i++) {
                sbt.append(findFirstExpressions[i].describe(session));
            }

            temp = sbt.toString();
        }

        sb.append("eStart=[").append(temp).append("]\n");

        temp = indexEndCondition == null ? "null"
                                         : indexEndCondition.describe(session);

        sb.append("eEnd=[").append(temp).append("]\n");

        temp = nonIndexJoinCondition == null ? "null"
                                             : nonIndexJoinCondition.describe(
                                             session);

        sb.append("eAnd=[").append(temp).append("]");

        return sb.toString();
    }

    public RangeIteratorMain getIterator(Session session) {

        RangeIteratorMain it = new RangeIteratorMain(session, this);

        session.sessionContext.setRangeIterator(it);

        return it;
    }

    public RangeIteratorMain getFullIterator(Session session,
            RangeIteratorMain mainIterator) {

        RangeIteratorMain it = new FullRangeIterator(session, this,
            mainIterator);

        session.sessionContext.setRangeIterator(it);

        return it;
    }

    public static RangeIteratorMain getIterator(Session session,
            RangeVariable[] rangeVars) {

        if (rangeVars.length == 1) {
            return rangeVars[0].getIterator(session);
        }

        RangeIteratorMain[] iterators =
            new RangeIteratorMain[rangeVars.length];

        for (int i = 0; i < rangeVars.length; i++) {
            iterators[i] = rangeVars[i].getIterator(session);
        }

        return new JoinedRangeIterator(iterators);
    }

    public static class RangeIteratorBase implements RangeIterator {

        Session         session;
        int             rangePosition;
        RowIterator     it;
        PersistentStore store;
        Object[]        currentData;
        Row             currentRow;
        boolean         isBeforeFirst;

        RangeIteratorBase() {}

        public RangeIteratorBase(Session session, PersistentStore store,
                                 TableBase t, int position) {

            this.session       = session;
            this.rangePosition = position;
            this.store         = store;
            it                 = t.rowIterator(store);
            isBeforeFirst      = true;
        }

        @Override
        public boolean isBeforeFirst() {
            return isBeforeFirst;
        }

        @Override
        public boolean next() {

            if (isBeforeFirst) {
                isBeforeFirst = false;
            } else {
                if (it == null) {
                    return false;
                }
            }

            currentRow = it.getNextRow();

            if (currentRow == null) {
                return false;
            } else {
                currentData = currentRow.getData();

                return true;
            }
        }

        @Override
        public Row getCurrentRow() {
            return currentRow;
        }

        @Override
        public Object[] getCurrent() {
            return currentData;
        }

        @Override
        public long getRowid() {
            return currentRow == null ? 0
                                      : currentRow.getId();
        }

        @Override
        public Object getRowidObject() {
            return currentRow == null ? null
                                      : Long.valueOf(currentRow.getId());
        }

        @Override
        public void remove() {}

        @Override
        public void reset() {

            if (it != null) {
                it.release();
            }

            it            = null;
            currentRow    = null;
            isBeforeFirst = true;
        }

        @Override
        public int getRangePosition() {
            return rangePosition;
        }
    }

    public static class RangeIteratorMain extends RangeIteratorBase {

        boolean       hasOuterRow;
        boolean       isFullIterator;
        RangeVariable rangeVar;

        //
        Table           lookupTable;
        PersistentStore lookupStore;

        RangeIteratorMain() {
            super();
        }

        public RangeIteratorMain(Session session, RangeVariable rangeVar) {

            this.rangePosition = rangeVar.rangePosition;
            this.store = session.sessionData.getRowStore(rangeVar.rangeTable);
            this.session       = session;
            this.rangeVar      = rangeVar;
            isBeforeFirst      = true;

            if (rangeVar.isRightJoin) {
                lookupTable = TableUtil.newLookupTable(session.database);
                lookupStore = session.sessionData.getRowStore(lookupTable);
            }
        }

        @Override
        public boolean isBeforeFirst() {
            return isBeforeFirst;
        }

        @Override
        public boolean next() {

            if (isBeforeFirst) {
                isBeforeFirst = false;

                initialiseIterator();
            } else {
                if (it == null) {
                    return false;
                }
            }

            return findNext();
        }

        @Override
        public void remove() {}

        @Override
        public void reset() {

            if (it != null) {
                it.release();
            }

            it            = null;
            currentData   = rangeVar.emptyData;
            currentRow    = null;
            hasOuterRow   = false;
            isBeforeFirst = true;
        }

        @Override
        public int getRangePosition() {
            return rangeVar.rangePosition;
        }

        /**
         */
        protected void initialiseIterator() {

            hasOuterRow = rangeVar.isLeftJoin;

            if (rangeVar.isMultiFindFirst) {
                getFirstRowMulti();

                if (!rangeVar.isJoinIndex) {
                    hasOuterRow = false;
                }
            } else if (rangeVar.indexCondition == null) {
                if (rangeVar.indexEndCondition == null
                        || rangeVar.indexEndCondition.getType()
                           == OpTypes.IS_NULL) {
                    it = rangeVar.rangeIndex.firstRow(session, store);
                } else {
                    it = rangeVar.rangeIndex.findFirstRowNotNull(session,
                            store);
                }
            } else {

                // only NOT NULL
                if (rangeVar.indexCondition.getType() == OpTypes.NOT) {
                    it = rangeVar.rangeIndex.findFirstRowNotNull(session,
                            store);
                } else {
                    getFirstRow();
                }

                if (!rangeVar.isJoinIndex) {
                    hasOuterRow = false;
                }
            }
        }

        /**
         */
        private void getFirstRow() {

            Object value =
                rangeVar.indexCondition.getRightNode().getValue(session);
            Type valueType =
                rangeVar.indexCondition.getRightNode().getDataType();
            Type targetType =
                rangeVar.indexCondition.getLeftNode().getDataType();
            int exprType = rangeVar.indexCondition.getType();
            int range    = 0;

            if (targetType != valueType) {
                range = targetType.compareToTypeRange(value);
            }

            if (range == 0) {
                value = targetType.convertToType(session, value, valueType);
                it = rangeVar.rangeIndex.findFirstRow(session, store, value,
                                                      exprType);
            } else if (range < 0) {
                switch (exprType) {

                    case OpTypes.GREATER_EQUAL :
                    case OpTypes.GREATER :
                        it = rangeVar.rangeIndex.findFirstRowNotNull(session,
                                store);
                        break;

                    default :
                        it = rangeVar.rangeIndex.emptyIterator();
                }
            } else {
                switch (exprType) {

                    case OpTypes.SMALLER_EQUAL :
                    case OpTypes.SMALLER :
                        it = rangeVar.rangeIndex.findFirstRowNotNull(session,
                                store);
                        break;

                    default :
                        it = rangeVar.rangeIndex.emptyIterator();
                }
            }

            return;
        }

        /**
         * Uses multiple EQUAL expressions
         */
        private void getFirstRowMulti() {

            boolean convertible = true;
            Object[] currentJoinData =
                new Object[rangeVar.rangeIndex.getVisibleColumns()];

            for (int i = 0; i < rangeVar.multiColumnCount; i++) {
                Type valueType =
                    rangeVar.findFirstExpressions[i].getRightNode()
                        .getDataType();
                Type targetType =
                    rangeVar.findFirstExpressions[i].getLeftNode()
                        .getDataType();
                Object value =
                    rangeVar.findFirstExpressions[i].getRightNode().getValue(
                        session);

                if (targetType.compareToTypeRange(value) != 0) {
                    convertible = false;

                    break;
                }

                currentJoinData[i] = targetType.convertToType(session, value,
                        valueType);
            }

            it = convertible
                 ? rangeVar.rangeIndex.findFirstRow(session, store,
                     currentJoinData, rangeVar.multiColumnCount)
                 : rangeVar.rangeIndex.emptyIterator();
        }

        /**
         * Advances to the next available value. <p>
         *
         * @return true if a next value is available upon exit
         */
        protected boolean findNext() {

            boolean result = false;

            while (true) {
                currentRow = it.getNextRow();

                if (currentRow == null) {
                    break;
                }

                currentData = currentRow.getData();

                if (rangeVar.indexEndCondition != null
                        && !rangeVar.indexEndCondition.testCondition(
                            session)) {
                    if (!rangeVar.isJoinIndex) {
                        hasOuterRow = false;
                    }

                    break;
                }

                if (rangeVar.nonIndexJoinCondition != null
                        && !rangeVar.nonIndexJoinCondition.testCondition(
                            session)) {
                    continue;
                }

                if (rangeVar.nonIndexWhereCondition != null
                        && !rangeVar.nonIndexWhereCondition.testCondition(
                            session)) {
                    hasOuterRow = false;

                    continue;
                }

                addFoundRow();

                result = true;

                break;
            }

            if (result) {
                hasOuterRow = false;

                return true;
            }

            it.release();

            currentRow  = null;
            currentData = rangeVar.emptyData;

            if (hasOuterRow) {
                result = (rangeVar.nonIndexWhereCondition == null
                          || rangeVar.nonIndexWhereCondition.testCondition(
                              session));
            }

            hasOuterRow = false;

            return result;
        }

        protected void addFoundRow() {

            if (rangeVar.isRightJoin) {
                try {
                    lookupTable.insertData(
                        lookupStore,
                        new Object[]{ ValuePool.getInt(currentRow.getPos()) });
                } catch (HsqlException e) {}
            }
        }
    }

    public static class FullRangeIterator extends RangeIteratorMain {

        public FullRangeIterator(Session session, RangeVariable rangeVar,
                                 RangeIteratorMain rangeIterator) {

            this.rangePosition = rangeVar.rangePosition;
            this.store = session.sessionData.getRowStore(rangeVar.rangeTable);
            this.session       = session;
            this.rangeVar      = rangeVar;
            isBeforeFirst      = true;
            lookupTable        = rangeIterator.lookupTable;
            lookupStore        = rangeIterator.lookupStore;
            it                 = rangeVar.rangeIndex.firstRow(session, store);
        }

        @Override
        protected void initialiseIterator() {}

        @Override
        protected boolean findNext() {

            boolean result;

            while (true) {
                currentRow = it.getNextRow();

                if (currentRow == null) {
                    result = false;

                    break;
                }

                RowIterator lookupIterator =
                    lookupTable.indexList[0].findFirstRow(session,
                        lookupStore, ValuePool.getInt(currentRow.getPos()),
                        OpTypes.EQUAL);

                result = !lookupIterator.hasNext();

                lookupIterator.release();

                if (result) {
                    currentData = currentRow.getData();

                    if (rangeVar.nonIndexWhereCondition != null
                            && !rangeVar.nonIndexWhereCondition.testCondition(
                                session)) {
                        continue;
                    }

                    isBeforeFirst = false;

                    return true;
                }
            }

            it.release();

            currentRow  = null;
            currentData = rangeVar.emptyData;

            return result;
        }
    }

    public static class JoinedRangeIterator extends RangeIteratorMain {

        RangeIteratorMain[] rangeIterators;
        int                 currentIndex = 0;

        public JoinedRangeIterator(RangeIteratorMain[] rangeIterators) {
            this.rangeIterators = rangeIterators;
        }

        @Override
        public boolean isBeforeFirst() {
            return isBeforeFirst;
        }

        @Override
        public boolean next() {

            while (currentIndex >= 0) {
                RangeIteratorMain it = rangeIterators[currentIndex];

                if (it.next()) {
                    if (currentIndex < rangeIterators.length - 1) {
                        currentIndex++;

                        continue;
                    }

                    currentRow  = rangeIterators[currentIndex].currentRow;
                    currentData = currentRow.getData();

                    return true;
                } else {
                    it.reset();

                    currentIndex--;

                    continue;
                }
            }

            currentData =
                rangeIterators[rangeIterators.length - 1].rangeVar.emptyData;
            currentRow = null;

            for (int i = 0; i < rangeIterators.length; i++) {
                rangeIterators[i].reset();
            }

            return false;
        }

        @Override
        public void reset() {}
    }

    /************************* Volt DB Extensions *************************/

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    VoltXMLElement voltGetRangeVariableXML(Session session)
    throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        Index        index;
        Index        primaryIndex;

        index        = rangeIndex;
        primaryIndex = rangeTable.getPrimaryIndex();

        // get the index for this scan (/filter)
        // note: ignored if scan if full table scan
        if (index == null)
            index = primaryIndex;

        // output open tag
        VoltXMLElement scan = new VoltXMLElement("tablescan");

        if (rangeTable.tableType == TableBase.SYSTEM_SUBQUERY) {
            if (rangeTable instanceof TableDerived) {
                if (tableAlias == null || tableAlias.name == null) {
                    // VoltDB require derived sub select table with user specified alias
                    throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException(
                            "SQL Syntax error: Every derived table must have its own alias.");
                }
                scan.attributes.put("table", tableAlias.name.toUpperCase());

                VoltXMLElement subQuery = ((TableDerived) rangeTable).dataExpression.voltGetXML(session);
                scan.children.add(subQuery);
            }
        } else {
            scan.attributes.put("table", rangeTable.getName().name.toUpperCase());
        }

        if (tableAlias != null && !rangeTable.getName().name.equals(tableAlias)) {
            scan.attributes.put("tablealias", tableAlias.name.toUpperCase());
        }

        // note if this is an outer join
        if (isLeftJoin && isRightJoin) {
            scan.attributes.put("jointype", "full");
        } else if (isLeftJoin) {
            scan.attributes.put("jointype", "left");
        } else if (isRightJoin) {
            scan.attributes.put("jointype", "right");
        } else {
            scan.attributes.put("jointype", "inner");
        }

        Expression joinCond = null;
        Expression whereCond = null;
        // if isJoinIndex and indexCondition are set then indexCondition is join condition
        // else if indexCondition is set then it is where condition
        if (isJoinIndex == true) {
            joinCond = indexCondition;
            if (indexEndCondition != null) {
                if (joinCond != null) {
                    joinCond = new ExpressionLogical(OpTypes.AND, joinCond, indexEndCondition);
                } else {
                    joinCond = indexEndCondition;
                }
            }
            // then go to the nonIndexJoinCondition
            if (nonIndexJoinCondition != null) {
                if (joinCond != null) {
                    joinCond = new ExpressionLogical(OpTypes.AND, joinCond, nonIndexJoinCondition);
                } else {
                    joinCond = nonIndexJoinCondition;
                }
            }
            // then go to the nonIndexWhereCondition
            whereCond = nonIndexWhereCondition;
        } else {
            joinCond = nonIndexJoinCondition;

            whereCond = indexCondition;
            if (indexEndCondition != null) {
                if (whereCond != null) {
                    whereCond = new ExpressionLogical(OpTypes.AND, whereCond, indexEndCondition);
                } else {
                    whereCond = indexEndCondition;
                }
            }
            // then go to the nonIndexWhereCondition
            if (nonIndexWhereCondition != null) {
                if (whereCond != null) {
                    whereCond = new ExpressionLogical(OpTypes.AND, whereCond, nonIndexWhereCondition);
                } else {
                    whereCond = nonIndexWhereCondition;
                }
            }

        }
        if (joinCond != null) {
            joinCond = joinCond.eliminateDuplicates(session);
            VoltXMLElement joinCondEl = new VoltXMLElement("joincond");
            joinCondEl.children.add(joinCond.voltGetXML(session));
            scan.children.add(joinCondEl);
        }

        if (whereCond != null) {
            whereCond = whereCond.eliminateDuplicates(session);
            VoltXMLElement whereCondEl = new VoltXMLElement("wherecond");
            whereCondEl.children.add(whereCond.voltGetXML(session));
            scan.children.add(whereCondEl);
        }

        return scan;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        String name = "";
        if (rangeTable != null) {
            name = ":" + rangeTable.getName().name;
        }
        return super.toString() + name;
    }
    /**********************************************************************/
}
