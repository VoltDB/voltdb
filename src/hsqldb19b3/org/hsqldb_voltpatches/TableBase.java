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
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.index.IndexAVL;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.types.Type;

/**
 * The  base of all HSQLDB table implementations.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class TableBase {

    // types of table
    public static final int SYSTEM_TABLE     = 0;
    public static final int SYSTEM_SUBQUERY  = 1;
    public static final int TEMP_TABLE       = 2;
    public static final int MEMORY_TABLE     = 3;
    public static final int CACHED_TABLE     = 4;
    public static final int TEMP_TEXT_TABLE  = 5;
    public static final int TEXT_TABLE       = 6;
    public static final int VIEW_TABLE       = 7;
    public static final int RESULT_TABLE     = 8;
    public static final int TRANSITION_TABLE = 9;
    public static final int FUNCTION_TABLE   = 10;

    //
    public static final int SCOPE_STATEMENT   = 11;
    public static final int SCOPE_TRANSACTION = 12;
    public static final int SCOPE_SESSION     = 13;
    public static final int SCOPE_FULL        = 14;

    //
    public static final int COLUMNS_UNREFERENCED = 15;
    public static final int COLUMNS_REFERENCED   = 16;

    //
    public PersistentStore store;
    public int             persistenceScope;
    public long            persistenceId;

    // columns in table
    int[]  primaryKeyCols;                      // column numbers for primary key
    Type[] primaryKeyTypes;
    int[]  primaryKeyColsSequence;              // {0,1,2,...}

    //
    //
    Index[]         indexList;                  // first index is the primary key index
    public Database database;
    int[]           bestRowIdentifierCols;      // column set for best index
    boolean         bestRowIdentifierStrict;    // true if it has no nullable column
    int[]           bestIndexForColumn;         // index of the 'best' index for each column
    Index           bestIndex;                  // the best index overall - null if there is no user-defined index
    Index         fullIndex;                    // index on all columns
    boolean[]     colNotNull;                   // nullability
    Type[]        colTypes;                     // types of columns
    protected int columnCount;

    //
    int               tableType;
    protected boolean isReadOnly;
    protected boolean isTemp;
    protected boolean isCached;
    protected boolean isText;
    boolean           isView;
    public boolean    isSessionBased;
    protected boolean isSchemaBased;
    protected boolean isLogged;
    private boolean   isTransactional = true;
    boolean           hasLobColumn;

    //
    TableBase() {}

    //
    public TableBase(Database database, int scope, int type, Type[] colTypes) {

        tableType        = type;
        persistenceScope = scope;
        isSessionBased   = true;
        persistenceId    = database.persistentStoreCollection.getNextId();
        this.database    = database;
        this.colTypes    = colTypes;
        columnCount      = colTypes.length;
        primaryKeyCols   = new int[]{};
        primaryKeyTypes  = new Type[]{};
        indexList        = new Index[0];

        createPrimaryIndex(primaryKeyCols, primaryKeyTypes, null);
    }

    public TableBase duplicate() {

        TableBase copy = new TableBase();

        copy.tableType        = tableType;
        copy.persistenceScope = persistenceScope;
        copy.isSessionBased   = isSessionBased;
        copy.persistenceId    = database.persistentStoreCollection.getNextId();
        copy.database         = database;
        copy.colTypes         = colTypes;
        copy.columnCount      = colTypes.length;
        copy.primaryKeyCols   = primaryKeyCols;
        copy.primaryKeyTypes  = primaryKeyTypes;
        copy.indexList        = indexList;

        return copy;
    }

    public final int getTableType() {
        return tableType;
    }

    public long getPersistenceId() {
        return persistenceId;
    }

    int getId() {
        return 0;
    }

    public final boolean onCommitPreserve() {
        return persistenceScope == TableBase.SCOPE_SESSION;
    }

    public final RowIterator rowIterator(Session session) {

        PersistentStore store = session.sessionData.getRowStore(this);

        return getPrimaryIndex().firstRow(store);
    }

    public final RowIterator rowIterator(PersistentStore store) {
        return getPrimaryIndex().firstRow(store);
    }

    public final int getIndexCount() {
        return indexList.length;
    }

    public final Index getPrimaryIndex() {
        return indexList[0];
    }

    public final Type[] getPrimaryKeyTypes() {
        return primaryKeyTypes;
    }

    public final boolean hasPrimaryKey() {
        return !(primaryKeyCols.length == 0);
    }

    public final int[] getPrimaryKey() {
        return primaryKeyCols;
    }

    /**
     *  Returns an array of Type indicating the SQL type of the columns
     */
    public final Type[] getColumnTypes() {
        return colTypes;
    }

    /**
     * Returns an index on all the columns
     */
    public Index getFullIndex() {
        return fullIndex;
    }

    /**
     *  Returns the Index object at the given index
     */
    public final Index getIndex(int i) {
        return indexList[i];
    }

    /**
     *  Returns the indexes
     */
    public final Index[] getIndexList() {
        return indexList;
    }

    /**
     * Returns empty boolean array.
     */
    public final boolean[] getNewColumnCheckList() {
        return new boolean[getColumnCount()];
    }

    /**
     *  Returns the count of all visible columns.
     */
    public int getColumnCount() {
        return columnCount;
    }

    /**
     *  Returns the count of all columns.
     */
    public final int getDataColumnCount() {
        return colTypes.length;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    public void setTransactional(boolean value) {
        isTransactional = value;
    }

    /**
     * This method is called whenever there is a change to table structure and
     * serves two porposes: (a) to reset the best set of columns that identify
     * the rows of the table (b) to reset the best index that can be used
     * to find rows of the table given a column value.
     *
     * (a) gives most weight to a primary key index, followed by a unique
     * address with the lowest count of nullable columns. Otherwise there is
     * no best row identifier.
     *
     * (b) finds for each column an index with a corresponding first column.
     * It uses any type of visible index and accepts the one with the largest
     * column count.
     *
     * bestIndex is the user defined, primary key, the first unique index, or
     * the first non-unique index. NULL if there is no user-defined index.
     *
     */
    public final void setBestRowIdentifiers() {

        int[]   briCols      = null;
        int     briColsCount = 0;
        boolean isStrict     = false;
        int     nNullCount   = 0;

        // ignore if called prior to completion of primary key construction
        if (colNotNull == null) {
            return;
        }

        bestIndex          = null;
        bestIndexForColumn = new int[colTypes.length];

        ArrayUtil.fillArray(bestIndexForColumn, -1);

        for (int i = 0; i < indexList.length; i++) {
            Index index     = indexList[i];
            // A VoltDB extension -- Don't consider non-column expression indexes for this purpose.
            // Expression-based indexes are not suitable for row identification.
            if (index.getExpressions() != null) {
                continue;
            }
            // End of VoltDB extension
            int[] cols      = index.getColumns();
            int   colsCount = index.getVisibleColumns();

            if (colsCount == 0) {
                continue;
            }

            if (i == 0) {
                isStrict = true;
            }

            if (bestIndexForColumn[cols[0]] == -1) {
                bestIndexForColumn[cols[0]] = i;
            } else {
                Index existing = indexList[bestIndexForColumn[cols[0]]];

                if (colsCount > existing.getColumns().length) {
                    bestIndexForColumn[cols[0]] = i;
                }
            }

            if (!index.isUnique()) {
                if (bestIndex == null) {
                    bestIndex = index;
                }

                continue;
            }

            int nnullc = 0;

            for (int j = 0; j < colsCount; j++) {
                if (colNotNull[cols[j]]) {
                    nnullc++;
                }
            }

            if (bestIndex != null) {
                bestIndex = index;
            }

            if (nnullc == colsCount) {
                if (briCols == null || briColsCount != nNullCount
                        || colsCount < briColsCount) {

                    //  nothing found before ||
                    //  found but has null columns ||
                    //  found but has more columns than this index
                    briCols      = cols;
                    briColsCount = colsCount;
                    nNullCount   = colsCount;
                    isStrict     = true;
                }

                continue;
            } else if (isStrict) {
                continue;
            } else if (briCols == null || colsCount < briColsCount
                       || nnullc > nNullCount) {

                //  nothing found before ||
                //  found but has more columns than this index||
                //  found but has fewer not null columns than this index
                briCols      = cols;
                briColsCount = colsCount;
                nNullCount   = nnullc;
            }
        }

        // remove rowID column from bestRowIdentiferCols
        bestRowIdentifierCols = briCols == null
                                || briColsCount == briCols.length ? briCols
                                                                  : ArrayUtil
                                                                  .arraySlice(briCols,
                                                                      0, briColsCount);
        bestRowIdentifierStrict = isStrict;

        if (indexList[0].getColumnCount() > 0) {
            bestIndex = indexList[0];
        }
    }

    public final void createPrimaryIndex(int[] pkcols, Type[] pktypes,
                                         HsqlName name) {

        long id = database.persistentStoreCollection.getNextId();
        Index newindex = new IndexAVL(name, id, this, pkcols, null, null,
                                      pktypes, true, true, true, false);

        try {
            addIndex(newindex);
        } catch (HsqlException e) {}
    }

    public final Index createAndAddIndexStructure(HsqlName name,
            int[] columns, boolean[] descending, boolean[] nullsLast,
            boolean unique, boolean constraint, boolean forward) {

        Index newindex = createIndexStructure(name, columns, descending,
                                              nullsLast, unique, constraint,
                                              forward);

        addIndex(newindex);

        return newindex;
    }

    final Index createIndexStructure(HsqlName name, int[] columns,
                                     boolean[] descending,
                                     boolean[] nullsLast, boolean unique,
                                     boolean constraint, boolean forward) {

        if (primaryKeyCols == null) {
            // A VoltDB extension to support matview-based indexes
            primaryKeyCols = new int[0];
            /* disable 1 line ...
            throw Error.runtimeError(ErrorCode.U_S0500, "createIndex");
            ... disabled 1 line */
            // End of VoltDB extension
        }

        int    s     = columns.length;
        int[]  cols  = new int[s];
        Type[] types = new Type[s];

        for (int j = 0; j < s; j++) {
            cols[j]  = columns[j];
            types[j] = colTypes[cols[j]];
        }

        long id = database.persistentStoreCollection.getNextId();
        Index newIndex = new IndexAVL(name, id, this, cols, descending,
                                      nullsLast, types, false, unique,
                                      constraint, forward);

        return newIndex;
    }

    final void addIndex(Index index) {

        int i = 0;

        for (; i < indexList.length; i++) {
            Index current = indexList[i];
            int order = index.getIndexOrderValue()
                        - current.getIndexOrderValue();

            if (order < 0) {
                break;
            }
        }

        indexList = (Index[]) ArrayUtil.toAdjustedArray(indexList, index, i,
                1);

        for (i = 0; i < indexList.length; i++) {
            indexList[i].setPosition(i);
        }

        if (store != null) {
            try {
                store.resetAccessorKeys(indexList);
            } catch (HsqlException e) {
                indexList = (Index[]) ArrayUtil.toAdjustedArray(indexList,
                        null, index.getPosition(), -1);

                for (i = 0; i < indexList.length; i++) {
                    indexList[i].setPosition(i);
                }

                throw e;
            }
        }

        setBestRowIdentifiers();
    }

    final void removeIndex(int position) {
        setBestRowIdentifiers();
    }

    public final Object[] getEmptyRowData() {
        return new Object[getDataColumnCount()];
    }

    /**
     *  Create new memory-resident index. For MEMORY and TEXT tables.
     */
    public final Index createIndex(PersistentStore store, HsqlName name,
                                   int[] columns, boolean[] descending,
                                   boolean[] nullsLast, boolean unique,
                                   boolean constraint, boolean forward) {

        Index newIndex = createAndAddIndexStructure(name, columns, descending,
            nullsLast, unique, constraint, forward);

        return newIndex;
    }

    public void clearAllData(Session session) {

        PersistentStore store = session.sessionData.getRowStore(this);

        store.removeAll();
    }

    public void clearAllData(PersistentStore store) {
        store.removeAll();
    }

    /**
     * @todo - this is wrong, as it returns true when table has no rows,
     * but not where it has rows that are not visible by session
     *  Returns true if the table has any rows at all.
     */
    public final boolean isEmpty(Session session) {

        if (getIndexCount() == 0) {
            return true;
        }

        PersistentStore store = session.sessionData.getRowStore(this);

        return getIndex(0).isEmpty(store);
    }

    public int getRowCount(PersistentStore store) {
        return getPrimaryIndex().size(store);
    }
}
