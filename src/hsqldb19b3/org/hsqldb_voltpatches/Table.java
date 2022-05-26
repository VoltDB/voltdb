/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
 * Copyright (c) 2010-2022, Volt Active Data Inc.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.index.IndexAVL;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.persist.CachedObject;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.LobData;
import org.hsqldb_voltpatches.types.Type;

// fredt@users 20020130 - patch 491987 by jimbag@users - made optional
// fredt@users 20020405 - patch 1.7.0 by fredt - quoted identifiers
// for sql standard quoted identifiers for column and table names and aliases
// applied to different places
// fredt@users 20020225 - patch 1.7.0 - restructuring
// some methods moved from Database.java, some rewritten
// changes to several methods
// fredt@users 20020225 - patch 1.7.0 - ON DELETE CASCADE
// fredt@users 20020225 - patch 1.7.0 - named constraints
// boucherb@users 20020225 - patch 1.7.0 - multi-column primary keys
// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// tony_lai@users 20020820 - patch 595099 - user defined PK name
// tony_lai@users 20020820 - patch 595172 - drop constraint fix
// fredt@users 20021210 - patch 1.7.2 - better ADD / DROP INDEX for non-CACHED tables
// fredt@users 20030901 - patch 1.7.2 - allow multiple nulls for UNIQUE columns
// fredt@users 20030901 - patch 1.7.2 - reworked IDENTITY support
// achnettest@users 20040130 - patch 878288 - bug fix for new indexes in memory tables by Arne Christensen
// boucherb@users 20040327 - doc 1.7.2 - javadoc updates
// boucherb@users 200404xx - patch 1.7.2 - proper uri for getCatalogName
// fredt@users 20050000 - 1.8.0 updates in several areas
// fredt@users 20050220 - patch 1.8.0 enforcement of DECIMAL precision/scale
// fredt@users 1.9.0 referential constraint enforcement moved to CompiledStatementExecutor
// fredt@users 1.9.0 base functionality moved to TableBase

/**
 * Holds the data structures and methods for creation of a database table.
 *
 *
 * Extensively rewritten and extended in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public class Table extends TableBase implements SchemaObject {

    public static final Table[] emptyArray = new Table[]{};

    // main properties
    protected HsqlName tableName;

    //
    public HashMappedList columnList;          // columns in table
    int                   identityColumn;      // -1 means no such column
    NumberSequence        identitySequence;    // next value of identity column

// -----------------------------------------------------------------------
    Constraint[]    constraintList;            // constrainst for the table
    Constraint[]    fkPath;                    //
    Constraint[]    fkConstraints;             //
    Constraint[]    fkMainConstraints;
    Constraint[]    checkConstraints;
    TriggerDef[]    triggerList;
    TriggerDef[][]  triggerLists;              // array of trigger lists
    Expression[]    colDefaults;               // fredt - expressions of DEFAULT values
    protected int[] defaultColumnMap;          // fred - holding 0,1,2,3,...
    private boolean hasDefaultValues;          //fredt - shortcut for above
    TimeToLiveVoltDB      timeToLive;          //time to live (VOLTDB)
    PersistentExport      persistentExport;    //export to target(VOLTDB)
    private boolean isStream = false;          //is this a stream (VOLTDB)
    private boolean hasMigrationTarget = false; // does the table has a migration target (VOLTDB)
    //
    public Table(Database database, HsqlName name, int type) {

        this.database = database;
        tableName     = name;
        persistenceId = database.persistentStoreCollection.getNextId();

        switch (type) {

            case SYSTEM_SUBQUERY :
                persistenceScope = SCOPE_STATEMENT;
                isSessionBased   = true;
                break;

            case SYSTEM_TABLE :
                persistenceScope = SCOPE_FULL;
                isSchemaBased    = true;
                break;

            case CACHED_TABLE :
                if (DatabaseURL.isFileBasedDatabaseType(database.getType())) {
                    persistenceScope = SCOPE_FULL;
                    isSchemaBased    = true;
                    isCached         = true;
                    isLogged         = !database.isFilesReadOnly();

                    break;
                }

                type = MEMORY_TABLE;

            // $FALL-THROUGH$
            case MEMORY_TABLE :
                persistenceScope = SCOPE_FULL;
                isSchemaBased    = true;
                isLogged         = !database.isFilesReadOnly();
                break;

            case TEMP_TABLE :
                persistenceScope = SCOPE_TRANSACTION;
                isTemp           = true;
                isSchemaBased    = true;
                isSessionBased   = true;
                break;

            case TEMP_TEXT_TABLE :
                persistenceScope = SCOPE_SESSION;

                if (!DatabaseURL.isFileBasedDatabaseType(database.getType())) {
                    throw Error.error(ErrorCode.DATABASE_IS_MEMORY_ONLY);
                }

                isSchemaBased  = true;
                isSessionBased = true;
                isTemp         = true;
                isText         = true;
                isReadOnly     = true;
                break;

            case TEXT_TABLE :
                persistenceScope = SCOPE_FULL;

                if (!DatabaseURL.isFileBasedDatabaseType(database.getType())) {
                    throw Error.error(ErrorCode.DATABASE_IS_MEMORY_ONLY);
                }

                isSchemaBased = true;
                isText        = true;
                break;

            case VIEW_TABLE :
                persistenceScope = SCOPE_STATEMENT;
                isSchemaBased    = true;
                isSessionBased   = true;
                isView           = true;
                break;

            case RESULT_TABLE :
                persistenceScope = SCOPE_SESSION;
                isSessionBased   = true;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }

        // type may have changed above for CACHED tables
        tableType         = type;
        primaryKeyCols    = null;
        primaryKeyTypes   = null;
        identityColumn    = -1;
        columnList        = new HashMappedList();
        indexList         = Index.emptyArray;
        constraintList    = Constraint.emptyArray;
        fkPath            = Constraint.emptyArray;
        fkConstraints     = Constraint.emptyArray;
        fkMainConstraints = Constraint.emptyArray;
        checkConstraints  = Constraint.emptyArray;
        triggerList       = TriggerDef.emptyArray;
        triggerLists      = new TriggerDef[TriggerDef.NUM_TRIGS][];

        for (int i = 0; i < TriggerDef.NUM_TRIGS; i++) {
            triggerLists[i] = TriggerDef.emptyArray;
        }

        if (database.isFilesReadOnly() && isFileBased()) {
            this.isReadOnly = true;
        }

        if (!isTemp) {
            createDefaultStore();
        }
    }

    public Table(Table table, HsqlName name) {

        persistenceScope    = SCOPE_STATEMENT;
        name.schema         = SqlInvariants.SYSTEM_SCHEMA_HSQLNAME;
        this.tableName      = name;
        this.database       = table.database;
        this.tableType      = RESULT_TABLE;
        this.columnList     = table.columnList;
        this.columnCount    = table.columnCount;
        this.indexList      = Index.emptyArray;
        this.constraintList = Constraint.emptyArray;

        createPrimaryKey();
    }

    public void createDefaultStore() {

        store = database.logger.newStore(null,
                                         database.persistentStoreCollection,
                                         this, true);
    }

    @Override
    public int getType() {
        return SchemaObject.TABLE;
    }

    /**
     *  Returns the HsqlName object for the table
     */
    @Override
    public final HsqlName getName() {
        return tableName;
    }

    /**
     * Returns the catalog name or null, depending on a database property.
     */
    @Override
    public HsqlName getCatalogName() {
        return database.getCatalogName();
    }

    /**
     * Returns the schema name.
     */
    @Override
    public HsqlName getSchemaName() {
        return tableName.schema;
    }

    @Override
    public Grantee getOwner() {
        return tableName.schema.owner;
    }

    @Override
    public OrderedHashSet getReferences() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < colTypes.length; i++) {
            if (colTypes[i].isDomainType() || colTypes[i].isDistinctType()) {
                HsqlName name = ((SchemaObject) colTypes[i]).getName();

                set.add(name);
            }
        }

        return set;
    }

    @Override
    public OrderedHashSet getComponents() {

        OrderedHashSet set = new OrderedHashSet();

        set.addAll(constraintList);
        set.addAll(triggerList);

        for (int i = 0; i < indexList.length; i++) {
            if (!indexList[i].isConstraint()) {
                set.add(indexList[i]);
            }
        }

        return set;
    }

    @Override
    public void compile(Session session) {}

    String[] getSQL(OrderedHashSet resolved, OrderedHashSet unresolved) {

        for (int i = 0; i < constraintList.length; i++) {
            Constraint c = constraintList[i];

            if (c.isForward) {
                unresolved.add(c);
            } else if (c.getConstraintType() == Constraint.UNIQUE
                       || c.getConstraintType() == Constraint.PRIMARY_KEY) {
                resolved.add(c.getName());
            }
        }

        HsqlArrayList list = new HsqlArrayList();

        list.add(getSQL());

        // readonly for TEXT tables only
        if (isText()) {
            if (((TextTable) this).isConnected() && isDataReadOnly()) {
                StringBuffer sb = new StringBuffer(64);

                sb.append(Tokens.T_SET).append(' ').append(
                    Tokens.T_TABLE).append(' ');
                sb.append(getName().getSchemaQualifiedStatementName());
                sb.append(' ').append(Tokens.T_READONLY).append(' ');
                sb.append(Tokens.T_TRUE);
                list.add(sb.toString());
            }

            // data source
            String dataSource = ((TextTable) this).getDataSourceDDL();

            if (dataSource != null) {
                list.add(dataSource);
            }

            // header
            String header = ((TextTable) this).getDataSourceHeader();

            if (header != null) {
                list.add(header);
            }
        }

        if (!isTemp && hasIdentityColumn()) {
            list.add(NumberSequence.getRestartSQL(this));
        }

        for (int i = 0; i < indexList.length; i++) {
            if (!indexList[i].isConstraint()) {
                list.add(indexList[i].getSQL());
            }
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    String[] getTriggerSQL() {

        String[] array = new String[triggerList.length];

        for (int i = 0; i < triggerList.length; i++) {
            array[i] = triggerList[i].getSQL();
        }

        return array;
    }

    @Override
    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_CREATE).append(' ');

        if (isTemp()) {
            sb.append(Tokens.T_GLOBAL).append(' ');
            sb.append(Tokens.T_TEMPORARY).append(' ');
        } else if (isText()) {
            sb.append(Tokens.T_TEXT).append(' ');
        } else if (isCached()) {
            sb.append(Tokens.T_CACHED).append(' ');
        } else {
            sb.append(Tokens.T_MEMORY).append(' ');
        }

        sb.append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append('(');

        int        columns = getColumnCount();
        int[]      pk      = getPrimaryKey();
        Constraint pkConst = getPrimaryConstraint();

        for (int j = 0; j < columns; j++) {
            ColumnSchema column  = getColumn(j);
            String       colname = column.getName().statementName;
            Type         type    = column.getDataType();

            if (j > 0) {
                sb.append(',');
            }

            sb.append(colname);
            sb.append(' ');
            sb.append(type.getTypeDefinition());

            String defaultString = column.getDefaultSQL();

            if (defaultString != null) {
                sb.append(' ').append(Tokens.T_DEFAULT).append(' ');
                sb.append(defaultString);
            }

            if (column.isIdentity()) {
                sb.append(' ').append(column.getIdentitySequence().getSQL());
            }

            if (!column.isNullable()) {
                Constraint c = getNotNullConstraintForColumn(j);

                if (c != null && !c.getName().isReservedName()) {
                    sb.append(' ').append(Tokens.T_CONSTRAINT).append(
                        ' ').append(c.getName().statementName);
                }

                sb.append(' ').append(Tokens.T_NOT).append(' ').append(
                    Tokens.T_NULL);
            }

            if (pk.length == 1 && j == pk[0]
                    && pkConst.getName().isReservedName()) {
                sb.append(' ').append(Tokens.T_PRIMARY).append(' ').append(
                    Tokens.T_KEY);
            }
        }

        Constraint[] constraintList = getConstraints();

        for (int j = 0, vSize = constraintList.length; j < vSize; j++) {
            Constraint c = constraintList[j];

            if (!c.isForward) {
                String d = c.getSQL();

                if (d.length() > 0) {
                    sb.append(',');
                    sb.append(d);
                }
            }
        }

        sb.append(')');

        if (onCommitPreserve()) {
            sb.append(' ').append(Tokens.T_ON).append(' ');
            sb.append(Tokens.T_COMMIT).append(' ').append(Tokens.T_PRESERVE);
            sb.append(' ').append(Tokens.T_ROWS);
        }

        return sb.toString();
    }

    public String getIndexRootsSQL() {

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_INDEX).append(' ').append('\'');
        sb.append(getIndexRoots());
        sb.append('\'');

        return sb.toString();
    }

    /**
     * Used to create row id's
     */
    @Override
    public int getId() {
        return tableName.hashCode();
    }

    public final boolean isText() {
        return isText;
    }

    public final boolean isTemp() {
        return isTemp;
    }

    public final boolean isReadOnly() {
        return isReadOnly;
    }

    public final boolean isView() {
        return isView;
    }

    public boolean isCached() {
        return isCached;
    }

    public boolean isDataReadOnly() {
        return isReadOnly;
    }

    /**
     * returns false if the table has to be recreated in order to add / drop
     * indexes. Only CACHED tables return false.
     */
    final boolean isIndexingMutable() {
        return !isIndexCached();
    }

    /**
     *  Returns true if table is CACHED
     */
    boolean isIndexCached() {
        return isCached;
    }

    /**
     * Used by INSERT, DELETE, UPDATE operations
     */
    void checkDataReadOnly() {

        if (isReadOnly) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }
    }

// ----------------------------------------------------------------------------
// akede@users - 1.7.2 patch Files readonly
    public void setDataReadOnly(boolean value) {

        // Changing the Read-Only mode for the table is only allowed if the
        // the database can realize it.
        if (!value && database.isFilesReadOnly() && isFileBased()) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }

        isReadOnly = value;
    }

    /**
     * Text or Cached Tables are normally file based
     */
    public boolean isFileBased() {
        return isCached || isText;
    }

    /**
     *  Adds a constraint.
     */
    public void addConstraint(Constraint c) {

        int index = c.getConstraintType() == Constraint.PRIMARY_KEY ? 0
                                                                    : constraintList
                                                                        .length;

        constraintList =
            (Constraint[]) ArrayUtil.toAdjustedArray(constraintList, c, index,
                1);

        updateConstraintLists();
    }

    void updateConstraintPath() {

        if (fkMainConstraints.length == 0) {
            return;
        }

        OrderedHashSet list = new OrderedHashSet();

        getConstraintPath(defaultColumnMap, list);

        if (list.size() == 0) {
            return;
        }

        fkPath = new Constraint[list.size()];

        list.toArray(fkPath);

        for (int i = 0; i < fkPath.length; i++) {
            Constraint c         = fkPath[i];
            HsqlName   tableName = c.getMain().getName();

            if (c.getMain()
                    != database.schemaManager.getUserTable(null, tableName)) {
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "table constraint");
            }

            tableName = c.getRef().getName();

            if (c.getRef()
                    != database.schemaManager.getUserTable(null, tableName)) {
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "table constraint");
            }
        }
    }

    void updateConstraintLists() {

        int fkCount    = 0;
        int mainCount  = 0;
        int checkCount = 0;

        for (int i = 0; i < constraintList.length; i++) {
            switch (constraintList[i].getConstraintType()) {

                case Constraint.FOREIGN_KEY :
                    fkCount++;
                    break;

                case Constraint.MAIN :
                    mainCount++;
                    break;

                case Constraint.CHECK :
                    if (constraintList[i].isNotNull()) {
                        break;
                    }

                    checkCount++;
                    break;
            }
        }

        fkConstraints     = fkCount == 0 ? Constraint.emptyArray
                                         : new Constraint[fkCount];
        fkCount           = 0;
        fkMainConstraints = mainCount == 0 ? Constraint.emptyArray
                                           : new Constraint[mainCount];
        mainCount         = 0;
        checkConstraints  = checkCount == 0 ? Constraint.emptyArray
                                            : new Constraint[checkCount];
        checkCount        = 0;

        for (int i = 0; i < constraintList.length; i++) {
            switch (constraintList[i].getConstraintType()) {

                case Constraint.FOREIGN_KEY :
                    fkConstraints[fkCount] = constraintList[i];

                    fkCount++;
                    break;

                case Constraint.MAIN :
                    fkMainConstraints[mainCount] = constraintList[i];

                    mainCount++;
                    break;

                case Constraint.CHECK :
                    if (constraintList[i].isNotNull()) {
                        break;
                    }

                    checkConstraints[checkCount] = constraintList[i];

                    checkCount++;
                    break;
            }
        }
    }

    /**
     *  Returns the list of constraints.
     */
    public Constraint[] getConstraints() {
        return constraintList;
    }

    /**
     *  Returns the primary constraint.
     */
    public Constraint getPrimaryConstraint() {
        return primaryKeyCols.length == 0 ? null
                                          : constraintList[0];
    }

    void getConstraintPath(int[] columnMap, OrderedHashSet list) {

        for (int i = 0; i < constraintList.length; i++) {
            // A VoltDB extension -- Don't consider non-column expression indexes for this purpose
            if (constraintList[i].hasExprs()) {
                continue;
            }
            // End of VoltDB extension
            if (constraintList[i].hasTriggeredAction()) {
                int[] mainColumns = constraintList[i].getMainColumns();

                if (ArrayUtil.countCommonElements(columnMap, mainColumns)
                        > 0) {
                    if (list.add(constraintList[i])) {
                        constraintList[i].getRef().getConstraintPath(
                            constraintList[i].getRefColumns(), list);
                    }
                }
            }
        }
    }

    Constraint getNotNullConstraintForColumn(int colIndex) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.isNotNull() && c.notNullColumnIndex == colIndex) {
                return c;
            }
        }

        return null;
    }

    /**
     * Returns the UNIQUE or PK constraint with the given column signature.
     */
    Constraint getUniqueConstraintForColumns(int[] cols) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.isUniqueWithColumns(cols)) {
                return c;
            }
        }

        return null;
    }

    /**
     * Returns the UNIQUE or PK constraint with the given column signature.
     * Modifies the composition of refTableCols if necessary.
     */
    Constraint getUniqueConstraintForColumns(int[] mainTableCols,
            int[] refTableCols) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c    = constraintList[i];
            // A VoltDB extension -- Don't consider non-column expression indexes for this purpose
            if (c.hasExprs()) {
                continue;
            }
            // End of VoltDB extension
            int        type = c.getConstraintType();

            if (type != Constraint.UNIQUE && type != Constraint.PRIMARY_KEY) {
                continue;
            }

            int[] constraintCols = c.getMainColumns();

            if (constraintCols.length != mainTableCols.length) {
                continue;
            }

            if (ArrayUtil.areEqual(constraintCols, mainTableCols,
                                   mainTableCols.length, true)) {
                return c;
            }

            if (ArrayUtil.areEqualSets(constraintCols, mainTableCols)) {
                int[] newRefTableCols = new int[mainTableCols.length];

                for (int j = 0; j < mainTableCols.length; j++) {
                    int pos = ArrayUtil.find(constraintCols, mainTableCols[j]);

                    newRefTableCols[pos] = refTableCols[j];
                }

                for (int j = 0; j < mainTableCols.length; j++) {
                    refTableCols[j] = newRefTableCols[j];
                }

                return c;
            }
        }

        return null;
    }

    /**
     *  Returns any foreign key constraint equivalent to the column sets
     */
    Constraint getFKConstraintForColumns(Table tableMain, int[] mainCols,
                                         int[] refCols) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.isEquivalent(tableMain, mainCols, this, refCols)) {
                return c;
            }
        }

        return null;
    }

    /**
     *  Returns any unique Constraint using this index
     *
     * @param  index
     */
    public Constraint getUniqueOrPKConstraintForIndex(Index index) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getMainIndex() == index
                    && (c.getConstraintType() == Constraint.UNIQUE
                        || c.getConstraintType() == Constraint.PRIMARY_KEY)) {
                return c;
            }
        }

        return null;
    }

    /**
     *  Returns the next constraint of a given type
     *
     * @param  from
     * @param  type
     */
    int getNextConstraintIndex(int from, int type) {

        for (int i = from, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == type) {
                return i;
            }
        }

        return -1;
    }

    /**
     *  Performs the table level checks and adds a column to the table at the
     *  DDL level. Only used at table creation, not at alter column.
     */
    public void addColumn(ColumnSchema column) {

        String name = column.getName().name;

        if (findColumn(name) >= 0) {
            throw Error.error(ErrorCode.X_42504, name);
        }

        if (column.isIdentity()) {
            if (identityColumn != -1) {
                throw Error.error(ErrorCode.X_42525, name);
            }

            identityColumn   = getColumnCount();
            identitySequence = column.getIdentitySequence();
        }

        addColumnNoCheck(column);
    }

    public void addColumnNoCheck(ColumnSchema column) {

        if (primaryKeyCols != null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }

        columnList.add(column.getName().name, column);

        columnCount++;

        if (column.getDataType().isLobType()) {
            hasLobColumn = true;
        }
    }

    public boolean hasIdentityColumn() {
        return identityColumn != -1;
    }

    public long getNextIdentity() {
        return identitySequence.peek();
    }

    /**
     * Match two valid, equal length, columns arrays for type of columns
     *
     * @param col column array from this Table
     * @param other the other Table object
     * @param othercol column array from the other Table
     */
    void checkColumnsMatch(int[] col, Table other, int[] othercol) {

        for (int i = 0; i < col.length; i++) {
            Type type      = colTypes[col[i]];
            Type otherType = other.colTypes[othercol[i]];

            if (type.typeComparisonGroup != otherType.typeComparisonGroup) {
                throw Error.error(ErrorCode.X_42562);
            }
        }
    }

    void checkColumnsMatch(ColumnSchema column, int colIndex) {

        Type type      = colTypes[colIndex];
        Type otherType = column.getDataType();

        if (type.typeComparisonGroup != otherType.typeComparisonGroup) {
            throw Error.error(ErrorCode.X_42562);
        }
    }

    /**
     * For removal or addition of columns, constraints and indexes
     *
     * Does not work in this form for FK's as Constraint.ConstraintCore
     * is not transfered to a referencing or referenced table
     */
    Table moveDefinition(Session session, int newType, ColumnSchema column,
                         Constraint constraint, Index index, int colIndex,
                         int adjust, OrderedHashSet dropConstraints,
                         OrderedHashSet dropIndexes) {

        boolean newPK = false;

        if (constraint != null
                && constraint.constType == Constraint.PRIMARY_KEY) {
            newPK = true;
        }

        Table tn = new Table(database, tableName, newType);

        if (tableType == TEMP_TABLE) {
            tn.persistenceScope = persistenceScope;
        }

        tn.persistentExport = this.persistentExport;
        for (int i = 0; i < getColumnCount(); i++) {
            ColumnSchema col = (ColumnSchema) columnList.get(i);

            if (i == colIndex) {
                if (column != null) {
                    tn.addColumn(column);
                }

                if (adjust <= 0) {
                    continue;
                }
            }

            tn.addColumn(col);
        }

        if (getColumnCount() == colIndex) {
            tn.addColumn(column);
        }

        int[] pkCols = null;

        if (hasPrimaryKey()
                && !dropConstraints.contains(
                    getPrimaryConstraint().getName())) {
            pkCols = primaryKeyCols;
            pkCols = ArrayUtil.toAdjustedColumnArray(pkCols, colIndex, adjust);
        } else if (newPK) {
            pkCols = constraint.getMainColumns();
        }

        tn.createPrimaryKey(getIndex(0).getName(), pkCols, false);

        for (int i = 1; i < indexList.length; i++) {
            Index idx = indexList[i];

            if (dropIndexes.contains(idx.getName())) {
                continue;
            }

            int[] colarr = ArrayUtil.toAdjustedColumnArray(idx.getColumns(),
                colIndex, adjust);

            // A VoltDB extension to support indexed expressions and assume unique attribute
            Expression[] exprArr = idx.getExpressions();
            boolean assumeUnique = idx.isAssumeUnique();
            Expression predicate = idx.getPredicate();
            // End of VoltDB extension
            idx = tn.createIndexStructure(idx.getName(), colarr,
                                          idx.getColumnDesc(), null,
                                          idx.isUnique(), idx.isMigrating(), idx.isConstraint(),
                                          idx.isForward());

            // A VoltDB extension to support indexed expressions and assume unique attribute and partial indexes
            if (exprArr != null) {
                idx = idx.withExpressions(adjustExprs(exprArr, colIndex, adjust));
            }
            if (predicate != null) {
                idx = idx.withPredicate(adjustExpr(predicate, colIndex, adjust));
            }
            idx = idx.setAssumeUnique(assumeUnique);
            // End of VoltDB extension
            tn.addIndex(idx);
        }

        if (index != null) {
            tn.addIndex(index);
        }

        HsqlArrayList newList = new HsqlArrayList();

        if (newPK) {
            constraint.core.mainIndex     = tn.indexList[0];
            constraint.core.mainTable     = tn;
            constraint.core.mainTableName = tn.tableName;

            newList.add(constraint);
        }

        for (int i = 0; i < constraintList.length; i++) {
            Constraint c = constraintList[i];

            if (dropConstraints.contains(c.getName())) {
                continue;
            }

            c = c.duplicate();

            c.updateTable(session, this, tn, colIndex, adjust);
            newList.add(c);
        }

        if (!newPK && constraint != null) {
            constraint.updateTable(session, this, tn, -1, 0);
            newList.add(constraint);
        }

        tn.constraintList = new Constraint[newList.size()];

        newList.toArray(tn.constraintList);
        tn.updateConstraintLists();
        tn.setBestRowIdentifiers();

        tn.triggerList  = triggerList;
        tn.triggerLists = triggerLists;

        return tn;
    }

    /**
     * Used for drop / retype column.
     */
    void checkColumnInCheckConstraint(int colIndex) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.constType == Constraint.CHECK && !c.isNotNull()
                    && c.hasColumn(colIndex)) {
                HsqlName name = c.getName();

                throw Error.error(ErrorCode.X_42502,
                                  name.getSchemaQualifiedStatementName());
            }
        }
    }

    /**
     * Used for retype column. Checks whether column is in an FK or is
     * referenced by a FK
     * @param colIndex index
     */
    void checkColumnInFKConstraint(int colIndex) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.hasColumn(colIndex)
                    && (c.getConstraintType() == Constraint.MAIN
                        || c.getConstraintType() == Constraint.FOREIGN_KEY)) {
                HsqlName name = c.getName();

                throw Error.error(ErrorCode.X_42533,
                                  name.getSchemaQualifiedStatementName());
            }
        }
    }

    /**
     * Returns list of constraints dependent only on one column
     */
    OrderedHashSet getDependentConstraints(int colIndex) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.hasColumnOnly(colIndex)) {
                set.add(c);
            }
        }

        return set;
    }

    /**
     * Returns list of constraints dependent on more than one column
     */
    OrderedHashSet getContainingConstraints(int colIndex) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.hasColumnPlus(colIndex)) {
                set.add(c);
            }
        }

        return set;
    }

    OrderedHashSet getContainingIndexNames(int colIndex) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = indexList.length; i < size; i++) {
            Index index = indexList[i];

            if (ArrayUtil.find(index.getColumns(), colIndex) != -1) {
                set.add(index.getName());
            }
        }

        return set;
    }

    /**
     * Returns list of MAIN constraints dependent on this PK or UNIQUE constraint
     */
    OrderedHashSet getDependentConstraints(Constraint constraint) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == Constraint.MAIN) {
                if (c.core.uniqueName == constraint.getName()) {
                    set.add(c);
                }
            }
        }

        return set;
    }

    public OrderedHashSet getDependentExternalConstraints() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == Constraint.MAIN
                    || c.getConstraintType() == Constraint.FOREIGN_KEY) {
                if (c.core.mainTable != c.core.refTable) {
                    set.add(c);
                }
            }
        }

        return set;
    }

    /**
     * Used for column defaults and nullability. Checks whether column is in an
     * FK with a given referential action type.
     *
     * @param colIndex index of column
     * @param actionType referential action of the FK
     */
    void checkColumnInFKConstraint(int colIndex, int actionType) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint c = constraintList[i];

            if (c.getConstraintType() == Constraint.FOREIGN_KEY
                    && c.hasColumn(colIndex)
                    && (actionType == c.getUpdateAction()
                        || actionType == c.getDeleteAction())) {
                HsqlName name = c.getName();

                throw Error.error(ErrorCode.X_42533,
                                  name.getSchemaQualifiedStatementName());
            }
        }
    }

    /**
     *  Returns the identity column index.
     */
    int getIdentityColumnIndex() {
        return identityColumn;
    }

    /**
     *  Returns the index of given column name or throws if not found
     */
    public int getColumnIndex(String name) {

        int i = findColumn(name);

        if (i == -1) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        return i;
    }

    /**
     *  Returns the index of given column name or -1 if not found.
     */
    public int findColumn(String name) {

        int index = columnList.getIndex(name);

        return index;
    }

    /**
     * Sets the SQL default value for a columm.
     */
    void setDefaultExpression(int columnIndex, Expression def) {

        ColumnSchema column = getColumn(columnIndex);

        column.setDefaultExpression(def);
        setColumnTypeVars(columnIndex);
    }

    /**
     * sets the flag for the presence of any default expression
     */
    void resetDefaultsFlag() {

        hasDefaultValues = false;

        for (int i = 0; i < colDefaults.length; i++) {
            hasDefaultValues = hasDefaultValues || colDefaults[i] != null;
        }
    }

    public int[] getBestRowIdentifiers() {
        return bestRowIdentifierCols;
    }

    public boolean isBestRowIdentifiersStrict() {
        return bestRowIdentifierStrict;
    }

    /**
     *  Finds an existing index for a column
     */
    Index getIndexForColumn(int col) {

        int i = bestIndexForColumn[col];

        return i == -1 ? null
                       : this.indexList[i];
    }

    boolean isIndexed(int colIndex) {
        return bestIndexForColumn[colIndex] != -1;
    }

    int[] getUniqueNotNullColumnGroup(boolean[] usedColumns) {

        for (int i = 0, count = constraintList.length; i < count; i++) {
            Constraint constraint = constraintList[i];

            if (constraint.constType == Constraint.UNIQUE) {
                int[] indexCols = constraint.getMainColumns();

                if (ArrayUtil.areIntIndexesInBooleanArray(
                        indexCols, colNotNull) && ArrayUtil
                            .areIntIndexesInBooleanArray(
                                indexCols, usedColumns)) {
                    return indexCols;
                }
            } else if (constraint.constType == Constraint.PRIMARY_KEY) {
                int[] indexCols = constraint.getMainColumns();

                if (ArrayUtil.areIntIndexesInBooleanArray(indexCols,
                        usedColumns)) {
                    return indexCols;
                }
            }
        }

        return null;
    }

    boolean areColumnsNotNull(int[] indexes) {
        return ArrayUtil.areIntIndexesInBooleanArray(indexes, colNotNull);
    }

    /**
     *  Shortcut for creating system table PK's.
     */
    void createPrimaryKey(int[] cols) {
        createPrimaryKey(null, cols, false);
    }

    /**
     *  Shortcut for creating default PK's.
     */
    public void createPrimaryKey() {
        createPrimaryKey(null, null, false);
    }

    /**
     *  Creates a single or multi-column primary key and index. sets the
     *  colTypes array. Finalises the creation of the table. (fredt@users)
     */
    public void createPrimaryKey(HsqlName indexName, int[] columns,
                                 boolean columnsNotNull) {

        if (primaryKeyCols != null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }

        if (columns == null) {
            columns = ValuePool.emptyIntArray;
        } else {
            for (int i = 0; i < columns.length; i++) {
                getColumn(columns[i]).setPrimaryKey(true);
            }
        }

        primaryKeyCols = columns;

        setColumnStructures();

        primaryKeyTypes = new Type[primaryKeyCols.length];

        ArrayUtil.projectRow(colTypes, primaryKeyCols, primaryKeyTypes);

        primaryKeyColsSequence = new int[primaryKeyCols.length];

        ArrayUtil.fillSequence(primaryKeyColsSequence);

        HsqlName name = indexName;

        if (name == null) {
            name = database.nameManager.newAutoName("IDX", getSchemaName(),
                    getName(), SchemaObject.INDEX);
        }

        createPrimaryIndex(primaryKeyCols, primaryKeyTypes, name);
        setBestRowIdentifiers();
    }

    void setColumnStructures() {

        colTypes         = new Type[columnCount];
        colDefaults      = new Expression[columnCount];
        colNotNull       = new boolean[columnCount];
        defaultColumnMap = new int[columnCount];

        for (int i = 0; i < columnCount; i++) {
            setColumnTypeVars(i);
        }

        resetDefaultsFlag();
    }

    void setColumnTypeVars(int i) {

        ColumnSchema column = getColumn(i);

        colTypes[i]         = column.getDataType();
        colNotNull[i]       = column.isPrimaryKey() || !column.isNullable();
        defaultColumnMap[i] = i;

        if (column.isIdentity()) {
            identitySequence = column.getIdentitySequence();
            identityColumn   = i;
        } else if (identityColumn == i) {
            identityColumn = -1;
        }

        colDefaults[i] = column.getDefaultExpression();

        resetDefaultsFlag();
    }

    /**
     * Returns direct mapping array.
     */
    int[] getColumnMap() {
        return defaultColumnMap;
    }

    /**
     * Returns empty mapping array.
     */
    int[] getNewColumnMap() {
        return new int[getColumnCount()];
    }

    boolean[] getColumnCheckList(int[] columnIndexes) {

        boolean[] columnCheckList = new boolean[getColumnCount()];

        for (int i = 0; i < columnIndexes.length; i++) {
            int index = columnIndexes[i];

            if (index > -1) {
                columnCheckList[index] = true;
            }
        }

        return columnCheckList;
    }

    int[] getColumnIndexes(OrderedHashSet set) {

        int[] cols = new int[set.size()];

        for (int i = 0; i < cols.length; i++) {
            cols[i] = getColumnIndex((String) set.get(i));
        }

        return cols;
    }

    int[] getColumnIndexes(HashMappedList list) {

        int[] cols = new int[list.size()];

        for (int i = 0; i < cols.length; i++) {
            cols[i] = ((Integer) list.get(i)).intValue();
        }

        return cols;
    }

    /**
     *  Returns the Column object at the given index
     */
    public ColumnSchema getColumn(int i) {
        return (ColumnSchema) columnList.get(i);
    }

    public OrderedHashSet getColumnNameSet(int[] columnIndexes) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < columnIndexes.length; i++) {
            set.add(columnList.get(i));
        }

        return set;
    }

    public OrderedHashSet getColumnNameSet(boolean[] columnCheckList) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < columnCheckList.length; i++) {
            if (columnCheckList[i]) {
                set.add(columnList.get(i));
            }
        }

        return set;
    }

    public void getColumnNames(boolean[] columnCheckList, Set set) {

        for (int i = 0; i < columnCheckList.length; i++) {
            if (columnCheckList[i]) {
                set.add(((ColumnSchema) columnList.get(i)).getName());
            }
        }
    }

    public OrderedHashSet getColumnNameSet() {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < getColumnCount(); i++) {
            set.add(((ColumnSchema) columnList.get(i)).getName());
        }

        return set;
    }

    /**
     * Returns array for a new row with SQL DEFAULT value for each column n
     * where exists[n] is false. This provides default values only where
     * required and avoids evaluating these values where they will be
     * overwritten.
     */
    Object[] getNewRowData(Session session) {

        Object[] data = new Object[getColumnCount()];
        int      i;

        if (hasDefaultValues) {
            for (i = 0; i < getColumnCount(); i++) {
                Expression def = colDefaults[i];

                if (def != null) {
                    data[i] = def.getValue(session, colTypes[i]);
                }
            }
        }

        return data;
    }

    boolean hasTrigger(int trigVecIndex) {
        return triggerLists[trigVecIndex].length != 0;
    }

    /**
     * Adds a trigger.
     */
    void addTrigger(TriggerDef td, HsqlName otherName) {

        int index = triggerList.length;

        if (otherName != null) {
            int pos = getTriggerIndex(otherName.name);

            if (pos != -1) {
                index = pos + 1;
            }
        }

        triggerList = (TriggerDef[]) ArrayUtil.toAdjustedArray(triggerList,
                td, index, 1);

        TriggerDef[] list = triggerLists[td.vectorIndex];

        index = list.length;

        if (otherName != null) {
            for (int i = 0; i < list.length; i++) {
                TriggerDef trigger = list[i];

                if (trigger.name.name.equals(otherName.name)) {
                    index = i + 1;

                    break;
                }
            }
        }

        list = (TriggerDef[]) ArrayUtil.toAdjustedArray(list, td, index, 1);
        triggerLists[td.vectorIndex] = list;
    }

    /**
     * Returns a trigger.
     */
    TriggerDef getTrigger(String name) {

        for (int i = triggerList.length - 1; i >= 0; i--) {
            if (triggerList[i].name.name.equals(name)) {
                return triggerList[i];
            }
        }

        return null;
    }

    public int getTriggerIndex(String name) {

        for (int i = 0; i < triggerList.length; i++) {
            if (triggerList[i].name.name.equals(name)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Drops a trigger.
     */
    void removeTrigger(String name) {

        TriggerDef td = null;

        for (int i = 0; i < triggerList.length; i++) {
            td = triggerList[i];

            if (td.name.name.equals(name)) {
                td.terminate();

                triggerList =
                    (TriggerDef[]) ArrayUtil.toAdjustedArray(triggerList,
                        null, i, -1);

                break;
            }
        }

        if (td == null) {
            return;
        }

        int index = td.vectorIndex;

        // look in each trigger list of each type of trigger
        for (int j = 0; j < triggerLists[index].length; j++) {
            td = triggerLists[index][j];

            if (td.name.name.equals(name)) {
                td.terminate();

                triggerLists[index] = (TriggerDef[]) ArrayUtil.toAdjustedArray(
                    triggerLists[index], null, j, -1);

                break;
            }
        }
    }

    /**
     * Drops all triggers.
     */
    void releaseTriggers() {

        // look in each trigger list of each type of trigger
        for (int i = 0; i < TriggerDef.NUM_TRIGS; i++) {
            for (int j = 0; j < triggerLists[i].length; j++) {
                triggerLists[i][j].terminate();
            }

            triggerLists[i] = TriggerDef.emptyArray;
        }
    }

    /**
     * Returns the index of the Index object of the given name or -1 if not found.
     */
    int getIndexIndex(String indexName) {

        Index[] indexes = indexList;

        for (int i = 0; i < indexes.length; i++) {
            if (indexName.equals(indexes[i].getName().name)) {
                return i;
            }
        }

        // no such index
        return -1;
    }

    /**
     * Returns the Index object of the given name or null if not found.
     */
    Index getIndex(String indexName) {

        Index[] indexes = indexList;
        int     i       = getIndexIndex(indexName);

        return i == -1 ? null
                       : indexes[i];
    }

    /**
     *  Return the position of the constraint within the list
     */
    int getConstraintIndex(String constraintName) {

        for (int i = 0, size = constraintList.length; i < size; i++) {
            if (constraintList[i].getName().name.equals(constraintName)) {
                return i;
            }
        }

        return -1;
    }

    /**
     *  return the named constriant
     */
    public Constraint getConstraint(String constraintName) {

        int i = getConstraintIndex(constraintName);

        return (i < 0) ? null
                       : constraintList[i];
    }

    /**
     * remove a named constraint
     */
    void removeConstraint(String name) {

        int index = getConstraintIndex(name);

        if (index != -1) {
            removeConstraint(index);
        }
    }

    void removeConstraint(int index) {

        constraintList =
            (Constraint[]) ArrayUtil.toAdjustedArray(constraintList, null,
                index, -1);

        updateConstraintLists();
    }

    void renameColumn(ColumnSchema column, String newName, boolean isquoted) {

        String oldname = column.getName().name;
        int    i       = getColumnIndex(oldname);

        columnList.setKey(i, newName);
        column.getName().rename(newName, isquoted);
    }

    void renameColumn(ColumnSchema column, HsqlName newName) {

        String oldname = column.getName().name;
        int    i       = getColumnIndex(oldname);

        columnList.setKey(i, newName);
        column.getName().rename(newName);
    }

    public TriggerDef[] getTriggers() {
        return triggerList;
    }

    public boolean isWritable() {
        return !isReadOnly && !database.databaseReadOnly
               && !(database.isFilesReadOnly() && (isCached || isText));
    }

    public boolean isInsertable() {
        return isWritable();
    }

    public boolean isUpdatable() {
        return isWritable();
    }

    public int[] getUpdatableColumns() {
        return defaultColumnMap;
    }

    public Table getBaseTable() {
        return this;
    }

    public int[] getBaseTableColumnMap() {
        return defaultColumnMap;
    }

//

    /**
     *  Used to create an index automatically for system tables.
     */
    Index createIndexForColumns(int[] columns) {

        HsqlName indexName = database.nameManager.newAutoName("IDX_T",
            getSchemaName(), getName(), SchemaObject.INDEX);

        try {
            Index index = createAndAddIndexStructure(indexName, columns, null,
                null, false, false, false, false);

            return index;
        } catch (Throwable t) {
            return null;
        }
    }

    void fireAfterTriggers(Session session, int trigVecIndex,
                           RowSetNavigator rowSet) {

        if (!database.isReferentialIntegrity()) {
            return;
        }

        TriggerDef[] trigVec = triggerLists[trigVecIndex];

        for (int i = 0, size = trigVec.length; i < size; i++) {
            TriggerDef td         = trigVec[i];
            boolean    sqlTrigger = td instanceof TriggerDefSQL;

            if (td.hasOldTable()) {

                //
            }

            if (td.isForEachRow()) {
                while (rowSet.hasNext()) {
                    Object[] oldData = null;
                    Object[] newData = null;

                    switch (td.triggerType) {

                        case Trigger.DELETE_AFTER_ROW :
                            oldData = rowSet.getNext();

                            if (!sqlTrigger) {
                                oldData = (Object[]) ArrayUtil.duplicateArray(
                                    oldData);
                            }
                            break;

                        case Trigger.INSERT_AFTER_ROW :
                            newData = rowSet.getNext();

                            if (!sqlTrigger) {
                                newData = (Object[]) ArrayUtil.duplicateArray(
                                    newData);
                            }
                            break;
                    }

                    td.pushPair(session, oldData, newData);
                }

                rowSet.beforeFirst();
            } else {
                td.pushPair(session, null, null);
            }
        }
    }

    /**
     *  Fires all row-level triggers of the given set (trigger type)
     *
     */
    void fireBeforeTriggers(Session session, int trigVecIndex,
                            Object[] oldData, Object[] newData, int[] cols) {

        if (!database.isReferentialIntegrity()) {
            return;
        }

        TriggerDef[] trigVec = triggerLists[trigVecIndex];

        for (int i = 0, size = trigVec.length; i < size; i++) {
            TriggerDef td         = trigVec[i];
            boolean    sqlTrigger = td instanceof TriggerDefSQL;

            if (cols != null && td.getUpdateColumnIndexes() != null
                    && !ArrayUtil.haveCommonElement(
                        td.getUpdateColumnIndexes(), cols, cols.length)) {
                continue;
            }

            if (td.isForEachRow()) {
                switch (td.triggerType) {

                    case Trigger.UPDATE_BEFORE_ROW :
                    case Trigger.DELETE_BEFORE_ROW :
                        if (!sqlTrigger) {
                            oldData =
                                (Object[]) ArrayUtil.duplicateArray(oldData);
                        }
                        break;
                }

                td.pushPair(session, oldData, newData);
            } else {
                td.pushPair(session, null, null);
            }
        }
    }

    /**
     *  Enforce max field sizes according to SQL column definition.
     *  SQL92 13.8
     */
    void enforceRowConstraints(Session session, Object[] data) {

        for (int i = 0; i < defaultColumnMap.length; i++) {
            Type type = colTypes[i];

            data[i] = type.convertToTypeLimits(session, data[i]);

            if (type.isDomainType()) {
                Constraint[] constraints =
                    type.userTypeModifier.getConstraints();

                for (int j = 0; j < constraints.length; j++) {
                    constraints[j].checkCheckConstraint(session, this,
                                                        data[i]);
                }
            }

            if (data[i] == null) {
                if (colNotNull[i]) {
                    Constraint c = getNotNullConstraintForColumn(i);

                    if (c == null) {
                        if (getColumn(i).isPrimaryKey()) {
                            c = this.getPrimaryConstraint();
                        }
                    }

                    String[] info = new String[] {
                        c.getName().name, tableName.name
                    };

                    throw Error.error(ErrorCode.X_23503, ErrorCode.CONSTRAINT,
                                      info);
                }
            }
        }
    }

    /**
     *  Finds an existing index for a column group
     */
    Index getIndexForColumns(int[] cols) {

        int i = bestIndexForColumn[cols[0]];

        if (i > -1) {
            return indexList[i];
        }

        switch (tableType) {

            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.SYSTEM_TABLE :
            case TableBase.VIEW_TABLE :
            case TableBase.TEMP_TABLE : {
                Index index = createIndexForColumns(cols);

                return index;
            }
        }

        return null;
    }

    /**
     * Finds an existing index for a column set or create one for temporary
     * tables
     */
    Index getIndexForColumns(OrderedIntHashSet set) {

        int   maxMatchCount = 0;
        Index selected      = null;

        if (set.isEmpty()) {
            return null;
        }

        for (int i = 0, count = indexList.length; i < count; i++) {
            Index currentindex = getIndex(i);
            int[] indexcols    = currentindex.getColumns();
            int   matchCount   = set.getOrderedMatchCount(indexcols);

            if (matchCount == 0) {
                continue;
            }

            if (matchCount == indexcols.length) {
                return currentindex;
            }

            if (matchCount > maxMatchCount) {
                maxMatchCount = matchCount;
                selected      = currentindex;
            }
        }

        if (selected != null) {
            return selected;
        }

        switch (tableType) {

            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.SYSTEM_TABLE :
            case TableBase.VIEW_TABLE :
            case TableBase.TEMP_TABLE : {
                selected = createIndexForColumns(set.toArray());
            }
        }

        return selected;
    }

    /**
     *  Return the list of file pointers to root nodes for this table's
     *  indexes.
     */
    public final int[] getIndexRootsArray() {

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);
        int[] roots = new int[getIndexCount()];

        for (int i = 0; i < getIndexCount(); i++) {
            CachedObject accessor = store.getAccessor(indexList[i]);

            roots[i] = accessor == null ? -1
                                        : accessor.getPos();
        }

        return roots;
    }

    /**
     * Returns the string consisting of file pointers to roots of indexes
     * plus the next identity value (hidden or user defined). This is used
     * with CACHED tables.
     */
    String getIndexRoots() {

        String       roots = StringUtil.getList(getIndexRootsArray(), " ", "");
        StringBuffer s     = new StringBuffer(roots);

/*
        s.append(' ');
        s.append(identitySequence.peek());
*/
        return s.toString();
    }

    /**
     *  Sets the index roots of a cached/text table to specified file
     *  pointers. If a
     *  file pointer is -1 then the particular index root is null. A null index
     *  root signifies an empty table. Accordingly, all index roots should be
     *  null or all should be a valid file pointer/reference.
     */
    public void setIndexRoots(int[] roots) {

        if (!isCached) {
            throw Error.error(ErrorCode.X_42501, tableName.name);
        }

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);

        for (int i = 0; i < getIndexCount(); i++) {
            store.setAccessor(indexList[i], roots[i]);
        }
    }

    /**
     *  Sets the index roots and next identity.
     */
    void setIndexRoots(Session session, String s) {

        if (!isCached) {
            throw Error.error(ErrorCode.X_42501, tableName.name);
        }

        ParserDQL p     = new ParserDQL(session, new Scanner(s));
        int[]     roots = new int[getIndexCount()];

        p.read();

        for (int i = 0; i < getIndexCount(); i++) {
            int v = p.readInteger();

            roots[i] = v;
        }

        setIndexRoots(roots);
    }

    /**
     *  Performs Table structure modification and changes to the index nodes
     *  to remove a given index from a MEMORY or TEXT table. Not for PK index.
     *
     */
    public void dropIndex(Session session, String indexname) {

        // find the array index for indexname and remove
        int todrop = getIndexIndex(indexname);

        indexList = (Index[]) ArrayUtil.toAdjustedArray(indexList, null,
                todrop, -1);

        for (int i = 0; i < indexList.length; i++) {
            indexList[i].setPosition(i);
        }

        setBestRowIdentifiers();

        if (store != null) {
            store.resetAccessorKeys(indexList);
        }
    }

    /**
     * Moves the data from table to table.
     * The colindex argument is the index of the column that was
     * added or removed. The adjust argument is {-1 | 0 | +1}
     */
    void moveData(Session session, Table from, int colindex, int adjust) {

        Object       colvalue = null;
        ColumnSchema column   = null;

        if (adjust >= 0 && colindex != -1) {
            column   = getColumn(colindex);
            colvalue = column.getDefaultValue(session);
        }

        PersistentStore store = session.sessionData.getRowStore(this);

        try {
            RowIterator it = from.rowIterator(session);

            while (it.hasNext()) {
                Row      row  = it.getNextRow();
                Object[] o    = row.getData();
                Object[] data = getEmptyRowData();

                if (adjust == 0 && colindex != -1) {
                    colvalue = column.getDataType().convertToType(session,
                            o[colindex],
                            from.getColumn(colindex).getDataType());
                }

                ArrayUtil.copyAdjustArray(o, data, colvalue, colindex, adjust);
                systemSetIdentityColumn(session, data);
                enforceRowConstraints(session, data);

                // get object without RowAction
                Row newrow = (Row) store.getNewCachedObject(null, data);

                if (row.rowAction != null) {
                    newrow.rowAction =
                        row.rowAction.duplicate(newrow.getPos());
                }

                store.indexRow(null, newrow);
            }
            this.timeToLive = from.timeToLive;
        } catch (Throwable t) {
            store.release();

            if (t instanceof HsqlException) {
                throw (HsqlException) t;
            }

            throw new HsqlException(t, "", 0);
        }
    }

    //

    /**
     *  Mid level method for inserting rows. Performs constraint checks and
     *  fires row level triggers.
     */
    void insertRow(Session session, PersistentStore store, Object[] data) {

        setIdentityColumn(session, data);

        if (triggerLists[Trigger.INSERT_BEFORE].length != 0) {
            fireBeforeTriggers(session, Trigger.INSERT_BEFORE, null, data,
                               null);
        }

        if (isView) {
            return;
        }

        checkRowDataInsert(session, data);
        insertNoCheck(session, store, data);
    }

    /**
     * Multi-row insert method. Used for CREATE TABLE AS ... queries.
     */
    void insertIntoTable(Session session, Result result) {

        PersistentStore store = session.sessionData.getRowStore(this);
        RowSetNavigator nav   = result.initialiseNavigator();

        while (nav.hasNext()) {
            Object[] data = nav.getNext();
            Object[] newData =
                (Object[]) ArrayUtil.resizeArrayIfDifferent(data,
                    getColumnCount());

            insertData(store, newData);
        }
    }

    /**
     *  Low level method for row insert.
     *  UNIQUE or PRIMARY constraints are enforced by attempting to
     *  add the row to the indexes.
     */
    private Row insertNoCheck(Session session, PersistentStore store,
                              Object[] data) {

        Row row = (Row) store.getNewCachedObject(session, data);

        store.indexRow(session, row);
        session.addInsertAction(this, row);

        return row;
    }

    /**
     *
     */
    public void insertNoCheckFromLog(Session session, Object[] data) {

        systemUpdateIdentityValue(data);

        PersistentStore store = session.sessionData.getRowStore(this);
        Row             row   = (Row) store.getNewCachedObject(session, data);

        store.indexRow(session, row);
        session.addInsertAction(this, row);
    }

    /**
     * Used for system table inserts. No checks. No identity
     * columns.
     */
    public int insertSys(PersistentStore store, Result ins) {

        RowSetNavigator nav   = ins.getNavigator();
        int             count = 0;

        while (nav.hasNext()) {
            insertSys(store, nav.getNext());

            count++;
        }

        return count;
    }

    /**
     * Used for subquery inserts. No checks. No identity
     * columns.
     */
    void insertResult(PersistentStore store, Result ins) {

        RowSetNavigator nav = ins.initialiseNavigator();

        while (nav.hasNext()) {
            Object[] data = nav.getNext();
            Object[] newData =
                (Object[]) ArrayUtil.resizeArrayIfDifferent(data,
                    getColumnCount());

            insertData(store, newData);
        }
    }

    /**
     * Not for general use.
     * Used by ScriptReader to unconditionally insert a row into
     * the table when the .script file is read.
     */
    public void insertFromScript(PersistentStore store, Object[] data) {
        systemUpdateIdentityValue(data);
        insertData(store, data);
    }

    /**
     * For system operations outside transaction constrol
     */
    public void insertData(PersistentStore store, Object[] data) {

        Row row = (Row) store.getNewCachedObject(null, data);

        store.indexRow(null, row);
    }

    /**
     * Used by the system tables only
     */
    public void insertSys(PersistentStore store, Object[] data) {

        Row row = (Row) store.getNewCachedObject(null, data);

        store.indexRow(null, row);
    }

    /**
     * If there is an identity column in the table, sets
     * the value and/or adjusts the identiy value for the table.
     */
    protected void setIdentityColumn(Session session, Object[] data) {

        if (identityColumn != -1) {
            Number id = (Number) data[identityColumn];

            if (id == null) {
                id = (Number) identitySequence.getValueObject();
                data[identityColumn] = id;
            } else {
                identitySequence.userUpdate(id.longValue());
            }

            if (session != null) {
                session.setLastIdentity(id);
            }
        }
    }

    protected void systemSetIdentityColumn(Session session, Object[] data) {

        if (identityColumn != -1) {
            Number id = (Number) data[identityColumn];

            if (id == null) {
                id = (Number) identitySequence.getValueObject();
                data[identityColumn] = id;
            } else {
                identitySequence.userUpdate(id.longValue());
            }
        }
    }

    /**
     * If there is an identity column in the table, sets
     * the max identity value.
     */
    protected void systemUpdateIdentityValue(Object[] data) {

        if (identityColumn != -1) {
            Number id = (Number) data[identityColumn];

            if (id != null) {
                identitySequence.systemUpdate(id.longValue());
            }
        }
    }

    /**
     *  Delete method for referential triggered actions.
     */
    void deleteRowAsTriggeredAction(Session session, Row row) {
        deleteNoCheck(session, row);
    }

    /**
     *  Mid level row delete method. Fires triggers but no integrity
     *  constraint checks.
     */
    void deleteNoRefCheck(Session session, Row row) {

        Object[] data = row.getData();

        fireBeforeTriggers(session, Trigger.DELETE_BEFORE, data, null, null);

        if (isView) {
            return;
        }

        deleteNoCheck(session, row);
    }

    /**
     * Low level row delete method. Removes the row from the indexes and
     * from the Cache.
     */
    private void deleteNoCheck(Session session, Row row) {

        if (row.isDeleted(session)) {
            return;
        }

        session.addDeleteAction(this, row);
    }

    /**
     * For log statements. Delete a single row.
     */
    public void deleteNoCheckFromLog(Session session, Object[] data) {

        Row             row   = null;
        PersistentStore store = session.sessionData.getRowStore(this);

        if (hasPrimaryKey()) {
            RowIterator it = getPrimaryIndex().findFirstRow(session, store,
                data, primaryKeyColsSequence);

            row = it.getNextRow();
        } else if (bestIndex == null) {
            RowIterator it = rowIterator(session);

            while (true) {
                row = it.getNextRow();

                if (row == null) {
                    break;
                }

                if (IndexAVL.compareRows(
                        row.getData(), data, defaultColumnMap,
                        colTypes) == 0) {
                    break;
                }
            }
        } else {
            RowIterator it = bestIndex.findFirstRow(session, store, data);

            while (true) {
                row = it.getNextRow();

                if (row == null) {
                    break;
                }

                Object[] rowdata = row.getData();

                // reached end of range
                if (bestIndex.compareRowNonUnique(
                        data, bestIndex.getColumns(), rowdata) != 0) {
                    row = null;

                    break;
                }

                if (IndexAVL.compareRows(
                        rowdata, data, defaultColumnMap, colTypes) == 0) {
                    break;
                }
            }
        }

        if (row == null) {
            return;
        }

        deleteNoCheck(session, row);
    }

    void updateRowSet(Session session, HashMappedList rowSet, int[] cols,
                      boolean isTriggeredSet) {

        PersistentStore store  = session.sessionData.getRowStore(this);

        for (int i = 0; i < rowSet.size(); i++) {
            Row row = (Row) rowSet.getKey(i);

            if (row.isDeleted(session)) {
                if (isTriggeredSet) {
                    rowSet.remove(i);

                    i--;

                    continue;
                } else {
                    throw Error.error(ErrorCode.X_27000);
                }
            }
        }

        for (int i = 0; i < rowSet.size(); i++) {
            Row      row  = (Row) rowSet.getKey(i);
            Object[] data = (Object[]) rowSet.get(i);

            checkRowData(session, data, cols);    // todo - see if check is necessary ??
            deleteNoCheck(session, row);
        }

        for (int i = 0; i < rowSet.size(); i++) {
            Object[] data = (Object[]) rowSet.get(i);

            insertNoCheck(session, store, data);
        }
    }

    void addLobUsageCount(Session session, Object[] data) {

        if (!hasLobColumn) {
            return;
        }

        for (int j = 0; j < columnCount; j++) {
            if (colTypes[j].isLobType()) {
                Object value = data[j];

                if (value == null) {
                    continue;
                }

                session.sessionData.addLobUsageCount(
                    ((LobData) value).getId());
            }
        }
    }

    void removeLobUsageCount(Session session, Object[] data) {

        if (!hasLobColumn) {
            return;
        }

        for (int j = 0; j < columnCount; j++) {
            if (colTypes[j].isLobType()) {
                Object value = data[j];

                if (value == null) {
                    continue;
                }

                session.sessionData.removeUsageCount(
                    ((LobData) value).getId());
            }
        }
    }

    void checkRowDataInsert(Session session, Object[] data) {

        enforceRowConstraints(session, data);

        if (database.isReferentialIntegrity()) {
            for (int i = 0, size = constraintList.length; i < size; i++) {
                constraintList[i].checkInsert(session, this, data);
            }
        }
    }

    void checkRowData(Session session, Object[] data, int[] cols) {

        enforceRowConstraints(session, data);

        for (int i = 0; i < checkConstraints.length; i++) {
            checkConstraints[i].checkCheckConstraint(session, this, data);
        }
    }

    @Override
    public void clearAllData(Session session) {

        super.clearAllData(session);

        if (identitySequence != null) {
            identitySequence.reset();
        }
    }

    @Override
    public void clearAllData(PersistentStore store) {

        super.clearAllData(store);

        if (identitySequence != null) {
            identitySequence.reset();
        }
    }

    /************************* Volt DB Extensions *************************/

    /**
     * Returns the UNIQUE constraint with the given expression signature -- a VoltDB extension.
     */
    Constraint getUniqueConstraintForExprs(Expression[] indexExprs) {
        for (int i = 0, size = constraintList.length; i < size; i++) {
            Constraint exprc = constraintList[i];

            if (exprc.isUniqueWithExprs(indexExprs)) {
                return exprc;
            }
        }
        return null;
    }

    /** Index expressions as exported to VoltDB are "column name based" not "column index based",
     *  so they are not thrown off by column re-numbering.
     *  VoltDB is responsible for re-resolving the names to the moved columns (changed column index numbers).
     *  This stubbed pass-through method is here in case that someday changes in a way that would require
     *  processing of the expression trees.
     */
    private Expression[] adjustExprs(Expression[] exprArr, int colIndex, int adjust) {
        return exprArr;
    }

    /** Index expressions as exported to VoltDB are "column name based" not "column index based",
     *  so they are not thrown off by column re-numbering.
     *  VoltDB is responsible for re-resolving the names to the moved columns (changed column index numbers).
     *  This stubbed pass-through method is here in case that someday changes in a way that would require
     *  processing of the expression trees.
     */
    private Expression adjustExpr(Expression expr, int colIndex, int adjust) {
        return expr;
    }

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    VoltXMLElement voltGetTableXML(Session session)
            throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        VoltXMLElement table = new VoltXMLElement("table");

        // add table metadata
        String tableName = getName().name;
        table.attributes.put("name", tableName);

        // read all the columns
        VoltXMLElement columns = new VoltXMLElement("columns");
        columns.attributes.put("name", "columns");
        table.children.add(columns);
        int[] columnIndices = getColumnMap();
        for (int i : columnIndices) {
            ColumnSchema column = getColumn(i);
            VoltXMLElement colChild = column.voltGetColumnXML(session);
            colChild.attributes.put("index", Integer.toString(i));
            columns.children.add(colChild);
            assert(colChild != null);
        }

        // read all the constraints
        VoltXMLElement constraints = new VoltXMLElement("constraints");
        constraints.attributes.put("name", "constraints");
        Map<String, VoltXMLElement> indexConstraintMap = new HashMap<>();
        for (Constraint constraint : getConstraints()) {
            VoltXMLElement constraintChild = constraint.voltGetConstraintXML();
            constraints.children.add(constraintChild);
            if (constraintChild.attributes.containsKey("index")) {
                indexConstraintMap.put(constraintChild.attributes.get("index"), constraintChild);
            }
        }

        // read all the indexes
        VoltXMLElement indexes = new VoltXMLElement("indexes");
        indexes.attributes.put("name", "indexes");
        for (Index index : indexList) {
            VoltXMLElement indexChild = index.voltGetIndexXML(session, tableName, indexConstraintMap);
            indexes.children.add(indexChild);
            assert(indexChild != null);
        }

        // Indexes must come before constraints when converting to the catalog.
        table.children.add(indexes);
        table.children.add(constraints);

        if (timeToLive != null) {
            VoltXMLElement ttl = new VoltXMLElement(TimeToLiveVoltDB.TTL_NAME);
            ttl.attributes.put("name", TimeToLiveVoltDB.TTL_NAME);
            ttl.attributes.put("value", Integer.toString(timeToLive.ttlValue));
            ttl.attributes.put("unit",  timeToLive.ttlUnit);
            ttl.attributes.put("column", timeToLive.ttlColumnName.name);
            ttl.attributes.put("maxFrequency", Integer.toString(timeToLive.maxFrequency));
            ttl.attributes.put("batchSize", Integer.toString(timeToLive.batchSize));
            table.children.add(ttl);
        }
        assert(indexConstraintMap.isEmpty());

        if (persistentExport != null) {
            VoltXMLElement pe = new VoltXMLElement(PersistentExport.PERSISTENT_EXPORT);
            pe.attributes.put("target", persistentExport.target);
            pe.attributes.put("triggers", String.join(",", persistentExport.triggers));
            pe.attributes.put("isTopic", Boolean.toString(persistentExport.isTopic));
            table.children.add(pe);
        }
        return table;
    }

    // A VoltDB extension to support indexed expressions
    public final Index createAndAddExprIndexStructure(
            HsqlName name, int[] columns, Expression[] indexExprs, boolean unique, boolean migrating, boolean constraint) {

        Index newExprIndex = createIndexStructure(name, columns, null, null, unique, migrating, constraint, false);
        newExprIndex = newExprIndex.withExpressions(indexExprs);
        addIndex(newExprIndex);
        return newExprIndex;
    } /* createAndAddExprIndexStructure */

    // A VoltDB extension to support TTL
    public void addTTL(int ttlValue, String ttlUnit, String ttlColumn, int batchSize,
            int maxFrequency) {
        dropTTL();
        timeToLive = new TimeToLiveVoltDB(ttlValue, ttlUnit, getColumn(findColumn(ttlColumn)),
                batchSize, maxFrequency);
    }

    public TimeToLiveVoltDB getTTL() {
        return timeToLive;
    }

    public void alterTTL(int ttlValue, String ttlUnit, String ttlColumn,
            int batchSize, int maxFrequency) {
        addTTL(ttlValue, ttlUnit, ttlColumn, batchSize, maxFrequency);
    }

    public void dropTTL() {
        timeToLive = null;
    }

    public void addPersistentExport(String target, List<String> triggers, boolean isTopic) {
        persistentExport = new PersistentExport(target, triggers, isTopic);
    }

    public PersistentExport getPersistentExport() {
        return persistentExport;
    }

    public void dropPersistentExport() {
        persistentExport = null;
    }
    // End of VoltDB extension

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return super.toString() + ":" + getName().name;
    }

    public boolean isStream() {
        return isStream;
    }

    public void setStream(boolean isStream) {
        this.isStream = isStream;
    }

    public boolean hasMigrationTarget() {
        return hasMigrationTarget;
    }

    public void setHasMigrationTarget(boolean hasMigrationTarget) {
        this.hasMigrationTarget = hasMigrationTarget;
    }
}
