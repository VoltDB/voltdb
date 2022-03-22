/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.RangeVariable.RangeIteratorBase;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rights.Grantee;

// fredt@users 20020225 - patch 1.7.0 by boucherb@users - named constraints
// fredt@users 20020320 - doc 1.7.0 - update
// tony_lai@users 20020820 - patch 595156 - violation of Integrity constraint name

/**
 * Implementation of a table constraint with references to the indexes used
 * by the constraint.<p>
 *
 * Partly based on Hypersonic code.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public final class Constraint implements SchemaObject {

    /*
     SQL CLI codes

     Referential Constraint 0 CASCADE
     Referential Constraint 1 RESTRICT
     Referential Constraint 2 SET NULL
     Referential Constraint 3 NO ACTION
     Referential Constraint 4 SET DEFAULT
     */
    public static final int CASCADE        = 0,
                            RESTRICT       = 1,
                            SET_NULL       = 2,
                            NO_ACTION      = 3,
                            SET_DEFAULT    = 4,
                            INIT_DEFERRED  = 5,
                            INIT_IMMEDIATE = 6,
                            NOT_DEFERRABLE = 7;
    public static final int FOREIGN_KEY    = 0,
                            MAIN           = 1,
                            UNIQUE         = 2,
                            CHECK          = 3,
                            PRIMARY_KEY    = 4,
                            TEMP           = 5,
    // A VoltDB extension to support LIMIT PARTITION ROWS syntax (DELETED)
    //                      LIMIT          = 6,
    // A VoltDB extension to support CREATE MIGRATING INDEX syntax
                            MIGRATING      = 7;
    // End of VoltDB extension
    ConstraintCore          core;
    private HsqlName        name;
    private boolean         isAutogeneratedName = true;
    int                     constType;
    boolean                 isForward;

    //
    Expression      check;
    String          checkStatement;
    private boolean isNotNull;
    int             notNullColumnIndex;
    RangeVariable   rangeVariable;
    OrderedHashSet  schemaObjectNames;

    // for temp constraints only
    OrderedHashSet mainColSet;
    OrderedHashSet refColSet;

    //
    final public static Constraint[] emptyArray = new Constraint[]{};

    /**
     *  Constructor declaration for PK and UNIQUE
     */
    public Constraint(HsqlName name, boolean isAutogeneratedName, Table t, Index index, int type) {

        core           = new ConstraintCore();
        this.name      = name;
        trimName(this.name);
        this.isAutogeneratedName = isAutogeneratedName;
        constType      = type;
        core.mainTable = t;
        core.mainIndex = index;
        core.mainCols  = index.getColumns();
    }

    /**
     *  Constructor for main constraints (foreign key references in PK table)
     */
    public Constraint(HsqlName name, Constraint fkconstraint) {

        this.name = name;
        trimName(this.name);
        constType = MAIN;
        core      = fkconstraint.core;
    }

    Constraint duplicate() {

        Constraint copy = new Constraint();

        copy.core      = core.duplicate();
        copy.name      = name;
        copy.isAutogeneratedName = isAutogeneratedName;
        copy.constType = constType;
        copy.isForward = isForward;

        //
        copy.check              = check;
        copy.isNotNull          = isNotNull;
        copy.notNullColumnIndex = notNullColumnIndex;
        copy.rangeVariable      = rangeVariable;
        copy.schemaObjectNames  = schemaObjectNames;
        // A VoltDB extension to support the assume unique attribute
        copy.assumeUnique       = assumeUnique;
        copy.migrating          = migrating;
        // End of VoltDB extension
        // A VoltDB extension to support indexed expressions
        copy.indexExprs         = indexExprs;
        // End of VoltDB extension

        return copy;
    }

    /**
     * General constructor for foreign key constraints.
     *
     * @param name name of constraint
     * @param refCols list of referencing columns
     * @param mainTableName referenced table
     * @param mainCols list of referenced columns
     * @param type constraint type
     * @param deleteAction triggered action on delete
     * @param updateAction triggered action on update
     *
     */
    public Constraint(HsqlName name, HsqlName refTableName,
                      OrderedHashSet refCols, HsqlName mainTableName,
                      OrderedHashSet mainCols, int type, int deleteAction,
                      int updateAction, int matchType) {

        core               = new ConstraintCore();
        this.name          = name;
        trimName(this.name);
        constType          = type;
        mainColSet         = mainCols;
        core.refTableName  = refTableName;
        core.mainTableName = mainTableName;
        refColSet          = refCols;
        core.deleteAction  = deleteAction;
        core.updateAction  = updateAction;
        core.matchType     = matchType;
    }

    public Constraint(HsqlName name, boolean isAutogeneratedName, OrderedHashSet mainCols, int type) {

        core       = new ConstraintCore();
        this.name  = name;
        trimName(this.name);
        this.isAutogeneratedName = isAutogeneratedName;
        constType  = type;
        mainColSet = mainCols;
    }

    /**
     *  ignore prefix VOLTDB_AUTOGEN_ since it will be automatically added by compiler
     */
    private void trimName(HsqlName name) {
        if (name != null) {
            String indexName = name.name;
            if (indexName.startsWith(HSQLInterface.AUTO_GEN_PREFIX) && indexName.length() > HSQLInterface.AUTO_GEN_PREFIX.length()) {
                indexName = indexName.substring(HSQLInterface.AUTO_GEN_PREFIX.length());
                name.name = indexName;
            }
        }
    }

    void setColumnsIndexes(Table table) {

        if (constType == Constraint.FOREIGN_KEY) {
            if (mainColSet == null) {
                core.mainCols = core.mainTable.getPrimaryKey();

                if (core.mainCols == null) {
                    throw Error.error(ErrorCode.X_42581);
                }
            } else if (core.mainCols == null) {
                core.mainCols = core.mainTable.getColumnIndexes(mainColSet);
            }

            if (core.refCols == null) {
                core.refCols = table.getColumnIndexes(refColSet);
            }
        } else if (mainColSet != null) {
            core.mainCols = table.getColumnIndexes(mainColSet);
        }
    }

    private Constraint() {}

    @Override
    public int getType() {
        return SchemaObject.CONSTRAINT;
    }

    /**
     * Returns the HsqlName.
     */
    @Override
    public HsqlName getName() {
        return name;
    }

    @Override
    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    @Override
    public HsqlName getSchemaName() {
        return name.schema;
    }

    @Override
    public Grantee getOwner() {
        return name.schema.owner;
    }

    @Override
    public OrderedHashSet getReferences() {

        switch (constType) {

            case Constraint.CHECK :
                return schemaObjectNames;

            case Constraint.FOREIGN_KEY :
                OrderedHashSet set = new OrderedHashSet();

                set.add(core.uniqueName);

                return set;
        }

        return null;
    }

    @Override
    public OrderedHashSet getComponents() {
        return null;
    }

    @Override
    public void compile(Session session) {}

    @Override
    public String getSQL() {

        StringBuffer sb = new StringBuffer();
        int[] col;

        switch (getConstraintType()) {

            case Constraint.PRIMARY_KEY :
                if (getMainColumns().length > 1
                        || (getMainColumns().length == 1
                            && !getName().isReservedName())) {
                    if (!getName().isReservedName()) {
                        sb.append(Tokens.T_CONSTRAINT).append(' ');
                        sb.append(getName().statementName).append(' ');
                    }

                    sb.append(Tokens.T_PRIMARY).append(' ').append(
                        Tokens.T_KEY);
                    getColumnList(getMain(), getMainColumns(),
                                  getMainColumns().length, sb);
                }
                break;

            case Constraint.UNIQUE :
                if (!getName().isReservedName()) {
                    sb.append(Tokens.T_CONSTRAINT).append(' ');
                    sb.append(getName().statementName);
                    sb.append(' ');
                }

                sb.append(Tokens.T_UNIQUE);

                // A VoltDB extension to support indexed expressions
                if (indexExprs != null) {
                    return getExprList(sb);
                }
                // End of VoltDB extension
                col = getMainColumns();

                getColumnList(getMain(), col, col.length, sb);
                break;

            case Constraint.MIGRATING:
                if (!getName().isReservedName()) {
                    sb.append(Tokens.T_CONSTRAINT).append(' ');
                    sb.append(getName().statementName);
                    sb.append(' ');
                }
                sb.append(Tokens.T_MIGRATING);
                col = getMainColumns();
                getColumnList(getMain(), col, col.length, sb);
                break;

            case Constraint.FOREIGN_KEY :
                if (isForward) {
                    sb.append(Tokens.T_ALTER).append(' ').append(
                        Tokens.T_TABLE).append(' ');
                    sb.append(
                        getRef().getName().getSchemaQualifiedStatementName());
                    sb.append(' ').append(Tokens.T_ADD).append(' ');
                    getFKStatement(sb);
                } else {
                    getFKStatement(sb);
                }
                break;

            case Constraint.CHECK :
                if (isNotNull()) {
                    break;
                }

                if (!getName().isReservedName()) {
                    sb.append(Tokens.T_CONSTRAINT).append(' ');
                    sb.append(getName().statementName).append(' ');
                }

                sb.append(Tokens.T_CHECK).append('(');
                sb.append(check.getSQL());
                sb.append(')');

                // should not throw as it is already tested OK
                break;
        }

        return sb.toString();
    }

    /**
     * Generates the foreign key declaration for a given Constraint object.
     */
    private void getFKStatement(StringBuffer a) {

        if (!getName().isReservedName()) {
            a.append(Tokens.T_CONSTRAINT).append(' ');
            a.append(getName().statementName);
            a.append(' ');
        }

        a.append(Tokens.T_FOREIGN).append(' ').append(Tokens.T_KEY);

        int[] col = getRefColumns();

        getColumnList(getRef(), col, col.length, a);
        a.append(' ').append(Tokens.T_REFERENCES).append(' ');
        a.append(getMain().getName().getSchemaQualifiedStatementName());

        col = getMainColumns();

        getColumnList(getMain(), col, col.length, a);

        if (getDeleteAction() != Constraint.NO_ACTION) {
            a.append(' ').append(Tokens.T_ON).append(' ').append(
                Tokens.T_DELETE).append(' ');
            a.append(getDeleteActionString());
        }

        if (getUpdateAction() != Constraint.NO_ACTION) {
            a.append(' ').append(Tokens.T_ON).append(' ').append(
                Tokens.T_UPDATE).append(' ');
            a.append(getUpdateActionString());
        }
    }

    /**
     * Generates the column definitions for a table.
     */
    private static void getColumnList(Table t, int[] col, int len,
                                      StringBuffer a) {

        a.append('(');

        for (int i = 0; i < len; i++) {
            a.append(t.getColumn(col[i]).getName().statementName);

            if (i < len - 1) {
                a.append(',');
            }
        }

        a.append(')');
    }

    public HsqlName getMainTableName() {
        return core.mainTableName;
    }

    public HsqlName getMainName() {
        return core.mainName;
    }

    public HsqlName getRefName() {
        return core.refName;
    }

    public HsqlName getUniqueName() {
        return core.uniqueName;
    }

    /**
     *  Returns the type of constraint
     */
    public int getConstraintType() {
        return constType;
    }

    /**
     *  Returns the main table
     */
    public Table getMain() {
        return core.mainTable;
    }

    /**
     *  Returns the main index
     */
    Index getMainIndex() {
        return core.mainIndex;
    }

    /**
     *  Returns the reference table
     */
    public Table getRef() {
        return core.refTable;
    }

    /**
     *  Returns the reference index
     */
    Index getRefIndex() {
        return core.refIndex;
    }

    /**
     * Returns the foreign key action rule.
     */
    private static String getActionString(int action) {

        switch (action) {

            case Constraint.RESTRICT :
                return Tokens.T_RESTRICT;

            case Constraint.CASCADE :
                return Tokens.T_CASCADE;

            case Constraint.SET_DEFAULT :
                return Tokens.T_SET + ' ' + Tokens.T_DEFAULT;

            case Constraint.SET_NULL :
                return Tokens.T_SET + ' ' + Tokens.T_NULL;

            default :
                return Tokens.T_NO + ' ' + Tokens.T_ACTION;
        }
    }

    /**
     *  The ON DELETE triggered action of (foreign key) constraint
     */
    public int getDeleteAction() {
        return core.deleteAction;
    }

    public String getDeleteActionString() {
        return getActionString(core.deleteAction);
    }

    /**
     *  The ON UPDATE triggered action of (foreign key) constraint
     */
    public int getUpdateAction() {
        return core.updateAction;
    }

    public String getUpdateActionString() {
        return getActionString(core.updateAction);
    }

    public boolean hasTriggeredAction() {

        if (constType == Constraint.FOREIGN_KEY) {
            switch (core.deleteAction) {

                case Constraint.CASCADE :
                case Constraint.SET_DEFAULT :
                case Constraint.SET_NULL :
                    return true;
            }

            switch (core.updateAction) {

                case Constraint.CASCADE :
                case Constraint.SET_DEFAULT :
                case Constraint.SET_NULL :
                    return true;
            }
        }

        return false;
    }

    public int getDeferability() {
        return NOT_DEFERRABLE;
    }

    /**
     *  Returns the main table column index array
     */
    public int[] getMainColumns() {
        return core.mainCols;
    }

    /**
     *  Returns the reference table column index array
     */
    public int[] getRefColumns() {
        return core.refCols;
    }

    /**
     * Returns the SQL for the expression in CHECK clause
     */
    public String getCheckSQL() {
        return check.getSQL();
    }

    /**
     * Returns true if the expression in CHECK is a simple IS NOT NULL
     */
    public boolean isNotNull() {
        return isNotNull;
    }

    boolean hasColumnOnly(int colIndex) {

        switch (constType) {

            case CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) == 1;

            case PRIMARY_KEY :
            case UNIQUE :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex;

            case MAIN :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex
                       && core.mainTable == core.refTable;

            case FOREIGN_KEY :
                return core.refCols.length == 1 && core.refCols[0] == colIndex
                       && core.mainTable == core.refTable;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    boolean hasColumnPlus(int colIndex) {

        switch (constType) {

            case CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) > 1;

            case PRIMARY_KEY :
            case UNIQUE :
                return core.mainCols.length != 1
                       && ArrayUtil.find(core.mainCols, colIndex) != -1;

            case MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable != core.refTable);

            case FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable == core.refTable);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    boolean hasColumn(int colIndex) {

        switch (constType) {

            case CHECK :
                return rangeVariable.usedColumns[colIndex];

            case PRIMARY_KEY :
            case UNIQUE :
            case MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1;

            case FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

// fredt@users 20020225 - patch 1.7.0 by fredt - duplicate constraints

    /**
     * Compares this with another constraint column set. This is used only for
     * UNIQUE constraints.
     */
    boolean isUniqueWithColumns(int[] cols) {

        if (constType != UNIQUE || core.mainCols.length != cols.length) {
            return false;
        }

        return ArrayUtil.haveEqualSets(core.mainCols, cols, cols.length);
    }

    /**
     * Compares this with another constraint column set. This implementation
     * only checks FOREIGN KEY constraints.
     */
    boolean isEquivalent(Table mainTable, int[] mainCols, Table refTable,
                         int[] refCols) {

        if (constType != Constraint.MAIN
                && constType != Constraint.FOREIGN_KEY) {
            return false;
        }

        if (mainTable != core.mainTable || refTable != core.refTable) {
            return false;
        }

        return ArrayUtil.areEqualSets(core.mainCols, mainCols)
               && ArrayUtil.areEqualSets(core.refCols, refCols);
    }

    /**
     * Used to update constrains to reflect structural changes in a table. Prior
     * checks must ensure that this method does not throw.
     *
     * @param session Session
     * @param oldTable reference to the old version of the table
     * @param newTable referenct to the new version of the table
     * @param colIndex index at which table column is added or removed
     * @param adjust -1, 0, +1 to indicate if column is added or removed
     * @
     */
    void updateTable(Session session, Table oldTable, Table newTable,
                     int colIndex, int adjust) {

        if (oldTable == core.mainTable) {
            core.mainTable = newTable;

            if (core.mainIndex != null) {
                core.mainIndex =
                    core.mainTable.getIndex(core.mainIndex.getName().name);
                core.mainCols = ArrayUtil.toAdjustedColumnArray(core.mainCols,
                        colIndex, adjust);
            }
        }

        if (oldTable == core.refTable) {
            core.refTable = newTable;

            if (core.refIndex != null) {
                core.refIndex =
                    core.refTable.getIndex(core.refIndex.getName().name);
                core.refCols = ArrayUtil.toAdjustedColumnArray(core.refCols,
                        colIndex, adjust);
            }
        }

        // CHECK
        if (constType == CHECK) {
            recompile(session, newTable);
        }
    }

    /**
     * Checks for foreign key or check constraint violation when
     * inserting a row into the child table.
     */
    void checkInsert(Session session, Table table, Object[] row) {

        switch (constType) {

            case CHECK :
                if (!isNotNull) {
                    checkCheckConstraint(session, table, row);
                }

                return;

            case FOREIGN_KEY :
                PersistentStore store =
                    session.sessionData.getRowStore(core.mainTable);

                if (ArrayUtil.hasNull(row, core.refCols)) {
                    if (core.matchType == OpTypes.MATCH_SIMPLE) {
                        return;
                    }

                    if (core.refCols.length == 1) {
                        return;
                    }

                    if (ArrayUtil.hasAllNull(row, core.refCols)) {
                        return;
                    }

                    // core.matchType == OpTypes.MATCH_FULL
                } else if (core.mainIndex.exists(session, store, row,
                                                 core.refCols)) {
                    return;
                } else if (core.mainTable == core.refTable) {

                    // special case: self referencing table and self referencing row
                    int compare = core.mainIndex.compareRowNonUnique(row,
                        core.refCols, row);

                    if (compare == 0) {
                        return;
                    }
                }

                String[] info = new String[] {
                    core.refName.name, core.mainTable.getName().name
                };

                throw Error.error(ErrorCode.X_23502, ErrorCode.CONSTRAINT,
                                  info);
        }
    }

    /*
     * Tests a row against this CHECK constraint.
     */
    void checkCheckConstraint(Session session, Table table, Object[] data) {

/*
        if (session.compiledStatementExecutor.rangeIterators[1] == null) {
            session.compiledStatementExecutor.rangeIterators[1] =
                rangeVariable.getIterator(session);
        }
*/
        RangeIteratorBase it =
            (RangeIteratorBase) session.sessionContext.getCheckIterator();

        if (it == null) {
            it = rangeVariable.getIterator(session);

            session.sessionContext.setCheckIterator(it);
        }

        it.currentData = data;

        boolean nomatch = Boolean.FALSE.equals(check.getValue(session));

        it.currentData = null;

        if (nomatch) {
            String[] info = new String[] {
                name.name, table.tableName.name
            };

            throw Error.error(ErrorCode.X_23504, ErrorCode.CONSTRAINT, info);
        }
    }

    void checkCheckConstraint(Session session, Table table, Object data) {

        session.sessionData.currentValue = data;

        boolean nomatch = Boolean.FALSE.equals(check.getValue(session));

        session.sessionData.currentValue = null;

        if (nomatch) {
            if (table == null) {
                throw Error.error(ErrorCode.X_23504, name.name);
            } else {
                String[] info = new String[] {
                    name.name, table.tableName.name
                };

                throw Error.error(ErrorCode.X_23504, ErrorCode.CONSTRAINT,
                                  info);
            }
        }
    }

// fredt@users 20020225 - patch 1.7.0 - cascading deletes

    /**
     * New method to find any referencing row for a foreign key (finds row in
     * child table). If ON DELETE CASCADE is specified for this constraint, then
     * the method finds the first row among the rows of the table ordered by the
     * index and doesn't throw. Without ON DELETE CASCADE, the method attempts
     * to finds any row that exists. If no
     * row is found, null is returned. (fredt@users)
     *
     * @param session Session
     * @param row array of objects for a database row
     * @param delete should we allow 'ON DELETE CASCADE' or 'ON UPDATE CASCADE'
     * @return iterator
     * @
     */
    RowIterator findFkRef(Session session, Object[] row, boolean delete) {

        if (row == null || ArrayUtil.hasNull(row, core.mainCols)) {
            return core.refIndex.emptyIterator();
        }

        PersistentStore store = session.sessionData.getRowStore(core.refTable);

        return core.refIndex.findFirstRow(session, store, row, core.mainCols);
    }

    /**
     * For the candidate table row, finds any referring node in the main table.
     * This is used to check referential integrity when updating a node. We
     * have to make sure that the main table still holds a valid main record.
     * returns true If a valid row is found, false if there are null in the data
     * Otherwise a 'INTEGRITY VIOLATION' Exception gets thrown.
     */
    boolean checkHasMainRef(Session session, Object[] row) {

        if (ArrayUtil.hasNull(row, core.refCols)) {
            return false;
        }

        PersistentStore store =
            session.sessionData.getRowStore(core.mainTable);
        boolean exists = core.mainIndex.exists(session, store, row,
                                               core.refCols);

        if (!exists) {
            String[] info = new String[] {
                core.refName.name, core.mainTable.getName().name
            };

            throw Error.error(ErrorCode.X_23502, ErrorCode.CONSTRAINT, info);
        }

        return exists;
    }

    /**
     * Check used before creating a new foreign key cosntraint, this method
     * checks all rows of a table to ensure they all have a corresponding
     * row in the main table.
     */
    void checkReferencedRows(Session session, Table table, int[] rowColArray) {

        Index           mainIndex = getMainIndex();
        PersistentStore store     = session.sessionData.getRowStore(table);
        RowIterator     it        = table.rowIterator(session);

        while (true) {
            Row row = it.getNextRow();

            if (row == null) {
                break;
            }

            Object[] rowData = row.getData();

            if (ArrayUtil.hasNull(rowData, rowColArray)) {
                if (core.matchType == OpTypes.MATCH_SIMPLE) {
                    continue;
                }
            } else if (mainIndex.exists(session, store, rowData,
                                        rowColArray)) {
                continue;
            }

            if (ArrayUtil.hasAllNull(rowData, rowColArray)) {
                continue;
            }

            String colValues = "";

            for (int i = 0; i < rowColArray.length; i++) {
                Object o = rowData[rowColArray[i]];

                colValues += table.getColumnTypes()[i].convertToString(o);
                colValues += ",";
            }

            String[] info = new String[] {
                getName().name, getMain().getName().name
            };

            throw Error.error(ErrorCode.X_23502, ErrorCode.CONSTRAINT, info);
        }
    }

    public Expression getCheckExpression() {
        return check;
    }

    public OrderedHashSet getCheckColumnExpressions() {

        OrderedHashSet set = new OrderedHashSet();

        Expression.collectAllExpressions(set, check,
                                         Expression.columnExpressionSet,
                                         Expression.emptyExpressionSet);

        return set;
    }

    void recompile(Session session, Table newTable) {

        String    ddl     = check.getSQL();
        Scanner   scanner = new Scanner(ddl);
        ParserDQL parser  = new ParserDQL(session, scanner);

        parser.read();

        parser.isCheckOrTriggerCondition = true;

        Expression condition = parser.XreadBooleanValueExpression();

        check             = condition;
        schemaObjectNames = parser.compileContext.getSchemaObjectNames();

        // this workaround is here to stop LIKE optimisation (for proper scripting)
        QuerySpecification s = Expression.getCheckSelect(session, newTable,
            check);

        rangeVariable = s.rangeVariables[0];

        rangeVariable.setForCheckConstraint();
    }

    void prepareCheckConstraint(Session session, Table table,
                                boolean checkValues) {

        // to ensure no subselects etc. are in condition
        check.checkValidCheckConstraint();

        if (table == null) {
            check.resolveTypes(session, null);
        } else {
            QuerySpecification s = Expression.getCheckSelect(session, table,
                check);
            Result r = s.getResult(session, 1);

            if (r.getNavigator().getSize() != 0) {
                String[] info = new String[] {
                    table.getName().name, ""
                };

                throw Error.error(ErrorCode.X_23504, ErrorCode.CONSTRAINT,
                                  info);
            }

            rangeVariable = s.rangeVariables[0];

            // removes reference to the Index object in range variable
            rangeVariable.setForCheckConstraint();
        }

        if (check.getType() == OpTypes.NOT
                && check.getLeftNode().getType() == OpTypes.IS_NULL
                && check.getLeftNode().getLeftNode().getType()
                   == OpTypes.COLUMN) {
            notNullColumnIndex =
                check.getLeftNode().getLeftNode().getColumnIndex();
            isNotNull = true;
        }
    }

    /************************* Volt DB Extensions *************************/

    // !!!!!!!!
    // NOTE!  IF YOU ARE GOING TO ADD NEW MEMBER FIELDS HERE YOU
    // NEED TO MAKE SURE THEY GET ADDED TO Constraint.duplicate()
    // AT THE TOP OF THE FILE OR ALTER WILL HATE YOU --izzy

    // A VoltDB extension to support indexed expressions
    Expression[] indexExprs;
    // End of VoltDB extension
    // A VoltDB extension to support the assume unique attribute
    boolean assumeUnique = false;
    // A VoltDB extension to support the MIGRATING index attribute
    boolean migrating = false;
    // End of VoltDB extension

    // A VoltDB extension to support indexed expressions
    // and new kinds of constraints
    public Constraint withExpressions(Expression[] exprs) {
        indexExprs = exprs;
        return this;
    }
    // End of VoltDB extension

    /**
     * @return The name of this constraint instance's type.
     */
    String getTypeName() {
        switch (constType) {
            case FOREIGN_KEY: return "FOREIGN_KEY";
            case MAIN: return "MAIN";
            case UNIQUE: return "UNIQUE";
            case CHECK: return isNotNull ? "NOT_NULL" : "CHECK";
            case PRIMARY_KEY: return "PRIMARY_KEY";
        }
        return "UNKNOWN";
    }

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     */
    VoltXMLElement voltGetConstraintXML()
    {
        // Skip "MAIN" constraints, as they are a side effect of foreign key constraints and add no new info.
        if (this.constType == MAIN) {
            return null;
        }

        VoltXMLElement constraint = new VoltXMLElement("constraint");
        // WARNING: the name attribute setting is tentative, subject to reset in the
        // calling function, Table.voltGetTableXML.
        constraint.attributes.put("name", getName().name);
        constraint.attributes.put("nameisauto", getIsAutogeneratedName() ? "true" : "false");
        constraint.attributes.put("constrainttype", getTypeName());
        constraint.attributes.put("assumeunique", assumeUnique ? "true" : "false");
        constraint.attributes.put("migrating", migrating ? "true" : "false");

        // VoltDB implements constraints by defining an index, by annotating metadata (such as for NOT NULL columns),
        // or by issuing a "not supported" warning (such as for foreign keys).
        // Any constraint implemented as an index must have an index name attribute.
        // No other constraint details are currently used by VoltDB.
        if (this.constType != FOREIGN_KEY && core.mainIndex != null) {
            // WARNING: the index attribute setting is tentative, subject to reset in
            // the calling function, Table.voltGetTableXML.
            constraint.attributes.put("index", core.mainIndex.getName().name);
        }
        return constraint;
    }

    // A VoltDB extension to support indexed expressions
    public boolean isUniqueWithExprs(Expression[] indexExprs2) {
        if (constType != UNIQUE || (indexExprs == null) || ! indexExprs.equals(indexExprs2)) {
            return false;
        }
        return true;
    }

    // A VoltDB extension to support indexed expressions
    // Is this for temp constraints only? What's a temp constraint?
    public boolean hasExprs() {
        return indexExprs != null;
    }

    // A VoltDB extension to support indexed expressions
    public String getExprList(StringBuffer sb) {
        String sep = "";
        for(Expression ex : indexExprs) {
            sb.append(sep).append(ex.getSQL());
            sep = ", ";
        }
        return sb.toString();
    }

    public Constraint setAssumeUnique(boolean assumeUnique) {
        this.assumeUnique = assumeUnique;
        return this;
    }

    public Constraint setMigrating(boolean migrating) {
        this.migrating = migrating;
        return this;
    }

    @Override
    public String toString() {
        String str = "CONSTRAINT " + getName().name + " " + getTypeName();
        return str;
    }
    /**********************************************************************/

    public boolean getIsAutogeneratedName() {
        return isAutogeneratedName;
    }
}
