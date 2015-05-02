/* Copyright (c) 2001-2011, The HSQL Development Group
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
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of a table constraint with references to the indexes used
 * by the constraint.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.6.0
 */
public final class Constraint implements SchemaObject {

    ConstraintCore   core;
    private HsqlName name;
    int              constType;
    boolean          isForward;

    //
    Expression      check;
    private boolean isNotNull;
    int             notNullColumnIndex;
    RangeVariable   rangeVariable;

    // for temp constraints only
    OrderedHashSet mainColSet;
    OrderedHashSet refColSet;

    //
    public static final Constraint[] emptyArray = new Constraint[]{};

    private Constraint() {}

    /**
     *  Constructor declaration for PK and UNIQUE
     */
    public Constraint(HsqlName name, Table t, Index index, int type) {

        this.name      = name;
        constType      = type;
        core           = new ConstraintCore();
        core.mainTable = t;
        core.mainIndex = index;
        core.mainCols  = index.getColumns();

        for (int i = 0; i < core.mainCols.length; i++) {
            Type dataType = t.getColumn(core.mainCols[i]).getDataType();

            if (dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }
    }

    public Constraint(HsqlName name, Table table, int[] cols, int type) {

        this.name      = name;
        constType      = type;
        core           = new ConstraintCore();
        core.mainTable = table;
        core.mainCols  = cols;
    }

    /**
     *  Constructor for main constraints (foreign key references in PK table)
     */
    public Constraint(HsqlName name, Constraint fkconstraint) {

        this.name = name;
        constType = SchemaObject.ConstraintTypes.MAIN;
        core      = fkconstraint.core;
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

        this.name          = name;
        constType          = type;
        mainColSet         = mainCols;
        refColSet          = refCols;
        core               = new ConstraintCore();
        core.refTableName  = refTableName;
        core.mainTableName = mainTableName;
        core.deleteAction  = deleteAction;
        core.updateAction  = updateAction;
        core.matchType     = matchType;

        switch (core.deleteAction) {

            case SchemaObject.ReferentialAction.CASCADE :
            case SchemaObject.ReferentialAction.SET_DEFAULT :
            case SchemaObject.ReferentialAction.SET_NULL :
                core.hasDeleteAction = true;
        }

        switch (core.updateAction) {

            case SchemaObject.ReferentialAction.CASCADE :
            case SchemaObject.ReferentialAction.SET_DEFAULT :
            case SchemaObject.ReferentialAction.SET_NULL :
                core.hasUpdateAction = true;
        }
    }

    public Constraint(HsqlName name, OrderedHashSet mainCols, int type) {

        this.name  = name;
        constType  = type;
        mainColSet = mainCols;
        core       = new ConstraintCore();
    }

    public Constraint(HsqlName uniqueName, HsqlName mainName,
                      HsqlName refName, Table mainTable, Table refTable,
                      int[] mainCols, int[] refCols, Index mainIndex,
                      Index refIndex, int deleteAction,
                      int updateAction) throws HsqlException {

        this.name         = refName;
        constType         = SchemaObject.ConstraintTypes.FOREIGN_KEY;
        core              = new ConstraintCore();
        core.uniqueName   = uniqueName;
        core.mainName     = mainName;
        core.refName      = refName;
        core.mainTable    = mainTable;
        core.refTable     = refTable;
        core.mainCols     = mainCols;
        core.refCols      = refCols;
        core.mainIndex    = mainIndex;
        core.refIndex     = refIndex;
        core.deleteAction = deleteAction;
        core.updateAction = updateAction;
    }

    Constraint duplicate() {

        Constraint copy = new Constraint();

        copy.core      = core.duplicate();
        copy.name      = name;
        copy.constType = constType;
        copy.isForward = isForward;

        //
        copy.check              = check;
        copy.isNotNull          = isNotNull;
        copy.notNullColumnIndex = notNullColumnIndex;
        copy.rangeVariable      = rangeVariable;

        // A VoltDB extension to support indexed expressions, assumeunique,
        // and LIMIT PARTITION ROWS
        copy.voltIndexedExprs = voltIndexedExprs;
        copy.voltAssumeUnique = voltAssumeUnique;
        copy.voltRowsLimit = voltRowsLimit;
        copy.voltRowLimitDeleteStmt = voltRowLimitDeleteStmt;
        // End of VoltDB extension
        return copy;
    }

    void setColumnsIndexes(Table table) {

        if (constType == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
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

            for (int i = 0; i < core.refCols.length; i++) {
                Type dataType = table.getColumn(core.refCols[i]).getDataType();

                if (dataType.isLobType()) {
                    throw Error.error(ErrorCode.X_42534);
                }
            }

            if (core.mainCols.length != core.refCols.length) {
                throw Error.error(ErrorCode.X_42593);
            }
        } else if (mainColSet != null) {
            core.mainCols = table.getColumnIndexes(mainColSet);

            for (int i = 0; i < core.mainCols.length; i++) {
                Type dataType =
                    table.getColumn(core.mainCols[i]).getDataType();

                if (dataType.isLobType()) {
                    throw Error.error(ErrorCode.X_42534);
                }
            }
        }
    }

    public int getType() {
        return SchemaObject.CONSTRAINT;
    }

    /**
     * Returns the HsqlName.
     */
    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                OrderedHashSet refs = new OrderedHashSet();

                check.collectObjectNames(refs);

                for (int j = refs.size() - 1; j >= 0; j--) {
                    HsqlName name = (HsqlName) refs.get(j);

                    if (name.type == SchemaObject.COLUMN
                            || name.type == SchemaObject.TABLE) {
                        refs.remove(j);
                    }
                }

                return refs;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                OrderedHashSet set = new OrderedHashSet();

                set.add(core.uniqueName);

                return set;
        }

        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (getConstraintType()) {

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
                if (getMainColumns().length > 1
                        || (getMainColumns().length == 1
                            && !getName().isReservedName())) {
                    if (!getName().isReservedName()) {
                        sb.append(Tokens.T_CONSTRAINT).append(' ');
                        sb.append(getName().statementName).append(' ');
                    }

                    sb.append(Tokens.T_PRIMARY).append(' ').append(
                        Tokens.T_KEY);
                    sb.append(
                        getMain().getColumnListSQL(
                            getMainColumns(), getMainColumns().length));
                }
                break;

            case SchemaObject.ConstraintTypes.UNIQUE :
                if (!getName().isReservedName()) {
                    sb.append(Tokens.T_CONSTRAINT).append(' ');
                    sb.append(getName().statementName);
                    sb.append(' ');
                }

                sb.append(Tokens.T_UNIQUE);

                // A VoltDB extension to support indexed expressions
                if (voltIndexedExprs != null) {
                    String sep = "";
                    for (Expression ex : voltIndexedExprs) {
                        sb.append(sep).append(ex.getSQL());
                        sep = ", ";
                    }
                    break;
                }
                // End of VoltDB extension
                int[] col = getMainColumns();

                sb.append(getMain().getColumnListSQL(col, col.length));
                break;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
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

            case SchemaObject.ConstraintTypes.CHECK :
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

    public long getChangeTimestamp() {
        return 0;
    }

    /**
     * Generates the foreign key declaration for a given Constraint object.
     */
    private void getFKStatement(StringBuffer sb) {

        if (!getName().isReservedName()) {
            sb.append(Tokens.T_CONSTRAINT).append(' ');
            sb.append(getName().statementName);
            sb.append(' ');
        }

        sb.append(Tokens.T_FOREIGN).append(' ').append(Tokens.T_KEY);

        int[] col = getRefColumns();

        sb.append(getRef().getColumnListSQL(col, col.length));
        sb.append(' ').append(Tokens.T_REFERENCES).append(' ');
        sb.append(getMain().getName().getSchemaQualifiedStatementName());

        col = getMainColumns();

        sb.append(getMain().getColumnListSQL(col, col.length));

        if (getDeleteAction() != SchemaObject.ReferentialAction.NO_ACTION) {
            sb.append(' ').append(Tokens.T_ON).append(' ').append(
                Tokens.T_DELETE).append(' ');
            sb.append(getDeleteActionString());
        }

        if (getUpdateAction() != SchemaObject.ReferentialAction.NO_ACTION) {
            sb.append(' ').append(Tokens.T_ON).append(' ').append(
                Tokens.T_UPDATE).append(' ');
            sb.append(getUpdateActionString());
        }
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

            case SchemaObject.ReferentialAction.RESTRICT :
                return Tokens.T_RESTRICT;

            case SchemaObject.ReferentialAction.CASCADE :
                return Tokens.T_CASCADE;

            case SchemaObject.ReferentialAction.SET_DEFAULT :
                return Tokens.T_SET + ' ' + Tokens.T_DEFAULT;

            case SchemaObject.ReferentialAction.SET_NULL :
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

        if (constType == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
            switch (core.deleteAction) {

                case SchemaObject.ReferentialAction.CASCADE :
                case SchemaObject.ReferentialAction.SET_DEFAULT :
                case SchemaObject.ReferentialAction.SET_NULL :
                    return true;
            }

            switch (core.updateAction) {

                case SchemaObject.ReferentialAction.CASCADE :
                case SchemaObject.ReferentialAction.SET_DEFAULT :
                case SchemaObject.ReferentialAction.SET_NULL :
                    return true;
            }
        }

        return false;
    }

    public int getDeferability() {
        return SchemaObject.Deferable.NOT_DEFERRABLE;
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

            case SchemaObject.ConstraintTypes.CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) == 1;

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex;

            case SchemaObject.ConstraintTypes.MAIN :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex
                       && core.mainTable == core.refTable;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                return core.refCols.length == 1 && core.refCols[0] == colIndex
                       && core.mainTable == core.refTable;

            // A VoltDB extension to support LIMIT PARTITION ROWS syntax
            case SchemaObject.ConstraintTypes.LIMIT :
                return false; // LIMIT PARTITION ROWS depends on no columns

            // End of VoltDB extension
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    boolean hasColumnPlus(int colIndex) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) > 1;

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
                return core.mainCols.length != 1
                       && ArrayUtil.find(core.mainCols, colIndex) != -1;

            case SchemaObject.ConstraintTypes.MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable != core.refTable);

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable != core.refTable);

            // A VoltDB extension to support LIMIT PARTITION ROWS syntax
            case SchemaObject.ConstraintTypes.LIMIT :
                return false; // LIMIT PARTITION ROWS depends on no columns

            // End of VoltDB extension
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    boolean hasColumn(int colIndex) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                return rangeVariable.usedColumns[colIndex];

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
            case SchemaObject.ConstraintTypes.MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1;

            // A VoltDB extension to support LIMIT PARTITION ROWS syntax
            case SchemaObject.ConstraintTypes.LIMIT :
                return false; // LIMIT PARTITION ROWS depends on no columns

            // End of VoltDB extension
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    /**
     * Compares this with another constraint column set. This is used only for
     * UNIQUE constraints.
     */
    boolean isUniqueWithColumns(int[] cols) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
                if (core.mainCols.length == cols.length) {
                    return ArrayUtil.haveEqualSets(core.mainCols, cols,
                                                   cols.length);
                }
        }

        return false;
    }

    /**
     * Compares this with another constraint column set. This implementation
     * only checks FOREIGN KEY constraints.
     */
    boolean isEquivalent(Table mainTable, int[] mainCols, Table refTable,
                         int[] refCols) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.MAIN :
            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                if (mainTable != core.mainTable || refTable != core.refTable) {
                    return false;
                }

                if (core.mainCols.length == mainCols.length
                        && core.refCols.length == refCols.length) {
                    return ArrayUtil.areEqualSets(core.mainCols, mainCols)
                           && ArrayUtil.areEqualSets(core.refCols, refCols);
                }
        }

        return false;
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

                core.mainIndex.setTable(newTable);
            }
        }

        if (oldTable == core.refTable) {
            core.refTable = newTable;

            if (core.refIndex != null) {
                core.refIndex =
                    core.refTable.getIndex(core.refIndex.getName().name);
                core.refCols = ArrayUtil.toAdjustedColumnArray(core.refCols,
                        colIndex, adjust);

                core.refIndex.setTable(newTable);
            }
        }

        // CHECK
        if (constType == SchemaObject.ConstraintTypes.CHECK) {
            recompile(session, newTable);
        }
    }

    /**
     * Checks for foreign key or check constraint violation when
     * inserting a row into the child table.
     */
    void checkInsert(Session session, Table table, Object[] data,
                     boolean isNew) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                if (!isNotNull) {
                    checkCheckConstraint(session, table, data);
                }

                return;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                PersistentStore store = core.mainTable.getRowStore(session);

                if (ArrayUtil.hasNull(data, core.refCols)) {
                    if (core.matchType == OpTypes.MATCH_SIMPLE) {
                        return;
                    }

                    if (core.refCols.length == 1) {
                        return;
                    }

                    if (ArrayUtil.hasAllNull(data, core.refCols)) {
                        return;
                    }

                    // core.matchType == OpTypes.MATCH_FULL
                } else if (core.mainIndex.existsParent(session, store, data,
                                                       core.refCols)) {
                    return;
                }

                throw getException(data);
        }
    }

    /*
     * Tests a row against this CHECK constraint.
     */
    void checkCheckConstraint(Session session, Table table, Object[] data) {

        RangeIteratorBase it =
            session.sessionContext.getCheckIterator(rangeVariable);

        it.currentData = data;

        boolean nomatch = Boolean.FALSE.equals(check.getValue(session));

        it.currentData = null;

        if (nomatch) {
            String[] info = new String[] {
                name.name, table.getName().name
            };

            throw Error.error(null, ErrorCode.X_23513, ErrorCode.CONSTRAINT,
                              info);
        }
    }

    void checkCheckConstraint(Session session, Table table,
                              ColumnSchema column, Object data) {

        session.sessionData.currentValue = data;

        boolean nomatch = Boolean.FALSE.equals(check.getValue(session));

        session.sessionData.currentValue = null;

        if (nomatch) {
            String[] info = new String[] {
                name.statementName,
                table == null ? ""
                              : table.getName().statementName,
                column == null ? ""
                               : column.getName().statementName,
            };

            throw Error.error(null, ErrorCode.X_23513,
                              ErrorCode.COLUMN_CONSTRAINT, info);
        }
    }

    public HsqlException getException(Object[] data) {

        switch (this.constType) {

            case SchemaObject.ConstraintTypes.CHECK : {
                String[] info = new String[]{ name.statementName };

                return Error.error(null, ErrorCode.X_23513,
                                   ErrorCode.CONSTRAINT, info);
            }
            case SchemaObject.ConstraintTypes.FOREIGN_KEY : {
                StringBuffer sb = new StringBuffer();

                for (int i = 0; i < core.refCols.length; i++) {
                    Object o = data[core.refCols[i]];

                    sb.append(core.refTable.getColumnTypes()[core.refCols[i]]
                        .convertToString(o));
                    sb.append(',');
                }

                String[] info = new String[] {
                    name.statementName, core.refTable.getName().statementName,
                    sb.toString()
                };

                return Error.error(null, ErrorCode.X_23503,
                                   ErrorCode.CONSTRAINT, info);
            }
            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE : {
                StringBuffer sb = new StringBuffer();

                for (int i = 0; i < core.mainCols.length; i++) {
                    Object o = data[core.mainCols[i]];

                    sb.append(core.mainTable.colTypes[core.mainCols[i]]
                        .convertToString(o));
                    sb.append(',');
                }

                return Error.error(null, ErrorCode.X_23505,
                                   ErrorCode.CONSTRAINT, new String[] {
                    name.statementName, core.mainTable.getName().statementName,
                    sb.toString()
                });
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
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
     * @return iterator
     */
    RowIterator findFkRef(Session session, Object[] row) {

        if (row == null || ArrayUtil.hasNull(row, core.mainCols)) {
            return core.refIndex.emptyIterator();
        }

        PersistentStore store = core.refTable.getRowStore(session);

        return core.refIndex.findFirstRow(session, store, row, core.mainCols);
    }

    /**
     * Check used before creating a new foreign key cosntraint, this method
     * checks all rows of a table to ensure they all have a corresponding
     * row in the main table.
     */
    void checkReferencedRows(Session session, Table table) {

        RowIterator it = table.rowIterator(session);

        while (true) {
            Row row = it.getNextRow();

            if (row == null) {
                break;
            }

            Object[] rowData = row.getData();

            checkInsert(session, table, rowData, false);
        }
    }

    public Expression getCheckExpression() {
        return check;
    }

    public OrderedHashSet getCheckColumnExpressions() {

        OrderedHashSet set = new OrderedHashSet();

        check.collectAllExpressions(set, Expression.columnExpressionSet,
                                    Expression.emptyExpressionSet);

        return set;
    }

    void recompile(Session session, Table newTable) {

        check = getNewCheckExpression(session);

        // this workaround is here to stop LIKE optimisation (for proper scripting)
        QuerySpecification checkSelect = Expression.getCheckSelect(session,
            newTable, check);

        rangeVariable = checkSelect.rangeVariables[0];

        rangeVariable.setForCheckConstraint();
    }

    private Expression getNewCheckExpression(Session session) {

        String    ddl     = check.getSQL();
        Scanner   scanner = new Scanner(ddl);
        ParserDQL parser  = new ParserDQL(session, scanner, null);

        parser.compileContext.setNextRangeVarIndex(0);
        parser.read();

        parser.isCheckOrTriggerCondition = true;

        Expression condition = parser.XreadBooleanValueExpression();

        return condition;
    }

    void prepareCheckConstraint(Session session, Table table) {

        // to ensure no subselects etc. are in condition
        check.checkValidCheckConstraint();

        if (table == null) {
            check.resolveTypes(session, null);
        } else {
            QuerySpecification checkSelect = Expression.getCheckSelect(session,
                table, check);

            rangeVariable = checkSelect.rangeVariables[0];

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

    void checkCheckConstraint(Session session, Table table) {

        if (table.getRowStore(session).elementCount() > 0) {
            Expression newCheck = getNewCheckExpression(session);
            QuerySpecification checkSelect = Expression.getCheckSelect(session,
                table, newCheck);
            Result r = checkSelect.getResult(session, 1);

            if (r.getNavigator().getSize() != 0) {
                String[] info = new String[] {
                    name.statementName, table.getName().statementName
                };

                throw Error.error(null, ErrorCode.X_23513,
                                  ErrorCode.CONSTRAINT, info);
            }
        }
    }
    // A VoltDB extension to export abstract parse trees and support indexed expressions

    // !!!!!!!!
    // NOTE!  IF YOU ARE GOING TO ADD NEW MEMBER FIELDS HERE YOU
    // NEED TO MAKE SURE THEY GET ADDED TO Constraint.duplicate()
    // AT THE TOP OF THE FILE OR ALTER WILL HATE YOU --izzy

    Expression[] voltIndexedExprs; // For VoltDB, support indexed expressions, not just columns
    boolean voltAssumeUnique = false; // For VoltDB, support the assume unique attribute.
    int voltRowsLimit = Integer.MAX_VALUE; // For VoltDB, support LIMIT PARTITION ROWS syntax
    String voltRowLimitDeleteStmt; // For VoltDB, support LIMIT PARTITION ROWS syntax

    // VoltDB support for indexed expressions
    // and new kinds of constraints
    Constraint voltWithExpressions(Expression[] exprs) {
        voltIndexedExprs = exprs;
        return this;
    }

    /**
     * @return The name of this constraint instance's type.
     */
    private String voltGetTypeName() {
        switch (constType) {
            case SchemaObject.ConstraintTypes.FOREIGN_KEY: return "FOREIGN_KEY";
            case SchemaObject.ConstraintTypes.MAIN: return "MAIN";
            case SchemaObject.ConstraintTypes.UNIQUE: return "UNIQUE";
            case SchemaObject.ConstraintTypes.CHECK: return isNotNull ? "NOT_NULL" : "CHECK";
            case SchemaObject.ConstraintTypes.PRIMARY_KEY: return "PRIMARY_KEY";
            case SchemaObject.ConstraintTypes.LIMIT: return "LIMIT";
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
        if (this.constType == SchemaObject.ConstraintTypes.MAIN) {
            return null;
        }

        VoltXMLElement constraint = new VoltXMLElement("constraint");
        // WARNING: the name attribute setting is tentative, subject to reset in the
        // calling function, Table.voltGetTableXML.
        constraint.attributes.put("name", getName().name);
        constraint.attributes.put("constrainttype", voltGetTypeName());
        constraint.attributes.put("assumeunique", voltAssumeUnique ? "true" : "false");
        constraint.attributes.put("rowslimit", String.valueOf(voltRowsLimit));
        if (voltRowLimitDeleteStmt != null) {
            constraint.attributes.put("rowslimitdeletestmt", voltRowLimitDeleteStmt);
        }

        // VoltDB implements constraints by defining an index, by annotating metadata (such as for NOT NULL columns),
        // or by issuing a "not supported" warning (such as for foreign keys).
        // Any constraint implemented as an index must have an index name attribute.
        // No other constraint details are currently used by VoltDB.
        if (this.constType != SchemaObject.ConstraintTypes.FOREIGN_KEY && core.mainIndex != null) {
            // WARNING: the index attribute setting is tentative, subject to reset in
            // the calling function, Table.voltGetTableXML.
            constraint.attributes.put("index", core.mainIndex.getName().name);
        }
        return constraint;
    }

    // VoltDB support for indexed expressions
    boolean voltIsUniqueWithExprs(Expression[] indexExprs2) {
        if (constType != SchemaObject.ConstraintTypes.UNIQUE ||
                (voltIndexedExprs == null) || ! voltIndexedExprs.equals(indexExprs2)) {
            return false;
        }
        return true;
    }

    // VoltDB support for indexed expressions
    // Is this for temp constraints only? What's a temp constraint?
    boolean voltHasExprs() { return voltIndexedExprs != null; }

    Constraint voltSetAssumeUnique(boolean assumeUnique) {
        this.voltAssumeUnique = assumeUnique;
        return this;
    }

    @Override
    public String toString() {
        String str = "CONSTRAINT " + getName().name + " " + voltGetTypeName();
        if (constType == SchemaObject.ConstraintTypes.LIMIT) {
            str += " " + voltRowsLimit;
            if (voltRowLimitDeleteStmt != null) {
                str += " EXECUTE (" + voltRowLimitDeleteStmt + ")";
            }
        }

        return str;
    }
    // End of VoltDB extension
}
