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

import java.lang.reflect.Method;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.RangeGroup.RangeGroupSimple;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.rights.GranteeManager;
import org.hsqldb_voltpatches.rights.Right;
import org.hsqldb_voltpatches.rights.User;
import org.hsqldb_voltpatches.types.Charset;
import org.hsqldb_voltpatches.types.Collation;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.UserTypeModifier;

/**
 * Parser for DDL statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 1.9.0
 */
public class ParserDDL extends ParserRoutine {

    static final int[]   schemaCommands             = new int[] {
        Tokens.CREATE, Tokens.GRANT
    };
    static final short[] startStatementTokens       = new short[] {
        Tokens.CREATE, Tokens.GRANT, Tokens.ALTER, Tokens.DROP
    };
    static final short[] startStatementTokensSchema = new short[] {
        Tokens.CREATE, Tokens.GRANT,
    };

    ParserDDL(Session session, Scanner scanner) {
        super(session, scanner);
    }

    void reset(String sql) {
        super.reset(sql);
    }

    StatementSchema compileCreate() {

        int     tableType   = TableBase.MEMORY_TABLE;
        boolean isTable     = false;
        boolean isOrReplace = false;

        read();

        switch (token.tokenType) {

            case Tokens.GLOBAL :
                read();
                readThis(Tokens.TEMPORARY);
                readIfThis(Tokens.MEMORY);
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.TEMP_TABLE;
                break;

            case Tokens.TEMP :
                read();
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.TEMP_TABLE;
                break;

            case Tokens.TEMPORARY :
                read();
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.TEMP_TABLE;
                break;

            case Tokens.MEMORY :
                read();
                readThis(Tokens.TABLE);

                isTable = true;
                break;

            case Tokens.CACHED :
                read();
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.CACHED_TABLE;
                break;

            case Tokens.TEXT :
                read();
                readThis(Tokens.TABLE);

                isTable   = true;
                tableType = TableBase.TEXT_TABLE;
                break;

            case Tokens.TABLE :
                read();

                isTable   = true;
                tableType = database.schemaManager.getDefaultTableType();
                break;

            case Tokens.OR :
                if (database.sqlSyntaxOra) {
                    read();
                    readThis(Tokens.REPLACE);

                    switch (token.tokenType) {

                        case Tokens.FUNCTION :
                        case Tokens.PROCEDURE :
                        case Tokens.TRIGGER :
                        case Tokens.TYPE :
                        case Tokens.VIEW :
                            break;

                        default :
                            throw unexpectedToken(Tokens.T_OR);
                    }

                    isOrReplace = true;
                }
                break;

            default :
        }

        if (isTable) {
            return compileCreateTable(tableType);
        }

        // A VoltDB extension to support the assume unique attribute
        boolean assumeUnique = false;
        // End of VoltDB extension
        switch (token.tokenType) {

            // other objects
            case Tokens.ALIAS :
                return compileCreateAlias();

            case Tokens.SEQUENCE :
                return compileCreateSequence();

            case Tokens.SCHEMA :
                return compileCreateSchema();

            case Tokens.TRIGGER :
                return compileCreateTrigger(isOrReplace);

            case Tokens.USER :
                return compileCreateUser();

            case Tokens.ROLE :
                return compileCreateRole();

            case Tokens.VIEW :
                return compileCreateView(false, isOrReplace);

            case Tokens.DOMAIN :
                return compileCreateDomain();

            case Tokens.TYPE :
                return compileCreateType(isOrReplace);

            case Tokens.CHARACTER :
                return compileCreateCharacterSet();

            case Tokens.COLLATION :
                return compileCreateCollation();

            // index
            // A VoltDB extension to support the assume unique attribute
            case Tokens.ASSUMEUNIQUE :
                assumeUnique = true;
                // fall through
            // End of VoltDB extension
            case Tokens.UNIQUE :
                read();
                checkIsThis(Tokens.INDEX);

                // A VoltDB extension to support the assume unique attribute
                return compileCreateIndex(true, assumeUnique);
                /* disable 1 line ...
                return compileCreateIndex(true);
                ... disabled 1 line */
                // End of VoltDB extension

            case Tokens.INDEX :
                // A VoltDB extension to support the assume unique attribute
                return compileCreateIndex(false, false);
                /* disable 1 line ...
                return compileCreateIndex(false);
                ... disabled 1 line */
                // End of VoltDB extension

            case Tokens.AGGREGATE :
            case Tokens.FUNCTION :
            case Tokens.PROCEDURE :
                return compileCreateProcedureOrFunction(isOrReplace);

            default : {
                throw unexpectedToken();
            }
        }
    }

    Statement compileAlter() {

        read();

        switch (token.tokenType) {

            case Tokens.INDEX : {
                read();

                HsqlName name = readNewSchemaObjectName(SchemaObject.INDEX,
                    true);

                name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

                if (token.tokenType == Tokens.RENAME) {
                    read();
                    readThis(Tokens.TO);

                    return compileRenameObject(name, SchemaObject.INDEX);
                }

                readThis(Tokens.AS);

                Index index =
                    (Index) database.schemaManager.getSchemaObject(name);

                if (index == null) {
                    throw Error.error(ErrorCode.X_42501);
                }

                Table table = (Table) database.schemaManager.getSchemaObject(
                    index.getName().parent);
                int[]      indexColumns   = readColumnList(table, true);
                String     sql            = getLastPart();
                Object[]   args           = new Object[] {
                    table, indexColumns, index.getName()
                };
                HsqlName[] writeLockNames = new HsqlName[] {
                    database.getCatalogName(), table.getName()
                };

                return new StatementSchema(sql, StatementTypes.ALTER_INDEX,
                                           args, null, writeLockNames);
            }
            case Tokens.SCHEMA : {
                read();

                HsqlName name = readSchemaName();

                readThis(Tokens.RENAME);
                readThis(Tokens.TO);

                return compileRenameObject(name, SchemaObject.SCHEMA);
            }
            case Tokens.CATALOG : {
                read();
                checkIsSimpleName();

                String name = token.tokenString;

                checkValidCatalogName(name);
                read();
                readThis(Tokens.RENAME);
                readThis(Tokens.TO);

                return compileRenameObject(database.getCatalogName(),
                                           SchemaObject.CATALOG);
            }
            case Tokens.SEQUENCE : {
                return compileAlterSequence();
            }
            case Tokens.TABLE : {
                return compileAlterTable();
            }
            case Tokens.USER : {
                return compileAlterUser();
            }
            case Tokens.DOMAIN : {
                return compileAlterDomain();
            }
            case Tokens.VIEW : {
                return compileCreateView(true, false);
            }
            case Tokens.SESSION : {
                return compileAlterSession();
            }
            case Tokens.SPECIFIC : {
                return compileAlterSpecificRoutine();
            }
            case Tokens.ROUTINE : {
                return compileAlterRoutine();
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    Statement compileAlterRoutine() {

        readThis(Tokens.ROUTINE);

        RoutineSchema routine =
            (RoutineSchema) readSchemaObjectName(SchemaObject.ROUTINE);

        readThis(Tokens.RENAME);
        readThis(Tokens.TO);

        return compileRenameObject(routine.getName(), routine.getName().type);
    }

    Statement compileDrop() {

        int          objectTokenType;
        int          objectType;
        int          statementType;
        boolean      canCascade  = false;
        boolean      cascade     = false;
        boolean      useIfExists = false;
        boolean      ifExists    = false;
        SchemaObject object;
        HsqlName[]   writeLockNames;

        read();

        objectTokenType = this.token.tokenType;

        switch (objectTokenType) {

            case Tokens.INDEX : {
                read();

                statementType = StatementTypes.DROP_INDEX;
                objectType    = SchemaObject.INDEX;
                useIfExists   = true;

                break;
            }
            case Tokens.ASSERTION : {
                read();

                statementType = StatementTypes.DROP_ASSERTION;
                objectType    = SchemaObject.ASSERTION;
                canCascade    = true;

                break;
            }
            case Tokens.SPECIFIC : {
                read();

                switch (token.tokenType) {

                    case Tokens.ROUTINE :
                    case Tokens.PROCEDURE :
                    case Tokens.FUNCTION :
                        read();
                        break;

                    default :
                        throw unexpectedToken();
                }

                statementType = StatementTypes.DROP_ROUTINE;
                objectType    = SchemaObject.SPECIFIC_ROUTINE;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.PROCEDURE : {
                read();

                statementType = StatementTypes.DROP_ROUTINE;
                objectType    = SchemaObject.PROCEDURE;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.FUNCTION : {
                read();

                statementType = StatementTypes.DROP_ROUTINE;
                objectType    = SchemaObject.FUNCTION;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.SCHEMA : {
                read();

                statementType = StatementTypes.DROP_SCHEMA;
                objectType    = SchemaObject.SCHEMA;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.SEQUENCE : {
                read();

                statementType = StatementTypes.DROP_SEQUENCE;
                objectType    = SchemaObject.SEQUENCE;
                canCascade    = true;
                useIfExists   = true;

                break;
            }
            case Tokens.TRIGGER : {
                read();

                statementType = StatementTypes.DROP_TRIGGER;
                objectType    = SchemaObject.TRIGGER;
                canCascade    = false;
                useIfExists   = true;

                break;
            }
            case Tokens.USER : {
                read();

                statementType = StatementTypes.DROP_USER;
                objectType    = SchemaObject.GRANTEE;
                canCascade    = true;

                break;
            }
            case Tokens.ROLE : {
                read();

                statementType = StatementTypes.DROP_ROLE;
                objectType    = SchemaObject.GRANTEE;
                canCascade    = true;

                break;
            }
            case Tokens.DOMAIN :
                read();

                statementType = StatementTypes.DROP_DOMAIN;
                objectType    = SchemaObject.DOMAIN;
                canCascade    = true;
                useIfExists   = true;
                break;

            case Tokens.TYPE :
                read();

                statementType = StatementTypes.DROP_TYPE;
                objectType    = SchemaObject.TYPE;
                canCascade    = true;
                useIfExists   = true;
                break;

            case Tokens.CHARACTER :
                read();
                readThis(Tokens.SET);

                statementType = StatementTypes.DROP_CHARACTER_SET;
                objectType    = SchemaObject.CHARSET;
                canCascade    = false;
                useIfExists   = true;
                break;

            case Tokens.COLLATION :
                read();

                statementType = StatementTypes.DROP_COLLATION;
                objectType    = SchemaObject.COLLATION;
                canCascade    = false;
                useIfExists   = true;
                break;

            case Tokens.VIEW :
                read();

                statementType = StatementTypes.DROP_VIEW;
                objectType    = SchemaObject.VIEW;
                canCascade    = true;
                useIfExists   = true;
                break;

            case Tokens.TABLE :
                read();

                statementType = StatementTypes.DROP_TABLE;
                objectType    = SchemaObject.TABLE;
                canCascade    = true;
                useIfExists   = true;
                break;

            default :
                throw unexpectedToken();
        }

        if (useIfExists && token.tokenType == Tokens.IF) {
            int position = getPosition();

            read();

            if (token.tokenType == Tokens.EXISTS) {
                read();

                ifExists = true;
            } else {
                rewind(position);
            }
        }

        checkIsIdentifier();

        HsqlName name = null;

        switch (objectTokenType) {

            case Tokens.USER : {
                checkIsSimpleName();
                checkDatabaseUpdateAuthorisation();

                object = database.getUserManager().get(token.tokenString);

                read();

                break;
            }
            case Tokens.ROLE : {
                checkIsSimpleName();
                checkDatabaseUpdateAuthorisation();

                object =
                    database.getGranteeManager().getRole(token.tokenString);

                read();

                break;
            }
            case Tokens.SCHEMA : {
                name   = readNewSchemaName();
                object = database.schemaManager.findSchema(name.name);

                break;
            }
            case Tokens.TABLE : {
                boolean isModule =
                    token.namePrePrefix == null
                    && (Tokens.T_MODULE.equals(token.namePrefix)
                        || Tokens.T_SESSION.equals(token.namePrefix));

                if (isModule) {
                    name = readNewSchemaObjectName(objectType, false);

                    if (!ifExists && token.tokenType == Tokens.IF) {
                        read();
                        readThis(Tokens.EXISTS);

                        ifExists = true;
                    }

                    Object[] args = new Object[] {
                        name, Boolean.valueOf(ifExists)
                    };

                    return new StatementSession(StatementTypes.DROP_TABLE,
                                                args);
                }
            }

            // fall through
            default :
                name = readNewSchemaObjectName(objectType, false);

                name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

                object = database.schemaManager.findSchemaObject(name.name,
                        name.schema.name, name.type);
        }

        if (!ifExists && useIfExists && token.tokenType == Tokens.IF) {
            read();
            readThis(Tokens.EXISTS);

            ifExists = true;
        }

        if (canCascade) {
            if (token.tokenType == Tokens.CASCADE) {
                cascade = true;

                read();

                if (database.sqlSyntaxOra) {
                    readIfThis(Tokens.CONSTRAINTS);
                }
            } else if (token.tokenType == Tokens.RESTRICT) {
                read();
            }
        }

        if (object == null) {
            writeLockNames = null;
        } else {
            name = object.getName();
            writeLockNames =
                database.schemaManager.getCatalogAndBaseTableNames(name);
        }

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            name, new Integer(objectType), Boolean.valueOf(cascade),
            Boolean.valueOf(ifExists)
        };
        Statement cs = new StatementSchema(sql, statementType, args, null,
                                           writeLockNames);

        return cs;
    }

    Statement compileAlterTable() {

        read();

        String   tableName = token.tokenString;
        HsqlName schema    = session.getSchemaHsqlName(token.namePrefix);

        checkSchemaUpdateAuthorisation(schema);

        Table t = database.schemaManager.getUserTable(session, tableName,
            schema.name);

        // A VoltDB extension to guard against alter table operations performed on views.
        // The named "table" was actually a view -- not considered a
        // valid target for alter table commands -- treat as "not found",
        // as if views and tables are not using the same name space.
        if (t.tableType == TableBase.VIEW_TABLE) {
            throw Error.error(ErrorCode.X_42501, tableName);
        }
        // End of VoltDB extension
        read();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);

                return compileRenameObject(t.getName(), SchemaObject.TABLE);
            }
            case Tokens.ADD : {
                read();

                HsqlName cname = null;

                if (token.tokenType == Tokens.CONSTRAINT) {
                    read();

                    cname = readNewDependentSchemaObjectName(t.getName(),
                            SchemaObject.CONSTRAINT);
                }

                // A VoltDB extension to support the assume unique attribute
                boolean assumeUnique = false; // For VoltDB
                // End of VoltDB extension
                switch (token.tokenType) {

                    case Tokens.FOREIGN :
                        read();
                        readThis(Tokens.KEY);

                        return compileAlterTableAddForeignKeyConstraint(t,
                                cname);

                    // A VoltDB extension to support the assume unique attribute
                    case Tokens.ASSUMEUNIQUE :
                        assumeUnique = true;
                        // fall through
                    // End of VoltDB extension
                    case Tokens.UNIQUE :
                        read();

                        if (database.sqlSyntaxMys) {
                            if (!readIfThis(Tokens.INDEX)) {
                                readIfThis(Tokens.KEY);
                            }
                        }

                        // A VoltDB extension to support the assume unique attribute
                        return compileAlterTableAddUniqueConstraint(t, cname, assumeUnique);
                        /* disable 1 line ...
                        return compileAlterTableAddUniqueConstraint(t, cname);
                        ... disabled 1 line */
                        // End of VoltDB extension

                    case Tokens.CHECK :
                        read();

                        return compileAlterTableAddCheckConstraint(t, cname);

                    case Tokens.PRIMARY :
                        read();
                        readThis(Tokens.KEY);

                        return compileAlterTableAddPrimaryKey(t, cname);

                    // A VoltDB extension to support LIMIT PARTITION ROWS syntax
                    case Tokens.LIMIT :
                        read();

                        return compileAlterTableAddLimitConstraint(t, cname);
                    // End of VoltDB extension
                    case Tokens.COLUMN :
                        if (cname != null) {
                            throw unexpectedToken();
                        }

                        read();
                        checkIsSimpleName();

                        return compileAlterTableAddColumn(t);

                    default :
                        if (cname != null) {
                            throw unexpectedToken();
                        }

                        checkIsSimpleName();

                        return compileAlterTableAddColumn(t);
                }
            }
            case Tokens.DROP : {
                read();

                switch (token.tokenType) {

                    case Tokens.PRIMARY : {
                        boolean cascade = false;

                        read();
                        readThis(Tokens.KEY);

                        return compileAlterTableDropPrimaryKey(t);
                    }
                    // A VoltDB extension to support LIMIT PARTITION ROWS syntax
                    case Tokens.LIMIT : {
                        // CASCADE currently has no meaning with this constraint,
                        // always false
                        boolean cascade = false;

                        read();
                        readThis(Tokens.PARTITION);
                        readThis(Tokens.ROWS);

                        Constraint c = t.getLimitConstraint();
                        if (c == null) {
                            throw Error.error(ErrorCode.X_42501);
                        }
                        return compileAlterTableDropConstraint(t, c, cascade);
                    }
                    // End of VoltDB extension
                    case Tokens.CONSTRAINT : {
                        read();

                        return compileAlterTableDropConstraint(t);
                    }
                    case Tokens.COLUMN :
                        read();

                    // fall through
                    default : {
                        checkIsSimpleName();

                        String  name    = token.tokenString;
                        boolean cascade = false;

                        read();

                        if (token.tokenType == Tokens.RESTRICT) {
                            read();
                        } else if (token.tokenType == Tokens.CASCADE) {
                            read();

                            cascade = true;
                        }

                        return compileAlterTableDropColumn(t, name, cascade);
                    }
                }
            }
            case Tokens.ALTER : {
                read();

                if (token.tokenType == Tokens.COLUMN) {
                    read();
                }

                int          columnIndex = t.getColumnIndex(token.tokenString);
                ColumnSchema column      = t.getColumn(columnIndex);

                read();

                return compileAlterColumn(t, column, columnIndex);
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    private Statement compileAlterTableDropConstraint(Table table) {

        boolean cascade = false;
        SchemaObject object = readSchemaObjectName(table.getSchemaName(),
            SchemaObject.CONSTRAINT);

        if (token.tokenType == Tokens.RESTRICT) {
            read();
        } else if (token.tokenType == Tokens.CASCADE) {
            read();

            cascade = true;
        }

// A VoltDB extension to support LIMIT PARTITION ROWS syntax
        return compileAlterTableDropConstraint(table, (Constraint) object, cascade);
    }

    private Statement compileAlterTableDropConstraint(Table table, Constraint object, boolean cascade) {
// End of VoltDB extension
        String   sql  = getLastPart();
        Object[] args = new Object[] {
            object.getName(), ValuePool.getInt(SchemaObject.CONSTRAINT),
            Boolean.valueOf(cascade), Boolean.FALSE
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());
        HsqlName mainTableName = ((Constraint) object).getMainTableName();

        if (mainTableName != null && mainTableName != table.getName()) {
            writeLockNames =
                (HsqlName[]) ArrayUtil.toAdjustedArray(writeLockNames,
                    mainTableName, writeLockNames.length, 1);
        }

        Statement cs = new StatementSchema(sql,
                                           StatementTypes.DROP_CONSTRAINT,
                                           args, null, writeLockNames);

        return cs;
    }

    private Statement compileAlterTableDropPrimaryKey(Table table) {

        boolean cascade = false;

        if (token.tokenType == Tokens.RESTRICT) {
            read();
        } else if (token.tokenType == Tokens.CASCADE) {
            read();

            cascade = true;
        }

        if (!table.hasPrimaryKey()) {
            throw Error.error(ErrorCode.X_42501);
        }

        String       sql    = getLastPart();
        SchemaObject object = table.getPrimaryConstraint();
        Object[]     args   = new Object[] {
            object.getName(), ValuePool.getInt(SchemaObject.CONSTRAINT),
            Boolean.valueOf(cascade), Boolean.FALSE
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());
        Statement cs = new StatementSchema(sql,
                                           StatementTypes.DROP_CONSTRAINT,
                                           args, null, writeLockNames);

        return cs;
    }

    StatementSession compileDeclareLocalTableOrNull() {

        int position = super.getPosition();

        try {
            readThis(Tokens.DECLARE);
            readThis(Tokens.LOCAL);
            readThis(Tokens.TEMPORARY);
            readThis(Tokens.TABLE);
        } catch (HsqlException e) {
            lastError = e;

            // may be cursor
            rewind(position);

            return null;
        }

        if (token.namePrePrefix == null
                && (token.namePrefix == null
                    || Tokens.T_MODULE.equals(token.namePrefix)
                    || Tokens.T_SESSION.equals(token.namePrefix))) {

            // valid name
        } else {
            throw unexpectedToken();
        }

        HsqlName name = readNewSchemaObjectName(SchemaObject.TABLE, false);

        name.schema = SqlInvariants.MODULE_HSQLNAME;

        Table table = new Table(database, name, TableBase.TEMP_TABLE);
        StatementSchema cs          = compileCreateTableBody(table, false);
        HsqlArrayList   constraints = (HsqlArrayList) cs.arguments[1];

        for (int i = 0; i < constraints.size(); i++) {
            Constraint c = (Constraint) constraints.get(i);

            if (c.getConstraintType()
                    == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
                throw unexpectedToken(Tokens.T_FOREIGN);
            }
        }

        StatementSession ss =
            new StatementSession(StatementTypes.DECLARE_SESSION_TABLE,
                                 cs.arguments);

        return ss;
    }

    StatementSchema compileCreateTable(int type) {

        boolean ifNot = false;

        if (token.tokenType == Tokens.IF) {
            int position = getPosition();

            read();

            if (token.tokenType == Tokens.NOT) {
                read();
                readThis(Tokens.EXISTS);

                ifNot = true;
            } else {
                rewind(position);
            }
        }

        HsqlName name = readNewSchemaObjectName(SchemaObject.TABLE, false);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

        Table table;

        switch (type) {

            case TableBase.TEMP_TEXT_TABLE :
            case TableBase.TEXT_TABLE : {
                table = new TextTable(database, name, type);

                break;
            }
            default : {
                table = new Table(database, name, type);
            }
        }

        return compileCreateTableBody(table, ifNot);
    }

    StatementSchema compileCreateTableBody(Table table, boolean ifNot) {

        HsqlArrayList tempConstraints = new HsqlArrayList();

        if (token.tokenType == Tokens.AS) {
            return readTableAsSubqueryDefinition(table);
        }

        int position = getPosition();

        readThis(Tokens.OPENBRACKET);

        {
            Constraint c = new Constraint(null, null,
                                          SchemaObject.ConstraintTypes.TEMP);

            tempConstraints.add(c);
        }

        boolean start     = true;
        boolean startPart = true;
        boolean end       = false;

        while (!end) {
            switch (token.tokenType) {

                case Tokens.LIKE : {
                    ColumnSchema[] likeColumns = readLikeTable(table);

                    for (int i = 0; i < likeColumns.length; i++) {
                        table.addColumn(likeColumns[i]);
                    }

                    start     = false;
                    startPart = false;

                    break;
                }
                case Tokens.CONSTRAINT :
                case Tokens.PRIMARY :
                case Tokens.FOREIGN :
                // A VoltDB extension to support the assume unique attribute
                case Tokens.ASSUMEUNIQUE :
                // End of VoltDB extension
                case Tokens.UNIQUE :
                case Tokens.CHECK :
                // A VoltDB extension to support LIMIT PARTITION ROWS
                case Tokens.LIMIT :
                // End of VoltDB extension
                    if (!startPart) {
                        throw unexpectedToken();
                    }

                    readConstraint(table, tempConstraints);

                    start     = false;
                    startPart = false;
                    break;

                case Tokens.COMMA :
                    if (startPart) {
                        throw unexpectedToken();
                    }

                    read();

                    startPart = true;
                    break;

                case Tokens.CLOSEBRACKET :
                    read();

                    end = true;
                    break;

                default :
                    if (!startPart) {
                        throw unexpectedToken();
                    }

                    checkIsSchemaObjectName();

                    HsqlName hsqlName =
                        database.nameManager.newColumnHsqlName(table.getName(),
                            token.tokenString, isDelimitedIdentifier());

                    read();

                    ColumnSchema newcolumn = readColumnDefinitionOrNull(table,
                        hsqlName, tempConstraints);

                    if (newcolumn == null) {
                        if (start) {
                            rewind(position);

                            return readTableAsSubqueryDefinition(table);
                        } else {
                            throw Error.error(ErrorCode.X_42000);
                        }
                    }

                    table.addColumn(newcolumn);

                    start     = false;
                    startPart = false;
            }
        }

        if (table.getColumnCount() == 0) {
            throw Error.error(ErrorCode.X_42591);
        }

        if (token.tokenType == Tokens.ON) {
            if (!table.isTemp()) {
                throw unexpectedToken();
            }

            read();
            readThis(Tokens.COMMIT);

            if (token.tokenType == Tokens.DELETE) {}
            else if (token.tokenType == Tokens.PRESERVE) {
                table.persistenceScope = TableBase.SCOPE_SESSION;
            }

            read();
            readThis(Tokens.ROWS);
        }

        if (database.sqlSyntaxMys) {
            if (readIfThis(Tokens.COMMENT)) {
                readIfThis(Tokens.EQUALS);

                String comment = readQuotedString();

                table.getName().comment = comment;
            }
        }

        OrderedHashSet names = new OrderedHashSet();

        names.add(database.getCatalogName());

        for (int i = 0; i < tempConstraints.size(); i++) {
            Constraint c    = (Constraint) tempConstraints.get(i);
            HsqlName   name = c.getMainTableName();

            if (name != null) {
                Table t = database.schemaManager.findUserTable(null,
                    name.name, name.schema.name);

                if (t != null && !t.isTemp()) {
                    names.add(table.getName());
                }
            }
        }

        String     sql            = getLastPart();
        Object[]   args           = new Object[] {
            table, tempConstraints, null, Boolean.valueOf(ifNot)
        };
        HsqlName[] writeLockNames = new HsqlName[names.size()];

        names.toArray(writeLockNames);

        return new StatementSchema(sql, StatementTypes.CREATE_TABLE, args,
                                   null, writeLockNames);
    }

    private ColumnSchema[] readLikeTable(Table table) {

        read();

        boolean           generated = false;
        boolean           identity  = false;
        boolean           defaults  = false;
        Table             likeTable = readTableName();
        OrderedIntHashSet set       = new OrderedIntHashSet();

        while (true) {
            boolean including = token.tokenType == Tokens.INCLUDING;

            if (!including && token.tokenType != Tokens.EXCLUDING) {
                break;
            }

            read();

            switch (token.tokenType) {

                case Tokens.GENERATED :
                    if (!set.add(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    generated = including;
                    break;

                case Tokens.IDENTITY :
                    if (!set.add(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    identity = including;
                    break;

                case Tokens.DEFAULTS :
                    if (!set.add(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    defaults = including;
                    break;

                default :
                    throw unexpectedToken();
            }

            read();
        }

        ColumnSchema[] columnList =
            new ColumnSchema[likeTable.getColumnCount()];

        for (int i = 0; i < columnList.length; i++) {
            ColumnSchema column = likeTable.getColumn(i).duplicate();
            HsqlName name =
                database.nameManager.newColumnSchemaHsqlName(table.getName(),
                    column.getName());

            column.setName(name);
            column.setNullable(true);
            column.setPrimaryKey(false);

            if (identity) {
                if (column.isIdentity()) {
                    column.setIdentity(
                        column.getIdentitySequence().duplicate());
                }
            } else {
                column.setIdentity(null);
            }

            if (!defaults) {
                column.setDefaultExpression(null);
            }

            if (!generated) {
                column.setGeneratingExpression(null);
            }

            columnList[i] = column;
        }

        return columnList;
    }

    StatementSchema readTableAsSubqueryDefinition(Table table) {

        HsqlName[] readName    = null;
        boolean    withData    = true;
        HsqlName[] columnNames = null;
        Statement  statement   = null;

        if (token.tokenType == Tokens.OPENBRACKET) {
            columnNames = readColumnNames(table.getName());
        }

        readThis(Tokens.AS);
        readThis(Tokens.OPENBRACKET);

        QueryExpression queryExpression = XreadQueryExpression();

        queryExpression.setReturningResult();
        queryExpression.resolve(session);
        readThis(Tokens.CLOSEBRACKET);
        readThis(Tokens.WITH);

        if (token.tokenType == Tokens.NO) {
            read();

            withData = false;
        } else if (table.getTableType() == TableBase.TEXT_TABLE) {
            throw unexpectedTokenRequire(Tokens.T_NO);
        }

        readThis(Tokens.DATA);

        if (token.tokenType == Tokens.ON) {
            if (!table.isTemp()) {
                throw unexpectedToken();
            }

            read();
            readThis(Tokens.COMMIT);

            if (token.tokenType == Tokens.DELETE) {}
            else if (token.tokenType == Tokens.PRESERVE) {
                table.persistenceScope = TableBase.SCOPE_SESSION;
            }

            read();
            readThis(Tokens.ROWS);
        }

        if (columnNames == null) {
            columnNames = queryExpression.getResultColumnNames();
        } else {
            if (columnNames.length != queryExpression.getColumnCount()) {
                throw Error.error(ErrorCode.X_42593);
            }
        }

        TableUtil.setColumnsInSchemaTable(table, columnNames,
                                          queryExpression.getColumnTypes());
        table.createPrimaryKey();

        if (table.isTemp() && table.hasLobColumn()) {
            throw Error.error(ErrorCode.X_42534);
        }

        if (withData) {
            statement = new StatementQuery(session, queryExpression,
                                           compileContext);
            readName = statement.getTableNamesForRead();
        }

        Object[]   args           = new Object[] {
            table, new HsqlArrayList(), statement, Boolean.FALSE
        };
        String     sql            = getLastPart();
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;
        StatementSchema st = new StatementSchema(sql,
            StatementTypes.CREATE_TABLE, args, readName, writeLockNames);

        return st;
    }

    /**
     * Adds a list of temp constraints to a new table
     */
    static Table addTableConstraintDefinitions(Session session, Table table,
            HsqlArrayList tempConstraints, HsqlArrayList constraintList,
            boolean addToSchema) {

        Constraint c        = (Constraint) tempConstraints.get(0);
        String     namePart = c.getName() == null ? null
                                                  : c.getName().name;
        HsqlName indexName = session.database.nameManager.newAutoName("IDX",
            namePart, table.getSchemaName(), table.getName(),
            SchemaObject.INDEX);

        c.setColumnsIndexes(table);
        table.createPrimaryKey(indexName, c.core.mainCols, true);

        if (c.core.mainCols != null) {
            Constraint newconstraint = new Constraint(c.getName(), table,
                table.getPrimaryIndex(),
                SchemaObject.ConstraintTypes.PRIMARY_KEY);

            table.addConstraint(newconstraint);

            if (addToSchema) {
                session.database.schemaManager.addSchemaObject(newconstraint);
            }
        }

        for (int i = 1; i < tempConstraints.size(); i++) {
            c = (Constraint) tempConstraints.get(i);

            switch (c.constType) {

                case SchemaObject.ConstraintTypes.UNIQUE : {
                    c.setColumnsIndexes(table);

                    // A VoltDB extension to support indexed expressions and the assume unique attribute
                    if (c.voltIndexedExprs != null) {
                        // Special case handling for VoltDB indexed expressions
                        if (table.getUniqueConstraintForExprs(c.voltIndexedExprs) != null) {
                            throw Error.error(ErrorCode.X_42522);
                        }
                    }
                    // TODO: enable a weaker form of this guard. The current form was
                    // causing problems with some innocent-looking primary key definitions.
                    /* disable 4 lines ...
                    if (table.getUniqueConstraintForColumns(c.core.mainCols)
                            != null) {
                        throw Error.error(ErrorCode.X_42522);
                    }
                    ... disabled 4 lines */
                    // End of VoltDB extension

                    // create an autonamed index
                    indexName = session.database.nameManager.newAutoName("IDX",
                            c.getName().name, table.getSchemaName(),
                            table.getName(), SchemaObject.INDEX);

                    Index index = table.createAndAddIndexStructure(session,
                        indexName, c.core.mainCols, null, null, true, true,
                        false);
                    Constraint newconstraint = new Constraint(c.getName(),
                        table, index, SchemaObject.ConstraintTypes.UNIQUE);
                    // A VoltDB extension to support indexed expressions and the assume unique attribute
                    index.withExpressions(c.voltIndexedExprs)
                    .setAssumeUnique(c.voltAssumeUnique);

                    newconstraint.voltWithExpressions(c.voltIndexedExprs)
                    .voltSetAssumeUnique(c.voltAssumeUnique);
                    // End of VoltDB extension

                    table.addConstraint(newconstraint);

                    if (addToSchema) {
                        session.database.schemaManager.addSchemaObject(
                            newconstraint);
                    }

                    break;
                }
                case SchemaObject.ConstraintTypes.FOREIGN_KEY : {
                    addForeignKey(session, table, c, constraintList);

                    break;
                }
                case SchemaObject.ConstraintTypes.CHECK : {
                    try {
                        c.prepareCheckConstraint(session, table);
                    } catch (HsqlException e) {
                        if (session.isProcessingScript()) {
                            break;
                        }

                        throw e;
                    }

                    table.addConstraint(c);

                    if (c.isNotNull()) {
                        ColumnSchema column =
                            table.getColumn(c.notNullColumnIndex);

                        column.setNullable(false);
                        table.setColumnTypeVars(c.notNullColumnIndex);
                    }

                    if (addToSchema) {
                        session.database.schemaManager.addSchemaObject(c);
                    }

                    break;
                }
                // A VoltDB extension to support LIMIT PARTITION ROWS
                case SchemaObject.ConstraintTypes.LIMIT : {
                    table.addConstraint(c);
                    session.database.schemaManager.addSchemaObject(c);
                    break;
                }
                // End of VoltDB extension
            }
        }

        return table;
    }

    static void addForeignKey(Session session, Table table, Constraint c,
                              HsqlArrayList constraintList) {

        HsqlName mainTableName = c.getMainTableName();

        if (mainTableName == table.getName()) {
            c.core.mainTable = table;
        } else {
            Table mainTable =
                session.database.schemaManager.findUserTable(session,
                    mainTableName.name, mainTableName.schema.name);

            if (mainTable == null) {
                if (constraintList == null) {
                    throw Error.error(ErrorCode.X_42501, mainTableName.name);
                }

                constraintList.add(c);

                return;
            }

            c.core.mainTable = mainTable;
        }

        c.setColumnsIndexes(table);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.checkCreateForeignKey(c);

        Constraint uniqueConstraint =
            c.core.mainTable.getUniqueConstraintForColumns(c.core.mainCols);

        if (uniqueConstraint == null) {
            throw Error.error(ErrorCode.X_42523);
        }

        Index mainIndex = uniqueConstraint.getMainIndex();
        boolean isForward = c.core.mainTable.getSchemaName()
                            != table.getSchemaName();
        int offset = session.database.schemaManager.getTableIndex(table);

        if (offset != -1
                && offset
                   < session.database.schemaManager.getTableIndex(
                       c.core.mainTable)) {
            isForward = true;
        }

        HsqlName refIndexName = session.database.nameManager.newAutoName("IDX",
            table.getSchemaName(), table.getName(), SchemaObject.INDEX);
        Index index = table.createAndAddIndexStructure(session, refIndexName,
            c.core.refCols, null, null, false, true, isForward);
        HsqlName mainName = session.database.nameManager.newAutoName("REF",
            c.getName().name, table.getSchemaName(), table.getName(),
            SchemaObject.INDEX);

        c.core.uniqueName = uniqueConstraint.getName();
        c.core.mainName   = mainName;
        c.core.mainIndex  = mainIndex;
        c.core.refTable   = table;
        c.core.refName    = c.getName();
        c.core.refIndex   = index;
        c.isForward       = isForward;

        table.addConstraint(c);
        c.core.mainTable.addConstraint(new Constraint(mainName, c));
        session.database.schemaManager.addSchemaObject(c);
    }

    private Constraint readFKReferences(Table refTable,
                                        HsqlName constraintName,
                                        OrderedHashSet refColSet) {

        HsqlName       mainTableName;
        OrderedHashSet mainColSet = null;

        readThis(Tokens.REFERENCES);

        HsqlName schema;

        if (token.namePrefix == null) {
            schema = refTable.getSchemaName();
        } else {
            schema =
                database.schemaManager.getSchemaHsqlName(token.namePrefix);
        }

        if (refTable.getSchemaName() == schema
                && refTable.getName().name.equals(token.tokenString)) {
            mainTableName = refTable.getName();

            read();
        } else {
            mainTableName = readFKTableName(schema);
        }

        if (token.tokenType == Tokens.OPENBRACKET) {
            mainColSet = readColumnNames(false);
        }

        int matchType = OpTypes.MATCH_SIMPLE;

        if (token.tokenType == Tokens.MATCH) {
            read();

            switch (token.tokenType) {

                case Tokens.SIMPLE :
                    read();
                    break;

                case Tokens.PARTIAL :
                    throw super.unsupportedFeature();
                case Tokens.FULL :
                    read();

                    matchType = OpTypes.MATCH_FULL;
                    break;

                default :
                    throw unexpectedToken();
            }
        }

        // -- In a while loop we parse a maximium of two
        // -- "ON" statements following the foreign key
        // -- definition this can be
        // -- ON [UPDATE|DELETE] [NO ACTION|RESTRICT|CASCADE|SET [NULL|DEFAULT]]
        int deleteAction      = SchemaObject.ReferentialAction.NO_ACTION;
        int updateAction      = SchemaObject.ReferentialAction.NO_ACTION;
        OrderedIntHashSet set = new OrderedIntHashSet();

        while (token.tokenType == Tokens.ON) {
            read();

            if (!set.add(token.tokenType)) {
                throw unexpectedToken();
            }

            if (token.tokenType == Tokens.DELETE) {
                read();

                if (token.tokenType == Tokens.SET) {
                    read();

                    switch (token.tokenType) {

                        case Tokens.DEFAULT : {
                            read();

                            deleteAction =
                                SchemaObject.ReferentialAction.SET_DEFAULT;

                            break;
                        }
                        case Tokens.NULL :
                            read();

                            deleteAction =
                                SchemaObject.ReferentialAction.SET_NULL;
                            break;

                        default :
                            throw unexpectedToken();
                    }
                } else if (token.tokenType == Tokens.CASCADE) {
                    read();

                    deleteAction = SchemaObject.ReferentialAction.CASCADE;
                } else if (token.tokenType == Tokens.RESTRICT) {
                    read();
                } else {
                    readThis(Tokens.NO);
                    readThis(Tokens.ACTION);
                }
            } else if (token.tokenType == Tokens.UPDATE) {
                read();

                if (token.tokenType == Tokens.SET) {
                    read();

                    switch (token.tokenType) {

                        case Tokens.DEFAULT : {
                            read();

                            updateAction =
                                SchemaObject.ReferentialAction.SET_DEFAULT;

                            break;
                        }
                        case Tokens.NULL :
                            read();

                            updateAction =
                                SchemaObject.ReferentialAction.SET_NULL;
                            break;

                        default :
                            throw unexpectedToken();
                    }
                } else if (token.tokenType == Tokens.CASCADE) {
                    read();

                    updateAction = SchemaObject.ReferentialAction.CASCADE;
                } else if (token.tokenType == Tokens.RESTRICT) {
                    read();
                } else {
                    readThis(Tokens.NO);
                    readThis(Tokens.ACTION);
                }
            } else {
                throw unexpectedToken();
            }
        }

        if (constraintName == null) {
            constraintName = database.nameManager.newAutoName("FK",
                    refTable.getSchemaName(), refTable.getName(),
                    SchemaObject.CONSTRAINT);
        }

        return new Constraint(constraintName, refTable.getName(), refColSet,
                              mainTableName, mainColSet,
                              SchemaObject.ConstraintTypes.FOREIGN_KEY,
                              deleteAction, updateAction, matchType);
    }

    private HsqlName readFKTableName(HsqlName schema) {

        HsqlName name;

        checkIsSchemaObjectName();

        Table table = database.schemaManager.findUserTable(session,
            token.tokenString, schema.name);

        if (table == null) {
            name = database.nameManager.newHsqlName(schema, token.tokenString,
                    isDelimitedIdentifier(), SchemaObject.TABLE);
        } else {
            name = table.getName();
        }

        read();

        return name;
    }

    StatementSchema compileCreateView(boolean alter, boolean orReplace) {

        read();

        HsqlName name = readNewSchemaObjectName(SchemaObject.VIEW, true);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());
        checkSchemaUpdateAuthorisation(name.schema);

        HsqlName[] colList = null;

        if (token.tokenType == Tokens.OPENBRACKET) {
            try {
                colList = readColumnNames(name);
            } catch (HsqlException e) {
                if (session.isProcessingScript()
                        && database.getProperties().isVersion18()) {
                    while (token.tokenType != Tokens.AS) {
                        read();
                    }
                } else {
                    throw e;
                }
            }
        }

        readThis(Tokens.AS);
        startRecording();

        int             position = getPosition();
        QueryExpression queryExpression;

        try {
            queryExpression = XreadQueryExpression();
        } catch (HsqlException e) {
            queryExpression = XreadJoinedTableAsView();
        }

        Token[] tokenisedStatement = getRecordedStatement();
        int     check              = SchemaObject.ViewCheckModes.CHECK_NONE;

        if (token.tokenType == Tokens.WITH) {
            read();

            check = SchemaObject.ViewCheckModes.CHECK_CASCADE;

            if (readIfThis(Tokens.LOCAL)) {
                check = SchemaObject.ViewCheckModes.CHECK_LOCAL;
            } else {
                readIfThis(Tokens.CASCADED);
            }

            readThis(Tokens.CHECK);
            readThis(Tokens.OPTION);
        }

        View view = new View(database, name, colList, check);

        queryExpression.setView(view);
        queryExpression.resolve(session);
        view.setStatement(Token.getSQL(tokenisedStatement));

        StatementQuery s = new StatementQuery(session, queryExpression,
                                              compileContext);
        String     fullSQL        = getLastPart();
        Object[]   args           = new Object[]{ view };
        int        type           = alter ? StatementTypes.ALTER_VIEW
                                          : StatementTypes.CREATE_VIEW;
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;

        return new StatementSchema(fullSQL, type, args, s.readTableNames,
                                   writeLockNames);
    }

    StatementSchema compileCreateSequence() {

        read();

        boolean ifNot = false;

        if (token.tokenType == Tokens.IF) {
            int position = getPosition();

            read();

            if (token.tokenType == Tokens.NOT) {
                read();
                readThis(Tokens.EXISTS);

                ifNot = true;
            } else {
                rewind(position);
            }
        }

        /*
                CREATE SEQUENCE <name>
                [AS {INTEGER | BIGINT}]
                [START WITH <value>]
                [INCREMENT BY <value>]
        */
        HsqlName name = readNewSchemaObjectName(SchemaObject.SEQUENCE, false);
        NumberSequence sequence = new NumberSequence(name, Type.SQL_INTEGER);

        readSequenceOptions(sequence, true, false, false);

        String     sql            = getLastPart();
        Object[]   args           = new Object[] {
            sequence, Boolean.valueOf(ifNot)
        };
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;

        return new StatementSchema(sql, StatementTypes.CREATE_SEQUENCE, args,
                                   null, writeLockNames);
    }

    StatementSchema compileCreateDomain() {

        UserTypeModifier userTypeModifier = null;
        HsqlName         name;

        read();

        name = readNewSchemaObjectName(SchemaObject.DOMAIN, false);

        readIfThis(Tokens.AS);

        Type       type          = readTypeDefinition(true, false).duplicate();
        Expression defaultClause = null;

        if (readIfThis(Tokens.DEFAULT)) {
            defaultClause = readDefaultClause(type);
        }

        userTypeModifier = new UserTypeModifier(name, SchemaObject.DOMAIN,
                type);

        userTypeModifier.setDefaultClause(defaultClause);

        type.userTypeModifier = userTypeModifier;

        HsqlArrayList tempConstraints = new HsqlArrayList();

        compileContext.currentDomain = type;

        while (true) {
            boolean end = false;

            switch (token.tokenType) {

                case Tokens.CONSTRAINT :
                case Tokens.CHECK :
                    readConstraint(type, tempConstraints);
                    break;

                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }
        }

        compileContext.currentDomain = null;

        for (int i = 0; i < tempConstraints.size(); i++) {
            Constraint c = (Constraint) tempConstraints.get(i);

            c.prepareCheckConstraint(session, null);
            userTypeModifier.addConstraint(c);
        }

        String     sql            = getLastPart();
        Object[]   args           = new Object[]{ type };
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;

        return new StatementSchema(sql, StatementTypes.CREATE_DOMAIN, args,
                                   null, writeLockNames);
    }

    StatementSchema compileCreateType(boolean orReplace) {

        read();

        HsqlName name = readNewSchemaObjectName(SchemaObject.TYPE, false);

        readThis(Tokens.AS);

        Type type = readTypeDefinition(true, false).duplicate();

        readIfThis(Tokens.FINAL);

        UserTypeModifier userTypeModifier = new UserTypeModifier(name,
            SchemaObject.TYPE, type);

        type.userTypeModifier = userTypeModifier;

        String     sql            = getLastPart();
        Object[]   args           = new Object[]{ type };
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;

        return new StatementSchema(sql, StatementTypes.CREATE_TYPE, args,
                                   null, writeLockNames);
    }

    StatementSchema compileCreateCharacterSet() {

        read();
        readThis(Tokens.SET);

        HsqlName name = readNewSchemaObjectName(SchemaObject.CHARSET, false);

        readIfThis(Tokens.AS);
        readThis(Tokens.GET);

        String schema = token.namePrefix;
        Charset source =
            (Charset) database.schemaManager.getCharacterSet(session,
                token.tokenString, schema);

        read();

        if (token.tokenType == Tokens.COLLATION) {
            read();
            readThis(Tokens.FROM);
            readThis(Tokens.DEFAULT);
        }

        Charset charset = new Charset(name);

        charset.base = source.getName();

        String     sql            = getLastPart();
        Object[]   args           = new Object[]{ charset };
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;

        return new StatementSchema(sql, StatementTypes.CREATE_CHARACTER_SET,
                                   args, null, writeLockNames);
    }

    StatementSchema compileCreateCollation() {

        read();

        HsqlName name = readNewSchemaObjectName(SchemaObject.COLLATION, false);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());
        readThis(Tokens.FOR);

        HsqlName charsetName = readNewSchemaObjectName(SchemaObject.CHARSET,
            false);

        readThis(Tokens.FROM);

        HsqlName sourceName = readNewSchemaObjectName(SchemaObject.COLLATION,
            false);
        Boolean padSpace = null;

        if (readIfThis(Tokens.NO)) {
            readThis(Tokens.PAD);

            padSpace = Boolean.FALSE;
        } else if (readIfThis(Tokens.PAD)) {
            readThis(Tokens.SPACE);

            padSpace = Boolean.TRUE;
        }

        String schemaName = charsetName.schema == null ? null
                                                       : charsetName.schema
                                                           .name;
        Charset charset =
            (Charset) database.schemaManager.getCharacterSet(session,
                charsetName.name, schemaName);

        if (charset == null) {
            throw Error.error(ErrorCode.X_42501,
                              charsetName.getSchemaQualifiedStatementName());
        }

        schemaName = sourceName.schema == null ? null
                                               : sourceName.schema.name;

        Collation source = database.schemaManager.getCollation(session,
            sourceName.name, schemaName);
        Collation  collation = new Collation(name, source, charset, padSpace);
        String     sql            = getLastPart();
        Object[]   args           = new Object[]{ collation };
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;

        return new StatementSchema(sql, StatementTypes.CREATE_COLLATION, args,
                                   null, writeLockNames);
    }

    StatementSchema compileCreateAlias() {

        HsqlName  name     = null;
        Routine[] routines = null;
        String    alias;
        String    methodFQN = null;

        if (!session.isProcessingScript()) {
            throw super.unsupportedFeature();
        }

        read();

        try {
            alias = token.tokenString;

            read();
            readThis(Tokens.FOR);

            methodFQN = token.tokenString;

            read();
        } catch (HsqlException e) {
            alias = null;
        }

        if (alias != null) {
            HsqlName schema =
                database.schemaManager.getDefaultSchemaHsqlName();

            name = database.nameManager.newHsqlName(schema, alias,
                    SchemaObject.FUNCTION);

            Method[] methods = Routine.getMethods(methodFQN);

            routines = Routine.newRoutines(session, methods);
        }

        String     sql            = getLastPart();
        Object[]   args           = new Object[] {
            name, routines
        };
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;

        return new StatementSchema(sql, StatementTypes.CREATE_ALIAS, args,
                                   null, writeLockNames);
    }

    StatementSchema compileCreateTrigger(boolean orReplace) {

        Table          table;
        Boolean        isForEachRow = null;
        boolean        isNowait     = false;
        boolean        hasQueueSize = false;
        int            queueSize    = 0;
        int            beforeOrAfterType;
        int            operationType;
        String         className;
        TriggerDef     td;
        HsqlName       name;
        HsqlName       otherName           = null;
        OrderedHashSet columns             = null;
        int[]          updateColumnIndexes = null;

        read();

        name = readNewSchemaObjectName(SchemaObject.TRIGGER, true);

        switch (token.tokenType) {

            case Tokens.INSTEAD :
                beforeOrAfterType = TriggerDef.getTiming(Tokens.INSTEAD);

                read();
                readThis(Tokens.OF);
                break;

            case Tokens.BEFORE :
            case Tokens.AFTER :
                beforeOrAfterType = TriggerDef.getTiming(token.tokenType);

                read();
                break;

            default :
                throw unexpectedToken();
        }

        switch (token.tokenType) {

            case Tokens.INSERT :
            case Tokens.DELETE :
                operationType = TriggerDef.getOperationType(token.tokenType);

                read();
                break;

            case Tokens.UPDATE :
                operationType = TriggerDef.getOperationType(token.tokenType);

                read();

                if (token.tokenType == Tokens.OF
                        && beforeOrAfterType != TriggerDef.INSTEAD) {
                    read();

                    columns = new OrderedHashSet();

                    readColumnNameList(columns, null, false);
                }
                break;

            default :
                throw unexpectedToken();
        }

        readThis(Tokens.ON);

        table = readTableName();

        if (token.tokenType == Tokens.BEFORE) {
            read();
            checkIsSimpleName();

            otherName = readNewSchemaObjectName(SchemaObject.TRIGGER, true);
        }

        name.setSchemaIfNull(table.getSchemaName());
        checkSchemaUpdateAuthorisation(name.schema);

        if (beforeOrAfterType == TriggerDef.INSTEAD) {
            if (!table.isView()
                    || ((View) table).getCheckOption()
                       == SchemaObject.ViewCheckModes.CHECK_CASCADE) {
                throw Error.error(ErrorCode.X_42538, name.schema.name);
            }
        } else {
            if (table.isView()) {
                throw Error.error(ErrorCode.X_42538, name.schema.name);
            }
        }

        if (name.schema != table.getSchemaName()) {
            throw Error.error(ErrorCode.X_42505, name.schema.name);
        }

        name.parent = table.getName();

        database.schemaManager.checkSchemaObjectNotExists(name);

        if (columns != null) {
            updateColumnIndexes = table.getColumnIndexes(columns);

            for (int i = 0; i < updateColumnIndexes.length; i++) {
                if (updateColumnIndexes[i] == -1) {
                    throw Error.error(ErrorCode.X_42544,
                                      (String) columns.get(i));
                }
            }
        }

        Expression      condition    = null;
        SimpleName      oldTableName = null;
        SimpleName      newTableName = null;
        SimpleName      oldRowName   = null;
        SimpleName      newRowName   = null;
        Table[]         transitions  = new Table[4];
        RangeVariable[] rangeVars    = new RangeVariable[4];
        String          conditionSQL = null;
        RangeGroup[] rangeGroups = new RangeGroup[]{
            new RangeGroup.RangeGroupSimple(rangeVars, false) };

        if (token.tokenType == Tokens.REFERENCING) {
            read();

            if (token.tokenType != Tokens.OLD
                    && token.tokenType != Tokens.NEW) {
                throw unexpectedToken();
            }

            while (true) {
                if (token.tokenType == Tokens.OLD) {
                    if (operationType == StatementTypes.INSERT) {
                        throw unexpectedToken();
                    }

                    read();

                    if (token.tokenType == Tokens.TABLE) {
                        if (Boolean.TRUE.equals(isForEachRow)
                                || oldTableName != null
                                || beforeOrAfterType == TriggerDef.BEFORE) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();
                        read();

                        oldTableName = HsqlNameManager.getSimpleName(
                            token.tokenString, token.isDelimitedIdentifier);

                        SimpleName n = oldTableName;

                        if (n.equals(newTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        isForEachRow = Boolean.FALSE;

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n.name,
                            isDelimitedIdentifier(), SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.OLD_TABLE] = transition;
                        rangeVars[TriggerDef.OLD_TABLE]   = range;
                    } else {
                        if (Boolean.FALSE.equals(isForEachRow)
                                || oldRowName != null) {
                            throw unexpectedToken();
                        }

                        readIfThis(Tokens.ROW);
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        oldRowName = HsqlNameManager.getSimpleName(
                            token.tokenString, token.isDelimitedIdentifier);

                        read();

                        SimpleName n = oldRowName;

                        if (n.equals(newTableName) || n.equals(oldTableName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        isForEachRow = Boolean.TRUE;

                        RangeVariable range =
                            new RangeVariable(table.columnList, oldRowName,
                                              false,
                                              RangeVariable.TRANSITION_RANGE);

                        range.rangePosition             = TriggerDef.OLD_ROW;
                        transitions[TriggerDef.OLD_ROW] = null;
                        rangeVars[TriggerDef.OLD_ROW]   = range;
                    }
                } else if (token.tokenType == Tokens.NEW) {
                    if (operationType == StatementTypes.DELETE_WHERE) {
                        throw unexpectedToken();
                    }

                    read();

                    if (token.tokenType == Tokens.TABLE) {
                        if (Boolean.TRUE.equals(isForEachRow)
                                || newTableName != null
                                || beforeOrAfterType == TriggerDef.BEFORE) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        newTableName = HsqlNameManager.getSimpleName(
                            token.tokenString, token.isDelimitedIdentifier);

                        read();

                        isForEachRow = Boolean.FALSE;

                        SimpleName n = newTableName;

                        if (n.equals(oldTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n.name,
                            isDelimitedIdentifier(), SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.NEW_TABLE] = transition;
                        rangeVars[TriggerDef.NEW_TABLE]   = range;
                    } else {
                        if (Boolean.FALSE.equals(isForEachRow)
                                || newRowName != null) {
                            throw unexpectedToken();
                        }

                        readIfThis(Tokens.ROW);
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        newRowName = HsqlNameManager.getSimpleName(
                            token.tokenString, token.isDelimitedIdentifier);

                        read();

                        SimpleName n = newRowName;

                        if (n.equals(oldTableName) || n.equals(newTableName)
                                || n.equals(oldRowName)) {
                            throw unexpectedToken();
                        }

                        isForEachRow = Boolean.TRUE;

                        RangeVariable range =
                            new RangeVariable(table.columnList, newRowName,
                                              false,
                                              RangeVariable.TRANSITION_RANGE);

                        range.rangePosition             = TriggerDef.NEW_ROW;
                        transitions[TriggerDef.NEW_ROW] = null;
                        rangeVars[TriggerDef.NEW_ROW]   = range;
                    }
                } else {
                    break;
                }
            }
        }

        if (Boolean.TRUE.equals(isForEachRow)
                && token.tokenType != Tokens.FOR) {
            throw unexpectedTokenRequire(Tokens.T_FOR);
        }

        if (token.tokenType == Tokens.FOR) {
            read();
            readThis(Tokens.EACH);

            if (token.tokenType == Tokens.ROW) {
                if (Boolean.FALSE.equals(isForEachRow)) {
                    throw unexpectedToken();
                }

                isForEachRow = Boolean.TRUE;
            } else if (token.tokenType == Tokens.STATEMENT) {
                if (Boolean.TRUE.equals(isForEachRow)
                        || beforeOrAfterType == TriggerDef.BEFORE) {
                    throw unexpectedToken();
                }

                isForEachRow = Boolean.FALSE;
            } else {
                throw unexpectedToken();
            }

            read();
        }

        //
        if (rangeVars[TriggerDef.OLD_TABLE] != null) {}

        if (rangeVars[TriggerDef.NEW_TABLE] != null) {}

        //
        if (Tokens.T_QUEUE.equals(token.tokenString)) {
            read();

            queueSize    = readInteger();
            hasQueueSize = true;
        }

        if (Tokens.T_NOWAIT.equals(token.tokenString)) {
            read();

            isNowait = true;
        }

        if (token.tokenType == Tokens.WHEN
                && beforeOrAfterType != TriggerDef.INSTEAD) {
            read();
            readThis(Tokens.OPENBRACKET);

            int position = getPosition();

            isCheckOrTriggerCondition = true;
            condition                 = XreadBooleanValueExpression();
            conditionSQL              = getLastPart(position);
            isCheckOrTriggerCondition = false;

            readThis(Tokens.CLOSEBRACKET);

            HsqlList unresolved = condition.resolveColumnReferences(session,
                rangeGroups[0], rangeGroups, null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);

            if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        if (isForEachRow == null) {
            isForEachRow = Boolean.FALSE;
        }

        if (token.tokenType == Tokens.CALL) {
            int position = getPosition();

            try {
                read();
                checkIsSimpleName();
                checkIsDelimitedIdentifier();

                className = token.tokenString;

                read();

                if (token.tokenType == Tokens.OPENBRACKET) {
                    throw unexpectedToken();
                }

                td = new TriggerDef(name, beforeOrAfterType, operationType,
                                    isForEachRow.booleanValue(), table,
                                    transitions, rangeVars, condition,
                                    conditionSQL, updateColumnIndexes,
                                    className, isNowait, queueSize);

                String     sql            = getLastPart();
                Object[]   args           = new Object[] {
                    td, otherName
                };
                HsqlName[] writeLockNames = new HsqlName[] {
                    database.getCatalogName(), table.getName()
                };

                return new StatementSchema(sql, StatementTypes.CREATE_TRIGGER,
                                           args, null, writeLockNames);
            } catch (HsqlException e) {
                rewind(position);
            }
        }

        //
        if (hasQueueSize) {
            throw unexpectedToken(Tokens.T_QUEUE);
        }

        if (isNowait) {
            throw unexpectedToken(Tokens.T_NOWAIT);
        }

        Routine routine = compileTriggerRoutine(table, rangeVars,
            beforeOrAfterType, operationType);

        td = new TriggerDefSQL(name, beforeOrAfterType, operationType,
                               isForEachRow.booleanValue(), table,
                               transitions, rangeVars, condition,
                               conditionSQL, updateColumnIndexes, routine);

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            td, otherName
        };

        return new StatementSchema(sql, StatementTypes.CREATE_TRIGGER, args,
                                   null, new HsqlName[] {
            database.getCatalogName(), table.getName()
        });
    }

    Routine compileTriggerRoutine(Table table, RangeVariable[] ranges,
                                  int beforeOrAfter, int operation) {

        int impact = (beforeOrAfter == TriggerDef.BEFORE) ? Routine.READS_SQL
                                                          : Routine
                                                              .MODIFIES_SQL;
        Routine routine = new Routine(table, ranges, impact, beforeOrAfter,
                                      operation);

        startRecording();

        StatementCompound parent =
            new StatementCompound(StatementTypes.BEGIN_END, null);

        parent.rangeVariables = ranges;

        Statement statement = compileSQLProcedureStatementOrNull(routine,
            null);

        if (statement == null) {
            throw unexpectedToken();
        }

        Token[] tokenisedStatement = getRecordedStatement();
        String  sql                = Token.getSQL(tokenisedStatement);

        statement.setSQL(sql);
        routine.setProcedure(statement);
        routine.resolve(session);

        return routine;
    }

    /**
     * Responsible for handling the creation of table columns during the process
     * of executing CREATE TABLE or ADD COLUMN etc. statements.
     *
     * @param table this table
     * @param hsqlName column name
     * @param constraintList list of constraints
     * @return a Column object with indicated attributes
     */
    ColumnSchema readColumnDefinitionOrNull(Table table, HsqlName hsqlName,
            HsqlArrayList constraintList) {

        boolean        isGenerated     = false;
        boolean        isIdentity      = false;
        boolean        isPKIdentity    = false;
        boolean        generatedAlways = false;
        Expression     generateExpr    = null;
        boolean        isNullable      = true;
        Expression     defaultExpr     = null;
        Type           typeObject      = null;
        NumberSequence sequence        = null;

        switch (token.tokenType) {

            case Tokens.GENERATED : {
                read();
                readThis(Tokens.ALWAYS);

                isGenerated     = true;
                generatedAlways = true;

                // not yet
                throw unexpectedToken(Tokens.T_GENERATED);
            }
            case Tokens.IDENTITY : {
                read();

                isIdentity   = true;
                isPKIdentity = true;
                typeObject   = Type.SQL_INTEGER;
                sequence     = new NumberSequence(null, 0, 1, typeObject);

                break;
            }
            case Tokens.COMMA : {
                return null;
            }
            case Tokens.CLOSEBRACKET : {
                return null;
            }
            default : {
                if (token.isUndelimitedIdentifier) {
                    if (Tokens.T_SERIAL.equals(token.tokenString)) {
                        if (database.sqlSyntaxMys) {
                            read();

                            isIdentity   = true;
                            isPKIdentity = true;
                            typeObject   = Type.SQL_BIGINT;
                            sequence = new NumberSequence(null, 1, 1,
                                                          typeObject);

                            break;
                        } else if (database.sqlSyntaxPgs) {
                            read();

                            isIdentity = true;
                            typeObject = Type.SQL_INTEGER;
                            sequence = new NumberSequence(null, 1, 1,
                                                          typeObject);

                            break;
                        }
                    } else if (Tokens.T_BIGSERIAL.equals(token.tokenString)) {
                        if (database.sqlSyntaxPgs) {
                            read();

                            isIdentity   = true;
                            isPKIdentity = true;
                            typeObject   = Type.SQL_BIGINT;
                            sequence = new NumberSequence(null, 1, 1,
                                                          typeObject);

                            break;
                        }
                    }
                }

                typeObject = readTypeDefinition(true, true);
            }
        }

        if (!isGenerated && !isIdentity) {
            if (database.sqlSyntaxMys) {
                switch (token.tokenType) {

                    case Tokens.NULL :
                        read();
                        break;

                    case Tokens.NOT :
                        read();
                        readThis(Tokens.NULL);

                        isNullable = false;
                        break;

                    default :
                }
            }

            switch (token.tokenType) {

                case Tokens.WITH : {
                    if (database.sqlSyntaxDb2) {
                        read();
                    } else {
                        throw unexpectedToken();
                    }
                }

                // fall through
                case Tokens.DEFAULT : {
                    read();

                    defaultExpr = readDefaultClause(typeObject);

                    if (defaultExpr.opType == OpTypes.SEQUENCE) {
                        if (database.sqlSyntaxPgs) {
                            sequence =
                                ((ExpressionColumn) defaultExpr).sequence;
                            defaultExpr = null;
                            isIdentity  = true;
                        }
                    }

                    break;
                }
                case Tokens.GENERATED : {
                    read();

                    if (token.tokenType == Tokens.BY) {
                        read();
                        readThis(Tokens.DEFAULT);
                    } else {
                        readThis(Tokens.ALWAYS);

                        generatedAlways = true;
                    }

                    readThis(Tokens.AS);

                    if (token.tokenType == Tokens.IDENTITY) {
                        read();

                        sequence = new NumberSequence(null, typeObject);

                        sequence.setAlways(generatedAlways);

                        if (token.tokenType == Tokens.OPENBRACKET) {
                            read();
                            readSequenceOptions(sequence, false, false, true);
                            readThis(Tokens.CLOSEBRACKET);
                        }

                        isIdentity = true;
                    } else if (token.tokenType == Tokens.OPENBRACKET) {
                        if (!generatedAlways) {
                            throw super.unexpectedTokenRequire(
                                Tokens.T_IDENTITY);
                        }

                        isGenerated = true;
                    } else if (token.tokenType == Tokens.SEQUENCE) {
                        if (generatedAlways) {
                            throw unexpectedToken();
                        }

                        read();

                        if (token.namePrefix != null) {
                            if (!token.namePrefix.equals(
                                    table.getSchemaName().name)) {
                                throw super.unexpectedToken(token.namePrefix);
                            }
                        }

                        sequence = database.schemaManager.getSequence(
                            token.tokenString, table.getSchemaName().name,
                            true);
                        isIdentity = true;

                        read();
                    }

                    break;
                }
                case Tokens.IDENTITY : {
                    read();

                    isIdentity   = true;
                    isPKIdentity = true;
                    sequence     = new NumberSequence(null, 0, 1, typeObject);
                }
                break;
            }
        }

        if (isGenerated) {
            readThis(Tokens.OPENBRACKET);

            generateExpr = XreadValueExpression();

            readThis(Tokens.CLOSEBRACKET);
        }

        if (!isGenerated && !isIdentity) {
            if (database.sqlSyntaxMys) {
                if (token.isUndelimitedIdentifier
                        && Tokens.T_AUTO_INCREMENT.equals(token.tokenString)) {
                    read();

                    isIdentity = true;
                    sequence   = new NumberSequence(null, 0, 1, typeObject);
                }
            }
        }

        ColumnSchema column = new ColumnSchema(hsqlName, typeObject,
                                               isNullable, false, defaultExpr);

        column.setGeneratingExpression(generateExpr);
        readColumnConstraints(table, column, constraintList);

        if (token.tokenType == Tokens.IDENTITY && !isIdentity) {
            read();

            isIdentity   = true;
            isPKIdentity = true;
            sequence     = new NumberSequence(null, 0, 1, typeObject);
        }

        // non-standard GENERATED after constraints
        if (token.tokenType == Tokens.GENERATED && !isIdentity
                && !isGenerated) {
            read();

            if (token.tokenType == Tokens.BY) {
                read();
                readThis(Tokens.DEFAULT);
            } else {
                readThis(Tokens.ALWAYS);

                generatedAlways = true;
            }

            readThis(Tokens.AS);
            readThis(Tokens.IDENTITY);

            sequence = new NumberSequence(null, typeObject);

            sequence.setAlways(generatedAlways);

            if (token.tokenType == Tokens.OPENBRACKET) {
                read();
                readSequenceOptions(sequence, false, false, true);
                readThis(Tokens.CLOSEBRACKET);
            }

            isIdentity = true;
        }

        if (isIdentity) {
            column.setIdentity(sequence);
        }

        if (isPKIdentity && !column.isPrimaryKey()) {
            OrderedHashSet set = new OrderedHashSet();

            set.add(column.getName().name);

            HsqlName constName = database.nameManager.newAutoName("PK",
                table.getSchemaName(), table.getName(),
                SchemaObject.CONSTRAINT);
            Constraint c =
                new Constraint(constName, set,
                               SchemaObject.ConstraintTypes.PRIMARY_KEY);

            constraintList.set(0, c);
            column.setPrimaryKey(true);
        }

        if (database.sqlSyntaxPgs && token.tokenType == Tokens.DEFAULT
                && column.getDefaultExpression() == null
                && column.getIdentitySequence() == null) {
            read();

            defaultExpr = readDefaultClause(typeObject);

            if (defaultExpr.opType == OpTypes.SEQUENCE) {
                sequence    = ((ExpressionColumn) defaultExpr).sequence;
                defaultExpr = null;
            }

            column.setDefaultExpression(defaultExpr);
            column.setIdentity(sequence);
        }

        return column;
    }

    /**
     * A comma after START WITH is accepted for 1.8.x compatibility
     */
    private void readSequenceOptions(NumberSequence sequence,
                                     boolean withType, boolean isAlter,
                                     boolean allowComma) {

        OrderedIntHashSet set = new OrderedIntHashSet();

        while (true) {
            boolean end = false;

            if (set.contains(token.tokenType)) {
                throw unexpectedToken();
            }

            switch (token.tokenType) {

                case Tokens.AS : {
                    if (withType) {
                        set.add(token.tokenType);
                        read();

                        Type type = readTypeDefinition(false, true);

                        sequence.setDefaults(sequence.getName(), type);

                        break;
                    }

                    throw unexpectedToken();
                }
                case Tokens.START : {
                    set.add(token.tokenType);
                    read();
                    readThis(Tokens.WITH);

                    long value = readBigint();

                    sequence.setStartValueNoCheck(value);

                    if (allowComma) {
                        readIfThis(Tokens.COMMA);
                    }

                    break;
                }
                case Tokens.RESTART : {
                    if (!isAlter) {
                        end = true;

                        break;
                    }

                    set.add(token.tokenType);
                    read();

                    if (readIfThis(Tokens.WITH)) {
                        long value = readBigint();

                        sequence.setCurrentValueNoCheck(value);
                    } else {
                        sequence.setStartValueDefault();
                    }

                    break;
                }
                case Tokens.INCREMENT : {
                    set.add(token.tokenType);
                    read();
                    readThis(Tokens.BY);

                    long value = readBigint();

                    sequence.setIncrement(value);

                    break;
                }
                case Tokens.NO : {
                    read();

                    if (set.contains(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    if (token.tokenType == Tokens.MAXVALUE) {
                        sequence.setDefaultMaxValue();
                    } else if (token.tokenType == Tokens.MINVALUE) {
                        sequence.setDefaultMinValue();
                    } else if (token.tokenType == Tokens.CYCLE) {
                        sequence.setCycle(false);
                    } else {
                        throw unexpectedToken();
                    }

                    set.add(token.tokenType);
                    read();

                    break;
                }
                case Tokens.MAXVALUE : {
                    set.add(token.tokenType);
                    read();

                    long value = readBigint();

                    sequence.setMaxValueNoCheck(value);

                    break;
                }
                case Tokens.MINVALUE : {
                    set.add(token.tokenType);
                    read();

                    long value = readBigint();

                    sequence.setMinValueNoCheck(value);

                    break;
                }
                case Tokens.CYCLE : {
                    set.add(token.tokenType);
                    read();
                    sequence.setCycle(true);

                    break;
                }
                default :
                    if ((database.sqlSyntaxOra || database.sqlSyntaxDb2)
                            && isSimpleName()) {
                        if (token.tokenString.equals("NOCACHE")
                                || token.tokenString.equals("NOCYCLE")
                                || token.tokenString.equals("NOMAXVALUE")
                                || token.tokenString.equals("NOMINVALUE")
                                || token.tokenString.equals("NOORDER")
                                || token.tokenString.equals("ORDER")) {
                            read();

                            break;
                        }

                        if (token.tokenString.equals("CACHE")) {
                            read();
                            readBigint();

                            break;
                        }
                    }

                    end = true;
                    break;
            }

            if (end) {
                break;
            }
        }

        sequence.checkValues();
    }

    /**
     * Reads and adds a table constraint definition to the list
     *
     * @param schemaObject table or domain
     * @param constraintList list of constraints
     */
    private void readConstraint(SchemaObject schemaObject,
                                HsqlArrayList constraintList) {

        HsqlName constName = null;

        if (token.tokenType == Tokens.CONSTRAINT) {
            read();

            constName =
                readNewDependentSchemaObjectName(schemaObject.getName(),
                                                 SchemaObject.CONSTRAINT);
        }

        // A VoltDB extension to support indexed expressions and the assume unique attribute
        boolean assumeUnique = false; // For VoltDB
        // End of VoltDB extension
        switch (token.tokenType) {

            case Tokens.PRIMARY : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();
                readThis(Tokens.KEY);

                Constraint mainConst;

                mainConst = (Constraint) constraintList.get(0);

                if (mainConst.constType
                        == SchemaObject.ConstraintTypes.PRIMARY_KEY) {
                    throw Error.error(ErrorCode.X_42532);
                }

                if (constName == null) {
                    constName = database.nameManager.newAutoName("PK",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                OrderedHashSet set = readColumnNames(false);
                Constraint c =
                    new Constraint(constName, set,
                                   SchemaObject.ConstraintTypes.PRIMARY_KEY);

                constraintList.set(0, c);

                break;
            }
            // A VoltDB extension to support indexed expressions and the assume unique attribute
            case Tokens.ASSUMEUNIQUE :
                assumeUnique = true;
                // fall through
            // End of VoltDB extension
            case Tokens.UNIQUE : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();

                if (database.sqlSyntaxMys) {
                    if (!readIfThis(Tokens.INDEX)) {
                        readIfThis(Tokens.KEY);
                    }
                }

                // A VoltDB extension to "readColumnNames(false)" to support indexed expressions.
                java.util.List<Expression> indexExprs = XreadExpressions(null);
                OrderedHashSet set = voltGetSimpleColumnNames(indexExprs);
                /* disable 1 line ...
                OrderedHashSet set = readColumnNames(false);
                ... disabled 1 line */
                // End of VoltDB extension

                if (constName == null) {
                    constName = database.nameManager.newAutoName("CT",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                // A VoltDB extension to support indexed expressions.
                boolean hasNonColumnExprs = false;
                if (set == null) {
                    hasNonColumnExprs = true;
                    set = voltGetBaseColumnNames(indexExprs);
                }
                // End of VoltDB extension
                Constraint c =
                    new Constraint(constName, set,
                                   SchemaObject.ConstraintTypes.UNIQUE);
                // A VoltDB extension to support indexed expressions and assume unique attribute.
                c.voltSetAssumeUnique(assumeUnique);
                if (hasNonColumnExprs) {
                    c = c.voltWithExpressions(indexExprs.toArray(new Expression[indexExprs.size()]));
                }
                // End of VoltDB extension

                constraintList.add(c);

                break;
            }
            case Tokens.FOREIGN : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();
                readThis(Tokens.KEY);

                OrderedHashSet set = readColumnNames(false);
                Constraint c = readFKReferences((Table) schemaObject,
                                                constName, set);

                constraintList.add(c);

                break;
            }
            case Tokens.CHECK : {
                read();

                if (constName == null) {
                    constName = database.nameManager.newAutoName("CT",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                Constraint c =
                    new Constraint(constName, null,
                                   SchemaObject.ConstraintTypes.CHECK);

                readCheckConstraintCondition(c);
                constraintList.add(c);

                break;
            }
            // A VoltDB extension to support LIMIT PARTITION ROWS
            case Tokens.LIMIT : {
                read();

                for (int i = 0;  i < constraintList.size(); i++) {
                    if (((Constraint)constraintList.get(i)).getConstraintType() ==
                            SchemaObject.ConstraintTypes.LIMIT) {
                        throw Error.error(ErrorCode.X_42524,
                                String.format("Multiple LIMIT PARTITION ROWS constraints on table %s are forbidden.", schemaObject.getName().name));
                    }
                }

                if (constName == null) {
                    constName = database.nameManager.newAutoName("LIMIT",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                Constraint c = new Constraint(constName, null, SchemaObject.ConstraintTypes.LIMIT);
                readLimitConstraintCondition(c);
                constraintList.add(c);
                break;
            }
            // End of VoltDB extension
            default : {
                if (constName != null) {
                    throw unexpectedToken();
                }
            }
        }
    }

    /**
     * Reads column constraints
     */
    void readColumnConstraints(Table table, ColumnSchema column,
                               HsqlArrayList constraintList) {

        boolean end                  = false;
        boolean hasNotNullConstraint = false;
        boolean hasNullNoiseWord     = false;
        boolean hasPrimaryKey        = false;

        while (true) {
            HsqlName constName = null;

            if (token.tokenType == Tokens.CONSTRAINT) {
                read();

                constName = readNewDependentSchemaObjectName(table.getName(),
                        SchemaObject.CONSTRAINT);
            }

            // A VoltDB extension to support indexed expressions and the assume unique attribute
            boolean assumeUnique = false; // For VoltDB
            // End of VoltDB extension
            switch (token.tokenType) {

                case Tokens.PRIMARY : {
                    if (hasNullNoiseWord || hasPrimaryKey) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.KEY);

                    Constraint existingConst =
                        (Constraint) constraintList.get(0);

                    if (existingConst.constType
                            == SchemaObject.ConstraintTypes.PRIMARY_KEY) {
                        throw Error.error(ErrorCode.X_42532);
                    }

                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("PK",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c = new Constraint(
                        constName, set,
                        SchemaObject.ConstraintTypes.PRIMARY_KEY);

                    constraintList.set(0, c);
                    column.setPrimaryKey(true);

                    hasPrimaryKey = true;

                    break;
                }
                // A VoltDB extension to support indexed expressions and the assume unique attribute
                case Tokens.ASSUMEUNIQUE :
                    assumeUnique = true;
                    // fall through
                // End of VoltDB extension
                case Tokens.UNIQUE : {
                    read();

                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("CT",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c =
                        new Constraint(constName, set,
                                       SchemaObject.ConstraintTypes.UNIQUE);
                    // A VoltDB extension to support indexed expressions and the assume unique attribute
                    c.voltSetAssumeUnique(assumeUnique);
                    // End of VoltDB extension

                    constraintList.add(c);

                    break;
                }
                case Tokens.FOREIGN : {
                    read();
                    readThis(Tokens.KEY);
                }

                // fall through
                case Tokens.REFERENCES : {
                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    Constraint c = readFKReferences(table, constName, set);

                    constraintList.add(c);

                    break;
                }
                case Tokens.CHECK : {
                    read();

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("CT",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c =
                        new Constraint(constName, null,
                                       SchemaObject.ConstraintTypes.CHECK);

                    readCheckConstraintCondition(c);

                    OrderedHashSet set = c.getCheckColumnExpressions();

                    for (int i = 0; i < set.size(); i++) {
                        ExpressionColumn e = (ExpressionColumn) set.get(i);

                        if (column.getName().name.equals(e.getColumnName())) {
                            if (e.getSchemaName() != null
                                    && !e.getSchemaName().equals(
                                        table.getSchemaName().name)) {
                                throw Error.error(ErrorCode.X_42505);
                            }
                        } else {
                            throw Error.error(ErrorCode.X_42501);
                        }
                    }

                    constraintList.add(c);

                    break;
                }
                case Tokens.NOT : {
                    if (hasNotNullConstraint || hasNullNoiseWord) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.NULL);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("CT",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c =
                        new Constraint(constName, null,
                                       SchemaObject.ConstraintTypes.CHECK);

                    c.check = new ExpressionLogical(column);

                    constraintList.add(c);

                    hasNotNullConstraint = true;

                    break;
                }
                case Tokens.NULL : {
                    if (hasNotNullConstraint || hasNullNoiseWord
                            || hasPrimaryKey) {
                        throw unexpectedToken();
                    }

                    if (constName != null) {
                        throw unexpectedToken();
                    }

                    read();

                    hasNullNoiseWord = true;

                    break;
                }
                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }
        }
    }

    /**
     * Responsible for handling check constraints section of CREATE TABLE ...
     *
     * @param c check constraint
     */
    void readCheckConstraintCondition(Constraint c) {

        readThis(Tokens.OPENBRACKET);
        startRecording();

        isCheckOrTriggerCondition = true;

        Expression condition = XreadBooleanValueExpression();

        isCheckOrTriggerCondition = false;

        Token[] tokens = getRecordedStatement();

        readThis(Tokens.CLOSEBRACKET);

        c.check = condition;
    }

    // A VoltDB extension to support indexed expressions and the assume unique attribute
    StatementSchema compileCreateIndex(boolean unique, boolean assumeUnique) {
    /* disable 1 line ...
    StatementSchema compileCreateIndex(boolean unique) {
    ... disabled 1 line */
    // End of VoltDB extension

        Table         table;
        HsqlName      indexHsqlName;
        String[]      qualifiers = null;
        HsqlArrayList list       = new HsqlArrayList();

        read();

        indexHsqlName = readNewSchemaObjectName(SchemaObject.INDEX, true);

        while (token.tokenType != Tokens.ON) {
            checkIsIdentifier();
            list.add(token.tokenString);
            read();
        }

        qualifiers = new String[list.size()];

        list.toArray(qualifiers);
        readThis(Tokens.ON);

        table = readTableName();

        HsqlName tableSchema = table.getSchemaName();

        indexHsqlName.setSchemaIfNull(tableSchema);

        indexHsqlName.parent = table.getName();

        if (indexHsqlName.schema != tableSchema) {
            throw Error.error(ErrorCode.X_42505);
        }

        indexHsqlName.schema = table.getSchemaName();

        // A VoltDB extension to support indexed expressions and the assume unique attribute
        java.util.List<Boolean> ascDesc = new java.util.ArrayList<Boolean>();
        java.util.List<Expression> indexExprs = XreadExpressions(ascDesc);
        Expression predicate = null;
        if (readIfThis(Tokens.WHERE)) {
            predicate = XreadBooleanValueExpression();
        }
        OrderedHashSet set = voltGetSimpleColumnNames(indexExprs);
        if (set == null) {
            // Not just indexing columns.
            // The meaning of set and indexColumns shifts here to be
            // the set of unique base columns for the indexed expressions.
            set = voltGetBaseColumnNames(indexExprs);
        } else {
            // Just indexing columns -- by-pass extended support for generalized index expressions.
            indexExprs = null;
        }

        int[]    indexColumns = voltGetColumnList(set, table);
        String   sql          = getLastPart();
        Object[] args         = new Object[] {
            table, indexColumns, indexHsqlName, Boolean.valueOf(unique),
            indexExprs, Boolean.valueOf(assumeUnique), predicate, qualifiers
        };
        /* disable 6 lines ...
        int[]    indexColumns = readColumnList(table, true);
        String   sql          = getLastPart();
        Object[] args         = new Object[] {
            table, indexColumns, indexHsqlName, Boolean.valueOf(unique),
            qualifiers
        };
        ... disabled 6 lines */
        // End of VoltDB extension

        return new StatementSchema(sql, StatementTypes.CREATE_INDEX, args,
                                   null, new HsqlName[] {
            database.getCatalogName(), table.getName()
        });
    }

    StatementSchema compileCreateSchema() {

        HsqlName schemaName       = null;
        String   authorisation    = null;
        HsqlName characterSetName = null;

        read();

        if (token.tokenType != Tokens.AUTHORIZATION) {
            schemaName = readNewSchemaName();
        }

        if (token.tokenType == Tokens.AUTHORIZATION) {
            read();
            checkIsSimpleName();

            authorisation = token.tokenString;

            read();

            if (schemaName == null) {
                Grantee owner =
                    database.getGranteeManager().get(authorisation);

                if (owner == null) {
                    throw Error.error(ErrorCode.X_28501, authorisation);
                }

                schemaName =
                    database.nameManager.newHsqlName(owner.getName().name,
                                                     isDelimitedIdentifier(),
                                                     SchemaObject.SCHEMA);

                SqlInvariants.checkSchemaNameNotSystem(token.tokenString);
            }
        }

        if (SqlInvariants.PUBLIC_ROLE_NAME.equals(authorisation)) {
            throw Error.error(ErrorCode.X_28502, authorisation);
        }

        Grantee owner = authorisation == null ? session.getGrantee()
                                              : database.getGranteeManager()
                                                  .get(authorisation);

        if (owner == null) {
            throw Error.error(ErrorCode.X_28501, authorisation);
        }

        if (!session.getGrantee().isSchemaCreator()) {
            throw Error.error(ErrorCode.X_0L501,
                              session.getGrantee().getName().getNameString());
        }

        if (owner instanceof User && ((User) owner).isExternalOnly) {
            throw Error.error(ErrorCode.X_0L000,
                              session.getGrantee().getName().getNameString());
        }

        if (database.schemaManager.schemaExists(schemaName.name)) {
            if (session.isProcessingScript()
                    && SqlInvariants.PUBLIC_SCHEMA.equals(schemaName.name)) {}
            else {
                throw Error.error(ErrorCode.X_42504, schemaName.name);
            }
        }

        if (schemaName.name.equals(SqlInvariants.LOBS_SCHEMA)) {
            schemaName = SqlInvariants.LOBS_SCHEMA_HSQLNAME;
            owner      = schemaName.owner;
        }

        if (readIfThis(Tokens.DEFAULT)) {
            readThis(Tokens.CHARACTER);
            readThis(Tokens.SET);

            characterSetName =
                this.readNewSchemaObjectName(SchemaObject.CHARSET, false);
        }

        String     sql            = getLastPart();
        Object[]   args           = new Object[] {
            schemaName, owner
        };
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;
        StatementSchema cs = new StatementSchema(sql,
            StatementTypes.CREATE_SCHEMA, args, null, writeLockNames);

        cs.setSchemaHsqlName(schemaName);

        HsqlArrayList list = new HsqlArrayList();

        list.add(cs);
        getCompiledStatementBody(list);

        StatementSchema[] array = new StatementSchema[list.size()];

        list.toArray(array);

        boolean swapped;

        do {
            swapped = false;

            for (int i = 0; i < array.length - 1; i++) {
                if (array[i].order > array[i + 1].order) {
                    StatementSchema temp = array[i + 1];

                    array[i + 1] = array[i];
                    array[i]     = temp;
                    swapped      = true;
                }
            }
        } while (swapped);

        return new StatementSchemaDefinition(array);
    }

    void getCompiledStatementBody(HsqlList list) {

        int    position;
        String sql;
        int    statementType;

        for (boolean end = false; !end; ) {
            StatementSchema cs = null;

            position = getPosition();

            switch (token.tokenType) {

                case Tokens.CREATE :
                    read();

                    switch (token.tokenType) {

                        // not in schema definition
                        case Tokens.SCHEMA :
                        case Tokens.USER :
                        case Tokens.UNIQUE :
                            throw unexpectedToken();
                        case Tokens.INDEX :
                            statementType = StatementTypes.CREATE_INDEX;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType);
                            break;

                        case Tokens.SEQUENCE :
                            cs     = compileCreateSequence();
                            cs.sql = getLastPart(position);
                            break;

                        case Tokens.ROLE :
                            cs     = compileCreateRole();
                            cs.sql = getLastPart(position);
                            break;

                        case Tokens.DOMAIN :
                            statementType = StatementTypes.CREATE_DOMAIN;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType);
                            break;

                        case Tokens.TYPE :
                            cs     = compileCreateType(false);
                            cs.sql = getLastPart(position);
                            break;

                        case Tokens.CHARACTER :
                            cs     = compileCreateCharacterSet();
                            cs.sql = getLastPart(position);
                            break;

                        // no supported
                        case Tokens.ASSERTION :
                            throw unexpectedToken();
                        case Tokens.TABLE :
                        case Tokens.MEMORY :
                        case Tokens.CACHED :
                        case Tokens.TEMP :
                        case Tokens.GLOBAL :
                        case Tokens.TEMPORARY :
                        case Tokens.TEXT :
                            statementType = StatementTypes.CREATE_TABLE;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType);
                            break;

                        case Tokens.TRIGGER :
                            statementType = StatementTypes.CREATE_TRIGGER;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType);
                            break;

                        case Tokens.VIEW :
                            statementType = StatementTypes.CREATE_VIEW;
                            sql = getStatement(position,
                                               startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType);
                            break;

                        case Tokens.FUNCTION :
                            statementType = StatementTypes.CREATE_ROUTINE;
                            sql = getStatementForRoutine(
                                position, startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType);
                            break;

                        case Tokens.PROCEDURE :
                            statementType = StatementTypes.CREATE_ROUTINE;
                            sql = getStatementForRoutine(
                                position, startStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType);
                            break;

                        default :
                            throw unexpectedToken();
                    }
                    break;

                case Tokens.GRANT :
                    cs     = compileGrantOrRevoke();
                    cs.sql = getLastPart(position);
                    break;

                case Tokens.SEMICOLON :
                    read();

                    end = true;
                    break;

                case Tokens.X_ENDPARSE :
                    end = true;
                    break;

                default :
                    throw unexpectedToken();
            }

            if (cs != null) {
                cs.isSchemaDefinition = true;

                list.add(cs);
            }
        }
    }

    StatementSchema compileCreateRole() {

        read();

        HsqlName   name           = readNewUserIdentifier();
        String     sql            = getLastPart();
        Object[]   args           = new Object[]{ name };
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;

        return new StatementSchema(sql, StatementTypes.CREATE_ROLE, args,
                                   null, writeLockNames);
    }

    StatementSchema compileCreateUser() {

        HsqlName name;
        String   password;
        Boolean  admin    = Boolean.FALSE;
        Boolean  isDigest = Boolean.FALSE;
        Grantee  grantor  = session.getGrantee();

        read();

        name = readNewUserIdentifier();

        readThis(Tokens.PASSWORD);

        if (readIfThis(Tokens.DIGEST)) {
            isDigest = Boolean.TRUE;
        }

        password = readPassword();

        if (token.tokenType == Tokens.ADMIN) {
            read();

            admin = Boolean.TRUE;
        }

        checkDatabaseUpdateAuthorisation();

        String     sql            = getLastPart();
        Object[]   args           = new Object[] {
            name, password, grantor, admin, isDigest
        };
        HsqlName[] writeLockNames = database.schemaManager.catalogNameArray;

        return new StatementSchema(sql, StatementTypes.CREATE_USER, args,
                                   null, writeLockNames);
    }

    HsqlName readNewUserIdentifier() {

        checkIsSimpleName();

        String  tokenS   = token.tokenString;
        boolean isQuoted = isDelimitedIdentifier();

        if (tokenS.equalsIgnoreCase("SA")) {
            tokenS   = "SA";
            isQuoted = false;
        }

        HsqlName name = database.nameManager.newHsqlName(tokenS, isQuoted,
            SchemaObject.GRANTEE);

        read();

        return name;
    }

    String readPassword() {

        String tokenS = token.tokenString;

        if (isUndelimitedSimpleName() || isDelimitedSimpleName()) {
            read();
        } else {
            readQuotedString();
        }

        return tokenS;
    }

    Statement compileRenameObject(HsqlName name, int type) {

        HsqlName newName = readNewSchemaObjectName(type, true);
        String   sql     = getLastPart();

        switch (type) {

            case SchemaObject.CATALOG :
                break;

            case SchemaObject.SCHEMA :
                checkSchemaUpdateAuthorisation(session, name);
                break;

            default :
                name.setSchemaIfNull(session.getCurrentSchemaHsqlName());
                checkSchemaUpdateAuthorisation(session, name.schema);
        }

        Object[] args = new Object[] {
            name, newName
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogNameArray();

        return new StatementSchema(sql, StatementTypes.RENAME_OBJECT, args,
                                   null, writeLockNames);
    }

    // A VoltDB extension to support the assume unique attribute
    Statement compileAlterTableAddUniqueConstraint(Table table,
            HsqlName name, boolean assumeUnique) {
    /* disable 2 lines ...
    Statement compileAlterTableAddUniqueConstraint(Table table,
            HsqlName name) {
    ... disabled 2 lines */
    // End of VoltDB extension

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        // A VoltDB extension to support indexed expressions.
        java.util.List<Expression> indexExprs = XreadExpressions(null);
        OrderedHashSet set = voltGetSimpleColumnNames(indexExprs);
        int[] cols = voltGetColumnList(set, table);
        if ((indexExprs != null) && (cols == null)) {
            // Not just indexing columns.
            // The meaning of cols shifts here to be
            // the set of unique base columns for the indexed expressions.
            set = voltGetBaseColumnNames(indexExprs);
            cols = voltGetColumnList(set, table);
        }
        /* disable 1 line ...
        int[] cols = this.readColumnList(table, false);
        ... disabled 1 line */
        // End of VoltDB extension
        HsqlName indexname = database.nameManager.newAutoName("IDX",
            name.name, table.getSchemaName(), table.getName(),
            SchemaObject.INDEX);
        Index index = table.createIndexStructure(indexname, cols, null, null,
            true, true, false);
        // A VoltDB extension to the assume unique attribute and indexed expressions
        if (indexExprs != null) {
            index = index.withExpressions(indexExprs.toArray(new Expression[]{}));
        }
        index.setAssumeUnique(assumeUnique);
        // End of VoltDB extension
        Constraint c = new Constraint(name, table, index,
                                      SchemaObject.ConstraintTypes.UNIQUE);
        // A VoltDB extension to support the assume unique attribute
        c.voltSetAssumeUnique(assumeUnique);
        // End of VoltDB extension
        String   sql  = getLastPart();
        Object[] args = new Object[] {
            StatementTypes.ADD_CONSTRAINT, table, c
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    Statement compileAlterTableAddForeignKeyConstraint(Table table,
            HsqlName name) {

        if (name == null) {
            name = database.nameManager.newAutoName("FK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        OrderedHashSet set           = readColumnNames(false);
        Constraint     c             = readFKReferences(table, name, set);
        HsqlName       mainTableName = c.getMainTableName();

        c.core.mainTable = database.schemaManager.getTable(session,
                mainTableName.name, mainTableName.schema.name);

        c.setColumnsIndexes(table);

        if (c.core.mainCols.length != c.core.refCols.length) {
            throw Error.error(ErrorCode.X_42593);
        }

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            StatementTypes.ADD_CONSTRAINT, table, c
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        if (mainTableName != table.getName()) {
            writeLockNames =
                (HsqlName[]) ArrayUtil.toAdjustedArray(writeLockNames,
                    mainTableName, writeLockNames.length, 1);
        }

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    Statement compileAlterTableAddCheckConstraint(Table table, HsqlName name) {

        Constraint check;

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        check = new Constraint(name, null, SchemaObject.ConstraintTypes.CHECK);

        readCheckConstraintCondition(check);

        String     sql            = getLastPart();
        Object[]   args           = new Object[] {
            StatementTypes.ADD_CONSTRAINT, table, check
        };
        HsqlName[] writeLockNames = new HsqlName[] {
            database.getCatalogName(), table.getName()
        };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    Statement compileAlterTableAddColumn(Table table) {

        int           colIndex = table.getColumnCount();
        HsqlArrayList list     = new HsqlArrayList();
        Constraint constraint =
            new Constraint(null, null, SchemaObject.ConstraintTypes.TEMP);

        list.add(constraint);
        checkIsSchemaObjectName();

        HsqlName hsqlName =
            database.nameManager.newColumnHsqlName(table.getName(),
                token.tokenString, isDelimitedIdentifier());

        read();

        ColumnSchema column = readColumnDefinitionOrNull(table, hsqlName,
            list);

        if (column == null) {
            throw Error.error(ErrorCode.X_42000);
        }

        if (token.tokenType == Tokens.BEFORE) {
            read();

            colIndex = table.getColumnIndex(token.tokenString);

            read();
        }

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            StatementTypes.ADD_COLUMN, table, column, new Integer(colIndex),
            list
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    Statement compileAlterTableAddPrimaryKey(Table table, HsqlName name) {

        if (name == null) {
            name = session.database.nameManager.newAutoName("PK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        OrderedHashSet set = readColumnNames(false);
        Constraint constraint =
            new Constraint(name, set,
                           SchemaObject.ConstraintTypes.PRIMARY_KEY);

        constraint.setColumnsIndexes(table);

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            StatementTypes.ADD_CONSTRAINT, table, constraint
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    Statement compileAlterTableDropColumn(Table table, String colName,
                                          boolean cascade) {

        int colindex = table.getColumnIndex(colName);

        if (table.getColumnCount() == 1) {
            throw Error.error(ErrorCode.X_42591);
        }

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            table.getColumn(colindex).getName(),
            ValuePool.getInt(SchemaObject.COLUMN), Boolean.valueOf(cascade),
            Boolean.FALSE
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(sql, StatementTypes.DROP_COLUMN, args,
                                   null, writeLockNames);
    }

    Statement compileAlterColumn(Table table, ColumnSchema column,
                                 int columnIndex) {

        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());
        int position = getPosition();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);

                return compileAlterColumnRename(table, column);
            }
            case Tokens.DROP : {
                read();

                if (token.tokenType == Tokens.DEFAULT) {
                    read();

                    String   sql  = getLastPart();
                    Object[] args = new Object[] {
                        StatementTypes.ALTER_COLUMN_DROP_DEFAULT, table,
                        column, columnIndex
                    };

                    return new StatementSchema(sql,
                                               StatementTypes.ALTER_TABLE,
                                               args, null, writeLockNames);
                } else if (token.tokenType == Tokens.GENERATED) {
                    read();

                    String   sql  = getLastPart();
                    Object[] args = new Object[] {
                        StatementTypes.ALTER_COLUMN_DROP_GENERATED, table,
                        column, columnIndex
                    };

                    return new StatementSchema(sql,
                                               StatementTypes.ALTER_TABLE,
                                               args, null, writeLockNames);
                } else {
                    throw unexpectedToken();
                }
            }
            case Tokens.SET : {
                read();

                switch (token.tokenType) {

                    case Tokens.DATA : {
                        read();
                        readThis(Tokens.TYPE);

                        return compileAlterColumnDataType(table, column);
                    }
                    case Tokens.DEFAULT : {
                        read();

                        //ALTER TABLE .. ALTER COLUMN .. SET DEFAULT
                        Type       type = column.getDataType();
                        Expression expr = readDefaultClause(type);
                        String     sql  = getLastPart();
                        Object[]   args = new Object[] {
                            StatementTypes.ALTER_COLUMN_DEFAULT, table, column,
                            columnIndex, expr
                        };

                        return new StatementSchema(sql,
                                                   StatementTypes.ALTER_TABLE,
                                                   args, null, writeLockNames);
                    }
                    case Tokens.NOT : {

                        //ALTER TABLE .. ALTER COLUMN .. SET NOT NULL
                        read();
                        readThis(Tokens.NULL);

                        return compileAlterColumnSetNullability(table, column,
                                false);
                    }
                    case Tokens.NULL : {
                        read();

                        return compileAlterColumnSetNullability(table, column,
                                true);
                    }
                    case Tokens.GENERATED : {
                        return compileAlterColumnAddSequence(table, column,
                                                             columnIndex);
                    }
                    default :
                        rewind(position);
                        read();
                        break;
                }

                break;
            }
            case Tokens.GENERATED :
                return compileAlterColumnAddSequence(table, column,
                                                     columnIndex);

            // fall through
            default :
        }

        if (token.tokenType == Tokens.SET
                || token.tokenType == Tokens.RESTART) {
            if (!column.isIdentity()) {
                throw Error.error(ErrorCode.X_42535);
            }

            return compileAlterColumnSequenceOptions(table, column,
                    columnIndex);
        } else {
            return compileAlterColumnDataTypeIdentity(table, column);
        }
    }

    /**
     * Allows changes to type of column or addition / removal of an IDENTITY generator.
     * IDENTITY is removed if it does not appear in new column definition
     * Constraint definitions are not allowed
     */
    private Statement compileAlterColumnDataTypeIdentity(Table table,
            ColumnSchema column) {

        if (column.isGenerated()) {
            throw Error.error(ErrorCode.X_42561);
        }

        NumberSequence sequence = column.getIdentitySequence();
        Type           type     = column.getDataType();
        // A VoltDB extension to restore alter column functionality dropped by hsql
        Expression defaultExpr = null;
        boolean isNullable = true;
        // End of VoltDB extension

        if (token.tokenType == Tokens.IDENTITY) {
            read();

            if (!type.isIntegralType()) {
                throw Error.error(ErrorCode.X_42561);
            }

            if (sequence == null) {
                sequence = new NumberSequence(null, type);
            }
        } else {
            type = readTypeDefinition(true, true);

            switch (token.tokenType) {

                case Tokens.IDENTITY : {
                    if (!type.isIntegralType()) {
                        throw Error.error(ErrorCode.X_42561);
                    }

                    read();

                    if (sequence == null) {
                        sequence = new NumberSequence(null, type);
                    }

                    break;
                }
                case Tokens.GENERATED : {
                    sequence = readSequence(column);

                    break;
                }
                default :
                    sequence = null;
                    // A VoltDB extension to restore alter column functionality dropped by hsql.
                    // This replicates parser code from the CREATE TABLE column clause.
                    // VoltDB may need to make a decision about adopting MySQL or Oracle
                    // syntax compatibility or both as a hard-coded feature or as an option.
                    switch (token.tokenType) {

                    case Tokens.WITH : {
                        ////if (database.sqlSyntaxDb2) {
                            read();
                        ////} else {
                        ////    throw unexpectedToken();
                        ////}
                    }

                    // fall through
                    case Tokens.DEFAULT : {
                        read();
                        defaultExpr = readDefaultClause(type);
                    }
                    }
                    
                    ////if (database.sqlSyntaxMys) {
                    switch (token.tokenType) {

                    case Tokens.NULL :
                        read();
                        break;

                    case Tokens.NOT :
                        read();
                        readThis(Tokens.NULL);

                        isNullable = false;
                        break;
                    }
                    ////}
                    // End of VoltDB extension
            }
        }

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            StatementTypes.ALTER_COLUMN_TYPE_IDENTITY, table, column, type,
            sequence
            // A VoltDB extension to restore alter column functionality dropped by hsql.
            , defaultExpr, isNullable
            // End of VoltDB extension
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    private Statement compileAlterColumnDataType(Table table,
            ColumnSchema column) {

        if (column.isGenerated()) {
            throw Error.error(ErrorCode.X_42561);
        }

        Type type = readTypeDefinition(true, true);

        if (column.isIdentity()) {
            if (!type.isIntegralType()) {
                throw Error.error(ErrorCode.X_42561);
            }
        }

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            StatementTypes.ALTER_COLUMN_TYPE, table, column, type
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    private Statement compileAlterColumnSetNullability(Table table,
            ColumnSchema column, boolean b) {

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            StatementTypes.ALTER_COLUMN_NULL, table, column, b
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    Statement compileAlterSequence() {

        read();

        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);
        NumberSequence sequence =
            database.schemaManager.getSequence(token.tokenString, schema.name,
                                               true);

        read();

        if (token.tokenType == Tokens.RENAME) {
            read();
            readThis(Tokens.TO);

            return compileRenameObject(sequence.getName(),
                                       SchemaObject.SEQUENCE);
        }

        checkSchemaUpdateAuthorisation(session, sequence.getName().schema);

        NumberSequence copy = sequence.duplicate();

        readSequenceOptions(copy, false, true, false);

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            sequence, copy
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogNameArray();

        return new StatementSchema(sql, StatementTypes.ALTER_SEQUENCE, args,
                                   null, writeLockNames);
    }

    StatementSchema compileAlterColumnAddSequence(Table table,
            ColumnSchema column, int colIndex) {

        if (!column.getDataType().isIntegralType()) {
            throw Error.error(ErrorCode.X_42525);
        }

        if (column.isIdentity()) {
            throw Error.error(ErrorCode.X_42525);
        }

        NumberSequence sequence = readSequence(column);
        String         sql      = getLastPart();
        Object[]       args     = new Object[] {
            StatementTypes.ALTER_COLUMN_SEQUENCE, table, column, colIndex,
            sequence
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogAndBaseTableNames(
                table.getName());

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    NumberSequence readSequence(ColumnSchema column) {

        readThis(Tokens.GENERATED);

        NumberSequence sequence;

        sequence = new NumberSequence(null, column.getDataType());

        boolean generatedAlways = false;

        if (token.tokenType == Tokens.BY) {
            read();
            readThis(Tokens.DEFAULT);
        } else {
            readThis(Tokens.ALWAYS);

            generatedAlways = true;
        }

        readThis(Tokens.AS);
        readThis(Tokens.IDENTITY);
        sequence.setAlways(generatedAlways);

        if (token.tokenType == Tokens.OPENBRACKET) {
            read();
            readSequenceOptions(sequence, false, false, false);
            readThis(Tokens.CLOSEBRACKET);
        }

        sequence.checkValues();

        return sequence;
    }

    StatementSchema compileAlterColumnSequenceOptions(Table table,
            ColumnSchema column, int columnIndex) {

        OrderedIntHashSet set      = new OrderedIntHashSet();
        NumberSequence    sequence = column.getIdentitySequence().duplicate();

        while (true) {
            boolean end = false;

            switch (token.tokenType) {

                case Tokens.RESTART : {
                    if (!set.add(token.tokenType)) {
                        throw unexpectedToken();
                    }

                    read();

                    if (readIfThis(Tokens.WITH)) {
                        long value = readBigint();

                        sequence.setCurrentValueNoCheck(value);
                    } else {
                        sequence.reset();
                    }

                    break;
                }
                case Tokens.SET :
                    read();

                    switch (token.tokenType) {

                        case Tokens.INCREMENT : {
                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();
                            readThis(Tokens.BY);

                            long value = readBigint();

                            sequence.setIncrement(value);

                            break;
                        }
                        case Tokens.NO :
                            read();

                            if (token.tokenType == Tokens.MAXVALUE) {
                                sequence.setDefaultMaxValue();
                            } else if (token.tokenType == Tokens.MINVALUE) {
                                sequence.setDefaultMinValue();
                            } else if (token.tokenType == Tokens.CYCLE) {
                                sequence.setCycle(false);
                            } else {
                                throw unexpectedToken();
                            }

                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();
                            break;

                        case Tokens.MAXVALUE : {
                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();

                            long value = readBigint();

                            sequence.setMaxValueNoCheck(value);

                            break;
                        }
                        case Tokens.MINVALUE : {
                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();

                            long value = readBigint();

                            sequence.setMinValueNoCheck(value);

                            break;
                        }
                        case Tokens.CYCLE :
                            if (!set.add(token.tokenType)) {
                                throw unexpectedToken();
                            }

                            read();
                            sequence.setCycle(true);
                            break;

                        default :
                            throw unexpectedToken();
                    }
                    break;

                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }
        }

        sequence.checkValues();

//        column.getIdentitySequence().reset(sequence);
        String   sql  = getLastPart();
        Object[] args = new Object[] {
            StatementTypes.ALTER_COLUMN_SEQUENCE, table, column, columnIndex,
            sequence
        };
        HsqlName[] writeLockNames = new HsqlName[] {
            database.getCatalogName(), table.getName()
        };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    /**
     * Responsible for handling tail of ALTER COLUMN ... RENAME ...
     */
    private Statement compileAlterColumnRename(Table table,
            ColumnSchema column) {

        checkIsSimpleName();

        HsqlName name = readNewSchemaObjectName(SchemaObject.COLUMN, true);

        if (table.findColumn(name.name) > -1) {
            throw Error.error(ErrorCode.X_42504, name.name);
        }

        database.schemaManager.checkColumnIsReferenced(table.getName(),
                column.getName());

        String     sql            = getLastPart();
        Object[]   args           = new Object[] {
            column.getName(), name
        };
        HsqlName[] writeLockNames = new HsqlName[] {
            database.getCatalogName(), table.getName()
        };

        return new StatementSchema(sql, StatementTypes.RENAME_OBJECT, args,
                                   null, writeLockNames);
    }

    Statement compileAlterSchemaRename() {

        HsqlName name = readSchemaName();

        checkSchemaUpdateAuthorisation(name);
        readThis(Tokens.RENAME);
        readThis(Tokens.TO);

        HsqlName newName = readNewSchemaName();
        String   sql     = getLastPart();
        Object[] args    = new Object[] {
            name, newName
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogNameArray();

        return new StatementSchema(sql, StatementTypes.RENAME_OBJECT, args,
                                   null, writeLockNames);
    }

    Statement compileAlterUser() {

        read();

        String   password;
        User     userObject;
        HsqlName userName = readNewUserIdentifier();

        userObject = database.getUserManager().get(userName.name);

        if (userName.name.equals(Tokens.T_PUBLIC)) {
            throw Error.error(ErrorCode.X_42503);
        }

        if (userName.name.equals(SqlInvariants.SYSTEM_AUTHORIZATION_NAME)) {
            throw Error.error(ErrorCode.X_42503);
        }

        readThis(Tokens.SET);

        switch (token.tokenType) {

            case Tokens.LOCAL : {
                read();

                Boolean  mode = processTrueOrFalseObject();
                Object[] args = new Object[] {
                    userObject, mode
                };

                return new StatementCommand(StatementTypes.SET_USER_LOCAL,
                                            args);
            }
            case Tokens.PASSWORD : {
                read();

                boolean isDigest = false;

                if (readIfThis(Tokens.DIGEST)) {
                    isDigest = Boolean.TRUE;
                }

                password = readPassword();

                Object[] args = new Object[] {
                    userObject, password, isDigest
                };
                Statement cs =
                    new StatementCommand(StatementTypes.SET_USER_PASSWORD,
                                         args);
                String sql = userObject.getSetUserPasswordDigestSQL(password,
                    isDigest);

                cs.setSQL(sql);

                return cs;
            }
            case Tokens.INITIAL : {
                read();
                readThis(Tokens.SCHEMA);

                HsqlName schemaName;

                if (token.tokenType == Tokens.DEFAULT) {
                    schemaName = null;
                } else {
                    schemaName = database.schemaManager.getSchemaHsqlName(
                        token.tokenString);
                }

                read();

                Object[] args = new Object[] {
                    userObject, schemaName
                };

                return new StatementCommand(
                    StatementTypes.SET_USER_INITIAL_SCHEMA, args);
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    Statement compileAlterDomain() {

        read();

        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);

        checkSchemaUpdateAuthorisation(schema);

        Type domain = database.schemaManager.getDomain(token.tokenString,
            schema.name, true);

        read();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);

                return compileRenameObject(domain.getName(),
                                           SchemaObject.DOMAIN);
            }
            case Tokens.DROP : {
                read();

                if (token.tokenType == Tokens.DEFAULT) {
                    read();

                    String   sql  = this.getLastPart();
                    Object[] args = new Object[] {
                        StatementTypes.DROP_DEFAULT, domain
                    };
                    HsqlName[] writeLockNames =
                        database.schemaManager.getCatalogAndBaseTableNames(
                            domain.getName());

                    return new StatementSchema(sql,
                                               StatementTypes.ALTER_DOMAIN,
                                               args, null, writeLockNames);
                } else if (token.tokenType == Tokens.CONSTRAINT) {
                    read();
                    checkIsSchemaObjectName();

                    HsqlName name = database.schemaManager.getSchemaObjectName(
                        domain.getSchemaName(), token.tokenString,
                        SchemaObject.CONSTRAINT, true);

                    read();

                    String   sql  = this.getLastPart();
                    Object[] args = new Object[] {
                        StatementTypes.DROP_CONSTRAINT, domain, name
                    };
                    HsqlName[] writeLockNames =
                        database.schemaManager.getCatalogAndBaseTableNames(
                            domain.getName());

                    return new StatementSchema(sql,
                                               StatementTypes.ALTER_DOMAIN,
                                               args, null, writeLockNames);
                } else {
                    throw unexpectedToken();
                }
            }
            case Tokens.SET : {
                read();
                readThis(Tokens.DEFAULT);

                Expression e    = readDefaultClause(domain);
                String     sql  = this.getLastPart();
                Object[]   args = new Object[] {
                    StatementTypes.ADD_DEFAULT, domain, e
                };
                HsqlName[] writeLockNames =
                    database.schemaManager.getCatalogAndBaseTableNames(
                        domain.getName());

                return new StatementSchema(sql, StatementTypes.ALTER_DOMAIN,
                                           args, null, writeLockNames);
            }
            case Tokens.ADD : {
                read();

                if (token.tokenType == Tokens.CONSTRAINT
                        || token.tokenType == Tokens.CHECK) {
                    HsqlArrayList tempConstraints = new HsqlArrayList();

                    compileContext.currentDomain = domain;

                    readConstraint(domain, tempConstraints);

                    compileContext.currentDomain = null;

                    Constraint c    = (Constraint) tempConstraints.get(0);
                    String     sql  = this.getLastPart();
                    Object[]   args = new Object[] {
                        StatementTypes.ADD_CONSTRAINT, domain, c
                    };
                    HsqlName[] writeLockNames =
                        database.schemaManager.getCatalogAndBaseTableNames(
                            domain.getName());

                    return new StatementSchema(sql,
                                               StatementTypes.ALTER_DOMAIN,
                                               args, null, writeLockNames);
                }
            }
        }

        throw unexpectedToken();
    }

    private boolean isGrantToken() {

        switch (token.tokenType) {

            case Tokens.ALL :
            case Tokens.INSERT :
            case Tokens.UPDATE :
            case Tokens.SELECT :
            case Tokens.DELETE :
            case Tokens.USAGE :
            case Tokens.EXECUTE :
            case Tokens.REFERENCES :
                return true;

            default :
                return false;
        }
    }

    StatementSchema compileGrantOrRevoke() {

        boolean grant = token.tokenType == Tokens.GRANT;

        read();

        if (isGrantToken()
                || (!grant
                    && (token.tokenType == Tokens.GRANT
                        || token.tokenType == Tokens.HIERARCHY))) {
            return compileRightGrantOrRevoke(grant);
        } else {
            return compileRoleGrantOrRevoke(grant);
        }
    }

    private StatementSchema compileRightGrantOrRevoke(boolean grant) {

        OrderedHashSet granteeList = new OrderedHashSet();
        Grantee        grantor     = null;
        Right          right       = null;

//        SchemaObject   schemaObject;
        HsqlName objectName    = null;
        boolean  isTable       = false;
        boolean  isUsage       = false;
        boolean  isExec        = false;
        boolean  isAll         = false;
        boolean  isGrantOption = false;
        boolean  cascade       = false;

        if (!grant) {
            if (token.tokenType == Tokens.GRANT) {
                read();
                readThis(Tokens.OPTION);
                readThis(Tokens.FOR);

                isGrantOption = true;
            } else if (token.tokenType == Tokens.HIERARCHY) {
                throw unsupportedFeature();
/*
                read();
                readThis(Token.OPTION);
                readThis(Token.FOR);
*/
            }
        }

        // ALL means all the rights the grantor can grant
        if (token.tokenType == Tokens.ALL) {
            read();

            if (token.tokenType == Tokens.PRIVILEGES) {
                read();
            }

            right = Right.fullRights;
            isAll = true;
        } else {
            right = new Right();

            boolean loop = true;

            while (loop) {
                checkIsNotQuoted();

                int rightType =
                    GranteeManager.getCheckSingleRight(token.tokenString);
                int            grantType = token.tokenType;
                OrderedHashSet columnSet = null;

                read();

                switch (grantType) {

                    case Tokens.REFERENCES :
                    case Tokens.SELECT :
                    case Tokens.INSERT :
                    case Tokens.UPDATE :
                        if (token.tokenType == Tokens.OPENBRACKET) {
                            columnSet = readColumnNames(false);
                        }

                    // fall through
                    case Tokens.TRIGGER :
                        if (right == null) {
                            right = new Right();
                        }

                        right.set(rightType, columnSet);

                        isTable = true;
                        break;

                    case Tokens.DELETE :
                        if (right == null) {
                            right = new Right();
                        }

                        right.set(rightType, null);

                        isTable = true;
                        break;

                    case Tokens.USAGE :
                        if (isTable) {
                            throw unexpectedToken();
                        }

                        right   = Right.fullRights;
                        isUsage = true;
                        loop    = false;

                        continue;
                    case Tokens.EXECUTE :
                        if (isTable) {
                            throw unexpectedToken();
                        }

                        right  = Right.fullRights;
                        isExec = true;
                        loop   = false;

                        continue;
                }

                if (token.tokenType == Tokens.COMMA) {
                    read();

                    continue;
                }

                break;
            }
        }

        readThis(Tokens.ON);

        int objectType = 0;

        switch (token.tokenType) {

            case Tokens.CLASS :
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                if (!isSimpleName() || !isDelimitedIdentifier()) {
                    throw Error.error(ErrorCode.X_42569);
                }

                objectType = SchemaObject.FUNCTION;
                objectName = readNewSchemaObjectName(SchemaObject.FUNCTION,
                                                     false);
                break;

            case Tokens.SPECIFIC : {
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                switch (token.tokenType) {

                    case Tokens.ROUTINE :
                    case Tokens.PROCEDURE :
                    case Tokens.FUNCTION :
                        read();
                        break;

                    default :
                        throw unexpectedToken();
                }

                objectType = SchemaObject.SPECIFIC_ROUTINE;

                break;
            }
            case Tokens.FUNCTION :
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.FUNCTION;
                break;

            case Tokens.PROCEDURE :
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.PROCEDURE;
                break;

            case Tokens.ROUTINE :
                if (!isExec && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.ROUTINE;
                break;

            case Tokens.TYPE :
                if (!isUsage && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.TYPE;
                break;

            case Tokens.DOMAIN :
                if (!isUsage && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.DOMAIN;
                break;

            case Tokens.SEQUENCE :
                if (!isUsage && !isAll) {
                    throw unexpectedToken();
                }

                read();

                objectType = SchemaObject.SEQUENCE;
                break;

            case Tokens.CHARACTER :
                if (!isUsage && !isAll) {
                    throw unexpectedToken();
                }

                read();
                readThis(Tokens.SET);

                objectType = SchemaObject.CHARSET;
                break;

            case Tokens.TABLE :
            default :
                if (!isTable && !isAll) {
                    throw unexpectedToken();
                }

                readIfThis(Tokens.TABLE);

                objectType = SchemaObject.TABLE;
        }

        objectName = readNewSchemaObjectName(objectType, false);

        if (grant) {
            readThis(Tokens.TO);
        } else {
            readThis(Tokens.FROM);
        }

        while (true) {
            checkIsSimpleName();
            granteeList.add(token.tokenString);
            read();

            if (token.tokenType == Tokens.COMMA) {
                read();
            } else {
                break;
            }
        }

        if (grant) {
            if (token.tokenType == Tokens.WITH) {
                read();
                readThis(Tokens.GRANT);
                readThis(Tokens.OPTION);

                isGrantOption = true;
            }

            /** @todo - implement */
            if (token.tokenType == Tokens.GRANTED) {
                read();
                readThis(Tokens.BY);

                if (token.tokenType == Tokens.CURRENT_USER) {
                    read();

                    //
                } else {
                    readThis(Tokens.CURRENT_ROLE);

                    if (session.getRole() == null) {
                        throw Error.error(ErrorCode.X_0P000);
                    }

                    grantor = session.getRole();
                }
            }
        } else {
            if (token.tokenType == Tokens.CASCADE) {
                cascade = true;

                read();
            } else {
                readThis(Tokens.RESTRICT);
            }
        }

        String   sql  = getLastPart();
        int      type = grant ? StatementTypes.GRANT
                              : StatementTypes.REVOKE;
        Object[] args = new Object[] {
            granteeList, objectName, right, grantor, Boolean.valueOf(cascade),
            Boolean.valueOf(isGrantOption)
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogNameArray();
        StatementSchema cs = new StatementSchema(sql, type, args, null,
            writeLockNames);

        return cs;
    }

    private StatementSchema compileRoleGrantOrRevoke(boolean grant) {

        Grantee        grantor     = session.getGrantee();
        OrderedHashSet roleList    = new OrderedHashSet();
        OrderedHashSet granteeList = new OrderedHashSet();
        boolean        cascade     = false;

        if (!grant && token.tokenType == Tokens.ADMIN) {
            throw unsupportedFeature();
/*
            read();
            readThis(Token.OPTION);
            readThis(Token.FOR);
*/
        }

        while (true) {
            checkIsSimpleName();
            roleList.add(token.tokenString);
            read();

            if (token.tokenType == Tokens.COMMA) {
                read();

                continue;
            }

            break;
        }

        if (grant) {
            readThis(Tokens.TO);
        } else {
            readThis(Tokens.FROM);
        }

        while (true) {
            checkIsSimpleName();
            granteeList.add(token.tokenString);
            read();

            if (token.tokenType == Tokens.COMMA) {
                read();
            } else {
                break;
            }
        }

        if (grant) {
            if (token.tokenType == Tokens.WITH) {
                throw unsupportedFeature();
/*
                read();
                readThis(Token.ADMIN);
                readThis(Token.OPTION);
*/
            }
        }

        if (token.tokenType == Tokens.GRANTED) {
            read();
            readThis(Tokens.BY);

            if (token.tokenType == Tokens.CURRENT_USER) {
                read();

                //
            } else {
                readThis(Tokens.CURRENT_ROLE);

                if (session.getRole() == null) {
                    throw Error.error(ErrorCode.X_0P000);
                }

                grantor = session.getRole();
            }
        }

        if (!grant) {
            if (token.tokenType == Tokens.CASCADE) {
                cascade = true;

                read();
            } else {
                readThis(Tokens.RESTRICT);
            }
        }

        String   sql  = getLastPart();
        int      type = grant ? StatementTypes.GRANT_ROLE
                              : StatementTypes.REVOKE_ROLE;
        Object[] args = new Object[] {
            granteeList, roleList, grantor, Boolean.valueOf(cascade)
        };
        HsqlName[] writeLockNames =
            database.schemaManager.getCatalogNameArray();
        StatementSchema cs = new StatementSchema(sql, type, args, null,
            writeLockNames);

        return cs;
    }

    void checkSchemaUpdateAuthorisation(HsqlName schema) {

        if (session.isProcessingLog()) {
            return;
        }

        SqlInvariants.checkSchemaNameNotSystem(schema.name);

        if (isSchemaDefinition) {
            if (schema != session.getCurrentSchemaHsqlName()) {
                throw Error.error(ErrorCode.X_42505);
            }
        } else {
            session.getGrantee().checkSchemaUpdateOrGrantRights(schema.name);
        }

        session.checkDDLWrite();
    }

    void checkDatabaseUpdateAuthorisation() {
        session.checkAdmin();
        session.checkDDLWrite();
    }

    StatementSchema compileComment() {

        HsqlName name;
        int      type;

        readThis(Tokens.COMMENT);
        readThis(Tokens.ON);

        switch (token.tokenType) {

            case Tokens.ROUTINE :
            case Tokens.TABLE : {
                type = token.tokenType == Tokens.ROUTINE ? SchemaObject.ROUTINE
                                                         : SchemaObject.TABLE;

                read();
                checkIsSchemaObjectName();

                name = database.nameManager.newHsqlName(token.tokenString,
                        token.isDelimitedIdentifier, type);

                if (token.namePrefix == null) {
                    name.schema = session.getCurrentSchemaHsqlName();
                } else {
                    name.schema = database.nameManager.newHsqlName(
                        token.namePrefix, token.isDelimitedPrefix,
                        SchemaObject.SCHEMA);
                }

                read();

                break;
            }
            case Tokens.COLUMN : {
                read();
                checkIsSchemaObjectName();

                name = database.nameManager.newHsqlName(token.tokenString,
                        token.isDelimitedIdentifier, SchemaObject.COLUMN);

                if (token.namePrefix == null) {
                    throw Error.error(ErrorCode.X_42501);
                }

                name.parent =
                    database.nameManager.newHsqlName(token.namePrefix,
                                                     token.isDelimitedPrefix,
                                                     SchemaObject.TABLE);

                if (token.namePrePrefix == null) {
                    name.parent.schema = session.getCurrentSchemaHsqlName();
                } else {
                    name.parent.schema = database.nameManager.newHsqlName(
                        token.namePrePrefix, token.isDelimitedPrePrefix,
                        SchemaObject.TABLE);
                }

                read();

                break;
            }
            default :
                throw unexpectedToken();
        }

        readThis(Tokens.IS);

        String   comment   = readQuotedString();
        Object[] arguments = new Object[] {
            name, comment
        };

        return new StatementSchema(null, StatementTypes.COMMENT, arguments,
                                   null, null);
    }

    Statement compileAlterSession() {

        read();
        session.checkAdmin();

        if (token.tokenType == Tokens.RESET) {
            read();

            int action = token.tokenType;

            switch (token.tokenType) {

                case Tokens.ALL :
                    read();
                    break;

                case Tokens.RESULT :
                    read();
                    readThis(Tokens.SETS);
                    break;

                case Tokens.TABLE :
                    read();
                    readThis(Tokens.DATA);
                    break;

                default :
                    throw unexpectedTokenRequire(Tokens.T_ALL + ','
                                                 + Tokens.T_RESULT + ','
                                                 + Tokens.T_TABLE);
            }

            Object[] args = new Object[] {
                Long.valueOf(session.getId()), Integer.valueOf(action)
            };

            return new StatementCommand(StatementTypes.ALTER_SESSION, args);
        }

        long    sessionID     = readBigint();
        Session targetSession = database.sessionManager.getSession(sessionID);

        if (targetSession == null) {
            throw Error.error(ErrorCode.X_2E000);
        }

        int action = token.tokenType;

        switch (token.tokenType) {

            case Tokens.CLOSE :
                read();
                break;

            case Tokens.RELEASE :
                read();
                break;

            default :
                throw unexpectedToken();
        }

        Object[] args = new Object[] {
            Long.valueOf(sessionID), Integer.valueOf(action)
        };

        return new StatementCommand(StatementTypes.ALTER_SESSION, args);
    }

    /**
     * Retrieves boolean value corresponding to the next token.
     *
     * @return   true if next token is "TRUE"; false if next token is "FALSE"
     * @throws  HsqlException if the next token is neither "TRUE" or "FALSE"
     */
    boolean processTrueOrFalse() {

        if (token.namePrefix != null) {
            throw unexpectedToken();
        }

        if (token.tokenType == Tokens.TRUE) {
            read();

            return true;
        } else if (token.tokenType == Tokens.FALSE) {
            read();

            return false;
        } else {
            throw unexpectedToken();
        }
    }

    Boolean processTrueOrFalseObject() {
        return Boolean.valueOf(processTrueOrFalse());
    }

    void checkSchemaUpdateAuthorisation(Session session, HsqlName schema) {

        if (session.isProcessingLog()) {
            return;
        }

        if (SqlInvariants.isSystemSchemaName(schema.name)) {
            throw Error.error(ErrorCode.X_42503);
        }

        if (session.parser.isSchemaDefinition) {
            if (schema == session.getCurrentSchemaHsqlName()) {
                return;
            }

            Error.error(ErrorCode.X_42505, schema.name);
        }

        session.getGrantee().checkSchemaUpdateOrGrantRights(schema.name);
        session.checkDDLWrite();
    }
    // A VoltDB extension to export abstract parse trees

    /**
     * Responsible for handling Volt limit constraints section of CREATE TABLE ...
     *
     * @param c check constraint
     */
    void readLimitConstraintCondition(Constraint c) {
        readThis(Tokens.PARTITION);
        readThis(Tokens.ROWS);

        int rowsLimit = readInteger();
        c.voltRowsLimit = rowsLimit;

        // The optional EXECUTE (DELETE ...) clause
        if (readIfThis(Tokens.EXECUTE)) {
            // Capture the statement between parentheses following the EXECUTE keyword,
            // as in
            //
            // LIMIT PARTITION ROWS 10 EXECUTE (DELETE FROM tbl WHERE b = 1)
            //
            readThis(Tokens.OPENBRACKET);

            startRecording();

            int numOpenBrackets = 1;
            while (numOpenBrackets > 0) {
                switch(token.tokenType) {
                case Tokens.OPENBRACKET:
                    numOpenBrackets++;
                    read();
                    break;

                case Tokens.CLOSEBRACKET:
                    numOpenBrackets--;
                    if (numOpenBrackets > 0) {
                        // don't want the final parenthesis
                        read();
                    }
                    break;

                case Tokens.X_ENDPARSE:
                    throw unexpectedToken();

                default:
                    read();
                }
            }

            Token[] stmtTokens = getRecordedStatement();

            // This captures the DELETE statement exactly, including embedded whitespace, etc.
            c.voltRowLimitDeleteStmt = Token.getSQL(stmtTokens);
            readThis(Tokens.CLOSEBRACKET);
        }
    }

    /// VoltDB added parsing behavior of the "readColumnList/readColumnNames" functions,
    /// adding support for indexed expressions.
    private java.util.List<Expression> XreadExpressions(java.util.List<Boolean> ascDesc) {
        readThis(Tokens.OPENBRACKET);

        java.util.List<Expression> indexExprs = new java.util.ArrayList<Expression>();

        while (true) {
            Expression expression = XreadValueExpression();
            indexExprs.add(expression);

            // VoltDB added "readColumnList(table, true)" support for descending-value indexes,
            // that similarly parses the asc/desc indicators but COLLECTS them so they can be ignored later,
            // rather than ignoring them on the spot.
            if (ascDesc != null) {
                Boolean is_asc = Boolean.TRUE;
                if (token.tokenType == Tokens.ASC
                        || token.tokenType == Tokens.DESC) {
                    read();
                    is_asc = (token.tokenType == Tokens.ASC);
                }
                ascDesc.add(is_asc);
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            break;
        }

        readThis(Tokens.CLOSEBRACKET);

        return indexExprs;
    }

    /// Collect the names of the columns being indexed, or null if indexing anything more general than columns.
    /// This adapts XreadExpressions output to the format originally produced by readColumnNames
    private OrderedHashSet voltGetSimpleColumnNames(java.util.List<Expression> indexExprs) {
        OrderedHashSet set = new OrderedHashSet();

        for (Expression expression : indexExprs) {
            if (expression instanceof ExpressionColumn) {
                String colName = ((ExpressionColumn)expression).columnName;
                if (!set.add(colName)) {
                    throw Error.error(ErrorCode.X_42577, colName);
                }
            } else {
                return null;
            }
        }

        return set;
    }

    /// Collect the names of the unique columns underlying a list of indexed expressions.
    private OrderedHashSet voltGetBaseColumnNames(java.util.List<Expression> indexExprs) {
        OrderedHashSet col_set = new OrderedHashSet();
        for (Expression expression : indexExprs) {
            expression.voltCollectAllColumnNames(col_set);
        }
        return col_set;
    }

    /// Collect the column indexes of the unique columns underlying a list of indexed expressions.
    /// This adapts XreadExpressions/getSimpleColumnNames output to the format originally produced by readColumnList.
    private int[] voltGetColumnList(OrderedHashSet set, Table table) {
        if (set == null) {
            return null;
        }
        return table.getColumnIndexes(set);
    }

    private Statement compileAlterTableAddLimitConstraint(Table table, HsqlName name)
    {
        if (name == null) {
            name = database.nameManager.newAutoName("LIMIT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        Constraint c = new Constraint(name, null, SchemaObject.ConstraintTypes.LIMIT);

        readLimitConstraintCondition(c);

        String   sql  = getLastPart();
        Object[] args = new Object[]{ StatementTypes.ADD_CONSTRAINT, table, c };
        HsqlName[] writeLockNames = new HsqlName[] {
            database.getCatalogName(), table.getName()
        };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, writeLockNames);
    }

    // End of VoltDB extension
}
