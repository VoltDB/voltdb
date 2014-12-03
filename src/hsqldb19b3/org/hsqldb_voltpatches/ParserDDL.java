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

import java.lang.reflect.Method;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.rights.GranteeManager;
import org.hsqldb_voltpatches.rights.Right;
import org.hsqldb_voltpatches.rights.User;
import org.hsqldb_voltpatches.types.Charset;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.UserTypeModifier;

/**
 * Parser for DDL statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ParserDDL extends ParserRoutine {

    final static int[]   schemaCommands           = new int[] {
        Tokens.CREATE, Tokens.GRANT
    };
    final static short[] endStatementTokens       = new short[] {
        Tokens.CREATE, Tokens.GRANT, Tokens.ALTER, Tokens.DROP
    };
    final static short[] endStatementTokensSchema = new short[] {
        Tokens.CREATE, Tokens.GRANT,
    };

    ParserDDL(Session session, Scanner scanner) {
        super(session, scanner);
    }

    @Override
    void reset(String sql) {
        super.reset(sql);
    }

    StatementSchema compileCreate() {

        int     tableType = TableBase.MEMORY_TABLE;
        boolean isTable   = false;

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
                return compileCreateTrigger();

            case Tokens.USER :
                return compileCreateUser();

            case Tokens.ROLE :
                return compileCreateRole();

            case Tokens.VIEW :
                return compileCreateView();

            case Tokens.DOMAIN :
                return compileCreateDomain();

            case Tokens.TYPE :
                return compileCreateType();

            case Tokens.CHARACTER :
                return compileCreateCharacterSet();

            // index
            // A VoltDB extension to support the assume unique attribute
            case Tokens.ASSUMEUNIQUE :
                assumeUnique = true;
                // $FALL-THROUGH$
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

            case Tokens.FUNCTION :
            case Tokens.PROCEDURE :
                return compileCreateProcedureOrFunction();

            default : {
                throw unexpectedToken();
            }
        }
    }

    void processAlter() {

        session.setScripting(true);
        readThis(Tokens.ALTER);

        switch (token.tokenType) {

            case Tokens.TABLE : {
                read();
                processAlterTable();

                break;
            }
            case Tokens.DOMAIN : {
                read();
                processAlterDomain();

                break;
            }
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

                HsqlName name =
                    readNewSchemaObjectNameNoCheck(SchemaObject.INDEX);

                readThis(Tokens.RENAME);
                readThis(Tokens.TO);

                return compileRenameObject(name, SchemaObject.INDEX);
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
                read();

                return compileAlterSequence();
            }
            case Tokens.TABLE : {
                read();

                return compileAlterTable();
            }
            case Tokens.USER : {
                read();

                return compileAlterUser();
            }
            case Tokens.DOMAIN : {
                read();

                return compileAlterDomain();
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

/*
    CompiledStatementInterface compileAlter() {

        CompiledStatementInterface cs = null;
        read();
        String sql = getStatement(getParsePosition(), endStatementTokensAlter);

        cs = new CompiledStatementSchema(sql, StatementCodes.ALTER_TYPE, null);

        return cs;
    }
*/
    Statement compileDrop() {

        int      objectTokenType;
        int      objectType;
        int      statementType;
        boolean  canCascade  = false;
        boolean  cascade     = false;
        boolean  useIfExists = false;
        boolean  ifExists    = false;
        HsqlName readName    = null;
        HsqlName writeName   = null;

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
            case Tokens.PROCEDURE : {
                read();

                statementType = StatementTypes.DROP_ROUTINE;
                objectType    = SchemaObject.PROCEDURE;
                canCascade    = true;

                break;
            }
            case Tokens.FUNCTION : {
                read();

                statementType = StatementTypes.DROP_ROUTINE;
                objectType    = SchemaObject.FUNCTION;
                canCascade    = true;

                break;
            }
            case Tokens.SCHEMA : {
                read();

                statementType = StatementTypes.DROP_SCHEMA;
                objectType    = SchemaObject.SCHEMA;
                useIfExists   = true;
                canCascade    = true;

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
                break;

            case Tokens.TYPE :
                read();

                statementType = StatementTypes.DROP_TYPE;
                objectType    = SchemaObject.TYPE;
                canCascade    = true;
                break;

            case Tokens.CHARACTER :
                read();
                readThis(Tokens.SET);

                statementType = StatementTypes.DROP_CHARACTER_SET;
                objectType    = SchemaObject.CHARSET;
                canCascade    = false;
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

        checkIsIdentifier();

        HsqlName name = null;

        switch (objectTokenType) {

            case Tokens.USER : {
                checkIsSimpleName();
                checkDatabaseUpdateAuthorisation();

                Grantee grantee =
                    database.getUserManager().get(token.tokenString);

                name = grantee.getName();

                read();

                break;
            }
            case Tokens.ROLE : {
                checkIsSimpleName();
                checkDatabaseUpdateAuthorisation();

                Grantee role =
                    database.getGranteeManager().getRole(token.tokenString);

                name = role.getName();

                read();

                break;
            }
            case Tokens.SCHEMA : {
                name      = readNewSchemaName();
                writeName = database.getCatalogName();

                break;
            }
            default :
                name = readNewSchemaObjectNameNoCheck(objectType);

                try {
                    String schemaName = name.schema == null
                                        ? session.getSchemaName(null)
                                        : name.schema.name;
                    SchemaObject object =
                        database.schemaManager.getSchemaObject(name.name,
                            schemaName, objectType);

                    if (cascade) {
                        writeName = database.getCatalogName();
                    } else {
                        writeName = object.getName();

                        if (writeName.parent != null) {
                            writeName = writeName.parent;
                        }

                        if (writeName.type != SchemaObject.TABLE) {
                            writeName = null;
                        }
                    }
                } catch (HsqlException e) {}
        }

        if (useIfExists && token.tokenType == Tokens.IF) {
            read();
            readThis(Tokens.EXISTS);

            ifExists = true;
        }

        if (canCascade) {
            if (token.tokenType == Tokens.CASCADE) {
                cascade = true;

                read();
            } else if (token.tokenType == Tokens.RESTRICT) {
                read();
            }
        }

        Object[] args = new Object[] {
            name, new Integer(objectType), Boolean.valueOf(cascade),
            Boolean.valueOf(ifExists)
        };
        String sql = getLastPart();

        return new StatementSchema(sql, statementType, args, readName,
                                   writeName);
    }

    private void processAlterTable() {

        String   tableName = token.tokenString;
        HsqlName schema    = session.getSchemaHsqlName(token.namePrefix);

        checkSchemaUpdateAuthorisation(schema);

        Table t = database.schemaManager.getUserTable(session, tableName,
            schema.name);

        if (t.isView()) {
            throw Error.error(ErrorCode.X_42501, tableName);
        }

        read();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);
                processAlterTableRename(t);

                return;
            }
            case Tokens.ADD : {
                read();

                HsqlName cname = null;

                if (token.tokenType == Tokens.CONSTRAINT) {
                    read();

                    cname = readNewDependentSchemaObjectName(t.getName(),
                            SchemaObject.CONSTRAINT);

                    database.schemaManager.checkSchemaObjectNotExists(cname);
                }

                // A VoltDB extension to support the assume unique attribute
                boolean assumeUnique = false; // For VoltDB
                // End of VoltDB extension
                switch (token.tokenType) {

                    case Tokens.FOREIGN :
                        read();
                        readThis(Tokens.KEY);
                        processAlterTableAddForeignKeyConstraint(t, cname);

                        return;

                    // A VoltDB extension to support the assume unique attribute
                    case Tokens.ASSUMEUNIQUE :
                        assumeUnique = true;
                        // $FALL-THROUGH$
                    // End of VoltDB extension
                    case Tokens.UNIQUE :
                        read();
                        // A VoltDB extension to support the assume unique attribute
                        processAlterTableAddUniqueConstraint(t, cname, assumeUnique);
                        /* disable 1 line ...
                        processAlterTableAddUniqueConstraint(t, cname);
                        ... disabled 1 line */
                        // End of VoltDB extension

                        return;

                    case Tokens.CHECK :
                        read();
                        processAlterTableAddCheckConstraint(t, cname);

                        return;

                    case Tokens.PRIMARY :
                        read();
                        readThis(Tokens.KEY);
                        processAlterTableAddPrimaryKey(t, cname);

                        return;

                    // A VoltDB extension to support LIMIT PARTITION ROWS syntax
                    case Tokens.LIMIT :
                        read();
                        processAlterTableAddLimitConstraint(t, cname);

                        return;
                    // End of VoltDB extension
                    case Tokens.COLUMN :
                        if (cname != null) {
                            throw unexpectedToken();
                        }

                        read();
                        checkIsSimpleName();
                        processAlterTableAddColumn(t);

                        return;

                    default :
                        if (cname != null) {
                            throw unexpectedToken();
                        }

                        checkIsSimpleName();
                        processAlterTableAddColumn(t);

                        return;
                }
            }
            case Tokens.DROP : {
                read();

                switch (token.tokenType) {

                    case Tokens.PRIMARY : {
                        boolean cascade = false;

                        read();
                        readThis(Tokens.KEY);

                        if (token.tokenType == Tokens.CASCADE) {
                            read();

                            cascade = true;
                        }

                        if (t.hasPrimaryKey()) {
                            processAlterTableDropConstraint(
                                t, t.getPrimaryConstraint().getName().name,
                                cascade);
                        } else {
                            throw Error.error(ErrorCode.X_42501);
                        }

                        return;
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
                        if (c != null) {
                            processAlterTableDropConstraint(
                                    t, c.getName().name, cascade);
                        }
                        else {
                            throw Error.error(ErrorCode.X_42501);
                        }

                        return;
                    }
                    // End of VoltDB extension
                    case Tokens.CONSTRAINT : {
                        boolean cascade = false;

                        read();

                        SchemaObject object = readDependentSchemaObjectName(
                            t.getName(), SchemaObject.CONSTRAINT);

                        if (token.tokenType == Tokens.RESTRICT) {
                            read();
                        } else if (token.tokenType == Tokens.CASCADE) {
                            read();

                            cascade = true;
                        }

                        processAlterTableDropConstraint(
                            t, object.getName().name, cascade);

//                        read();
                        return;
                    }
                    case Tokens.COLUMN :
                        read();

                    // $FALL-THROUGH$
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

                        processAlterTableDropColumn(t, name, cascade);

                        return;
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
                processAlterColumn(t, column, columnIndex);

                return;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    Statement compileAlterTable() {

        String   tableName = token.tokenString;
        HsqlName schema    = session.getSchemaHsqlName(token.namePrefix);
        Table t = database.schemaManager.getUserTable(session, tableName,
            schema.name);

        if (t.isView()) {
            throw Error.error(ErrorCode.X_42501, tableName);
        }

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
                boolean assumeUnique = false;
                // End of VoltDB extension
                switch (token.tokenType) {

                    case Tokens.FOREIGN :
                        read();
                        readThis(Tokens.KEY);

                        return compileAlterTableAddForeignKeyConstraint(t,
                                cname);

                    // A VoltDB extension to support the assume unique attribute
                    case Tokens.ASSUMEUNIQUE:
                        assumeUnique = true;
                        // $FALL-THROUGH$
                    // End of VoltDB extension
                    case Tokens.UNIQUE :
                        read();

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
                        read();
                        readThis(Tokens.PARTITION);
                        readThis(Tokens.ROWS);
                        return compileAlterTableDropLimit(t);
                    }
                    // End of VoltDB extension
                    case Tokens.CONSTRAINT : {
                        read();

                        return compileAlterTableDropConstraint(t);
                    }
                    case Tokens.COLUMN :
                        read();

                    // $FALL-THROUGH$
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

    private Statement compileAlterTableDropConstraint(Table t) {

        HsqlName readName  = null;
        HsqlName writeName = null;
        boolean  cascade   = false;
        SchemaObject object = readDependentSchemaObjectName(t.getName(),
            SchemaObject.CONSTRAINT);

        if (token.tokenType == Tokens.RESTRICT) {
            read();
        } else if (token.tokenType == Tokens.CASCADE) {
            read();

            cascade = true;
        }

        if (cascade) {
            writeName = database.getCatalogName();
        } else {
            writeName = t.getName();
        }

        Object[] args = new Object[] {
            object.getName(), Integer.valueOf(SchemaObject.CONSTRAINT),
            Boolean.valueOf(cascade), Boolean.valueOf(false)
        };
        String sql = getLastPart();

        return new StatementSchema(sql, StatementTypes.DROP_CONSTRAINT, args,
                                   readName, writeName);
    }

    private Statement compileAlterTableDropPrimaryKey(Table t) {

        HsqlName readName  = null;
        HsqlName writeName = null;
        boolean  cascade   = false;

        if (token.tokenType == Tokens.RESTRICT) {
            read();
        } else if (token.tokenType == Tokens.CASCADE) {
            read();

            cascade = true;
        }

        if (!t.hasPrimaryKey()) {
            throw Error.error(ErrorCode.X_42501);
        }

        if (cascade) {
            writeName = database.getCatalogName();
        } else {
            writeName = t.getName();
        }

        SchemaObject object = t.getPrimaryConstraint();
        Object[]     args   = new Object[] {
            object.getName(), Integer.valueOf(SchemaObject.CONSTRAINT),
            Boolean.valueOf(cascade), Boolean.valueOf(false)
        };
        String sql = getLastPart();

        return new StatementSchema(sql, StatementTypes.DROP_CONSTRAINT, args,
                                   readName, writeName);
    }

    StatementSchema compileCreateTable(int type) {

        HsqlName name = readNewSchemaObjectNameNoCheck(SchemaObject.TABLE);
        HsqlArrayList tempConstraints = new HsqlArrayList();

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

        Table table = TableUtil.newTable(database, type, name);

        if (token.tokenType == Tokens.AS) {
            return readTableAsSubqueryDefinition(table);
        }

        int position = getPosition();

        readThis(Tokens.OPENBRACKET);

        {
            Constraint c = new Constraint(null, null, Constraint.TEMP);

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
                        database.nameManager.newColumnHsqlName(name,
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

        Object[] args = new Object[] {
            table, tempConstraints, null
        };
        String   sql  = getLastPart();

        return new StatementSchema(sql, StatementTypes.CREATE_TABLE, args,
                                   null, null);
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

        HsqlName   readName    = null;
        boolean    withData    = true;
        HsqlName[] columnNames = null;
        Statement  statement   = null;

        if (token.tokenType == Tokens.OPENBRACKET) {
            columnNames = readColumnNames(table.getName());
        }

        readThis(Tokens.AS);
        readThis(Tokens.OPENBRACKET);

        QueryExpression queryExpression = XreadQueryExpression();

        queryExpression.setAsTopLevel();
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

        TableUtil.setColumnsInSchemaTable(
            table, queryExpression.getResultColumnNames(),
            queryExpression.getColumnTypes());

        if (columnNames != null) {
            if (columnNames.length != queryExpression.getColumnCount()) {
                throw Error.error(ErrorCode.X_42593);
            }

            for (int i = 0; i < columnNames.length; i++) {
                table.getColumn(i).getName().rename(columnNames[i]);
            }
        }

        table.createPrimaryKey();

        if (withData) {
            statement = new StatementQuery(session, queryExpression,
                                           compileContext);
        }

        Object[] args = new Object[] {
            table, null, statement
        };
        String   sql  = getLastPart();
        StatementSchema st = new StatementSchema(sql,
            StatementTypes.CREATE_TABLE, args, null, null);

        st.readTableNames = statement.getTableNamesForRead();

        return st;
    }

    /**
     * Adds a list of temp constraints to a new table
     */
    static Table addTableConstraintDefinitions(Session session, Table table,
            HsqlArrayList tempConstraints, HsqlArrayList constraintList) {

        Constraint c        = (Constraint) tempConstraints.get(0);
        String     namePart = c.getName() == null ? null
                                                  : c.getName().name;
        HsqlName indexName = session.database.nameManager.newAutoName("IDX",
            namePart, table.getSchemaName(), table.getName(),
            SchemaObject.INDEX);

        if (c.mainColSet != null) {
            c.core.mainCols = table.getColumnIndexes(c.mainColSet);
        }

        table.createPrimaryKey(indexName, c.core.mainCols, true);

        if (c.core.mainCols != null) {
            Constraint newconstraint = new Constraint(c.getName(), table,
                table.getPrimaryIndex(), Constraint.PRIMARY_KEY);

            table.addConstraint(newconstraint);
            session.database.schemaManager.addSchemaObject(newconstraint);
        }

        for (int i = 1; i < tempConstraints.size(); i++) {
            c = (Constraint) tempConstraints.get(i);

            switch (c.constType) {

                case Constraint.UNIQUE : {
                    c.setColumnsIndexes(table);

                    // A VoltDB extension to support indexed expressions and the assume unique attribute
                    if (c.indexExprs != null) {
                        // Special case handling for VoltDB indexed expressions
                        if (table.getUniqueConstraintForExprs(c.indexExprs) != null) {
                            throw Error.error(ErrorCode.X_42522);
                        }
                    }
                    else
                    // End of VoltDB extension
                    if (table.getUniqueConstraintForColumns(c.core.mainCols)
                            != null) {
                        throw Error.error(ErrorCode.X_42522);
                    }

                    // create an autonamed index
                    indexName = session.database.nameManager.newAutoName("IDX",
                            c.getName().name, table.getSchemaName(),
                            table.getName(), SchemaObject.INDEX);

                    // A VoltDB extension to support indexed expressions and the assume unique attribute
                    Index index = null;
                    if (c.indexExprs != null) {
                        // Special case handling for VoltDB indexed expressions
                        index = table.createAndAddExprIndexStructure(indexName, c.core.mainCols, c.indexExprs, true, true).setAssumeUnique(c.assumeUnique);
                    } else {
                        index = table.createAndAddIndexStructure(indexName,
                            c.core.mainCols, null, null, true, true, false).setAssumeUnique(c.assumeUnique);
                    }
                    /* disable 2 lines ...
                    Index index = table.createAndAddIndexStructure(indexName,
                        c.core.mainCols, null, null, true, true, false);
                    ... disabled 2 lines */
                    // End of VoltDB extension
                    Constraint newconstraint = new Constraint(c.getName(),
                        table, index, Constraint.UNIQUE);
                    // A VoltDB extension to support the assume unique attribute
                    newconstraint = newconstraint.setAssumeUnique(c.assumeUnique);
                    // End of VoltDB extension

                    table.addConstraint(newconstraint);
                    session.database.schemaManager.addSchemaObject(
                        newconstraint);

                    break;
                }
                case Constraint.FOREIGN_KEY : {
                    addForeignKey(session, table, c, constraintList);

                    break;
                }
                case Constraint.CHECK : {
                    c.prepareCheckConstraint(session, table, false);
                    table.addConstraint(c);

                    if (c.isNotNull()) {
                        ColumnSchema column =
                            table.getColumn(c.notNullColumnIndex);

                        column.setNullable(false);
                        table.setColumnTypeVars(c.notNullColumnIndex);
                    }

                    session.database.schemaManager.addSchemaObject(c);

                    break;
                }
                // A VoltDB extension to support LIMIT PARTITION ROWS
                case Constraint.LIMIT : {
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

        Constraint uniqueConstraint =
            c.core.mainTable.getUniqueConstraintForColumns(c.core.mainCols,
                c.core.refCols);
        Index      mainIndex  = uniqueConstraint.getMainIndex();
        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.checkCreateForeignKey(c);

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
        Index index = table.createAndAddIndexStructure(refIndexName,
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
        } else {

            // columns are resolved in the calling method
            if (mainTableName == refTable.getName()) {

                // fredt - FK statement is part of CREATE TABLE and is self-referencing
                // reference must be to same table being created
            } else {
/*
                if (!mainTable.hasPrimaryKey()) {
                    throw Trace.error(Trace.CONSTRAINT_NOT_FOUND,
                                      Trace.TABLE_HAS_NO_PRIMARY_KEY);

                }
*/
            }
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
        int               deleteAction = Constraint.NO_ACTION;
        int               updateAction = Constraint.NO_ACTION;
        OrderedIntHashSet set          = new OrderedIntHashSet();

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

                            deleteAction = Constraint.SET_DEFAULT;

                            break;
                        }
                        case Tokens.NULL :
                            read();

                            deleteAction = Constraint.SET_NULL;
                            break;

                        default :
                            throw unexpectedToken();
                    }
                } else if (token.tokenType == Tokens.CASCADE) {
                    read();

                    deleteAction = Constraint.CASCADE;
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

                            deleteAction = Constraint.SET_DEFAULT;

                            break;
                        }
                        case Tokens.NULL :
                            read();

                            deleteAction = Constraint.SET_NULL;
                            break;

                        default :
                            throw unexpectedToken();
                    }
                } else if (token.tokenType == Tokens.CASCADE) {
                    read();

                    updateAction = Constraint.CASCADE;
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
                              Constraint.FOREIGN_KEY, deleteAction,
                              updateAction, matchType);
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

    void processCreateView() {

        StatementSchema cs   = compileCreateView();
        View            view = (View) cs.arguments[0];

        checkSchemaUpdateAuthorisation(view.getSchemaName());
        database.schemaManager.checkSchemaObjectNotExists(view.getName());
        database.schemaManager.addSchemaObject(view);
    }

    StatementSchema compileCreateView() {

        read();

        HsqlName name = readNewSchemaObjectName(SchemaObject.VIEW);

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

        HsqlName[] colList = null;

        if (token.tokenType == Tokens.OPENBRACKET) {
            colList = readColumnNames(name);
        }

        readThis(Tokens.AS);
        startRecording();

        int             position = getPosition();
        QueryExpression queryExpression;

        try {
            queryExpression = XreadQueryExpression();
        } catch (HsqlException e) {
            queryExpression = XreadJoinedTable();
        }

        Token[] statement = getRecordedStatement();
        String  sql       = getLastPart(position);
        int     check     = SchemaObject.ViewCheckModes.CHECK_NONE;

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

        View view = new View(session, database, name, colList, sql, check);

        queryExpression.setAsTopLevel();
        queryExpression.setView(view);
        queryExpression.resolve(session);
        view.compile(session);
        checkSchemaUpdateAuthorisation(name.schema);
        database.schemaManager.checkSchemaObjectNotExists(name);

        String statementSQL = Token.getSQL(statement);

        view.statement = statementSQL;

        String   fullSQL = getLastPart();
        Object[] args    = new Object[]{ view };

        return new StatementSchema(fullSQL, StatementTypes.CREATE_VIEW, args,
                                   null, null);
    }

    StatementSchema compileCreateSequence() {

        read();

        /*
                CREATE SEQUENCE <name>
                [AS {INTEGER | BIGINT}]
                [START WITH <value>]
                [INCREMENT BY <value>]
        */
        HsqlName name = readNewSchemaObjectNameNoCheck(SchemaObject.SEQUENCE);
        NumberSequence sequence = new NumberSequence(name, Type.SQL_INTEGER);

        readSequenceOptions(sequence, true, false);

        String   sql  = getLastPart();
        Object[] args = new Object[]{ sequence };

        return new StatementSchema(sql, StatementTypes.CREATE_SEQUENCE, args,
                                   null, null);
    }

    StatementSchema compileCreateDomain() {

        UserTypeModifier userTypeModifier = null;
        HsqlName         name;

        read();

        name = readNewSchemaObjectNameNoCheck(SchemaObject.DOMAIN);

        readIfThis(Tokens.AS);

        Type       type          = readTypeDefinition(false).duplicate();
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

            c.prepareCheckConstraint(session, null, false);
            userTypeModifier.addConstraint(c);
        }

        String   sql  = getLastPart();
        Object[] args = new Object[]{ type };

        return new StatementSchema(sql, StatementTypes.CREATE_DOMAIN, args,
                                   null, null);
    }

    StatementSchema compileCreateType() {

        read();

        HsqlName name = readNewSchemaObjectNameNoCheck(SchemaObject.TYPE);

        readThis(Tokens.AS);

        Type type = readTypeDefinition(false).duplicate();

        readIfThis(Tokens.FINAL);

        UserTypeModifier userTypeModifier = new UserTypeModifier(name,
            SchemaObject.TYPE, type);

        type.userTypeModifier = userTypeModifier;

        String   sql  = getLastPart();
        Object[] args = new Object[]{ type };

        return new StatementSchema(sql, StatementTypes.CREATE_TYPE, args,
                                   null, null);
    }

    StatementSchema compileCreateCharacterSet() {

        read();
        readThis(Tokens.SET);

        HsqlName name = readNewSchemaObjectNameNoCheck(SchemaObject.CHARSET);

        readIfThis(Tokens.AS);
        readThis(Tokens.GET);

        String schema = session.getSchemaName(token.namePrefix);
        Charset source =
            (Charset) database.schemaManager.getSchemaObject(token.tokenString,
                schema, SchemaObject.CHARSET);

        read();

        if (token.tokenType == Tokens.COLLATION) {
            read();
            readThis(Tokens.FROM);
            readThis(Tokens.DEFAULT);
        }

        Charset charset = new Charset(name);

        charset.base = source.getName();

        String   sql  = getLastPart();
        Object[] args = new Object[]{ charset };

        return new StatementSchema(sql, StatementTypes.CREATE_CHARACTER_SET,
                                   args, null, null);
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

            routines = Routine.newRoutines(methods);
        }

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            name, routines
        };

        return new StatementSchema(sql, StatementTypes.CREATE_ALIAS, args,
                                   null, null);
    }

    StatementSchema compileCreateTrigger() {

        Table          table;
        boolean        isForEachRow = false;
        boolean        isNowait     = false;
        boolean        hasQueueSize = false;
        Integer        queueSize    = TriggerDef.defaultQueueSize;
        String         beforeOrAfter;
        int            beforeOrAfterType;
        String         operation;
        int            operationType;
        String         className;
        TriggerDef     td;
        HsqlName       name;
        HsqlName       otherName = null;
        OrderedHashSet columns   = null;
        int[]          updateColumnIndexes = null;

        read();

        name = readNewSchemaObjectName(SchemaObject.TRIGGER);

        switch (token.tokenType) {

            case Tokens.INSTEAD :
                beforeOrAfter     = token.tokenString;
                beforeOrAfterType = token.tokenType;

                read();
                readThis(Tokens.OF);
                break;

            case Tokens.BEFORE :
            case Tokens.AFTER :
                beforeOrAfter     = token.tokenString;
                beforeOrAfterType = token.tokenType;

                read();
                break;

            default :
                throw unexpectedToken();
        }

        switch (token.tokenType) {

            case Tokens.INSERT :
            case Tokens.DELETE :
                operation     = token.tokenString;
                operationType = token.tokenType;

                read();
                break;

            case Tokens.UPDATE :
                operation     = token.tokenString;
                operationType = token.tokenType;

                read();

                if (token.tokenType == Tokens.OF
                        && beforeOrAfterType != Tokens.INSTEAD) {
                    read();

                    columns = readColumnNames(false);
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

            otherName = readNewSchemaObjectName(SchemaObject.TRIGGER);
        }

        name.setSchemaIfNull(table.getSchemaName());
        checkSchemaUpdateAuthorisation(name.schema);

        if (beforeOrAfterType == Tokens.INSTEAD) {
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

        Expression      condition          = null;
        String          oldTableName       = null;
        String          newTableName       = null;
        String          oldRowName         = null;
        String          newRowName         = null;
        Table[]         transitions        = new Table[4];
        RangeVariable[] rangeVars          = new RangeVariable[4];
        HsqlArrayList   compiledStatements = new HsqlArrayList();
        String          conditionSQL       = null;
        String          procedureSQL       = null;

        if (token.tokenType == Tokens.REFERENCING) {
            read();

            if (token.tokenType != Tokens.OLD
                    && token.tokenType != Tokens.NEW) {
                throw unexpectedToken();
            }

            while (true) {
                if (token.tokenType == Tokens.OLD) {
                    if (operationType == Tokens.INSERT) {
                        throw unexpectedToken();
                    }

                    read();

                    if (token.tokenType == Tokens.TABLE) {
                        if (oldTableName != null
                                || beforeOrAfterType == Tokens.BEFORE) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        oldTableName = token.tokenString;

                        String n = oldTableName;

                        if (n.equals(newTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isDelimitedIdentifier(),
                            SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.OLD_TABLE] = transition;
                        rangeVars[TriggerDef.OLD_TABLE]   = range;
                    } else if (token.tokenType == Tokens.ROW) {
                        if (oldRowName != null) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        oldRowName = token.tokenString;

                        String n = oldRowName;

                        if (n.equals(newTableName) || n.equals(oldTableName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        isForEachRow = true;

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isDelimitedIdentifier(),
                            SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.OLD_ROW] = transition;
                        rangeVars[TriggerDef.OLD_ROW]   = range;
                    } else {
                        throw unexpectedToken();
                    }
                } else if (token.tokenType == Tokens.NEW) {
                    if (operationType == Tokens.DELETE) {
                        throw unexpectedToken();
                    }

                    read();

                    if (token.tokenType == Tokens.TABLE) {
                        if (newTableName != null
                                || beforeOrAfterType == Tokens.BEFORE) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        newTableName = token.tokenString;

                        String n = newTableName;

                        if (n.equals(oldTableName) || n.equals(oldRowName)
                                || n.equals(newRowName)) {
                            throw unexpectedToken();
                        }

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isDelimitedIdentifier(),
                            SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.NEW_TABLE] = transition;
                        rangeVars[TriggerDef.NEW_TABLE]   = range;
                    } else if (token.tokenType == Tokens.ROW) {
                        if (newRowName != null) {
                            throw unexpectedToken();
                        }

                        read();
                        readIfThis(Tokens.AS);
                        checkIsSimpleName();

                        newRowName   = token.tokenString;
                        isForEachRow = true;

                        String n = newRowName;

                        if (n.equals(oldTableName) || n.equals(newTableName)
                                || n.equals(oldRowName)) {
                            throw unexpectedToken();
                        }

                        HsqlName hsqlName = database.nameManager.newHsqlName(
                            table.getSchemaName(), n, isDelimitedIdentifier(),
                            SchemaObject.TRANSITION);
                        Table transition = new Table(table, hsqlName);
                        RangeVariable range = new RangeVariable(transition,
                            null, null, null, compileContext);

                        transitions[TriggerDef.NEW_ROW] = transition;
                        rangeVars[TriggerDef.NEW_ROW]   = range;
                    } else {
                        throw unexpectedToken();
                    }
                } else {
                    break;
                }

                read();
            }
        }

        if (isForEachRow && token.tokenType != Tokens.FOR) {
            throw unexpectedToken();
        }

        // "FOR EACH ROW" or "CALL"
        if (token.tokenType == Tokens.FOR) {
            read();
            readThis(Tokens.EACH);

            if (token.tokenType == Tokens.ROW) {
                isForEachRow = true;
            } else if (token.tokenType == Tokens.STATEMENT) {
                if (isForEachRow) {
                    throw unexpectedToken();
                }
            } else {
                throw unexpectedToken();
            }

            read();
        }

        //
        if (rangeVars[TriggerDef.OLD_TABLE] != null) {}

        if (rangeVars[TriggerDef.NEW_TABLE] != null) {}

        //
        if (Tokens.T_NOWAIT.equals(token.tokenString)) {
            read();

            isNowait = true;
        } else if (Tokens.T_QUEUE.equals(token.tokenString)) {
            read();

            queueSize    = readInteger();
            hasQueueSize = true;
        }

        if (token.tokenType == Tokens.WHEN
                && beforeOrAfterType != Tokens.INSTEAD) {
            read();
            readThis(Tokens.OPENBRACKET);

            int position = getPosition();

            isCheckOrTriggerCondition = true;
            condition                 = XreadBooleanValueExpression();
            conditionSQL              = getLastPart(position);
            isCheckOrTriggerCondition = false;

            readThis(Tokens.CLOSEBRACKET);

            HsqlList unresolved = condition.resolveColumnReferences(rangeVars,
                null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);

            if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }

        if (token.tokenType == Tokens.CALL) {
            read();
            checkIsSimpleName();
            checkIsDelimitedIdentifier();

            className = token.tokenString;

            read();

            td = new TriggerDef(name, beforeOrAfter, operation, isForEachRow,
                                table, transitions, rangeVars, condition,
                                conditionSQL, updateColumnIndexes, className, isNowait,
                                queueSize);

            String   sql  = getLastPart();
            Object[] args = new Object[] {
                td, otherName
            };

            return new StatementSchema(sql, StatementTypes.CREATE_TRIGGER,
                                       args, null, table.getName());
        }

        //
        if (isNowait) {
            throw unexpectedToken(Tokens.T_NOWAIT);
        }

        if (hasQueueSize) {
            throw unexpectedToken(Tokens.T_QUEUE);
        }

        // procedure
        boolean isBlock = false;

        if (readIfThis(Tokens.BEGIN)) {
            readThis(Tokens.ATOMIC);

            isBlock = true;
        }

        int position = getPosition();

        while (true) {
            StatementDMQL cs = null;

            switch (token.tokenType) {

                case Tokens.INSERT :
                    if (beforeOrAfterType == Tokens.BEFORE) {
                        throw unexpectedToken();
                    }

                    cs = compileInsertStatement(rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Tokens.SEMICOLON);
                    }
                    break;

                case Tokens.UPDATE :
                    if (beforeOrAfterType == Tokens.BEFORE) {
                        throw unexpectedToken();
                    }

                    cs = compileUpdateStatement(rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Tokens.SEMICOLON);
                    }
                    break;

                case Tokens.DELETE :
                    if (beforeOrAfterType == Tokens.BEFORE) {
                        throw unexpectedToken();
                    }

                    cs = compileDeleteStatement(rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Tokens.SEMICOLON);
                    }
                    break;

                case Tokens.MERGE :
                    if (beforeOrAfterType == Tokens.BEFORE) {
                        throw unexpectedToken();
                    }

                    cs = compileMergeStatement(rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Tokens.SEMICOLON);
                    }
                    break;

                case Tokens.SET :
                    if (beforeOrAfterType != Tokens.BEFORE
                            || operationType == Tokens.DELETE) {
                        throw unexpectedToken();
                    }

                    cs = compileTriggerSetStatement(table, rangeVars);

                    compiledStatements.add(cs);

                    if (isBlock) {
                        readThis(Tokens.SEMICOLON);
                    }
                    break;

                case Tokens.END :
                    break;

                default :
                    throw unexpectedToken();
            }

            if (!isBlock || token.tokenType == Tokens.END) {
                break;
            }
        }

        procedureSQL = getLastPart(position);

        if (isBlock) {
            readThis(Tokens.END);
        }

        StatementDMQL[] csArray = new StatementDMQL[compiledStatements.size()];

        compiledStatements.toArray(csArray);

        OrderedHashSet references = compileContext.getSchemaObjectNames();

        for (int i = 0; i < csArray.length; i++) {
            Table     targetTable = csArray[i].targetTable;
            boolean[] check = csArray[i].getInsertOrUpdateColumnCheckList();

            if (check != null) {
                targetTable.getColumnNames(check, references);
            }
        }

        references.remove(table.getName());

        td = new TriggerDefSQL(name, beforeOrAfter, operation, isForEachRow,
                               table, transitions, rangeVars, condition,
                               conditionSQL, updateColumnIndexes, csArray, procedureSQL,
                               references);

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            td, otherName
        };

        return new StatementSchema(sql, StatementTypes.CREATE_TRIGGER, args,
                                   null, table.getName());
    }

    /**
     * Creates SET Statement for a trigger row from this parse context.
     */
    StatementDMQL compileTriggerSetStatement(Table table,
            RangeVariable[] rangeVars) {

        read();

        Expression[]   updateExpressions;
        int[]          columnMap;
        OrderedHashSet colNames = new OrderedHashSet();
        HsqlArrayList  exprList = new HsqlArrayList();
        RangeVariable[] targetRangeVars = new RangeVariable[]{
            rangeVars[TriggerDef.NEW_ROW] };

        readSetClauseList(targetRangeVars, colNames, exprList);

        columnMap         = table.getColumnIndexes(colNames);
        updateExpressions = new Expression[exprList.size()];

        exprList.toArray(updateExpressions);
        resolveUpdateExpressions(table, rangeVars, columnMap,
                                 updateExpressions, RangeVariable.emptyArray);

        StatementDMQL cs = new StatementDML(session, table, rangeVars,
                                            columnMap, updateExpressions,
                                            compileContext);

        return cs;
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

        boolean        isIdentity     = false;
        boolean        isPKIdentity   = false;
        boolean        identityAlways = false;
        Expression     generateExpr   = null;
        boolean        isNullable     = true;
        Expression     defaultExpr    = null;
        Type           typeObject;
        NumberSequence sequence = null;

        if (token.tokenType == Tokens.IDENTITY) {
            read();

            isIdentity   = true;
            isPKIdentity = true;
            typeObject   = Type.SQL_INTEGER;
            sequence     = new NumberSequence(null, 0, 1, typeObject);
        } else if (token.tokenType == Tokens.COMMA) {
            ;
            return null;
        } else {
            typeObject = readTypeDefinition(true);
        }

        if (isIdentity) {}
        else if (token.tokenType == Tokens.DEFAULT) {
            read();

            defaultExpr = readDefaultClause(typeObject);
        } else if (token.tokenType == Tokens.GENERATED && !isIdentity) {
            read();

            if (token.tokenType == Tokens.BY) {
                read();
                readThis(Tokens.DEFAULT);
            } else {
                readThis(Tokens.ALWAYS);

                identityAlways = true;
            }

            readThis(Tokens.AS);

            if (token.tokenType == Tokens.IDENTITY) {
                read();

                sequence = new NumberSequence(null, typeObject);

                sequence.setAlways(identityAlways);

                if (token.tokenType == Tokens.OPENBRACKET) {
                    read();
                    readSequenceOptions(sequence, false, false);
                    readThis(Tokens.CLOSEBRACKET);
                }

                isIdentity = true;
            } else if (token.tokenType == Tokens.OPENBRACKET) {
                read();

                generateExpr = XreadValueExpression();

                readThis(Tokens.CLOSEBRACKET);
            }
        }

        ColumnSchema column = new ColumnSchema(hsqlName, typeObject,
                                               isNullable, false, defaultExpr);

        readColumnConstraints(table, column, constraintList);

        if (token.tokenType == Tokens.IDENTITY && !isIdentity) {
            read();

            isIdentity   = true;
            isPKIdentity = true;
            sequence     = new NumberSequence(null, 0, 1, typeObject);
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
            Constraint c = new Constraint(constName, set,
                                          Constraint.PRIMARY_KEY);

            constraintList.set(0, c);
            column.setPrimaryKey(true);
        }

        return column;
    }

    private void readSequenceOptions(NumberSequence sequence,
                                     boolean withType, boolean isAlter) {

        OrderedIntHashSet set = new OrderedIntHashSet();

        while (true) {
            boolean end = false;

            if (set.contains(token.tokenType)) {
                throw unexpectedToken();
            }

            switch (token.tokenType) {

                case Tokens.AS : {
                    if (withType) {
                        read();

                        Type type = readTypeDefinition(true);

                        sequence.setDefaults(sequence.name, type);

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

                    set.add(token.tokenType);
                    read();
                    break;

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
                case Tokens.CYCLE :
                    set.add(token.tokenType);
                    read();
                    sequence.setCycle(true);
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

                if (mainConst.constType == Constraint.PRIMARY_KEY) {
                    throw Error.error(ErrorCode.X_42532);
                }

                if (constName == null) {
                    constName = database.nameManager.newAutoName("PK",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                OrderedHashSet set = readColumnNames(false);
                Constraint c = new Constraint(constName, set,
                                              Constraint.PRIMARY_KEY);

                constraintList.set(0, c);

                break;
            }
            // A VoltDB extension to support indexed expressions and the assume unique attribute
            case Tokens.ASSUMEUNIQUE :
                assumeUnique = true;
                // $FALL-THROUGH$
            // End of VoltDB extension
            case Tokens.UNIQUE : {
                if (schemaObject.getName().type != SchemaObject.TABLE) {
                    throw this.unexpectedTokenRequire(Tokens.T_CHECK);
                }

                read();

                // A VoltDB extension to "readColumnNames(false)" to support indexed expressions.
                java.util.List<Expression> indexExprs = XreadExpressions(null);
                OrderedHashSet set = getSimpleColumnNames(indexExprs);
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
                    set = getBaseColumnNames(indexExprs);
                }
                // End of VoltDB extension
                Constraint c = new Constraint(constName, set,
                                              Constraint.UNIQUE);
                // A VoltDB extension to support indexed expressions and assume unique attribute.
                c.setAssumeUnique(assumeUnique);
                if (hasNonColumnExprs) {
                    c = c.withExpressions(indexExprs.toArray(new Expression[indexExprs.size()]));
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

                Constraint c = new Constraint(constName, null,
                                              Constraint.CHECK);

                readCheckConstraintCondition(c);
                constraintList.add(c);

                break;
            }
            // A VoltDB extension to support LIMIT PARTITION ROWS
            case Tokens.LIMIT : {
                read();

                for (int i = 0;  i < constraintList.size(); i++) {
                    if (((Constraint)constraintList.get(i)).getConstraintType() == Constraint.LIMIT) {
                        throw Error.error(ErrorCode.X_42524,
                                String.format("Multiple LIMIT PARTITION ROWS constraints on table %s are forbidden.", schemaObject.getName().name));
                    }
                }

                if (constName == null) {
                    constName = database.nameManager.newAutoName("LIMIT",
                            schemaObject.getSchemaName(),
                            schemaObject.getName(), SchemaObject.CONSTRAINT);
                }

                Constraint c = new Constraint(constName, null, Constraint.LIMIT);
                readLimitConstraintCondition(c);
                constraintList.add(c);

                break;
            }
            // End of VoltDB extension
            default : {
                if (constName != null) {
                    throw Error.error(ErrorCode.X_42581);
                }
            }
        }
    }

    /**
     * Reads column constraints
     */
    void readColumnConstraints(Table table, ColumnSchema column,
                               HsqlArrayList constraintList) {

        boolean end = false;

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
                    read();
                    readThis(Tokens.KEY);

                    Constraint existingConst =
                        (Constraint) constraintList.get(0);

                    if (existingConst.constType == Constraint.PRIMARY_KEY) {
                        throw Error.error(ErrorCode.X_42532);
                    }

                    OrderedHashSet set = new OrderedHashSet();

                    set.add(column.getName().name);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("PK",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c = new Constraint(constName, set,
                                                  Constraint.PRIMARY_KEY);

                    constraintList.set(0, c);
                    column.setPrimaryKey(true);

                    break;
                }
                // A VoltDB extension to support indexed expressions and the assume unique attribute
                case Tokens.ASSUMEUNIQUE :
                    assumeUnique = true;
                    // $FALL-THROUGH$
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

                    Constraint c = new Constraint(constName, set,
                                                  Constraint.UNIQUE);
                    // A VoltDB extension to support indexed expressions and the assume unique attribute
                    c.setAssumeUnique(assumeUnique);
                    // End of VoltDB extension

                    constraintList.add(c);

                    break;
                }
                case Tokens.FOREIGN : {
                    read();
                    readThis(Tokens.KEY);
                }

                // $FALL-THROUGH$
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

                    Constraint c = new Constraint(constName, null,
                                                  Constraint.CHECK);

                    readCheckConstraintCondition(c);

                    OrderedHashSet set = c.getCheckColumnExpressions();

                    for (int i = 0; i < set.size(); i++) {
                        ExpressionColumn e = (ExpressionColumn) set.get(i);

                        if (column.getName().name.equals(e.getColumnName())) {
                            if (e.getSchemaName() != null
                                    && e.getSchemaName()
                                       != table.getSchemaName().name) {
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
                    read();
                    readThis(Tokens.NULL);

                    if (constName == null) {
                        constName = database.nameManager.newAutoName("CT",
                                table.getSchemaName(), table.getName(),
                                SchemaObject.CONSTRAINT);
                    }

                    Constraint c = new Constraint(constName, null,
                                                  Constraint.CHECK);

                    c.check = new ExpressionLogical(column);

                    constraintList.add(c);

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

        c.check          = condition;
        c.checkStatement = Token.getSQL(tokens);
    }

    /**
     * Process a bracketed column list as used in the declaration of SQL
     * CONSTRAINTS and return an array containing the indexes of the columns
     * within the table.
     *
     * @param table table that contains the columns
     * @param ascOrDesc boolean
     * @return array of column indexes
     */
    private int[] readColumnList(Table table, boolean ascOrDesc) {

        OrderedHashSet set = readColumnNames(ascOrDesc);

        return table.getColumnIndexes(set);
    }

    // A VoltDB extension to support indexed expressions and the assume unique attribute
    StatementSchema compileCreateIndex(boolean unique, boolean assumeUnique) {
    /* disable 1 line ...
    StatementSchema compileCreateIndex(boolean unique) {
    ... disabled 1 line */
    // End of VoltDB extension

        Table    table;
        HsqlName indexHsqlName;

        read();

        indexHsqlName = readNewSchemaObjectName(SchemaObject.INDEX);

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
        // A VoltDB extension to "readColumnList(table, true)" to support indexed expressions.
        java.util.List<Expression> indexExprs = XreadExpressions(ascDesc);
        OrderedHashSet set = getSimpleColumnNames(indexExprs);
        int[] indexColumns = null;
        if (set == null) {
            // A VoltDB extension to support indexed expressions.
            // Not just indexing columns.
            // The meaning of set and indexColumns shifts here to be
            // the set of unique base columns for the indexed expressions.
            set = getBaseColumnNames(indexExprs);
        } else {
            // Just indexing columns -- by-pass extended support for generalized index expressions.
            indexExprs = null;
        }

        indexColumns = getColumnList(set, table);
        String   sql          = getLastPart();
        Object[] args         = new Object[] {
            table, indexColumns, indexHsqlName, Boolean.valueOf(unique), indexExprs,
            Boolean.valueOf(assumeUnique)
        /* disable 4 lines ...
        int[]    indexColumns = readColumnList(table, true);
        String   sql          = getLastPart();
        Object[] args         = new Object[] {
            table, indexColumns, indexHsqlName, Boolean.valueOf(unique)
        ... disabled 4 lines */
        // End of VoltDB extension
        };

        return new StatementSchema(sql, StatementTypes.CREATE_INDEX, args,
                                   null, table.getName());
    }

    StatementSchema compileCreateSchema() {

        HsqlName schemaName    = null;
        String   authorisation = null;

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
            throw Error.error(ErrorCode.X_0L000,
                              session.getGrantee().getNameString());
        }

        if (database.schemaManager.schemaExists(schemaName.name)) {
            throw Error.error(ErrorCode.X_42504, schemaName.name);
        }

        String        sql  = getLastPart();
        Object[]      args = new Object[] {
            schemaName, owner
        };
        HsqlArrayList list = new HsqlArrayList();
        StatementSchema cs = new StatementSchema(sql,
            StatementTypes.CREATE_SCHEMA, args, null, null);

        cs.setSchemaHsqlName(schemaName);
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
                                               endStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null,
                                                     null, null);
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
                                               endStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null,
                                                     null, null);
                            break;

                        case Tokens.TYPE :
                            cs     = compileCreateType();
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
                                               endStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null,
                                                     null, null);
                            break;

                        case Tokens.TRIGGER :
                            statementType = StatementTypes.CREATE_TRIGGER;
                            sql = getStatement(position,
                                               endStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null,
                                                     null, null);
                            break;

                        case Tokens.VIEW :
                            statementType = StatementTypes.CREATE_VIEW;
                            sql = getStatement(position,
                                               endStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null,
                                                     null, null);
                            break;

                        case Tokens.FUNCTION :
                            statementType = StatementTypes.CREATE_ROUTINE;
                            sql = getStatement(position,
                                               endStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null,
                                                     null, null);
                            break;

                        case Tokens.PROCEDURE :
                            statementType = StatementTypes.CREATE_ROUTINE;
                            sql = getStatement(position,
                                               endStatementTokensSchema);
                            cs = new StatementSchema(sql, statementType, null,
                                                     null, null);
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

        HsqlName name = readNewUserIdentifier();
        String   sql  = getLastPart();
        Object[] args = new Object[]{ name };

        return new StatementSchema(sql, StatementTypes.CREATE_ROLE, args,
                                   null, null);
    }

    StatementSchema compileCreateUser() {

        HsqlName name;
        String   password;
        boolean  admin   = false;
        Grantee  grantor = session.getGrantee();

        read();

        name = readNewUserIdentifier();

        readThis(Tokens.PASSWORD);

        password = readPassword();

        if (token.tokenType == Tokens.ADMIN) {
            read();

            admin = true;
        }

        checkDatabaseUpdateAuthorisation();

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            name, password, grantor, Boolean.valueOf(admin)
        };

        return new StatementSchema(sql, StatementTypes.CREATE_USER, args,
                                   null, null);
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

        read();

        return tokenS;
    }

    Statement compileRenameObject(HsqlName name, int objectType) {

        HsqlName newName = readNewSchemaObjectName(objectType);
        String   sql     = getLastPart();
        Object[] args    = new Object[] {
            name, newName
        };

        return new StatementSchema(sql, StatementTypes.RENAME_OBJECT, args,
                                   null, null);
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... RENAME ...
     * @param table table
     */
    void processAlterTableRename(Table table) {

        HsqlName name = readNewSchemaObjectName(SchemaObject.TABLE);

        name.setSchemaIfNull(table.getSchemaName());

        if (table.getSchemaName() != name.schema) {
            throw Error.error(ErrorCode.X_42505);
        }

        database.schemaManager.renameSchemaObject(table.getName(), name);
    }

    // A VoltDB extension to support indexed expressions and the assume unique attribute
    void processAlterTableAddUniqueConstraint(Table table, HsqlName name, boolean assumeUnique) {
    /* disable 1 line ...
    void processAlterTableAddUniqueConstraint(Table table, HsqlName name) {
    ... disabled 1 line */
    // End of VoltDB extension

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        // A VoltDB extension to "readColumnList(table, false)" to support indexed expressions.
        java.util.List<Expression> indexExprs = XreadExpressions(null);
        OrderedHashSet set = getSimpleColumnNames(indexExprs);
        int[] cols = getColumnList(set, table);
        /* disable 1 line ...
        int[] cols = this.readColumnList(table, false);
        ... disabled 1 line */
        // End of VoltDB extension

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        // A VoltDB extension to support indexed expressions and the assume unique attribute
        if ((indexExprs != null) && (cols == null)) {
            // A VoltDB extension to support indexed expressions.
            // Not just indexing columns.
            // The meaning of cols shifts here to be
            // the set of unique base columns for the indexed expressions.
            set = getBaseColumnNames(indexExprs);
            cols = getColumnList(set, table);
            tableWorks.addUniqueExprConstraint(cols, indexExprs.toArray(new Expression[indexExprs.size()]), name, assumeUnique);
            return;
        }
        tableWorks.addUniqueConstraint(cols, name, assumeUnique);
        /* disable 1 line ...
        tableWorks.addUniqueConstraint(cols, name);
        ... disabled 1 line */
        // End of VoltDB extension
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
        OrderedHashSet set = getSimpleColumnNames(indexExprs);
        int[] cols = getColumnList(set, table);
        if ((indexExprs != null) && (cols == null)) {
            // Not just indexing columns.
            // The meaning of cols shifts here to be
            // the set of unique base columns for the indexed expressions.
            set = getBaseColumnNames(indexExprs);
            cols = getColumnList(set, table);
        }
        /* disable 1 line ...
        int[] cols = this.readColumnList(table, false);
        ... disabled 1 line */
        // End of VoltDB extension
        String   sql  = getLastPart();
        // A VoltDB extension to support the assume unique attribute and indexed expressions
        Object[] args = new Object[] {
            cols, name, indexExprs,
            Boolean.valueOf(assumeUnique)
        };
        /* disable 3 lines ...
        Object[] args = new Object[] {
            cols, name
        };
        .. disabled 3 lines */
        // End of VoltDB extension

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }

    void processAlterTableAddForeignKeyConstraint(Table table, HsqlName name) {

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
        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addForeignKey(c);
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

        String   sql  = getLastPart();
        Object[] args = new Object[]{ c };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   c.core.mainTableName, table.getName());
    }

    void processAlterTableAddCheckConstraint(Table table, HsqlName name) {

        Constraint check;

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        check = new Constraint(name, null, Constraint.CHECK);

        readCheckConstraintCondition(check);
        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addCheckConstraint(check);
    }

    Statement compileAlterTableAddCheckConstraint(Table table, HsqlName name) {

        Constraint check;

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        check = new Constraint(name, null, Constraint.CHECK);

        readCheckConstraintCondition(check);

        String   sql  = getLastPart();
        Object[] args = new Object[]{ check };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }

    void processAlterTableAddColumn(Table table) {

        int           colIndex   = table.getColumnCount();
        HsqlArrayList list       = new HsqlArrayList();
        Constraint    constraint = new Constraint(null, null, Constraint.TEMP);

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

        TableWorks tableWorks = new TableWorks(session, table);

        session.commit(false);
        tableWorks.addColumn(column, colIndex, list);

        return;
    }

    Statement compileAlterTableAddColumn(Table table) {

        int           colIndex   = table.getColumnCount();
        HsqlArrayList list       = new HsqlArrayList();
        Constraint    constraint = new Constraint(null, null, Constraint.TEMP);

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
            column, new Integer(colIndex), list
        };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }

    void processAlterTableAddPrimaryKey(Table table, HsqlName name) {

        if (name == null) {
            name = session.database.nameManager.newAutoName("PK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        int[] cols = readColumnList(table, false);
        Constraint constraint = new Constraint(name, null,
                                               Constraint.PRIMARY_KEY);

        constraint.core.mainCols = cols;

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addPrimaryKey(constraint, name);
    }

    Statement compileAlterTableAddPrimaryKey(Table table, HsqlName name) {

        if (name == null) {
            name = session.database.nameManager.newAutoName("PK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        int[] cols = readColumnList(table, false);
        Constraint constraint = new Constraint(name, null,
                                               Constraint.PRIMARY_KEY);

        constraint.core.mainCols = cols;

        String   sql  = getLastPart();
        Object[] args = new Object[]{ constraint };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... DROP COLUMN ...
     */
    void processAlterTableDropColumn(Table table, String colName,
                                     boolean cascade) {

        int colindex = table.getColumnIndex(colName);

        if (table.getColumnCount() == 1) {
            throw Error.error(ErrorCode.X_42591);
        }

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.dropColumn(colindex, cascade);
    }

    Statement compileAlterTableDropColumn(Table table, String colName,
                                          boolean cascade) {

        HsqlName writeName = null;
        int      colindex  = table.getColumnIndex(colName);

        if (table.getColumnCount() == 1) {
            throw Error.error(ErrorCode.X_42591);
        }

        Object[] args = new Object[] {
            table.getColumn(colindex).getName(),
            Integer.valueOf(SchemaObject.CONSTRAINT), Boolean.valueOf(cascade),
            Boolean.valueOf(false)
        };

        if (!table.isTemp()) {
            writeName = table.getName();
        }

        return new StatementSchema(null, StatementTypes.DROP_COLUMN, args,
                                   null, writeName);
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... DROP CONSTRAINT ...
     */
    void processAlterTableDropConstraint(Table table, String name,
                                         boolean cascade) {

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.dropConstraint(name, cascade);

        return;
    }

    void processAlterColumn(Table table, ColumnSchema column,
                            int columnIndex) {

        int position = getPosition();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);
                processAlterColumnRename(table, column);

                return;
            }
            case Tokens.DROP : {
                read();

                if (token.tokenType == Tokens.DEFAULT) {
                    read();

                    TableWorks tw = new TableWorks(session, table);

                    tw.setColDefaultExpression(columnIndex, null);

                    return;
                } else if (token.tokenType == Tokens.GENERATED) {
                    read();
                    column.setIdentity(null);
                    table.setColumnTypeVars(columnIndex);

                    return;
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
                        processAlterColumnDataType(table, column);

                        return;
                    }
                    case Tokens.DEFAULT : {
                        read();

                        //ALTER TABLE .. ALTER COLUMN .. SET DEFAULT
                        TableWorks tw   = new TableWorks(session, table);
                        Type       type = column.getDataType();
                        Expression expr = this.readDefaultClause(type);

                        tw.setColDefaultExpression(columnIndex, expr);

                        return;
                    }
                    case Tokens.NOT : {

                        //ALTER TABLE .. ALTER COLUMN .. SET NOT NULL
                        read();
                        readThis(Tokens.NULL);
                        session.commit(false);

                        TableWorks tw = new TableWorks(session, table);

                        tw.setColNullability(column, false);

                        return;
                    }
                    case Tokens.NULL : {
                        read();

                        //ALTER TABLE .. ALTER COLUMN .. SET NULL
                        session.commit(false);

                        TableWorks tw = new TableWorks(session, table);

                        tw.setColNullability(column, true);

                        return;
                    }
                    default :
                        rewind(position);
                        read();
                        break;
                }
            }
            default :
        }

        if (token.tokenType == Tokens.SET
                || token.tokenType == Tokens.RESTART) {
            if (!column.isIdentity()) {
                throw Error.error(ErrorCode.X_42535);
            }

            processAlterColumnSequenceOptions(column);

            return;
        } else {
            processAlterColumnType(table, column, true);

            return;
        }
    }

    Statement compileAlterColumn(Table table, ColumnSchema column,
                                 int columnIndex) {

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

                    return compileAlterColumnDropDefault(table, column,
                                                         columnIndex);
                } else if (token.tokenType == Tokens.GENERATED) {
                    read();

                    return compileAlterColumnDropGenerated(table, column,
                                                           columnIndex);
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
                        Expression expr = this.readDefaultClause(type);

                        return compileAlterColumnSetDefault(table, column,
                                                            expr);
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
                    default :
                        rewind(position);
                        read();
                        break;
                }
            }

            // $FALL-THROUGH$
            default :
        }

        if (token.tokenType == Tokens.SET
                || token.tokenType == Tokens.RESTART) {
            if (!column.isIdentity()) {
                throw Error.error(ErrorCode.X_42535);
            }

            return compileAlterColumnSequenceOptions(column);
        } else {
            return compileAlterColumnType(table, column);
        }
    }

    private Statement compileAlterColumnDataType(Table table,
            ColumnSchema column) {

        HsqlName writeName  = null;
        Type     typeObject = readTypeDefinition(false);
        String   sql        = getLastPart();
        Object[] args       = new Object[] {
            table, column, typeObject
        };

        if (!table.isTemp()) {
            writeName = table.getName();
        }

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   null, writeName);
    }

    private Statement compileAlterColumnType(Table table,
            ColumnSchema column) {

        HsqlName writeName = null;
        String sql = super.getStatement(getParsePosition(),
                                        endStatementTokens);

        if (!table.isTemp()) {
            writeName = table.getName();
        }

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   writeName);
    }

    private Statement compileAlterColumnSequenceOptions(ColumnSchema column) {

        String sql = super.getStatement(getParsePosition(),
                                        endStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, null,
                                   null);
    }

    private Statement compileAlterColumnSetNullability(Table table,
            ColumnSchema column, boolean b) {

        String sql = super.getStatement(getParsePosition(),
                                        endStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE,
                                   table.getName(), null);
    }

    private Statement compileAlterColumnSetDefault(Table table,
            ColumnSchema column, Expression expr) {

        String sql = super.getStatement(getParsePosition(),
                                        endStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE,
                                   table.getName(), null);
    }

    private Statement compileAlterColumnDropGenerated(Table table,
            ColumnSchema column, int columnIndex) {

        String sql = super.getStatement(getParsePosition(),
                                        endStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE,
                                   table.getName(), null);
    }

    private Statement compileAlterColumnDropDefault(Table table,
            ColumnSchema column, int columnIndex) {

        String sql = super.getStatement(getParsePosition(),
                                        endStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE,
                                   table.getName(), null);
    }

    Statement compileAlterSequence() {

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

        NumberSequence copy = sequence.duplicate();

        readSequenceOptions(copy, false, true);

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            sequence, copy
        };

        return new StatementSchema(sql, StatementTypes.ALTER_SEQUENCE, args,
                                   null, null);
    }

    void processAlterColumnSequenceOptions(ColumnSchema column) {

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
                    readThis(Tokens.WITH);

                    long value = readBigint();

                    sequence.setStartValue(value);

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
                            throw Error.error(ErrorCode.X_42581,
                                              token.tokenString);
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
        column.getIdentitySequence().reset(sequence);
    }

    /**
     * Should allow only limited changes to column type
     */
    private void processAlterColumnDataType(Table table, ColumnSchema oldCol) {
        processAlterColumnType(table, oldCol, false);
    }

    /**
     * Allows changes to type of column or addition of an IDENTITY generator.
     * IDENTITY is not removed if it does not appear in new column definition
     * Constraint definitions are not allowed
     */
    private void processAlterColumnType(Table table, ColumnSchema oldCol,
                                        boolean fullDefinition) {

        ColumnSchema newCol;

        if (oldCol.isGenerated()) {
            throw Error.error(ErrorCode.X_42561);
        }

        if (fullDefinition) {
            HsqlArrayList list = new HsqlArrayList();
            Constraint    c    = table.getPrimaryConstraint();

            if (c == null) {
                c = new Constraint(null, null, Constraint.TEMP);
            }

            list.add(c);

            newCol = readColumnDefinitionOrNull(table, oldCol.getName(), list);

            if (newCol == null) {
                throw Error.error(ErrorCode.X_42000);
            }

            if (oldCol.isIdentity() && newCol.isIdentity()) {
                throw Error.error(ErrorCode.X_42525);
            }

            if (list.size() > 1) {
                // A VoltDB extension to support establishing or preserving the NOT NULL
                // attribute of an altered column.
                if (voltDBacceptNotNullConstraint(list)) {
                    newCol.setNullable(false);
                } else
                // End of VoltDB extension
                throw Error.error(ErrorCode.X_42524);
            }
        } else {
            Type type = readTypeDefinition(true);

            if (oldCol.isIdentity()) {
                if (!type.isIntegralType()) {
                    throw Error.error(ErrorCode.X_42561);
                }
            }

            newCol = oldCol.duplicate();

            newCol.setType(type);
        }

        TableWorks tw = new TableWorks(session, table);

        tw.retypeColumn(oldCol, newCol);
    }

    /**
     * Responsible for handling tail of ALTER COLUMN ... RENAME ...
     */
    private void processAlterColumnRename(Table table, ColumnSchema column) {

        checkIsSimpleName();

        if (table.findColumn(token.tokenString) > -1) {
            throw Error.error(ErrorCode.X_42504, token.tokenString);
        }

        database.schemaManager.checkColumnIsReferenced(table.getName(),
                column.getName());
        session.commit(false);
        table.renameColumn(column, token.tokenString, isDelimitedIdentifier());
        read();
    }

    private Statement compileAlterColumnRename(Table table,
            ColumnSchema column) {

        checkIsSimpleName();

        HsqlName name = readNewSchemaObjectNameNoCheck(SchemaObject.COLUMN);

        if (table.findColumn(name.name) > -1) {
            throw Error.error(ErrorCode.X_42504, name.name);
        }

        database.schemaManager.checkColumnIsReferenced(table.getName(),
                column.getName());

        String   sql  = getLastPart();
        Object[] args = new Object[] {
            column.getName(), name
        };

        return new StatementSchema(sql, StatementTypes.RENAME_OBJECT, args,
                                   null, null);
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

        return new StatementSchema(sql, StatementTypes.RENAME_OBJECT, args,
                                   null, null);
    }

    Statement compileAlterUser() {

        String   password;
        User     userObject;
        HsqlName userName = readNewUserIdentifier();

        userObject = database.getUserManager().get(userName.name);

        if (userName.name.equals(Tokens.T_PUBLIC)) {
            throw Error.error(ErrorCode.X_42503);
        }

        readThis(Tokens.SET);

        if (token.tokenType == Tokens.PASSWORD) {
            read();

            password = readPassword();

            Object[] args = new Object[] {
                userObject, password
            };

            return new StatementCommand(StatementTypes.SET_USER_PASSWORD,
                                        args, null, null);
        } else if (token.tokenType == Tokens.INITIAL) {
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

            return new StatementCommand(StatementTypes.SET_USER_INITIAL_SCHEMA,
                                        args, null, null);
        } else {
            throw unexpectedToken();
        }
    }

    void processAlterDomain() {

        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);

        checkSchemaUpdateAuthorisation(schema);

        Type domain = database.schemaManager.getDomain(token.tokenString,
            schema.name, true);

        read();

        switch (token.tokenType) {

            case Tokens.RENAME : {
                read();
                readThis(Tokens.TO);

                HsqlName newName =
                    readNewSchemaObjectName(SchemaObject.DOMAIN);

                newName.setSchemaIfNull(schema);

                if (domain.getSchemaName() != newName.schema) {
                    throw Error.error(ErrorCode.X_42505, newName.schema.name);
                }

                checkSchemaUpdateAuthorisation(schema);
                database.schemaManager.renameSchemaObject(domain.getName(),
                        newName);

                return;
            }
            case Tokens.DROP : {
                read();

                if (token.tokenType == Tokens.DEFAULT) {
                    read();
                    domain.userTypeModifier.removeDefaultClause();

                    return;
                } else if (token.tokenType == Tokens.CONSTRAINT) {
                    read();
                    checkIsSchemaObjectName();

                    HsqlName name = database.schemaManager.getSchemaObjectName(
                        domain.getSchemaName(), token.tokenString,
                        SchemaObject.CONSTRAINT, true);

                    read();

//                    domain.removeConstraint(tokenString);
                    database.schemaManager.removeSchemaObject(name);

                    return;
                } else {
                    throw unexpectedToken();
                }
            }
            case Tokens.SET : {
                read();
                readThis(Tokens.DEFAULT);

                Expression e = readDefaultClause(domain);

                domain.userTypeModifier.setDefaultClause(e);

                return;
            }
            case Tokens.ADD : {
                read();

                if (token.tokenType == Tokens.CONSTRAINT
                        || token.tokenType == Tokens.CHECK) {
                    HsqlArrayList tempConstraints = new HsqlArrayList();

                    readConstraint(domain, tempConstraints);

                    Constraint c = (Constraint) tempConstraints.get(0);

                    domain.userTypeModifier.addConstraint(c);
                    database.schemaManager.addSchemaObject(c);

                    return;
                }
            }
        }

        throw unexpectedToken();
    }

    Statement compileAlterDomain() {

        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);
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

                    return compileAlterDomainDropDefault(domain);
                } else if (token.tokenType == Tokens.CONSTRAINT) {
                    read();
                    checkIsSchemaObjectName();

                    HsqlName name = database.schemaManager.getSchemaObjectName(
                        domain.getSchemaName(), token.tokenString,
                        SchemaObject.CONSTRAINT, true);

                    read();

                    return compileAlterDomainDropConstraint(domain, name);
                } else {
                    throw unexpectedToken();
                }
            }
            case Tokens.SET : {
                read();
                readThis(Tokens.DEFAULT);

                Expression e = readDefaultClause(domain);

                return compileAlterDomainSetDefault(domain, e);
            }
            case Tokens.ADD : {
                read();

                if (token.tokenType == Tokens.CONSTRAINT
                        || token.tokenType == Tokens.CHECK) {
                    HsqlArrayList tempConstraints = new HsqlArrayList();

                    readConstraint(domain, tempConstraints);

                    Constraint c = (Constraint) tempConstraints.get(0);

                    return compileAlterDomainAddConstraint(domain, c);
                }
            }
        }

        throw unexpectedToken();
    }

    private Statement compileAlterDomainAddConstraint(Type domain,
            Constraint c) {

        String sql = super.getStatement(getParsePosition(),
                                        endStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_DOMAIN, null,
                                   null);
    }

    private Statement compileAlterDomainSetDefault(Type domain, Expression e) {

        String sql = super.getStatement(getParsePosition(),
                                        endStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_DOMAIN, null,
                                   null);
    }

    private Statement compileAlterDomainDropConstraint(Type domain,
            HsqlName name) {

        String sql = super.getStatement(getParsePosition(),
                                        endStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_DOMAIN, null,
                                   null);
    }

    private Statement compileAlterDomainDropDefault(Type domain) {

        String sql = getStatement(getParsePosition(), endStatementTokens);

        return new StatementSchema(sql, StatementTypes.ALTER_DOMAIN, null,
                                   null);
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

                // throw not suppoerted
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

                    // $FALL-THROUGH$
                    case Tokens.DELETE :
                    case Tokens.TRIGGER :
                        if (right == null) {
                            right = new Right();
                        }

                        right.set(rightType, columnSet);

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

        if (token.tokenString.equals(Tokens.T_CLASS)) {
            if (!isExec && !isAll) {
                throw unexpectedToken();
            }

            read();

            if (!isSimpleName() || !isDelimitedIdentifier()) {
                throw Error.error(ErrorCode.X_42569);
            }

            objectName = readNewSchemaObjectNameNoCheck(SchemaObject.FUNCTION);
        } else if (token.tokenType == Tokens.TYPE
                   || token.tokenType == Tokens.DOMAIN
                   || token.tokenType == Tokens.SEQUENCE
                   || token.tokenType == Tokens.CHARACTER) {
            if (!isUsage && !isAll) {
                throw unexpectedToken();
            }

            int type = 0;

            switch (token.tokenType) {

                case Tokens.TYPE :
                    read();

                    type = SchemaObject.TYPE;
                    break;

                case Tokens.DOMAIN :
                    read();

                    type = SchemaObject.DOMAIN;
                    break;

                case Tokens.SEQUENCE :
                    read();

                    type = SchemaObject.SEQUENCE;
                    break;

                case Tokens.CHARACTER :
                    read();
                    readThis(Tokens.SET);

                    type = SchemaObject.CHARSET;
                    break;
            }

            objectName = readNewSchemaObjectNameNoCheck(type);
        } else {
            if (!isTable && !isAll) {
                throw unexpectedToken();
            }

            readIfThis(Tokens.TABLE);

            objectName = readNewSchemaObjectNameNoCheck(SchemaObject.TABLE);
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

        int      type = grant ? StatementTypes.GRANT
                              : StatementTypes.REVOKE;
        Object[] args = new Object[] {
            granteeList, objectName, right, grantor, Boolean.valueOf(cascade),
            Boolean.valueOf(isGrantOption)
        };
        String          sql = getLastPart();
        StatementSchema cs  = new StatementSchema(sql, type, args, null, null);

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
            } else {
                readThis(Tokens.CURRENT_ROLE);
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

        int             type = grant ? StatementTypes.GRANT_ROLE
                                     : StatementTypes.REVOKE_ROLE;
        Object[]        args = new Object[] {
            granteeList, roleList, grantor, Boolean.valueOf(cascade)
        };
        String          sql  = getLastPart();
        StatementSchema cs = new StatementSchema(sql, type, args, null, null);

        return cs;
    }

    void checkSchemaUpdateAuthorisation(HsqlName schema) {

        if (session.isProcessingLog) {
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


    /************************* Volt DB Extensions *************************/
    /**
     * Responsible for handling Volt limit constraints section of CREATE TABLE ...
     *
     * @param c check constraint
     */
    void readLimitConstraintCondition(Constraint c) {
        readThis(Tokens.PARTITION);
        readThis(Tokens.ROWS);

        int rowsLimit = readInteger();
        c.rowsLimit = rowsLimit;

        // The optional EXECUTE (DELETE ...) clause
        if (readIfThis(Tokens.EXECUTE)) {
            // Capture the statement between parentheses following the EXECUTE keyword,
            // as in
            //
            // LIMIT PARTITION ROWS 10 EXECUTE (DELETE FROM tbl WHERE b = 1)
            //
            readThis(Tokens.OPENBRACKET);
            int position = getPosition();
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

            // This captures the DELETE statement exactly, including embedded whitespace, etc.
            c.rowsLimitDeleteStmt = getLastPart(position);
            readThis(Tokens.CLOSEBRACKET);
        }
    }

    /// A VoltDB extension to the parsing behavior of the "readColumnList/readColumnNames" functions,
    /// adding support for indexed expressions.
    private java.util.List<Expression> XreadExpressions(java.util.List<Boolean> ascDesc) {
        readThis(Tokens.OPENBRACKET);

        java.util.List<Expression> indexExprs = new java.util.ArrayList<Expression>();

        while (true) {
            Expression expression = XreadValueExpression();
            indexExprs.add(expression);

            // A VoltDB extension to the "readColumnList(table, true)" support for descending-value indexes,
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
    private OrderedHashSet getSimpleColumnNames(java.util.List<Expression> indexExprs) {
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
    private OrderedHashSet getBaseColumnNames(java.util.List<Expression> indexExprs) {
        OrderedHashSet set = new OrderedHashSet();

        HsqlList col_list = new HsqlArrayList();
        for (Expression expression : indexExprs) {
            expression.collectAllColumnExpressions(col_list);
        }

        for (int i = 0; i < col_list.size(); i++) {
            String colName = ((ExpressionColumn)col_list.get(i)).columnName;
            set.add(colName);
        }

        return set;
    }

    /// Collect the column indexes of the unique columns underlying a list of indexed expressions.
    /// This adapts XreadExpressions/getSimpleColumnNames output to the format originally produced by readColumnList.
    private int[] getColumnList(OrderedHashSet set, Table table) {
        if (set == null) {
            return null;
        }
        return table.getColumnIndexes(set);
    }

    private boolean voltDBacceptNotNullConstraint(HsqlArrayList list) {
		if (list.size() != 2) {
			return false;
		}
		if (! (list.get(1) instanceof Constraint)) {
			return false;
		}
        // This replicates the logic that controls the setting of the Consraint.isNotNull member.
        // Unfortunately that member only gets set a little later.
		Constraint constraint = (Constraint)list.get(1);
		if ( constraint.getConstraintType() != Constraint.CHECK ) {
			return false;
		}
		Expression check = constraint.getCheckExpression();
		if (check.getType() != OpTypes.NOT) {
			return false;
		}
		if (check.getLeftNode().getType() != OpTypes.IS_NULL) {
			return false;
		}
		if (check.getLeftNode().getLeftNode().getType() != OpTypes.COLUMN) {
			return false;
		}
		return true;
	}

    // A VoltDB extension to support LIMIT PARTITION ROWS syntax
    private Statement compileAlterTableAddLimitConstraint(Table table, HsqlName name)
    {
        if (name == null) {
            name = database.nameManager.newAutoName("LIMIT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        Constraint c = new Constraint(name, null, Constraint.LIMIT);

        readLimitConstraintCondition(c);

        String   sql  = getLastPart();
        Object[] args = new Object[]{ c };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }
    // End of VoltDB extension

    // A VoltDB extension to support LIMIT PARTITION ROWS syntax
    private void processAlterTableAddLimitConstraint(Table table, HsqlName name) {
        if (name == null) {
            name = database.nameManager.newAutoName("LIMIT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
        }

        Constraint c = new Constraint(name, null, Constraint.LIMIT);

        readLimitConstraintCondition(c);
        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);
        tableWorks.addLimitConstraint(c);
    }
    // End of VoltDB extension

    // A VoltDB extension to support LIMIT PARTITION ROWS syntax
    private Statement compileAlterTableDropLimit(Table t) {

        HsqlName readName  = null;
        HsqlName writeName = null;
        boolean  cascade   = false;

        if (token.tokenType == Tokens.RESTRICT) {
            read();
        } else if (token.tokenType == Tokens.CASCADE) {
            read();

            cascade = true;
        }

        SchemaObject object = t.getLimitConstraint();
        if (object == null) {
            throw Error.error(ErrorCode.X_42501);
        }

        if (cascade) {
            writeName = database.getCatalogName();
        } else {
            writeName = t.getName();
        }

        Object[] args = new Object[] {
            object.getName(), Integer.valueOf(SchemaObject.CONSTRAINT),
            Boolean.valueOf(cascade), Boolean.valueOf(false)
        };
        String sql = getLastPart();

        return new StatementSchema(sql, StatementTypes.DROP_CONSTRAINT, args,
                                   readName, writeName);
    }
    // End of VoltDB extension

    /**********************************************************************/

}
