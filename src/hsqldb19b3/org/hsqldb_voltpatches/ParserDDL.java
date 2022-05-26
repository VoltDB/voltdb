/* Copyright (c) 2001-2009, The HSQL Development Group
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    private static class Pair<T, U> {
        private final T m_first;
        private final U m_second;
        Pair(T first, U second) {
            m_first = first;
            m_second = second;
        }
        T getFirst() {
            return m_first;
        }
        U getSecond() {
            return m_second;
        }
    }

    @Override
    void reset(String sql) {
        super.reset(sql);
    }

    StatementSchema compileCreate() {

        int     tableType = TableBase.MEMORY_TABLE;
        boolean isTable   = false;
        boolean isStream   = false;

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

            // A VoltDB extension to support the STREAM alias
            case Tokens.STREAM :
                read();

                isStream   = true;
                tableType = database.schemaManager.getDefaultTableType();
                break;
            // End of VoltDB extension

            default :
        }

        if (isTable) {
            return compileCreateTable(tableType);
        }

        if (isStream) {
            return compileCreateStream(tableType);
        }

        // A VoltDB extension to support the assume unique attribute
        boolean unique = false;
        boolean assumeUnique = false;
        boolean migratingIndex = false;
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
            case Tokens.MIGRATING:
                migratingIndex = true;
                // $FALL-THROUGH$
            case Tokens.ASSUMEUNIQUE :
                assumeUnique = token.tokenType == Tokens.ASSUMEUNIQUE;
                // $FALL-THROUGH$
            // End of VoltDB extension
            case Tokens.UNIQUE :
                unique = token.tokenType == Tokens.UNIQUE;
                read();
                checkIsThis(Tokens.INDEX);
                assert ! migratingIndex || ! (unique || assumeUnique) :
                        "MIGRATING index cannot be UNIQUE or ASSUMEUNIQUE";
                // A VoltDB extension to support the assume unique attribute
                return compileCreateIndex(unique, assumeUnique, migratingIndex);
                /* disable 1 line ...
                return compileCreateIndex(true);
                ... disabled 1 line */
                // End of VoltDB extension

            case Tokens.INDEX :
                // A VoltDB extension to support the assume unique attribute
                return compileCreateIndex(false, false, migratingIndex);
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

    // Called from StatementSchema
    void processAlter() {

        session.setScripting(true);
        readThis(Tokens.ALTER);

        switch (token.tokenType) {

            case Tokens.STREAM : {
                read();
                processAlterStream();
                break;
            }

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

            case Tokens.VIEW : {
                read();
                return compileAlterView();
            }

            case Tokens.STREAM : {
                read();
                return compileAlterStream();
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

            case Tokens.STREAM :
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
                    // A VoltDB extension to avoid exceptions in
                    // the normal control flow.
                    // findSchemaObject returns null when
                    // getSchemaObject would needlessly
                    // throw into the catch block below.
                    /* disable 3 lines ...
                        database.schemaManager.getSchemaObject(name.name,
                            schemaName, objectType);

                    ... disabled 3 lines */
                        database.schemaManager.findSchemaObject(name.name,
                                schemaName, objectType);
                    if (object == null) {
                        writeName = null;
                    }
                    else  // AS IN else if (cascade) {
                    // End of VoltDB extension
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

    // Process ALTER TABLE or ALTER STREAM
    // (no differences between TABLE and STREAM yet)
    private void processAlterTable() {
        processAlterTableOrStream();
    }

    private void processAlterStream() {
        processAlterTableOrStream();
    }

    private void processAlterTableOrStream() {
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

    // Compiles ALTER TABLE statement
    Statement compileAlterTable() {

        String tableName = token.tokenString;
        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);
        Table t = database.schemaManager.getUserTable(session, tableName, schema.name);

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

                switch (token.tokenType) {

                    case Tokens.FOREIGN :
                        read();
                        readThis(Tokens.KEY);
                        return compileAlterTableAddForeignKeyConstraint(t, cname);

                    // A VoltDB extension to support the assume unique attribute
                    case Tokens.ASSUMEUNIQUE:
                        read();
                        return compileAlterTableAddUniqueConstraint(t, cname, true);
                    // End of VoltDB extension

                    case Tokens.UNIQUE :
                        read();
                        return compileAlterTableAddUniqueConstraint(t, cname, false);

                    case Tokens.CHECK :
                        read();
                        return compileAlterTableAddCheckConstraint(t, cname);

                    case Tokens.PRIMARY :
                        read();
                        readThis(Tokens.KEY);
                        return compileAlterTableAddPrimaryKey(t, cname);

                    case Tokens.COLUMN :
                        if (cname != null) {
                            throw unexpectedToken();
                        }
                        read();
                        checkIsSimpleName();
                        return compileAlterTableAddColumn(t);

                    case Tokens.USING :
                        if (t.getTTL() != null) {
                            throw Error.error(ErrorCode.X_42504); // object already exists
                        }
                        if (t.hasMigrationTarget()) {
                            // Warning, there's a unit test depending on this exact wording
                            throw Error.error(ErrorCode.X_42513, "May not add TTL column");
                        }
                        return readTimeToLive(t, true);

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
                        read();
                        readThis(Tokens.KEY);
                        return compileAlterTableDropPrimaryKey(t);
                    }

                    case Tokens.CONSTRAINT : {
                        read();
                        return compileAlterTableDropConstraint(t);
                    }

                    case Tokens.TTL :
                        read();
                        return compileAlterTableDropTTL(t);

                    case Tokens.COLUMN :
                        read();
                        // $FALL-THROUGH$

                    default : {
                        checkIsSimpleName();
                        String column = token.tokenString;
                        read(); // advance over identifier
                        boolean cascade = false;
                        if (token.tokenType == Tokens.RESTRICT) {
                            read();
                        }
                        else if (token.tokenType == Tokens.CASCADE) {
                            read();
                            cascade = true;
                        }
                        return compileAlterTableDropColumn(t, column, cascade);
                    }
                }
            }

            case Tokens.ALTER : {
                read();

                if (token.tokenType == Tokens.EXPORT) {
                    return readPersistentExport(t);
                }

                if (token.tokenType == Tokens.USING) {
                    return readTimeToLive(t, true);
                }

                if (token.tokenType == Tokens.COLUMN) {
                    read();
                }

                int columnIndex = t.getColumnIndex(token.tokenString);
                ColumnSchema column = t.getColumn(columnIndex);
                read(); // advance over identifier
                return compileAlterColumn(t, column, columnIndex);
            }

            default : {
                throw unexpectedToken();
            }
        }
    }

    // Compiles ALTER VIEW statement.
    // Only ALTER USING TTL... is supported, with
    // syntax the same as used on the CREATE VIEW.
    Statement compileAlterView() {

        String tableName = token.tokenString;
        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);
        Table t = database.schemaManager.getUserTable(session, tableName, schema.name);

        if (!t.isView()) {
            throw Error.error(ErrorCode.X_42501, tableName);
        }

        read();  // skip past table name
        readThis(Tokens.ALTER);
        checkIsThis(Tokens.USING);

        return readTimeToLive(t, true);
    }

    // Compiles ALTER STREAM statement
    Statement compileAlterStream() {

        String tableName = token.tokenString;
        HsqlName schema = session.getSchemaHsqlName(token.namePrefix);
        Table t = database.schemaManager.getUserTable(session, tableName, schema.name);

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
                readIfThis(Tokens.COLUMN);
                checkIsSimpleName();
                // do not advance past identifier in this case
                return compileAlterTableAddColumn(t);
            }

            case Tokens.DROP : {
                read();
                readIfThis(Tokens.COLUMN);
                checkIsSimpleName();
                String column = token.tokenString;
                read(); // advance past identifier
                return compileAlterTableDropColumn(t, column, false);
            }

            case Tokens.ALTER : {
                read();
                if (token.tokenType == Tokens.EXPORT) {
                    return readPersistentExport(t);
                }
                readIfThis(Tokens.COLUMN);
                int columnIndex = t.getColumnIndex(token.tokenString);
                ColumnSchema column = t.getColumn(columnIndex);
                read(); // advance past identifier
                return compileAlterColumn(t, column, columnIndex);
            }

            default : {
                throw unexpectedToken();
            }
        }
    }

    // VoltDB extension, DROP TTL
    private Statement compileAlterTableDropTTL(Table t) {
        if (t.hasMigrationTarget()) {
            // Warning, there's a unit test depending on this exact wording
            throw Error.error(ErrorCode.X_42513, "May not drop TTL column");
        }
        if (t.getTTL() == null) {
            throw Error.error(ErrorCode.X_42501); // object not found
        }
        Object[] args = new Object[] {
            t.getName(),
            Integer.valueOf(SchemaObject.CONSTRAINT), Boolean.valueOf(false),
            Boolean.valueOf(false)
        };
        return new StatementSchema(null, StatementTypes.DROP_TTL, args,
                                   null, t.getName());
    }

    // This is used for ALTER TABLE... ALTER
    // - parses 'EXPORT TO TARGET/TOPIC FOO ON INSERT, DELETE, UPDATE'
    // and for ALTER STREAM... ALTER
    // - parses 'EXPORT TO TARGET/TOPIC' (no ON clause allowed)
    // In both cases
    // - parse must end with semicolon, no more clauses after this.
    private Statement readPersistentExport(Table table) {
        if (token.tokenType != Tokens.EXPORT) {
            return null;
        }
        Pair<String,Integer> target = readExportTargetOrTopic();
        List<String> triggers = table.isStream() ? Collections.emptyList()
                                                 : readExportTrigger();
        if (token.tokenType != Tokens.SEMICOLON) {
            throw unexpectedToken();
        }

        Object[] args = new Object[] {
                table.getName(),
                target.getFirst().toUpperCase(),
                triggers,
                Boolean.valueOf(target.getSecond() == Tokens.TOPIC),
                // TODO: Unclear what used following args, maybe nothing?
                // Integer.valueOf(SchemaObject.CONSTRAINT), Boolean.valueOf(false),
                // Boolean.valueOf(false)
            };

        return new StatementSchema(null/*sql*/, StatementTypes.ALTER_EXPORT, args,
                                   null/*readName*/, table.getName()/*writeName*/);
    }

    // Parses 'ON trigger...' syntax.
    // - On entry, the current token is the potential ON token;
    //   if not ON, we return without reading anything further.
    // - On return, the current token will be the first token that
    //   is not part of the trigger list.
    private List<String> readExportTrigger() {
        if (token.tokenType != Tokens.ON) {
            return Arrays.asList("INSERT");
        }

        List<String> triggers = new ArrayList<>();
        boolean delimiter = true; // a trigger is next
        boolean endNow = false;

        while (!endNow) {
            read();
            switch(token.tokenType) {

            // Triggers
            case Tokens.DELETE:
            case Tokens.INSERT:
            case Tokens.UPDATE:
            case Tokens.UPDATEOLD:
            case Tokens.UPDATENEW:
                String theToken = Tokens.getKeyword(token.tokenType);
                // TODO: should we enforce use of comma? Wasn't checked before.
                if (triggers.contains(theToken)) {
                    throw unexpectedToken(theToken + " is repeated");
                }
                triggers.add(theToken);
                delimiter = false;
                break;

            // Separator between triggers
            case Tokens.COMMA:
                if (delimiter) {
                    throw unexpectedToken(); // comma should only follow a trigger
                }
                delimiter = true;
                break;

            // Any other token starts some other clause
            default:
                if (delimiter) {
                    throw unexpectedToken(); // there must be a trigger
                }
                endNow = true;
                break;
            }
        }

        boolean isUpdateOld = triggers.contains(Tokens.T_UPDATEOLD);
        boolean isUpdateNew = triggers.contains(Tokens.T_UPDATENEW);
        if (isUpdateOld | isUpdateNew) {
            // TODO: better error - this is not a syntax error much less an unexpected token
            if (triggers.contains(Tokens.T_UPDATE)) {
                throw unexpectedToken("Can't combine " + Tokens.T_UPDATE + " with " + Tokens.T_UPDATEOLD +
                                      " or " + Tokens.T_UPDATENEW);
            }
            if (isUpdateOld & isUpdateNew) {
                throw unexpectedToken("Use " + Tokens.T_UPDATE + " instead of both " + Tokens.T_UPDATEOLD +
                                      " and " + Tokens.T_UPDATENEW);
            }
        }

        return triggers;
    }

    // Parses USING TTL 10 SECONDS ON COLUMN aaa [BATCH_SIZE 1000] [MAX_FREQUENCY 1]
    // terminates on reading semicolon.
    // TTL, COLUMN required.
    // BATCH_SIZE, MAX_FREQUENCY defaulted if not specified.
    private Statement readTimeToLive(Table table, boolean alter) {
        if (!alter && token.tokenType != Tokens.USING) {
            return null;
        }

        int timeLiveValue = 0;
        String ttlUnit = "SECONDS";
        String ttlColumn = "";
        int batchSize = 1000;
        int maxFrequency = 1;

        // Time to live
        read();
        checkIsThis(Tokens.TTL);
        timeLiveValue = readInt();
        read();
        if (token.tokenType == Tokens.SECONDS || token.tokenType == Tokens.MINUTES ||
             token.tokenType == Tokens.HOURS || token.tokenType == Tokens.DAYS) {
            ttlUnit = token.tokenString;
            read();
        }

        // Column for TTL determination
        readThis(Tokens.ON);
        readThis(Tokens.COLUMN);
        checkIsValidIdentifier();
        ttlColumn = token.tokenString;

        // BATCH_SIZE, MAX_FREQUENCY, in either order
        read();
        boolean seenBatch = false, seenFreq = false;
        while (token.tokenType != Tokens.SEMICOLON && token.tokenType != Tokens.X_ENDPARSE) {
            switch (token.tokenType) {

            case Tokens.BATCH_SIZE:
                batchSize = readInt();
                if (batchSize < 1) {
                    throw unexpectedToken("BATCH_SIZE must be a positive integer");
                }
                if (seenBatch) {
                    throw unexpectedToken("BATCH_SIZE is repeated");
                }
                seenBatch = true;
                break;

            case Tokens.MAX_FREQUENCY:
                maxFrequency = readInt();
                if (maxFrequency < 1) {
                    throw unexpectedToken("MAX_FREQUENCY must be a positive integer");
                }
                if (seenFreq) {
                    throw unexpectedToken("MAX_FREQUENCY is repeated");
                }
                seenFreq = true;
                break;

            default:
                String require = String.format("%s%s%s",
                                               seenBatch ? "" : Tokens.T_BATCH_SIZE,
                                               seenBatch|seenFreq ? "" : " or ",
                                               seenFreq ? "" : Tokens.T_MAX_FREQUENCY);
                if (require.isEmpty()) {
                    throw unexpectedToken();
                }
                else {
                    throw unexpectedTokenRequire(require);
                }
            }

            read();
        }

        // Semantic checks on column
        // Only limited checks possible for views during creation
        if (table instanceof View && !alter) {
            View view = (View)table;
            if (view.findColumnName(ttlColumn) == null) {
                throw Error.error(ErrorCode.X_42501, ttlColumn); // object not found
            }
            view.addTTL(timeLiveValue, ttlUnit, ttlColumn, batchSize, maxFrequency);
            return null;
        }

        // Table or stream.
        // Check on type of TTL column is done in Volt's DDLCompiler.
        int index = table.findColumn(ttlColumn);
        if (index < 0) {
            throw Error.error(ErrorCode.X_42501, ttlColumn); // object not found
        }

        // At this moment we don't allow alter TTL column of migrate table on the fly
        if (alter && table.hasMigrationTarget()) {
            String oldColumn = table.getTTL().ttlColumnName.name;
            if (!ttlColumn.equals(oldColumn)) {
                throw Error.error(ErrorCode.X_42513, "TTL column"); // property cannot be changed
            }
        }

        return createTimeToLive(table, timeLiveValue, ttlUnit, ttlColumn, batchSize, maxFrequency);
    }

    // Read next token, return primitive int value.
    // (Differs from ParserBase.readInteger in that it first reads
    //  the next token, and does not read past the integer).
    private int readInt() {
        read();
        if (token.tokenType != Tokens.X_VALUE) {
            throw unexpectedTokenRequire("integer");
        }
        return ((Integer)(token.tokenValue)).intValue();
    }

    // Parses '[MIGRATE|EXPORT] TO [TARGET|TOPIC] name' syntax.
    // - On entry, the current token is the potential MIGRATE or EXPORT token.
    //   If neither of those, we return without reading further.
    // - On return, the current token is the first token after the
    //   parsed clause (i.e., after the target/topic name)
    private Pair<String,Integer> readMigrateTargetOrTopic() {
        return token.tokenType == Tokens.MIGRATE
            ? readToTargetOrTopic()
            : null;
    }

    private Pair<String,Integer> readExportTargetOrTopic() {
        return token.tokenType == Tokens.EXPORT
            ? readToTargetOrTopic()
            : null;
    }

    // Common code for readMigrateTargetOrTopic, readExportTargetOrTopic
    private Pair<String,Integer> readToTargetOrTopic() {
        read();
        readThis(Tokens.TO);
        if (token.tokenType != Tokens.TARGET && token.tokenType != Tokens.TOPIC) {
            throw unexpectedTokenRequire(Tokens.T_TARGET + " or " + Tokens.T_TOPIC);
        }
        int targetType = token.tokenType;
        read();
        checkIsValidIdentifier();
        String ident = token.tokenString;
        read();
        return new Pair<>(ident, targetType);
    }

    // Acceptable identifier for MIGRATE TO, EXPORT TO, or
    // TTL .. ON COLUMN clauses.
    // - any unquoted ident, including SQL reserved keywords
    //   such as 'default'.
    // - quoted idents are not allowed, since SQLParser does
    //   not support them.
    private boolean isValidIdentifier() {
        return token.isUndelimitedIdentifier;
    }

    private void checkIsValidIdentifier() {
        if (!token.isUndelimitedIdentifier) {
            throw unexpectedTokenRequire("identifier");
        }
    }

    private Statement createTimeToLive(Table table, int value, String unit, String column,
            int batchSize, int maxFrequency) {
        table.addTTL(value, unit, column, batchSize, maxFrequency);

        Object[] args = new Object[] {
                table.getName(),
                value,
                unit,
                column,
                batchSize,
                maxFrequency,
                Integer.valueOf(SchemaObject.CONSTRAINT), Boolean.valueOf(false),
                Boolean.valueOf(false)
            };
        return new StatementSchema(null, StatementTypes.ALTER_TTL, args,
                                       null, table.getName());
    }
    //End of VoltDB extension

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

        // Here we might have EXPORT TO or MIGRATE TO, and the
        // object of that can be a TARGET or a TOPIC. If EXPORT,
        // there may be 'ON triggers'. If TOPIC, there may be
        // 'WITH KEY ... VALUE ...'.
        Pair<String,Integer> target = null;
        if (token.tokenType == Tokens.MIGRATE) {
            table.setHasMigrationTarget(true);
            target = readMigrateTargetOrTopic();
        }
        else if (token.tokenType == Tokens.EXPORT) {
            target = readExportTargetOrTopic();
            List<String> triggers = readExportTrigger();
            table.addPersistentExport(target.getFirst(), triggers,
                                      target.getSecond() == Tokens.TOPIC);
        }
        if (token.tokenType == Tokens.WITH) {
            if (target == null || target.getSecond() != Tokens.TOPIC) {
                throw unexpectedToken(); // stopping here gives a better diagnostic
            }
            skipTopicKeysAndValues();
        }

        // Migration/export will be handled later in DDLCompiler.processCreateTableStatement.
        // But we want to collect any TTL information here, in this damnable syntax,
        // First skip to column definitions, assumed to be next '(...)' part.
        // And then read past the column definitions to locate any TTL definition.
        // TODO: this results in a lousy diagnostic in many cases. Is it true that
        //       we should already be positioned at the column definitions? If so,
        //       then we can do a better job.
        readUntilThis(Tokens.OPENBRACKET);
        readThis(Tokens.OPENBRACKET);

        tempConstraints.add(new Constraint(null, true, null, Constraint.TEMP));

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

                    // A VoltDB extension to support TTL
                    readTimeToLive(table, false);
                    // End of VoltDB extension
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

        // zero-column table is not allowed
        if (table.getColumnCount() == 0) {
            throw Error.error(ErrorCode.X_47002);
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

    StatementSchema compileCreateStream(int type) {

        // Read name and create stream object
        HsqlName name = readNewSchemaObjectNameNoCheck(SchemaObject.TABLE);
        HsqlArrayList tempConstraints = new HsqlArrayList();

        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

        Table table = TableUtil.newTable(database, type, name);
        table.setStream(true);
        if (token.tokenType == Tokens.AS) {
            return readTableAsSubqueryDefinition(table);
        }

        // Skip some stream-specific tokens that will be analyzed in DDLCompiler.processCreateStreamStatement
        skipStreamSpecificTokens();

        // Parse the stream columns
        int position = getPosition();

        readUntilThis(Tokens.OPENBRACKET);

        readThis(Tokens.OPENBRACKET);

        {
            Constraint c = new Constraint(null, true, null, Constraint.TEMP);

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

    /**
     * Skip stream-specific tokens preceding the column definitions
     * <p>
     * CREATE STREAM foo [ PARTITION ON COLUMN ... ] [ EXPORT TO ... ] (column-definitions)
     * <p>
     * PARTITION and EXPORT can be in any order, EXPORT TO TOPIC ... WITH may contains clauses
     * with brackets ()  that must be ignored at this level.
     */
    private void skipStreamSpecificTokens() {
        boolean parsedPartition = false;
        boolean parsedExport = false;

        do {
            if (!parsedPartition && token.tokenType == Tokens.PARTITION) {
                skipStreamPartition();
                parsedPartition = true;
            }
            else if (!parsedExport && token.tokenType == Tokens.EXPORT) {
                skipStreamExport();
                parsedExport = true;
            }
            else if (token.tokenType != Tokens.OPENBRACKET) {
                throw unexpectedToken();
            }
        } while (token.tokenType != Tokens.OPENBRACKET);
    }

    private void skipStreamPartition() {
        assert token.tokenType == Tokens.PARTITION;

        read();
        readThis(Tokens.ON);
        readThis(Tokens.COLUMN);
        checkIsValidIdentifier();
        read();
    }

    private void skipStreamExport() {
        assert token.tokenType == Tokens.EXPORT;
        boolean toTopic = false;

        // Common skipping EXPORT TO [ TARGET | TOPIC ] <identifier>
        read();
        readThis(Tokens.TO);
        if (token.tokenType == Tokens.TOPIC) {
            toTopic = true;
        }
        else if (token.tokenType != Tokens.TARGET) {
            throw unexpectedToken();
        }
        read();
        checkIsValidIdentifier();
        read();

        // TO TOPIC might have WITH keys and values, but for
        // TARGET it's not allowed. Better to reject it now.
        if (token.tokenType == Tokens.WITH) {
            if (!toTopic) {
                throw unexpectedToken();
            }
            skipTopicKeysAndValues();
        }
    }

    // Skip over 'WITH [ KEY (...) ] [ VALUE (....) ]'
    // KEY and VALUE tokens are ordered
    // At least one of the two is required after WITH
    // - On entry the current token is the potential 'WITH'
    // - Returns with current token set to the token after
    //   the parsed clause
    private void skipTopicKeysAndValues() {
        if (token.tokenType != Tokens.WITH) {
            return;
        }

        boolean hasKey = false;
        boolean hasValue = false;
        read();
        if (token.tokenType == Tokens.KEY) {
            hasKey = true;
            read();
            if (token.tokenType != Tokens.OPENBRACKET) {
                throw unexpectedTokenRequire(Tokens.getKeyword(Tokens.OPENBRACKET));
            }
            readUntilThis(Tokens.CLOSEBRACKET);
            readThis(Tokens.CLOSEBRACKET);
        }
        if (token.tokenType == Tokens.VALUE) {
            hasValue = true;
            read();
            if (token.tokenType != Tokens.OPENBRACKET) {
                throw unexpectedTokenRequire(Tokens.getKeyword(Tokens.OPENBRACKET));
            }
            readUntilThis(Tokens.CLOSEBRACKET);
            readThis(Tokens.CLOSEBRACKET);
        }
        if (!hasKey && !hasValue) {
            // Must have one of them with WITH
            throw unexpectedToken();
        }

        // Return with current token just after any KEY (...) VALUE (...)
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
            Constraint newconstraint = new Constraint(c.getName(), c.getIsAutogeneratedName(), table,
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
                        index = table.createAndAddExprIndexStructure(
                                indexName, c.core.mainCols, c.indexExprs, true, false, true)
                                .setAssumeUnique(c.assumeUnique);
                    } else {
                        index = table.createAndAddIndexStructure(
                                indexName, c.core.mainCols, null, null, true, false, true, false)
                                .setAssumeUnique(c.assumeUnique);
                    }

                    Constraint newconstraint = new Constraint(c.getName(), c.getIsAutogeneratedName(),
                        table, index, Constraint.UNIQUE);
                    // A VoltDB extension to support the assume unique attribute
                    newconstraint = newconstraint.setAssumeUnique(c.assumeUnique);
                    // End of VoltDB extension

                    table.addConstraint(newconstraint);
                    session.database.schemaManager.addSchemaObject(
                        newconstraint);

                    break;
                }
                case Constraint.MIGRATING : {
                    // TODO
                    Index index = table.createAndAddIndexStructure(indexName,
                            c.core.mainCols, null, null, true, true, false, false);
                    Constraint newconstraint = new Constraint(c.getName(), c.getIsAutogeneratedName(),
                            table, index, Constraint.MIGRATING)
                            .setMigrating(c.migrating);

                    table.addConstraint(newconstraint);
                    session.database.schemaManager.addSchemaObject(
                            newconstraint);
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
            c.core.refCols, null, null, false, false, true, isForward);
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

    // Called from StatementSchema
    void processCreateView() {

        StatementSchema cs = compileCreateView();
        View view = (View) cs.arguments[0];

        checkSchemaUpdateAuthorisation(view.getSchemaName());
        database.schemaManager.checkSchemaObjectNotExists(view.getName());
        database.schemaManager.addSchemaObject(view);
    }

    StatementSchema compileCreateView() {

        read(); // skip over VIEW

        HsqlName name = readNewSchemaObjectName(SchemaObject.VIEW);
        name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

        // Here we might have MIGRATE TO, and the object of that can be a
        // TARGET or a TOPIC. If TOPIC, there may be 'WITH KEY ... VALUE ...'.
        Pair<String,Integer> target = null;
        if (token.tokenType == Tokens.MIGRATE) {
            target = readMigrateTargetOrTopic();
        }
        if (token.tokenType == Tokens.WITH) {
            if (target == null || target.getSecond() != Tokens.TOPIC) {
                throw unexpectedToken();
            }
            skipTopicKeysAndValues();
        }

        HsqlName[] colList = null;

        // Optional list of columns in view
        if (token.tokenType == Tokens.OPENBRACKET) {
            colList = readColumnNames(name);
        }

        // AS selection
        readThis(Tokens.AS);
        startRecording();

        int position = getPosition();
        QueryExpression queryExpression;

        try {
            queryExpression = XreadQueryExpression();
        } catch (HsqlException e) {
            queryExpression = XreadJoinedTable();
        }

        Token[] statement = getRecordedStatement();
        String sql = getLastPart(position);

        // Undocumented WITH ... CHECK OPTION
        int check = SchemaObject.ViewCheckModes.CHECK_NONE;
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

        // Create basic view
        View view = new View(session, database, name, colList, sql, check);
        if (target != null) {
            view.setHasMigrationTarget(true);
        }

        // Now we can parse any TTL
        if (token.tokenType == Tokens.USING) {
            readTimeToLive(view, false);
        }

        queryExpression.setAsTopLevel();
        queryExpression.setView(view);
        queryExpression.resolve(session);
        view.compile(session); // column schema gets filled in here
        checkSchemaUpdateAuthorisation(name.schema);
        database.schemaManager.checkSchemaObjectNotExists(name);

        String statementSQL = Token.getSQL(statement);
        view.statement = statementSQL;

        String fullSQL = getLastPart();
        Object[] args = new Object[] { view };

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
            Constraint c = new Constraint(constName, true, set,
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
        boolean isAutogeneratedName = true;

        if (token.tokenType == Tokens.CONSTRAINT) {
            read();

            constName =
                readNewDependentSchemaObjectName(schemaObject.getName(),
                                                 SchemaObject.CONSTRAINT);
            isAutogeneratedName = false;
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
                Constraint c = new Constraint(constName, isAutogeneratedName, set,
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
                Constraint c = new Constraint(constName, isAutogeneratedName, set,
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

                Constraint c = new Constraint(constName, isAutogeneratedName, null,
                                              Constraint.CHECK);

                readCheckConstraintCondition(c);
                constraintList.add(c);

                break;
            }
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
        boolean isAutogeneratedName = true;

        while (true) {
            HsqlName constName = null;

            if (token.tokenType == Tokens.CONSTRAINT) {
                read();

                constName = readNewDependentSchemaObjectName(table.getName(),
                        SchemaObject.CONSTRAINT);
                isAutogeneratedName = false;
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

                    Constraint c = new Constraint(constName, isAutogeneratedName, set,
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

                    Constraint c = new Constraint(constName, isAutogeneratedName, set,
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

                    Constraint c = new Constraint(constName, isAutogeneratedName, null,
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

                    Constraint c = new Constraint(constName, isAutogeneratedName, null,
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
    StatementSchema compileCreateIndex(boolean unique, boolean assumeUnique, boolean migrating) {
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
        java.util.List<Expression> indexExprs = XreadExpressions(ascDesc, migrating);
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

        // A VoltDB extension to support partial index
        Expression predicate = null;
        if (readIfThis(Tokens.WHERE)) {
            predicate = XreadBooleanValueExpression();
        }

        indexColumns = getColumnList(set, table);
        String   sql          = getLastPart();
        Object[] args         = new Object[] {
            table, indexColumns, indexHsqlName, unique, migrating, indexExprs, assumeUnique, predicate
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
    private void processAlterTableRename(Table table) {

        HsqlName name = readNewSchemaObjectName(SchemaObject.TABLE);

        name.setSchemaIfNull(table.getSchemaName());

        if (table.getSchemaName() != name.schema) {
            throw Error.error(ErrorCode.X_42505);
        }

        database.schemaManager.renameSchemaObject(table.getName(), name);
    }

    // A VoltDB extension to support indexed expressions and the assume unique attribute
    private void processAlterTableAddUniqueConstraint(Table table, HsqlName name, boolean assumeUnique) {
        boolean isAutogeneratedName = false;
        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
            isAutogeneratedName = true;

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
            tableWorks.addUniqueExprConstraint(cols, indexExprs.toArray(new Expression[indexExprs.size()]), name, isAutogeneratedName, assumeUnique);
            return;
        }
        tableWorks.addUniqueConstraint(cols, name, isAutogeneratedName, assumeUnique);
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

    private void processAlterTableAddForeignKeyConstraint(Table table, HsqlName name) {

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

    private void processAlterTableAddCheckConstraint(Table table, HsqlName name) {

        Constraint check;
        boolean isAutogeneratedName = false;

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
            isAutogeneratedName = true;
        }

        check = new Constraint(name, isAutogeneratedName, null, Constraint.CHECK);

        readCheckConstraintCondition(check);
        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addCheckConstraint(check);
    }

    Statement compileAlterTableAddCheckConstraint(Table table, HsqlName name) {

        Constraint check;
        boolean isAutogeneratedName = false;

        if (name == null) {
            name = database.nameManager.newAutoName("CT",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
            isAutogeneratedName = true;
        }

        check = new Constraint(name, isAutogeneratedName, null, Constraint.CHECK);

        readCheckConstraintCondition(check);

        String   sql  = getLastPart();
        Object[] args = new Object[]{ check };

        return new StatementSchema(sql, StatementTypes.ALTER_TABLE, args,
                                   null, table.getName());
    }

    private void processAlterTableAddColumn(Table table) {

        int           colIndex   = table.getColumnCount();
        HsqlArrayList list       = new HsqlArrayList();
        Constraint    constraint = new Constraint(null, true, null, Constraint.TEMP);

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
        Constraint    constraint = new Constraint(null, true, null, Constraint.TEMP);

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

    private void processAlterTableAddPrimaryKey(Table table, HsqlName name) {
        boolean isAutogeneratedName = false;

        if (name == null) {
            name = session.database.nameManager.newAutoName("PK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
            isAutogeneratedName = true;
        }

        int[] cols = readColumnList(table, false);
        Constraint constraint = new Constraint(name, isAutogeneratedName, null,
                                               Constraint.PRIMARY_KEY);

        constraint.core.mainCols = cols;

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.addPrimaryKey(constraint, name);
    }

    Statement compileAlterTableAddPrimaryKey(Table table, HsqlName name) {
        boolean isAutogeneratedName = false;
        if (name == null) {
            name = session.database.nameManager.newAutoName("PK",
                    table.getSchemaName(), table.getName(),
                    SchemaObject.CONSTRAINT);
            isAutogeneratedName = true;
        }

        int[] cols = readColumnList(table, false);
        Constraint constraint = new Constraint(name, isAutogeneratedName, null,
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
    private void processAlterTableDropColumn(Table table, String colName,
                                     boolean cascade) {

        int colindex = table.getColumnIndex(colName);

        if (table.getColumnCount() == 1) {
            throw Error.error(ErrorCode.X_42591);
        }

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.dropColumn(colindex, cascade);

        //VoltDB extension to support Time to live
        if (table.getTTL() != null && colName.equalsIgnoreCase(table.getTTL().ttlColumn.getName().name)) {
            table.dropTTL();
        }
    }

    Statement compileAlterTableDropColumn(Table table, String colName,
                                          boolean cascade) {

        HsqlName writeName = null;
        int      colindex  = table.getColumnIndex(colName);

        if (table.getColumnCount() == 1) {
            throw Error.error(ErrorCode.X_42591);
        }

        if (table.timeToLive != null) {
            final String ttlColumn = table.timeToLive.ttlColumn.getNameString();
            if (colName.equalsIgnoreCase(ttlColumn)) {
                throw Error.error("Columns used by TTL cannot be dropped.");
            }
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
    private void processAlterTableDropConstraint(Table table, String name,
                                         boolean cascade) {

        session.commit(false);

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.dropConstraint(name, cascade);

        return;
    }

    private void processAlterColumn(Table table, ColumnSchema column,
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

    private void processAlterColumnSequenceOptions(ColumnSchema column) {

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
                c = new Constraint(null, true, null, Constraint.TEMP);
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

    private void processAlterDomain() {

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

    // Default disallow empty parenthesis
    private java.util.List<Expression> XreadExpressions(java.util.List<Boolean> ascDesc) {
        return XreadExpressions(ascDesc, false);
    }

    /// A VoltDB extension to the parsing behavior of the "readColumnList/readColumnNames" functions,
    /// adding support for indexed expressions.
    private java.util.List<Expression> XreadExpressions(java.util.List<Boolean> ascDesc, boolean allowEmpty) {
        readThis(Tokens.OPENBRACKET);

        java.util.List<Expression> indexExprs = new java.util.ArrayList<>();

        while (true) {
            if (allowEmpty && readIfThis(Tokens.CLOSEBRACKET)) {    // empty bracket
                return indexExprs;
            }
            Expression expression = XreadValueExpression();
            indexExprs.add(expression);

            if (token.tokenType == Tokens.DESC) {
                throw unexpectedToken();
            }
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

    // End of VoltDB extension

    /**********************************************************************/

}
