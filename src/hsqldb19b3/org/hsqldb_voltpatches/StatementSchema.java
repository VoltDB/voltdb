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
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.rights.GranteeManager;
import org.hsqldb_voltpatches.rights.Right;
import org.hsqldb_voltpatches.types.Charset;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of Statement for DDL statements.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementSchema extends Statement {

    int      order;
    Object[] arguments;
    boolean  isSchemaDefinition;
    Token[]  statementTokens;

    StatementSchema() {

        super(StatementTypes.CREATE_SCHEMA,
              StatementTypes.X_SQL_SCHEMA_DEFINITION);

        isTransactionStatement = true;
    }

    StatementSchema(String sql, int type, HsqlName readName,
                    HsqlName writeName) {

        super(type);

        isTransactionStatement = true;
        group                  = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
        this.sql               = sql;

        if (readName != null) {
            readTableNames = new HsqlName[]{ readName };
        }

        if (writeName != null) {
            writeTableNames = new HsqlName[]{ writeName };
        }
    }

    StatementSchema(String sql, int type, Object[] args, HsqlName readName,
                    HsqlName writeName) {

        super(type);

        isTransactionStatement = true;
        this.sql               = sql;
        arguments              = args;

        if (readName != null) {
            readTableNames = new HsqlName[]{ readName };
        }

        if (writeName != null) {
            writeTableNames = new HsqlName[]{ writeName };
        }

        switch (type) {

            case StatementTypes.RENAME_OBJECT :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                break;

            case StatementTypes.ALTER_DOMAIN :
            case StatementTypes.ALTER_ROUTINE :
            case StatementTypes.ALTER_SEQUENCE :
            case StatementTypes.ALTER_TYPE :
            case StatementTypes.ALTER_TABLE :
            case StatementTypes.ALTER_TRANSFORM :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                break;

            case StatementTypes.DROP_ASSERTION :
            case StatementTypes.DROP_CHARACTER_SET :
            case StatementTypes.DROP_COLLATION :
            case StatementTypes.DROP_TYPE :
            case StatementTypes.DROP_DOMAIN :
            case StatementTypes.DROP_ROLE :
            case StatementTypes.DROP_USER :
            case StatementTypes.DROP_ROUTINE :
            case StatementTypes.DROP_SCHEMA :
            case StatementTypes.DROP_SEQUENCE :
            case StatementTypes.DROP_TABLE :
            case StatementTypes.DROP_TRANSFORM :
            case StatementTypes.DROP_TRANSLATION :
            case StatementTypes.DROP_TRIGGER :
            case StatementTypes.DROP_CAST :
            case StatementTypes.DROP_ORDERING :
            case StatementTypes.DROP_VIEW :
            case StatementTypes.DROP_INDEX :
            case StatementTypes.DROP_CONSTRAINT :
            case StatementTypes.DROP_COLUMN :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                break;

            case StatementTypes.GRANT :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                order = 10;
                break;

            case StatementTypes.GRANT_ROLE :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                order = 10;
                break;

            case StatementTypes.REVOKE :
            case StatementTypes.REVOKE_ROLE :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                break;

            case StatementTypes.CREATE_SCHEMA :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                break;

            case StatementTypes.CREATE_ROLE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_ROUTINE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 7;
                break;

            case StatementTypes.CREATE_SEQUENCE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_TABLE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 2;
                break;

            case StatementTypes.CREATE_TRANSFORM :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_TRANSLATION :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_TRIGGER :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 7;
                break;

            case StatementTypes.CREATE_CAST :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 2;
                break;

            case StatementTypes.CREATE_TYPE :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_ORDERING :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_VIEW :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 5;
                break;

            case StatementTypes.CREATE_USER :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_ASSERTION :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 9;
                break;

            case StatementTypes.CREATE_CHARACTER_SET :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_COLLATION :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_DOMAIN :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 1;
                break;

            case StatementTypes.CREATE_ALIAS :
                group = StatementTypes.X_SQL_SCHEMA_DEFINITION;
                order = 8;
                break;

            case StatementTypes.CREATE_INDEX :
                group = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                order = 4;
                break;

            case StatementTypes.CHECK :
                group           = StatementTypes.X_SQL_SCHEMA_MANIPULATION;
                statementTokens = (Token[]) args[0];
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatemntSchema");
        }
    }

    public Result execute(Session session) {

        Result result = getResult(session);

        if (result.isError()) {
            result.getException().setStatementType(group, type);

            return result;
        }

        try {
            if (isLogged) {
                session.database.logger.writeToLog(session, sql);
            }
        } catch (HsqlException e) {
            return Result.newErrorResult(e, sql);
        }

        return result;
    }

    Result getResult(Session session) {

        if (this.isExplain) {
            return Result.newSingleColumnStringResult("OPERATION",
                    describe(session));
        }

        session.database.setMetaDirty(true);

        switch (type) {

            case StatementTypes.RENAME_OBJECT : {
                HsqlName     name    = (HsqlName) arguments[0];
                HsqlName     newName = (HsqlName) arguments[1];
                SchemaObject object;

                if (name.type == SchemaObject.CATALOG) {
                    try {
                        session.checkAdmin();
                        session.checkDDLWrite();
                        name.rename(newName);

                        break;
                    } catch (HsqlException e) {
                        return Result.newErrorResult(e, sql);
                    }
                }

                try {
                    name.setSchemaIfNull(session.getCurrentSchemaHsqlName());

                    object =
                        session.database.schemaManager.getSchemaObject(name);

                    if (object == null) {
                        throw Error.error(ErrorCode.X_42501, name.name);
                    }

                    name = object.getName();

                    checkSchemaUpdateAuthorisation(session, name.schema);
                    newName.setSchemaIfNull(name.schema);

                    if (name.schema != newName.schema) {
                        HsqlException e = Error.error(ErrorCode.X_42505);

                        return Result.newErrorResult(e, sql);
                    }

                    newName.parent = name.parent;

                    switch (object.getType()) {

                        case SchemaObject.COLUMN :
                            HsqlName parent = object.getName().parent;

                            session.database.schemaManager
                                .checkColumnIsReferenced(parent,
                                                         object.getName());

                            Table table =
                                session.database.schemaManager.getUserTable(
                                    session, parent);

                            table.renameColumn((ColumnSchema) object, newName);
                            break;

                        case SchemaObject.SCHEMA :

                            /**
                             * @todo 1.9.0 - review for schemas referenced in
                             *  external view or trigger definitions
                             */
                            session.database.schemaManager.renameSchema(name,
                                    newName);
                            break;

                        default :
                            session.database.schemaManager.renameSchemaObject(
                                name, newName);
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.ALTER_SEQUENCE : {
                try {
                    NumberSequence sequence = (NumberSequence) arguments[0];
                    NumberSequence settings = (NumberSequence) arguments[1];

                    checkSchemaUpdateAuthorisation(session,
                                                   sequence.getSchemaName());
                    sequence.reset(settings);

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.ALTER_DOMAIN :
            case StatementTypes.ALTER_ROUTINE :
            case StatementTypes.ALTER_TYPE :
            case StatementTypes.ALTER_TABLE :
            case StatementTypes.ALTER_TRANSFORM : {
                try {
                    session.parser.reset(sql);
                    session.parser.read();
                    session.parser.processAlter();

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.DROP_COLUMN : {
                try {
                    HsqlName name       = (HsqlName) arguments[0];
                    int      objectType = ((Integer) arguments[1]).intValue();
                    boolean  cascade = ((Boolean) arguments[2]).booleanValue();
                    boolean ifExists = ((Boolean) arguments[3]).booleanValue();
                    Table table =
                        session.database.schemaManager.getUserTable(session,
                            name.parent);
                    int colindex = table.getColumnIndex(name.name);

                    if (table.getColumnCount() == 1) {
                        throw Error.error(ErrorCode.X_42591);
                    }

                    checkSchemaUpdateAuthorisation(session,
                                                   table.getSchemaName());
                    session.commit(false);

                    TableWorks tableWorks = new TableWorks(session, table);

                    tableWorks.dropColumn(colindex, cascade);

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.DROP_ASSERTION :
            case StatementTypes.DROP_CHARACTER_SET :
            case StatementTypes.DROP_COLLATION :
            case StatementTypes.DROP_TYPE :
            case StatementTypes.DROP_DOMAIN :
            case StatementTypes.DROP_ROLE :
            case StatementTypes.DROP_USER :
            case StatementTypes.DROP_ROUTINE :
            case StatementTypes.DROP_SCHEMA :
            case StatementTypes.DROP_SEQUENCE :
            case StatementTypes.DROP_TABLE :
            case StatementTypes.DROP_TRANSFORM :
            case StatementTypes.DROP_TRANSLATION :
            case StatementTypes.DROP_TRIGGER :
            case StatementTypes.DROP_CAST :
            case StatementTypes.DROP_ORDERING :
            case StatementTypes.DROP_VIEW :
            case StatementTypes.DROP_INDEX :
            case StatementTypes.DROP_CONSTRAINT : {
                try {
                    HsqlName name       = (HsqlName) arguments[0];
                    int      objectType = ((Integer) arguments[1]).intValue();
                    boolean  cascade = ((Boolean) arguments[2]).booleanValue();
                    boolean ifExists = ((Boolean) arguments[3]).booleanValue();

                    switch (type) {

                        case StatementTypes.DROP_ROLE :
                        case StatementTypes.DROP_USER :
                            session.checkAdmin();
                            session.checkDDLWrite();
                            break;

                        case StatementTypes.DROP_SCHEMA :
                            checkSchemaUpdateAuthorisation(session, name);

                            if (!session.database.schemaManager.schemaExists(
                                    name.name)) {
                                if (ifExists) {
                                    return Result.updateZeroResult;
                                }
                            }
                            break;

                        default :
                            if (name.schema == null) {
                                name.schema =
                                    session.getCurrentSchemaHsqlName();
                            } else {
                                if (!session.database.schemaManager
                                        .schemaExists(name.schema.name)) {
                                    if (ifExists) {
                                        return Result.updateZeroResult;
                                    }
                                }
                            }

                            name.schema =
                                session.database.schemaManager
                                    .getUserSchemaHsqlName(name.schema.name);

                            checkSchemaUpdateAuthorisation(session,
                                                           name.schema);

                            name = session.database.schemaManager
                                .getSchemaObjectName(name.schema, name.name,
                                                     objectType, !ifExists);

                            if (name == null) {
                                return Result.updateZeroResult;
                            }
                    }

                    if (!cascade) {
                        session.database.schemaManager.checkObjectIsReferenced(
                            name);
                    }

                    switch (type) {

                        case StatementTypes.DROP_ROLE :
                            dropRole(session, name, cascade);
                            break;

                        case StatementTypes.DROP_USER :
                            dropUser(session, name, cascade);
                            break;

                        case StatementTypes.DROP_SCHEMA :
                            dropSchema(session, name, cascade);
                            break;

                        case StatementTypes.DROP_ASSERTION :
                            break;

                        case StatementTypes.DROP_CHARACTER_SET :
                        case StatementTypes.DROP_COLLATION :
                        case StatementTypes.DROP_SEQUENCE :
                        case StatementTypes.DROP_TRIGGER :
                            dropObject(session, name, cascade);
                            break;

                        case StatementTypes.DROP_TYPE :
                            dropType(session, name, cascade);
                            break;

                        case StatementTypes.DROP_DOMAIN :
                            dropDomain(session, name, cascade);
                            break;

                        case StatementTypes.DROP_ROUTINE :
                            dropRoutine(session, name, cascade);
                            break;

                        case StatementTypes.DROP_TABLE :
                        case StatementTypes.DROP_VIEW :
                            dropTable(session, name, cascade);
                            break;

                        case StatementTypes.DROP_TRANSFORM :
                        case StatementTypes.DROP_TRANSLATION :
                        case StatementTypes.DROP_CAST :
                        case StatementTypes.DROP_ORDERING :
                            break;

                        case StatementTypes.DROP_INDEX :
                            checkSchemaUpdateAuthorisation(session,
                                                           name.schema);
                            session.database.schemaManager.dropIndex(session,
                                    name);
                            break;

                        case StatementTypes.DROP_CONSTRAINT :
                            checkSchemaUpdateAuthorisation(session,
                                                           name.schema);
                            session.database.schemaManager.dropConstraint(
                                session, name, cascade);
                            break;
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.GRANT :
            case StatementTypes.REVOKE : {
                try {
                    boolean        grant       = type == StatementTypes.GRANT;
                    OrderedHashSet granteeList = (OrderedHashSet) arguments[0];
                    HsqlName       name        = (HsqlName) arguments[1];

                    this.setSchemaName(session, null, name);

                    name = session.database.schemaManager.getSchemaObjectName(
                        name.schema, name.name, name.type, true);

                    SchemaObject schemaObject =
                        session.database.schemaManager.getSchemaObject(name);
                    Right   right   = (Right) arguments[2];
                    Grantee grantor = (Grantee) arguments[3];
                    boolean cascade = ((Boolean) arguments[4]).booleanValue();
                    boolean isGrantOption =
                        ((Boolean) arguments[5]).booleanValue();

                    if (grantor == null) {
                        grantor = isSchemaDefinition ? schemaName.owner
                                                     : session.getGrantee();
                    }

                    GranteeManager gm = session.database.granteeManager;

                    switch (schemaObject.getType()) {

                        case SchemaObject.CHARSET :
                            System.out.println("grant charset!");
                            break;

                        case SchemaObject.VIEW :
                        case SchemaObject.TABLE : {
                            Table t = (Table) schemaObject;

                            right.setColumns(t);

                            if (t.getTableType() == TableBase.TEMP_TABLE
                                    && !right.isFull()) {
                                return Result.newErrorResult(
                                    Error.error(ErrorCode.X_42595), sql);
                            }
                        }
                    }

                    if (grant) {
                        gm.grant(granteeList, schemaObject, right, grantor,
                                 isGrantOption);
                    } else {
                        gm.revoke(granteeList, schemaObject, right, grantor,
                                  isGrantOption, cascade);
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.GRANT_ROLE :
            case StatementTypes.REVOKE_ROLE : {
                try {
                    boolean        grant = type == StatementTypes.GRANT_ROLE;
                    OrderedHashSet granteeList = (OrderedHashSet) arguments[0];
                    OrderedHashSet roleList    = (OrderedHashSet) arguments[1];
                    Grantee        grantor     = (Grantee) arguments[2];
                    boolean        cascade     = (Boolean) arguments[3];
                    GranteeManager gm = session.database.granteeManager;

                    gm.checkGranteeList(granteeList);

                    for (int i = 0; i < granteeList.size(); i++) {
                        String grantee = (String) granteeList.get(i);

                        gm.checkRoleList(grantee, roleList, grantor, grant);
                    }

                    if (grant) {
                        for (int i = 0; i < granteeList.size(); i++) {
                            String grantee = (String) granteeList.get(i);

                            for (int j = 0; j < roleList.size(); j++) {
                                String roleName = (String) roleList.get(j);

                                gm.grant(grantee, roleName, grantor);
                            }
                        }
                    } else {
                        for (int i = 0; i < granteeList.size(); i++) {
                            String grantee = (String) granteeList.get(i);

                            for (int j = 0; j < roleList.size(); j++) {
                                gm.revoke(grantee, (String) roleList.get(j),
                                          grantor);
                            }
                        }
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_ASSERTION : {
                return Result.updateZeroResult;
            }
            case StatementTypes.CREATE_CHARACTER_SET : {
                Charset charset = (Charset) arguments[0];

                try {
                    setOrCheckObjectName(session, null, charset.getName(),
                                         true);
                    session.database.schemaManager.addSchemaObject(charset);

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_COLLATION : {
                return Result.updateZeroResult;
            }
            case StatementTypes.CREATE_ROLE : {
                try {
                    session.checkAdmin();
                    session.checkDDLWrite();

                    HsqlName name = (HsqlName) arguments[0];

                    session.database.getGranteeManager().addRole(name);

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_USER : {
                HsqlName name     = (HsqlName) arguments[0];
                String   password = (String) arguments[1];
                Grantee  grantor  = (Grantee) arguments[2];
                boolean  admin    = ((Boolean) arguments[3]).booleanValue();

                try {
                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.getUserManager().createUser(name,
                            password);

                    if (admin) {
                        session.database.getGranteeManager().grant(name.name,
                                SqlInvariants.DBA_ADMIN_ROLE_NAME, grantor);
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_SCHEMA : {
                HsqlName name  = (HsqlName) arguments[0];
                Grantee  owner = (Grantee) arguments[1];

                try {
                    session.checkDDLWrite();

                    if (session.database.schemaManager.schemaExists(
                            name.name)) {
                        if (session.isProcessingScript
                                && SqlInvariants.PUBLIC_SCHEMA.equals(
                                    name.name)) {}
                        else {
                            throw Error.error(ErrorCode.X_42504, name.name);
                        }
                    } else {
                        session.database.schemaManager.createSchema(name,
                                owner);
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_ROUTINE : {
                Routine routine = (Routine) arguments[0];

                try {
                    routine.resolve();
                    setOrCheckObjectName(session, null, routine.getName(),
                                         false);
                    session.database.schemaManager.addSchemaObject(routine);

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_ALIAS : {
                HsqlName  name     = (HsqlName) arguments[0];
                Routine[] routines = (Routine[]) arguments[1];

                try {
                    session.checkAdmin();
                    session.checkDDLWrite();

                    if (name != null) {
                        for (int i = 0; i < routines.length; i++) {
                            routines[i].setName(name);
                            session.database.schemaManager.addSchemaObject(
                                routines[i]);
                        }
                    }

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_SEQUENCE : {
                NumberSequence sequence = (NumberSequence) arguments[0];

                try {
                    setOrCheckObjectName(session, null, sequence.getName(),
                                         true);
                    session.database.schemaManager.addSchemaObject(sequence);

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_DOMAIN : {
                Type type = (Type) arguments[0];
                Constraint[] constraints =
                    type.userTypeModifier.getConstraints();

                try {
                    setOrCheckObjectName(session, null, type.getName(), true);

                    for (int i = 0; i < constraints.length; i++) {
                        Constraint c = constraints[i];

                        setOrCheckObjectName(session, type.getName(),
                                             c.getName(), true);
                        session.database.schemaManager.addSchemaObject(c);
                    }

                    session.database.schemaManager.addSchemaObject(type);

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_TABLE : {
                Table         table              = (Table) arguments[0];
                HsqlArrayList tempConstraints = (HsqlArrayList) arguments[1];
                StatementDMQL statement = (StatementDMQL) arguments[2];
                HsqlArrayList foreignConstraints = null;

                try {
                    setOrCheckObjectName(session, null, table.getName(), true);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }

                try {
                    if (isSchemaDefinition) {
                        foreignConstraints = new HsqlArrayList();
                    }

                    if (tempConstraints != null) {
                        table =
                            ParserDDL.addTableConstraintDefinitions(session,
                                table, tempConstraints, foreignConstraints);
                        arguments[1] = foreignConstraints;
                    }

                    session.database.schemaManager.addSchemaObject(table);

                    if (statement != null) {
                        Result result = statement.execute(session);

                        table.insertIntoTable(session, result);
                    }

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    session.database.schemaManager.removeExportedKeys(table);
                    session.database.schemaManager.removeDependentObjects(
                        table.getName());

                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_TRANSFORM :
                return Result.updateZeroResult;

            case StatementTypes.CREATE_TRANSLATION :
                return Result.updateZeroResult;

            case StatementTypes.CREATE_TRIGGER : {
                TriggerDef trigger   = (TriggerDef) arguments[0];
                HsqlName   otherName = (HsqlName) arguments[1];

                try {
                    checkSchemaUpdateAuthorisation(session,
                                                   trigger.getSchemaName());
                    session.database.schemaManager.checkSchemaObjectNotExists(
                        trigger.getName());

                    if (otherName != null) {
                        if (session.database.schemaManager.getSchemaObject(
                                otherName) == null) {
                            throw Error.error(ErrorCode.X_42501,
                                              otherName.name);
                        }
                    }

                    trigger.table.addTrigger(trigger, otherName);
                    session.database.schemaManager.addSchemaObject(trigger);

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_CAST :
                return Result.updateZeroResult;

            case StatementTypes.CREATE_TYPE : {
                Type type = (Type) arguments[0];

                try {
                    setOrCheckObjectName(session, null, type.getName(), true);
                    session.database.schemaManager.addSchemaObject(type);

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CREATE_ORDERING :
                return Result.updateZeroResult;

            case StatementTypes.CREATE_VIEW : {
                try {
                    session.parser.reset(sql);
                    session.parser.read();
                    session.parser.read();
                    session.parser.processCreateView();

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
/*
                View view = (View) arguments[0];

                try {
                    checkSchemaUpdateAuthorisation(session,
                                                   view.getSchemaName());
                    session.database.schemaManager.checkSchemaObjectNotExists(
                        view.getName());
                    session.database.schemaManager.addSchemaObject(view);
                    session.database.logger.writeToLog(session, sql);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
*/
            }
            case StatementTypes.CREATE_INDEX : {
                Table    table;
                HsqlName name;
                int[]    indexColumns;
                boolean  unique;

                table        = (Table) arguments[0];
                indexColumns = (int[]) arguments[1];
                name         = (HsqlName) arguments[2];
                unique       = ((Boolean) arguments[3]).booleanValue();

                try {
                    /*
                            Index index        = table.getIndexForColumns(indexColumns);

                            if (index != null
                                    && ArrayUtil.areEqual(indexColumns, index.getColumns(),
                                                          indexColumns.length, unique)) {
                                if (index.isUnique() || !unique) {
                                    return;
                                }
                            }
                    */
                    setOrCheckObjectName(session, table.getName(), name, true);

                    TableWorks tableWorks = new TableWorks(session, table);

                    // A VoltDB extension to support indexed expressions
                    @SuppressWarnings("unchecked")
                    java.util.List<Expression> indexExprs = (java.util.List<Expression>)arguments[4];
                    boolean assumeUnique = ((Boolean) arguments[5]).booleanValue();
                    if (indexExprs != null) {
                        tableWorks.addExprIndex(indexColumns, indexExprs.toArray(new Expression[indexExprs.size()]), name, unique).setAssumeUnique(assumeUnique);
                        break;
                    }
                    org.hsqldb_voltpatches.index.Index addedIndex = 
                    // End of VoltDB extension
                    tableWorks.addIndex(indexColumns, name, unique);
                    // A VoltDB extension to support assume unique attribute
                    addedIndex.setAssumeUnique(assumeUnique);
                    // End of VoltDB extension

                    break;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "CompiledStateemntSchema");
        }

        return Result.updateZeroResult;
    }

    private void dropType(Session session, HsqlName name, boolean cascade) {

        checkSchemaUpdateAuthorisation(session, name.schema);

        Type distinct =
            (Type) session.database.schemaManager.getSchemaObject(name);

        session.database.schemaManager.removeSchemaObject(name, cascade);

        distinct.userTypeModifier = null;
    }

    private static void dropDomain(Session session, HsqlName name,
                                   boolean cascade) {

        Type domain =
            (Type) session.database.schemaManager.getSchemaObject(name);
        OrderedHashSet set =
            session.database.schemaManager.getReferencingObjects(
                domain.getName());

        if (!cascade && set.size() > 0) {
            HsqlName objectName = (HsqlName) set.get(0);

            throw Error.error(ErrorCode.X_42502,
                              objectName.getSchemaQualifiedStatementName());
        }

        Constraint[] constraints = domain.userTypeModifier.getConstraints();

        set.clear();

        for (int i = 0; i < constraints.length; i++) {
            set.add(constraints[i].getName());
        }

        session.database.schemaManager.removeSchemaObjects(set);
        session.database.schemaManager.removeSchemaObject(domain.getName(),
                cascade);

        domain.userTypeModifier = null;
    }

    private static void dropRole(Session session, HsqlName name,
                                 boolean cascade) {

        Grantee role = session.database.getGranteeManager().getRole(name.name);

        if (!cascade && session.database.schemaManager.hasSchemas(role)) {
            HsqlArrayList list =
                session.database.schemaManager.getSchemas(role);
            Schema schema = (Schema) list.get(0);

            throw Error.error(ErrorCode.X_42502,
                              schema.getName().statementName);
        }

        session.database.schemaManager.dropSchemas(role, cascade);
        session.database.getGranteeManager().dropRole(name.name);
    }

    private static void dropUser(Session session, HsqlName name,
                                 boolean cascade) {

        Grantee grantee = session.database.getUserManager().get(name.name);

        if (session.database.getSessionManager().isUserActive(name.name)) {
            throw Error.error(ErrorCode.X_42539);
        }

        if (!cascade && session.database.schemaManager.hasSchemas(grantee)) {
            HsqlArrayList list =
                session.database.schemaManager.getSchemas(grantee);
            Schema schema = (Schema) list.get(0);

            throw Error.error(ErrorCode.X_42502,
                              schema.getName().statementName);
        }

        session.database.schemaManager.dropSchemas(grantee, cascade);
        session.database.getUserManager().dropUser(name.name);
    }

    private void dropSchema(Session session, HsqlName name, boolean cascade) {

        HsqlName schema =
            session.database.schemaManager.getUserSchemaHsqlName(name.name);

        checkSchemaUpdateAuthorisation(session, schema);
        session.database.schemaManager.dropSchema(name.name, cascade);
    }

    private void dropRoutine(Session session, HsqlName name, boolean cascade) {

        HsqlName routineName =
            session.database.schemaManager.getSchemaObjectName(name.schema,
                name.name, name.type, true);

        checkSchemaUpdateAuthorisation(session, name.schema);
        session.database.schemaManager.removeSchemaObject(routineName,
                cascade);
    }

    private void dropObject(Session session, HsqlName name, boolean cascade) {

        name = session.database.schemaManager.getSchemaObjectName(name.schema,
                name.name, name.type, true);

        session.database.schemaManager.removeSchemaObject(name, cascade);
    }

    private void dropTable(Session session, HsqlName name, boolean cascade) {

        Table table = session.database.schemaManager.findUserTable(session,
            name.name, name.schema.name);

        session.database.schemaManager.dropTableOrView(session, table,
                cascade);
    }

    void checkSchemaUpdateAuthorisation(Session session, HsqlName schema) {

        if (session.isProcessingLog) {
            return;
        }

        if (session.database.schemaManager.isSystemSchema(schema.name)) {
            throw Error.error(ErrorCode.X_42503);
        }

        if (session.parser.isSchemaDefinition) {
            if (schema == session.currentSchema) {
                return;
            }

            Error.error(ErrorCode.X_42505, schema.name);
        }

        session.getGrantee().checkSchemaUpdateOrGrantRights(schema.name);
        session.checkDDLWrite();
    }

    void setOrCheckObjectName(Session session, HsqlName parent, HsqlName name,
                              boolean check) {

        if (name.schema == null) {
            name.schema = schemaName == null
                          ? session.getCurrentSchemaHsqlName()
                          : schemaName;
        } else {
            name.schema = session.getSchemaHsqlName(name.schema.name);

            if (name.schema == null) {
                throw Error.error(ErrorCode.X_42505);
            }

            if (isSchemaDefinition && schemaName != name.schema) {
                throw Error.error(ErrorCode.X_42505);
            }
        }

        name.parent = parent;

        if (!isSchemaDefinition) {
            checkSchemaUpdateAuthorisation(session, name.schema);
        }

        if (check) {
            session.database.schemaManager.checkSchemaObjectNotExists(name);
        }
    }

    void setSchemaName(Session session, HsqlName parent, HsqlName name) {

        if (name.schema == null) {
            name.schema = schemaName == null
                          ? session.getCurrentSchemaHsqlName()
                          : schemaName;
        } else {
            name.schema = session.getSchemaHsqlName(name.schema.name);

            if (name.schema == null) {
                throw Error.error(ErrorCode.X_42505);
            }

            if (isSchemaDefinition && schemaName != name.schema) {
                throw Error.error(ErrorCode.X_42505);
            }
        }
    }

    public boolean isAutoCommitStatement() {
        return true;
    }

    public String describe(Session session) {
        return sql;
    }
}
