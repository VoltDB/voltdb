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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.MultiValueHashMap;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.lib.WrapperIterator;
import org.hsqldb_voltpatches.navigator.RowIterator;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.Charset;
import org.hsqldb_voltpatches.types.Collation;
import org.hsqldb_voltpatches.types.Type;

/**
 * Manages all SCHEMA related database objects
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 1.8.0
 */
public class SchemaManager {

    Database          database;
    HsqlName          defaultSchemaHsqlName;
    HashMappedList    schemaMap        = new HashMappedList();
    MultiValueHashMap referenceMap     = new MultiValueHashMap();
    int               defaultTableType = TableBase.MEMORY_TABLE;
    long              schemaChangeTimestamp;
    HsqlName[]        catalogNameArray;

    //
    ReadWriteLock lock      = new ReentrantReadWriteLock();
    Lock          readLock  = lock.readLock();
    Lock          writeLock = lock.writeLock();

    //
    Table        dualTable;
    public Table dataChangeTable;

    public SchemaManager(Database database) {

        this.database         = database;
        defaultSchemaHsqlName = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
        catalogNameArray      = new HsqlName[]{ database.getCatalogName() };

        Schema schema =
            new Schema(SqlInvariants.INFORMATION_SCHEMA_HSQLNAME,
                       SqlInvariants.INFORMATION_SCHEMA_HSQLNAME.owner);

        schemaMap.put(schema.getName().name, schema);

        try {
            schema.charsetLookup.add(Charset.SQL_TEXT);
            schema.charsetLookup.add(Charset.SQL_IDENTIFIER_CHARSET);
            schema.charsetLookup.add(Charset.SQL_CHARACTER);
            schema.collationLookup.add(Collation.getDefaultInstance());
            schema.collationLookup.add(
                Collation.getDefaultIgnoreCaseInstance());
            schema.typeLookup.add(TypeInvariants.CARDINAL_NUMBER);
            schema.typeLookup.add(TypeInvariants.YES_OR_NO);
            schema.typeLookup.add(TypeInvariants.CHARACTER_DATA);
            schema.typeLookup.add(TypeInvariants.SQL_IDENTIFIER);
            schema.typeLookup.add(TypeInvariants.TIME_STAMP);
        } catch (HsqlException e) {}
    }

    public void setSchemaChangeTimestamp() {
        schemaChangeTimestamp = database.txManager.getGlobalChangeTimestamp();
    }

    public long getSchemaChangeTimestamp() {
        return schemaChangeTimestamp;
    }

    // pre-defined
    public HsqlName getSQLJSchemaHsqlName() {
        return SqlInvariants.SQLJ_SCHEMA_HSQLNAME;
    }

    // SCHEMA management
    public void createPublicSchema() {

        writeLock.lock();

        try {
            HsqlName name = database.nameManager.newHsqlName(null,
                SqlInvariants.PUBLIC_SCHEMA, SchemaObject.SCHEMA);
            Schema schema =
                new Schema(name, database.getGranteeManager().getDBARole());

            defaultSchemaHsqlName = schema.getName();

            schemaMap.put(schema.getName().name, schema);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Creates a schema belonging to the given grantee.
     */
    public void createSchema(HsqlName name, Grantee owner) {

        writeLock.lock();

        try {
            SqlInvariants.checkSchemaNameNotSystem(name.name);

            Schema schema = new Schema(name, owner);

            schemaMap.add(name.name, schema);
        } finally {
            writeLock.unlock();
        }
    }

    public void dropSchema(Session session, String name, boolean cascade) {

        writeLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(name);

            if (schema == null) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            if (SqlInvariants.isLobsSchemaName(name)) {
                throw Error.error(ErrorCode.X_42503, name);
            }

            if (!cascade && !schema.isEmpty()) {
                throw Error.error(ErrorCode.X_2B000);
            }

            OrderedHashSet externalReferences = new OrderedHashSet();

            getCascadingReferencesToSchema(schema.getName(),
                                           externalReferences);
            removeSchemaObjects(externalReferences);

            Iterator tableIterator =
                schema.schemaObjectIterator(SchemaObject.TABLE);

            while (tableIterator.hasNext()) {
                Table        table = ((Table) tableIterator.next());
                Constraint[] list  = table.getFKConstraints();

                for (int i = 0; i < list.length; i++) {
                    Constraint constraint = list[i];

                    if (constraint.getMain().getSchemaName()
                            != schema.getName()) {
                        constraint.getMain().removeConstraint(
                            constraint.getMainName().name);
                        removeReferencesFrom(constraint);
                    }
                }

                removeTable(session, table);
            }

            Iterator sequenceIterator =
                schema.schemaObjectIterator(SchemaObject.SEQUENCE);

            while (sequenceIterator.hasNext()) {
                NumberSequence sequence =
                    ((NumberSequence) sequenceIterator.next());

                database.getGranteeManager().removeDbObject(
                    sequence.getName());
            }

            schema.release();
            schemaMap.remove(name);

            if (defaultSchemaHsqlName.name.equals(name)) {
                HsqlName hsqlName = database.nameManager.newHsqlName(name,
                    false, SchemaObject.SCHEMA);

                schema = new Schema(hsqlName,
                                    database.getGranteeManager().getDBARole());
                defaultSchemaHsqlName = schema.getName();

                schemaMap.put(schema.getName().name, schema);
            }

            // these are called last and in this particular order
            database.getUserManager().removeSchemaReference(name);
            database.getSessionManager().removeSchemaReference(schema);
        } finally {
            writeLock.unlock();
        }
    }

    public void renameSchema(HsqlName name, HsqlName newName) {

        writeLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(name.name);
            Schema exists = (Schema) schemaMap.get(newName.name);

            if (schema == null) {
                throw Error.error(ErrorCode.X_42501, name.name);
            }

            if (exists != null) {
                throw Error.error(ErrorCode.X_42504, newName.name);
            }

            SqlInvariants.checkSchemaNameNotSystem(name.name);
            SqlInvariants.checkSchemaNameNotSystem(newName.name);

            int index = schemaMap.getIndex(name.name);

            schema.getName().rename(newName);
            schemaMap.set(index, newName.name, schema);
        } finally {
            writeLock.unlock();
        }
    }

    public void release() {

        writeLock.lock();

        try {
            Iterator it = schemaMap.values().iterator();

            while (it.hasNext()) {
                Schema schema = (Schema) it.next();

                schema.release();
            }
        } finally {
            writeLock.unlock();
        }
    }

    public String[] getSchemaNamesArray() {

        readLock.lock();

        try {
            String[] array = new String[schemaMap.size()];

            schemaMap.toKeysArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public Schema[] getAllSchemas() {

        readLock.lock();

        try {
            Schema[] objects = new Schema[schemaMap.size()];

            schemaMap.toValuesArray(objects);

            return objects;
        } finally {
            readLock.unlock();
        }
    }

    public HsqlName getUserSchemaHsqlName(String name) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(name);

            if (schema == null) {
                throw Error.error(ErrorCode.X_3F000, name);
            }

            if (schema.getName()
                    == SqlInvariants.INFORMATION_SCHEMA_HSQLNAME) {
                throw Error.error(ErrorCode.X_3F000, name);
            }

            return schema.getName();
        } finally {
            readLock.unlock();
        }
    }

    public Grantee toSchemaOwner(String name) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(name);

            return schema == null ? null
                                  : schema.getOwner();
        } finally {
            readLock.unlock();
        }
    }

    public HsqlName getDefaultSchemaHsqlName() {
        return defaultSchemaHsqlName;
    }

    public void setDefaultSchemaHsqlName(HsqlName name) {
        defaultSchemaHsqlName = name;
    }

    public boolean schemaExists(String name) {

        readLock.lock();

        try {
            return schemaMap.containsKey(name);
        } finally {
            readLock.unlock();
        }
    }

    public HsqlName findSchemaHsqlName(String name) {

        readLock.lock();

        try {
            Schema schema = ((Schema) schemaMap.get(name));

            if (schema == null) {
                return null;
            }

            return schema.getName();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * If schemaName is null, return the default schema name, else return
     * the HsqlName object for the schema. If schemaName does not exist,
     * throw.
     */
    public HsqlName getSchemaHsqlName(String name) {

        if (name == null) {
            return defaultSchemaHsqlName;
        }

        readLock.lock();

        try {
            Schema schema = ((Schema) schemaMap.get(name));

            if (schema == null) {
                throw Error.error(ErrorCode.X_3F000, name);
            }

            return schema.getName();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Same as above, but return string
     */
    public String getSchemaName(String name) {
        return getSchemaHsqlName(name).name;
    }

    public Schema findSchema(String name) {

        readLock.lock();

        try {
            return ((Schema) schemaMap.get(name));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * drop all schemas with the given authorisation
     */
    public void dropSchemas(Session session, Grantee grantee,
                            boolean cascade) {

        writeLock.lock();

        try {
            HsqlArrayList list = getSchemas(grantee);
            Iterator      it   = list.iterator();

            while (it.hasNext()) {
                Schema schema = (Schema) it.next();

                dropSchema(session, schema.getName().name, cascade);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public HsqlArrayList getSchemas(Grantee grantee) {

        readLock.lock();

        try {
            HsqlArrayList list = new HsqlArrayList();
            Iterator      it   = schemaMap.values().iterator();

            while (it.hasNext()) {
                Schema schema = (Schema) it.next();

                if (grantee.equals(schema.getOwner())) {
                    list.add(schema);
                }
            }

            return list;
        } finally {
            readLock.unlock();
        }
    }

    public boolean hasSchemas(Grantee grantee) {

        readLock.lock();

        try {
            Iterator it = schemaMap.values().iterator();

            while (it.hasNext()) {
                Schema schema = (Schema) it.next();

                if (grantee.equals(schema.getOwner())) {
                    return true;
                }
            }

            return false;
        } finally {
            readLock.unlock();
        }
    }

    /**
     *  Returns an HsqlArrayList containing references to all non-system
     *  tables and views. This includes all tables and views registered with
     *  this Database.
     */
    public HsqlArrayList getAllTables(boolean withLobTables) {

        readLock.lock();

        try {
            HsqlArrayList alltables = new HsqlArrayList();
            String[]      schemas   = getSchemaNamesArray();

            for (int i = 0; i < schemas.length; i++) {
                String name = schemas[i];

                if (!withLobTables && SqlInvariants.isLobsSchemaName(name)) {
                    continue;
                }

                if (SqlInvariants.isSystemSchemaName(name)) {
                    continue;
                }

                HashMappedList current = getTables(name);

                alltables.addAll(current.values());
            }

            return alltables;
        } finally {
            readLock.unlock();
        }
    }

    public HashMappedList getTables(String schema) {

        readLock.lock();

        try {
            Schema temp = (Schema) schemaMap.get(schema);

            return temp.tableList;
        } finally {
            readLock.unlock();
        }
    }

    public HsqlName[] getCatalogNameArray() {
        return catalogNameArray;
    }

    public HsqlName[] getCatalogAndBaseTableNames() {

        readLock.lock();

        try {
            OrderedHashSet names  = new OrderedHashSet();
            HsqlArrayList  tables = getAllTables(false);

            for (int i = 0; i < tables.size(); i++) {
                Table table = (Table) tables.get(i);

                if (!table.isTemp()) {
                    names.add(table.getName());
                }
            }

            names.add(database.getCatalogName());

            HsqlName[] array = new HsqlName[names.size()];

            names.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public HsqlName[] getCatalogAndBaseTableNames(HsqlName name) {

        if (name == null) {
            return catalogNameArray;
        }

        readLock.lock();

        try {
            switch (name.type) {

                case SchemaObject.SCHEMA : {
                    if (findSchemaHsqlName(name.name) == null) {
                        return catalogNameArray;
                    }

                    OrderedHashSet names = new OrderedHashSet();

                    names.add(database.getCatalogName());

                    HashMappedList list = getTables(name.name);

                    for (int i = 0; i < list.size(); i++) {
                        names.add(((SchemaObject) list.get(i)).getName());
                    }

                    HsqlName[] array = new HsqlName[names.size()];

                    names.toArray(array);

                    return array;
                }
                case SchemaObject.GRANTEE : {
                    return catalogNameArray;
                }
                case SchemaObject.INDEX :
                case SchemaObject.CONSTRAINT :
                    findSchemaObject(name.name, name.schema.name, name.type);
            }

            SchemaObject object = findSchemaObject(name.name,
                                                   name.schema.name,
                                                   name.type);

            if (object == null) {
                return catalogNameArray;
            }

            HsqlName       parent     = object.getName().parent;
            OrderedHashSet references = getReferencesTo(object.getName());
            OrderedHashSet names      = new OrderedHashSet();

            names.add(database.getCatalogName());

            if (parent != null) {
                SchemaObject parentObject = findSchemaObject(parent.name,
                    parent.schema.name, parent.type);

                if (parentObject != null
                        && parentObject.getName().type == SchemaObject.TABLE) {
                    names.add(parentObject.getName());
                }
            }

            if (object.getName().type == SchemaObject.TABLE) {
                names.add(object.getName());
            }

            for (int i = 0; i < references.size(); i++) {
                HsqlName reference = (HsqlName) references.get(i);

                if (reference.type == SchemaObject.TABLE) {
                    Table table = findUserTable(null, reference.name,
                                                reference.schema.name);

                    if (table != null && !table.isTemp()) {
                        names.add(reference);
                    }
                }
            }

            HsqlName[] array = new HsqlName[names.size()];

            names.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    private SchemaObjectSet getSchemaObjectSet(Schema schema, int type) {

        readLock.lock();

        try {
            SchemaObjectSet set = null;

            switch (type) {

                case SchemaObject.SEQUENCE :
                    set = schema.sequenceLookup;
                    break;

                case SchemaObject.TABLE :
                case SchemaObject.VIEW :
                    set = schema.tableLookup;
                    break;

                case SchemaObject.CHARSET :
                    set = schema.charsetLookup;
                    break;

                case SchemaObject.COLLATION :
                    set = schema.collationLookup;
                    break;

                case SchemaObject.PROCEDURE :
                    set = schema.procedureLookup;
                    break;

                case SchemaObject.FUNCTION :
                    set = schema.functionLookup;
                    break;

                case SchemaObject.DOMAIN :
                case SchemaObject.TYPE :
                    set = schema.typeLookup;
                    break;

                case SchemaObject.INDEX :
                    set = schema.indexLookup;
                    break;

                case SchemaObject.CONSTRAINT :
                    set = schema.constraintLookup;
                    break;

                case SchemaObject.TRIGGER :
                    set = schema.triggerLookup;
                    break;

                case SchemaObject.SPECIFIC_ROUTINE :
                    set = schema.specificRoutineLookup;
            }

            return set;
        } finally {
            readLock.unlock();
        }
    }

    public void checkSchemaObjectNotExists(HsqlName name) {

        readLock.lock();

        try {
            Schema          schema = (Schema) schemaMap.get(name.schema.name);
            SchemaObjectSet set    = getSchemaObjectSet(schema, name.type);

            set.checkAdd(name);
        } finally {
            readLock.unlock();
        }
    }

    /**
     *  Returns the specified user-defined table or view visible within the
     *  context of the specified Session, or any system table of the given
     *  name. It excludes any temp tables created in other Sessions.
     *  Throws if the table does not exist in the context.
     */
    public Table getTable(Session session, String name, String schema) {

        readLock.lock();

        try {
            Table t = null;

            if (Tokens.T_MODULE.equals(schema)
                    || Tokens.T_SESSION.equals(schema)) {
                t = findSessionTable(session, name);

                if (t == null) {
                    throw Error.error(ErrorCode.X_42501, name);
                }

                return t;
            }

            if (schema == null) {
                if (session.database.sqlSyntaxOra
                        || session.database.sqlSyntaxDb2) {
                    if (Tokens.T_DUAL.equals(name)) {
                        return dualTable;
                    }
                }

                t = findSessionTable(session, name);
            }

            if (t == null) {
                schema = session.getSchemaName(schema);
                t      = findUserTable(session, name, schema);
            }

            if (t == null) {
                if (SqlInvariants.INFORMATION_SCHEMA.equals(schema)
                        && database.dbInfo != null) {
                    t = database.dbInfo.getSystemTable(session, name);
                }
            }

            if (t == null) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return t;
        } finally {
            readLock.unlock();
        }
    }

    public Table getUserTable(Session session, HsqlName name) {
        return getUserTable(session, name.name, name.schema.name);
    }

    /**
     *  Returns the specified user-defined table or view visible within the
     *  context of the specified Session. It excludes system tables and
     *  any temp tables created in different Sessions.
     *  Throws if the table does not exist in the context.
     */
    public Table getUserTable(Session session, String name, String schema) {

        Table t = findUserTable(session, name, schema);

        if (t == null) {
            String longName = schema == null ? name
                                             : schema + '.' + name;

            throw Error.error(ErrorCode.X_42501, longName);
        }

        return t;
    }

    /**
     *  Returns the specified user-defined table or view visible within the
     *  context of the specified schema. It excludes system tables.
     *  Returns null if the table does not exist in the context.
     */
    public Table findUserTable(Session session, String name,
                               String schemaName) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(schemaName);

            if (schema == null) {
                return null;
            }

            int i = schema.tableList.getIndex(name);

            if (i == -1) {
                return null;
            }

            return (Table) schema.tableList.get(i);
        } finally {
            readLock.unlock();
        }
    }

    /**
     *  Returns the specified session context table.
     *  Returns null if the table does not exist in the context.
     */
    public Table findSessionTable(Session session, String name) {
        return session.sessionContext.findSessionTable(name);
    }

    /**
     * Drops the specified user-defined view or table from this Database object.
     *
     * <p> The process of dropping a table or view includes:
     * <OL>
     * <LI> checking that the specified Session's currently connected User has
     * the right to perform this operation and refusing to proceed if not by
     * throwing.
     * <LI> checking for referential constraints that conflict with this
     * operation and refusing to proceed if they exist by throwing.</LI>
     * <LI> removing the specified Table from this Database object.
     * <LI> removing any exported foreign keys Constraint objects held by any
     * tables referenced by the table to be dropped. This is especially
     * important so that the dropped Table ceases to be referenced, eventually
     * allowing its full garbage collection.
     * <LI>
     * </OL>
     *
     * <p>
     *
     * @param session the connected context in which to perform this operation
     * @param table if true and if the Table to drop does not exist, fail
     *   silently, else throw
     * @param cascade true if the name argument refers to a View
     */
    public void dropTableOrView(Session session, Table table,
                                boolean cascade) {

        writeLock.lock();

        try {
            if (table.isView()) {
                dropView(table, cascade);
            } else {
                dropTable(session, table, cascade);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void dropView(Table table, boolean cascade) {

        Schema schema = (Schema) schemaMap.get(table.getSchemaName().name);

        removeSchemaObject(table.getName(), cascade);
        schema.triggerLookup.removeParent(table.getName());
    }

    private void dropTable(Session session, Table table, boolean cascade) {

        Schema schema    = (Schema) schemaMap.get(table.getSchemaName().name);
        int    dropIndex = schema.tableList.getIndex(table.getName().name);
        OrderedHashSet externalConstraints =
            table.getDependentExternalConstraints();
        OrderedHashSet externalReferences = new OrderedHashSet();

        getCascadingReferencesTo(table.getName(), externalReferences);

        if (!cascade) {
            for (int i = 0; i < externalConstraints.size(); i++) {
                Constraint c       = (Constraint) externalConstraints.get(i);
                HsqlName   refname = c.getRefName();

                if (c.getConstraintType()
                        == SchemaObject.ConstraintTypes.MAIN) {
                    throw Error.error(
                        ErrorCode.X_42533,
                        refname.getSchemaQualifiedStatementName());
                }
            }

            if (!externalReferences.isEmpty()) {
                int i = 0;

                for (; i < externalReferences.size(); i++) {
                    HsqlName name = (HsqlName) externalReferences.get(i);

                    if (name.parent == table.getName()) {
                        continue;
                    }

                    throw Error.error(ErrorCode.X_42502,
                                      name.getSchemaQualifiedStatementName());
                }
            }
        }

        OrderedHashSet tableSet          = new OrderedHashSet();
        OrderedHashSet constraintNameSet = new OrderedHashSet();
        OrderedHashSet indexNameSet      = new OrderedHashSet();

        for (int i = 0; i < externalConstraints.size(); i++) {
            Constraint c = (Constraint) externalConstraints.get(i);
            Table      t = c.getMain();

            if (t != table) {
                tableSet.add(t);
            }

            t = c.getRef();

            if (t != table) {
                tableSet.add(t);
            }

            constraintNameSet.add(c.getMainName());
            constraintNameSet.add(c.getRefName());
            indexNameSet.add(c.getRefIndex().getName());
        }

        OrderedHashSet uniqueConstraintNames =
            table.getUniquePKConstraintNames();
        TableWorks tw = new TableWorks(session, table);

        tableSet = tw.makeNewTables(tableSet, constraintNameSet, indexNameSet);

        tw.setNewTablesInSchema(tableSet);
        tw.updateConstraints(tableSet, constraintNameSet);
        removeSchemaObjects(externalReferences);
        removeTableDependentReferences(table);    //
        removeReferencesTo(uniqueConstraintNames);
        removeReferencesTo(table.getName());
        removeReferencesFrom(table);
        schema.tableList.remove(dropIndex);
        schema.indexLookup.removeParent(table.getName());
        schema.constraintLookup.removeParent(table.getName());
        schema.triggerLookup.removeParent(table.getName());
        removeTable(session, table);
        recompileDependentObjects(tableSet);
    }

    private void removeTable(Session session, Table table) {

        database.getGranteeManager().removeDbObject(table.getName());
        table.releaseTriggers();

        if (!table.isView() && table.hasLobColumn()) {
            RowIterator it = table.rowIterator(session);

            while (it.hasNext()) {
                Row      row  = it.getNextRow();
                Object[] data = row.getData();

                session.sessionData.adjustLobUsageCount(table, data, -1);
            }
        }

        if (table.tableType == TableBase.TEMP_TABLE) {
            Session sessions[] = database.sessionManager.getAllSessions();

            for (int i = 0; i < sessions.length; i++) {
                sessions[i].sessionData.persistentStoreCollection.setStore(
                    table, null);
            }
        } else {
            database.persistentStoreCollection.removeStore(table);
        }
    }

    public void setTable(int index, Table table) {

        writeLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(table.getSchemaName().name);

            schema.tableList.set(index, table.getName().name, table);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     *  Returns index of a table or view in the HashMappedList that
     *  contains the table objects for this Database.
     *
     * @param  table the Table object
     * @return  the index of the specified table or view, or -1 if not found
     */
    public int getTableIndex(Table table) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(table.getSchemaName().name);

            if (schema == null) {
                return -1;
            }

            HsqlName name = table.getName();

            return schema.tableList.getIndex(name.name);
        } finally {
            readLock.unlock();
        }
    }

    public void recompileDependentObjects(OrderedHashSet tableSet) {

        writeLock.lock();

        try {
            OrderedHashSet set = new OrderedHashSet();

            for (int i = 0; i < tableSet.size(); i++) {
                Table table = (Table) tableSet.get(i);

                set.addAll(getReferencesTo(table.getName()));
            }

            Session session = database.sessionManager.getSysSession();

            for (int i = 0; i < set.size(); i++) {
                HsqlName name = (HsqlName) set.get(i);

                switch (name.type) {

                    case SchemaObject.VIEW :
                    case SchemaObject.CONSTRAINT :
                    case SchemaObject.ASSERTION :
                    case SchemaObject.ROUTINE :
                    case SchemaObject.PROCEDURE :
                    case SchemaObject.FUNCTION :
                    case SchemaObject.SPECIFIC_ROUTINE :
                    case SchemaObject.TRIGGER :
                        SchemaObject object = getSchemaObject(name);

                        object.compile(session, null);
                        break;
                }
            }

            if (Error.TRACE) {
                HsqlArrayList list = getAllTables(false);

                for (int i = 0; i < list.size(); i++) {
                    Table t = (Table) list.get(i);

                    t.verifyConstraintsIntegrity();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * After addition or removal of columns and indexes all views that
     * reference the table should be recompiled.
     */
    public void recompileDependentObjects(Table table) {

        writeLock.lock();

        try {
            OrderedHashSet set = new OrderedHashSet();

            getCascadingReferencesTo(table.getName(), set);

            Session session = database.sessionManager.getSysSession();

            for (int i = 0; i < set.size(); i++) {
                HsqlName name = (HsqlName) set.get(i);

                switch (name.type) {

                    case SchemaObject.VIEW :
                    case SchemaObject.CONSTRAINT :
                    case SchemaObject.ASSERTION :
                    case SchemaObject.ROUTINE :
                    case SchemaObject.PROCEDURE :
                    case SchemaObject.FUNCTION :
                    case SchemaObject.SPECIFIC_ROUTINE :
                    case SchemaObject.TRIGGER :
                        SchemaObject object = getSchemaObject(name);

                        object.compile(session, null);
                        break;
                }
            }

            if (Error.TRACE) {
                HsqlArrayList list = getAllTables(false);

                for (int i = 0; i < list.size(); i++) {
                    Table t = (Table) list.get(i);

                    t.verifyConstraintsIntegrity();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Collation getCollation(Session session, String name,
                                  String schemaName) {

        Collation collation = null;

        if (schemaName == null
                || SqlInvariants.INFORMATION_SCHEMA.equals(schemaName)) {
            try {
                collation = Collation.getCollation(name);
            } catch (HsqlException e) {}
        }

        if (collation == null) {
            schemaName = session.getSchemaName(schemaName);
            collation = (Collation) getSchemaObject(name, schemaName,
                    SchemaObject.COLLATION);
        }

        return collation;
    }

    public NumberSequence getSequence(String name, String schemaName,
                                      boolean raise) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(schemaName);

            if (schema != null) {
                NumberSequence object =
                    (NumberSequence) schema.sequenceList.get(name);

                if (object != null) {
                    return object;
                }
            }

            if (raise) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Type getUserDefinedType(String name, String schemaName,
                                   boolean raise) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(schemaName);

            if (schema != null) {
                SchemaObject object = schema.typeLookup.getObject(name);

                if (object != null) {
                    return (Type) object;
                }
            }

            if (raise) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Type getDomainOrUDT(String name, String schemaName, boolean raise) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(schemaName);

            if (schema != null) {
                SchemaObject object = schema.typeLookup.getObject(name);

                if (object != null) {
                    return (Type) object;
                }
            }

            if (raise) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Type getDomain(String name, String schemaName, boolean raise) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(schemaName);

            if (schema != null) {
                SchemaObject object = schema.typeLookup.getObject(name);

                if (object != null && ((Type) object).isDomainType()) {
                    return (Type) object;
                }
            }

            if (raise) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Type getDistinctType(String name, String schemaName,
                                boolean raise) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(schemaName);

            if (schema != null) {
                SchemaObject object = schema.typeLookup.getObject(name);

                if (object != null && ((Type) object).isDistinctType()) {
                    return (Type) object;
                }
            }

            if (raise) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public SchemaObject getSchemaObject(String name, String schemaName,
                                        int type) {

        readLock.lock();

        try {
            SchemaObject object = findSchemaObject(name, schemaName, type);

            if (object == null) {
                throw Error.error(SchemaObjectSet.getGetErrorCode(type), name);
            }

            return object;
        } finally {
            readLock.unlock();
        }
    }

    public SchemaObject getCharacterSet(Session session, String name,
                                        String schemaName) {

        if (schemaName == null
                || SqlInvariants.INFORMATION_SCHEMA.equals(schemaName)) {
            if (name.equals("SQL_IDENTIFIER")) {
                return Charset.SQL_IDENTIFIER_CHARSET;
            }

            if (name.equals("SQL_TEXT")) {
                return Charset.SQL_TEXT;
            }

            if (name.equals("LATIN1")) {
                return Charset.LATIN1;
            }

            if (name.equals("ASCII_GRAPHIC")) {
                return Charset.ASCII_GRAPHIC;
            }
        }

        if (schemaName == null) {
            schemaName = session.getSchemaName(null);
        }

        return getSchemaObject(name, schemaName, SchemaObject.CHARSET);
    }

    public SchemaObject findSchemaObject(String name, String schemaName,
                                         int type) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(schemaName);

            if (schema == null) {
                return null;
            }

            SchemaObjectSet set = null;
            HsqlName        objectName;
            Table           table;

            switch (type) {

                case SchemaObject.SEQUENCE :
                    return schema.sequenceLookup.getObject(name);

                case SchemaObject.TABLE :
                case SchemaObject.VIEW :
                    return schema.tableLookup.getObject(name);

                case SchemaObject.CHARSET :
                    return schema.charsetLookup.getObject(name);

                case SchemaObject.COLLATION :
                    return schema.collationLookup.getObject(name);

                case SchemaObject.PROCEDURE :
                    return schema.procedureLookup.getObject(name);

                case SchemaObject.FUNCTION :
                    return schema.functionLookup.getObject(name);

                case SchemaObject.ROUTINE : {
                    SchemaObject object =
                        schema.procedureLookup.getObject(name);

                    if (object == null) {
                        object = schema.functionLookup.getObject(name);
                    }

                    return object;
                }
                case SchemaObject.SPECIFIC_ROUTINE :
                    return schema.specificRoutineLookup.getObject(name);

                case SchemaObject.DOMAIN :
                case SchemaObject.TYPE :
                    return schema.typeLookup.getObject(name);

                case SchemaObject.INDEX :
                    set        = schema.indexLookup;
                    objectName = set.getName(name);

                    if (objectName == null) {
                        return null;
                    }

                    table =
                        (Table) schema.tableList.get(objectName.parent.name);

                    return table.getIndex(name);

                case SchemaObject.CONSTRAINT :
                    set        = schema.constraintLookup;
                    objectName = set.getName(name);

                    if (objectName == null) {
                        return null;
                    }

                    table =
                        (Table) schema.tableList.get(objectName.parent.name);

                    if (table == null) {
                        return null;
                    }

                    return table.getConstraint(name);

                case SchemaObject.TRIGGER :
                    set        = schema.indexLookup;
                    objectName = set.getName(name);

                    if (objectName == null) {
                        return null;
                    }

                    table =
                        (Table) schema.tableList.get(objectName.parent.name);

                    return table.getTrigger(name);

                default :
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "SchemaManager");
            }
        } finally {
            readLock.unlock();
        }
    }

    // INDEX management

    /**
     * Returns the table that has an index with the given name and schema.
     */
    Table findUserTableForIndex(Session session, String name,
                                String schemaName) {

        readLock.lock();

        try {
            Schema   schema    = (Schema) schemaMap.get(schemaName);
            HsqlName indexName = schema.indexLookup.getName(name);

            if (indexName == null) {
                return null;
            }

            return findUserTable(session, indexName.parent.name, schemaName);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Drops the index with the specified name.
     */
    void dropIndex(Session session, HsqlName name) {

        writeLock.lock();

        try {
            Table t = getTable(session, name.parent.name,
                               name.parent.schema.name);
            TableWorks tw = new TableWorks(session, t);

            tw.dropIndex(name.name);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Drops the index with the specified name.
     */
    void dropConstraint(Session session, HsqlName name, boolean cascade) {

        writeLock.lock();

        try {
            Table t = getTable(session, name.parent.name,
                               name.parent.schema.name);
            TableWorks tw = new TableWorks(session, t);

            tw.dropConstraint(name.name, cascade);
        } finally {
            writeLock.unlock();
        }
    }

    void removeDependentObjects(HsqlName name) {

        writeLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(name.schema.name);

            schema.indexLookup.removeParent(name);
            schema.constraintLookup.removeParent(name);
            schema.triggerLookup.removeParent(name);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     *  Removes any foreign key Constraint objects (exported keys) held by any
     *  tables referenced by the specified table. <p>
     *
     *  This method is called as the last step of a successful call to
     *  dropTable() in order to ensure that the dropped Table ceases to be
     *  referenced when enforcing referential integrity.
     *
     * @param  toDrop The table to which other tables may be holding keys.
     *      This is a table that is in the process of being dropped.
     */
    void removeExportedKeys(Table toDrop) {

        writeLock.lock();

        try {

            // toDrop.schema may be null because it is not registerd
            Schema schema =
                (Schema) schemaMap.get(toDrop.getSchemaName().name);

            for (int i = 0; i < schema.tableList.size(); i++) {
                Table        table       = (Table) schema.tableList.get(i);
                Constraint[] constraints = table.getConstraints();

                for (int j = constraints.length - 1; j >= 0; j--) {
                    Table refTable = constraints[j].getRef();

                    if (toDrop == refTable) {
                        table.removeConstraint(j);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public Iterator databaseObjectIterator(String schemaName, int type) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(schemaName);

            return schema.schemaObjectIterator(type);
        } finally {
            readLock.unlock();
        }
    }

    public Iterator databaseObjectIterator(int type) {

        readLock.lock();

        try {
            Iterator it      = schemaMap.values().iterator();
            Iterator objects = new WrapperIterator();

            while (it.hasNext()) {
                int targetType = type;

                if (type == SchemaObject.ROUTINE) {
                    targetType = SchemaObject.FUNCTION;
                }

                Schema          temp = (Schema) it.next();
                SchemaObjectSet set  = temp.getObjectSet(targetType);
                Object[]        values;

                if (set.map.size() != 0) {
                    values = new Object[set.map.size()];

                    set.map.valuesToArray(values);

                    objects = new WrapperIterator(objects,
                                                  new WrapperIterator(values));
                }

                if (type == SchemaObject.ROUTINE) {
                    set = temp.getObjectSet(SchemaObject.PROCEDURE);

                    if (set.map.size() != 0) {
                        values = new Object[set.map.size()];

                        set.map.valuesToArray(values);

                        objects =
                            new WrapperIterator(objects,
                                                new WrapperIterator(values));
                    }
                }
            }

            return objects;
        } finally {
            readLock.unlock();
        }
    }

    // references
    private void addReferencesFrom(SchemaObject object) {

        OrderedHashSet set  = object.getReferences();
        HsqlName       name = object.getName();

        if (set == null) {
            return;
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = (HsqlName) set.get(i);

            if (object instanceof Routine) {
                name = ((Routine) object).getSpecificName();
            }

            referenceMap.put(referenced, name);
        }
    }

    private void removeReferencesTo(OrderedHashSet set) {

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = (HsqlName) set.get(i);

            referenceMap.remove(referenced);
        }
    }

    private void removeReferencesTo(HsqlName referenced) {
        referenceMap.remove(referenced);
    }

    private void removeReferencesFrom(SchemaObject object) {

        HsqlName       name = object.getName();
        OrderedHashSet set  = object.getReferences();

        if (set == null) {
            return;
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = (HsqlName) set.get(i);

            if (object instanceof Routine) {
                name = ((Routine) object).getSpecificName();
            }

            referenceMap.remove(referenced, name);
        }
    }

    private void removeTableDependentReferences(Table table) {

        OrderedHashSet mainSet = table.getReferencesForDependents();

        for (int i = 0; i < mainSet.size(); i++) {
            HsqlName     name   = (HsqlName) mainSet.get(i);
            SchemaObject object = null;

            switch (name.type) {

                case SchemaObject.CONSTRAINT :
                    object = table.getConstraint(name.name);
                    break;

                case SchemaObject.TRIGGER :
                    object = table.getTrigger(name.name);
                    break;

                case SchemaObject.COLUMN :
                    object = table.getColumn(table.getColumnIndex(name.name));
                    break;

                default :
                    continue;
            }

            removeReferencesFrom(object);
        }
    }

    public OrderedHashSet getReferencesTo(HsqlName object) {

        readLock.lock();

        try {
            OrderedHashSet set = new OrderedHashSet();
            Iterator       it  = referenceMap.get(object);

            while (it.hasNext()) {
                HsqlName name = (HsqlName) it.next();

                set.add(name);
            }

            return set;
        } finally {
            readLock.unlock();
        }
    }

    public OrderedHashSet getReferencesTo(HsqlName table, HsqlName column) {

        readLock.lock();

        try {
            OrderedHashSet set = new OrderedHashSet();
            Iterator       it  = referenceMap.get(table);

            while (it.hasNext()) {
                HsqlName       name       = (HsqlName) it.next();
                SchemaObject   object     = getSchemaObject(name);
                OrderedHashSet references = object.getReferences();

                if (references.contains(column)) {
                    set.add(name);
                }
            }

            it = referenceMap.get(column);

            while (it.hasNext()) {
                HsqlName name = (HsqlName) it.next();

                set.add(name);
            }

            return set;
        } finally {
            readLock.unlock();
        }
    }

    private boolean isReferenced(HsqlName object) {

        writeLock.lock();

        try {
            return referenceMap.containsKey(object);
        } finally {
            writeLock.unlock();
        }
    }

    //
    public void getCascadingReferencesTo(HsqlName object, OrderedHashSet set) {

        readLock.lock();

        try {
            OrderedHashSet newSet = new OrderedHashSet();
            Iterator       it     = referenceMap.get(object);

            while (it.hasNext()) {
                HsqlName name  = (HsqlName) it.next();
                boolean  added = set.add(name);

                if (added) {
                    newSet.add(name);
                }
            }

            for (int i = 0; i < newSet.size(); i++) {
                HsqlName name = (HsqlName) newSet.get(i);

                getCascadingReferencesTo(name, set);
            }
        } finally {
            readLock.unlock();
        }
    }

    public void getCascadingReferencesToSchema(HsqlName schema,
            OrderedHashSet set) {

        Iterator mainIterator = referenceMap.keySet().iterator();

        while (mainIterator.hasNext()) {
            HsqlName name = (HsqlName) mainIterator.next();

            if (name.schema != schema) {
                continue;
            }

            getCascadingReferencesTo(name, set);
        }

        for (int i = set.size() - 1; i >= 0; i--) {
            HsqlName name = (HsqlName) set.get(i);

            if (name.schema == schema) {
                set.remove(i);
            }
        }
    }

    public MultiValueHashMap getReferencesToSchema(String schemaName) {

        MultiValueHashMap map          = new MultiValueHashMap();
        Iterator          mainIterator = referenceMap.keySet().iterator();

        while (mainIterator.hasNext()) {
            HsqlName name = (HsqlName) mainIterator.next();

            if (!name.schema.name.equals(schemaName)) {
                continue;
            }

            Iterator it = referenceMap.get(name);

            while (it.hasNext()) {
                map.put(name, it.next());
            }
        }

        return map;
    }

    //
    public HsqlName getSchemaObjectName(HsqlName schemaName, String name,
                                        int type, boolean raise) {

        readLock.lock();

        try {
            Schema          schema = (Schema) schemaMap.get(schemaName.name);
            SchemaObjectSet set    = null;

            if (schema == null) {
                if (raise) {
                    throw Error.error(SchemaObjectSet.getGetErrorCode(type));
                } else {
                    return null;
                }
            }

            if (type == SchemaObject.ROUTINE) {
                set = schema.functionLookup;

                SchemaObject object = schema.functionLookup.getObject(name);

                if (object == null) {
                    set    = schema.procedureLookup;
                    object = schema.procedureLookup.getObject(name);
                }
            } else {
                set = getSchemaObjectSet(schema, type);
            }

            if (raise) {
                set.checkExists(name);
            }

            return set.getName(name);
        } finally {
            readLock.unlock();
        }
    }

    public SchemaObject getSchemaObject(HsqlName name) {

        readLock.lock();

        try {
            Schema schema = (Schema) schemaMap.get(name.schema.name);

            if (schema == null) {
                return null;
            }

            switch (name.type) {

                case SchemaObject.SEQUENCE :
                    return (SchemaObject) schema.sequenceList.get(name.name);

                case SchemaObject.TABLE :
                case SchemaObject.VIEW :
                    return (SchemaObject) schema.tableList.get(name.name);

                case SchemaObject.CHARSET :
                    return schema.charsetLookup.getObject(name.name);

                case SchemaObject.COLLATION :
                    return schema.collationLookup.getObject(name.name);

                case SchemaObject.PROCEDURE :
                    return schema.procedureLookup.getObject(name.name);

                case SchemaObject.FUNCTION :
                    return schema.functionLookup.getObject(name.name);

                case RoutineSchema.SPECIFIC_ROUTINE :
                    return schema.specificRoutineLookup.getObject(name.name);

                case RoutineSchema.ROUTINE :
                    SchemaObject object =
                        schema.functionLookup.getObject(name.name);

                    if (object == null) {
                        object = schema.procedureLookup.getObject(name.name);
                    }

                    return object;

                case SchemaObject.DOMAIN :
                case SchemaObject.TYPE :
                    return schema.typeLookup.getObject(name.name);

                case SchemaObject.TRIGGER : {
                    name = schema.triggerLookup.getName(name.name);

                    if (name == null) {
                        return null;
                    }

                    HsqlName tableName = name.parent;
                    Table table = (Table) schema.tableList.get(tableName.name);

                    return table.getTrigger(name.name);
                }
                case SchemaObject.CONSTRAINT : {
                    name = schema.constraintLookup.getName(name.name);

                    if (name == null) {
                        return null;
                    }

                    HsqlName tableName = name.parent;
                    Table table = (Table) schema.tableList.get(tableName.name);

                    return table.getConstraint(name.name);
                }
                case SchemaObject.ASSERTION :
                    return null;

                case SchemaObject.INDEX :
                    name = schema.indexLookup.getName(name.name);

                    if (name == null) {
                        return null;
                    }

                    HsqlName tableName = name.parent;
                    Table table = (Table) schema.tableList.get(tableName.name);

                    return table.getIndex(name.name);
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public void checkColumnIsReferenced(HsqlName tableName, HsqlName name) {

        OrderedHashSet set = getReferencesTo(tableName, name);

        if (!set.isEmpty()) {
            HsqlName objectName = (HsqlName) set.get(0);

            throw Error.error(ErrorCode.X_42502,
                              objectName.getSchemaQualifiedStatementName());
        }
    }

    public void checkObjectIsReferenced(HsqlName name) {

        OrderedHashSet set     = getReferencesTo(name);
        HsqlName       refName = null;

        for (int i = 0; i < set.size(); i++) {
            refName = (HsqlName) set.get(i);

            // except columns of same table
            if (refName.parent != name) {
                break;
            }

            refName = null;
        }

        if (refName == null) {
            return;
        }

        int errorCode = ErrorCode.X_42502;

        if (refName.type == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
            errorCode = ErrorCode.X_42533;
        }

        throw Error.error(errorCode,
                          refName.getSchemaQualifiedStatementName());
    }

    public void checkSchemaNameCanChange(HsqlName name) {

        readLock.lock();

        try {
            Iterator it      = referenceMap.values().iterator();
            HsqlName refName = null;

            mainLoop:
            while (it.hasNext()) {
                refName = (HsqlName) it.next();

                switch (refName.type) {

                    case SchemaObject.VIEW :
                    case SchemaObject.ROUTINE :
                    case SchemaObject.FUNCTION :
                    case SchemaObject.PROCEDURE :
                    case SchemaObject.TRIGGER :
                    case SchemaObject.SPECIFIC_ROUTINE :
                        if (refName.schema == name) {
                            break mainLoop;
                        }
                        break;

                    default :
                        break;
                }

                refName = null;
            }

            if (refName == null) {
                return;
            }

            throw Error.error(ErrorCode.X_42502,
                              refName.getSchemaQualifiedStatementName());
        } finally {
            readLock.unlock();
        }
    }

    public void addSchemaObject(SchemaObject object) {

        writeLock.lock();

        try {
            HsqlName        name   = object.getName();
            Schema          schema = (Schema) schemaMap.get(name.schema.name);
            SchemaObjectSet set    = getSchemaObjectSet(schema, name.type);

            switch (name.type) {

                case SchemaObject.PROCEDURE :
                case SchemaObject.FUNCTION : {
                    RoutineSchema routine =
                        (RoutineSchema) set.getObject(name.name);

                    if (routine == null) {
                        routine = new RoutineSchema(name.type, name);

                        routine.addSpecificRoutine(database, (Routine) object);
                        set.checkAdd(name);

                        SchemaObjectSet specificSet =
                            getSchemaObjectSet(schema,
                                               SchemaObject.SPECIFIC_ROUTINE);

                        specificSet.checkAdd(
                            ((Routine) object).getSpecificName());
                        set.add(routine);
                        specificSet.add(object);
                    } else {
                        SchemaObjectSet specificSet =
                            getSchemaObjectSet(schema,
                                               SchemaObject.SPECIFIC_ROUTINE);
                        HsqlName specificName =
                            ((Routine) object).getSpecificName();

                        if (specificName != null) {
                            specificSet.checkAdd(specificName);
                        }

                        routine.addSpecificRoutine(database, (Routine) object);
                        specificSet.add(object);
                    }

                    addReferencesFrom(object);

                    return;
                }
                case SchemaObject.TABLE : {
                    OrderedHashSet refs =
                        ((Table) object).getReferencesForDependents();

                    for (int i = 0; i < refs.size(); i++) {
                        HsqlName ref = (HsqlName) refs.get(i);

                        switch (ref.type) {

                            case SchemaObject.COLUMN : {
                                int index =
                                    ((Table) object).findColumn(ref.name);
                                ColumnSchema column =
                                    ((Table) object).getColumn(index);

                                addSchemaObject(column);

                                break;
                            }
                        }
                    }

                    break;
                }
                case SchemaObject.COLUMN : {
                    OrderedHashSet refs = object.getReferences();

                    if (refs == null || refs.isEmpty()) {
                        return;
                    }

                    break;
                }
            }

            if (set != null) {
                set.add(object);
            }

            addReferencesFrom(object);
        } finally {
            writeLock.unlock();
        }
    }

    public void removeSchemaObject(HsqlName name, boolean cascade) {

        writeLock.lock();

        try {
            OrderedHashSet objectSet = new OrderedHashSet();

            switch (name.type) {

                case SchemaObject.ROUTINE :
                case SchemaObject.PROCEDURE :
                case SchemaObject.FUNCTION : {
                    RoutineSchema routine =
                        (RoutineSchema) getSchemaObject(name);

                    if (routine != null) {
                        Routine[] specifics = routine.getSpecificRoutines();

                        for (int i = 0; i < specifics.length; i++) {
                            getCascadingReferencesTo(
                                specifics[i].getSpecificName(), objectSet);
                        }
                    }
                }
                break;

                case SchemaObject.SEQUENCE :
                case SchemaObject.TABLE :
                case SchemaObject.VIEW :
                case SchemaObject.TYPE :
                case SchemaObject.CHARSET :
                case SchemaObject.COLLATION :
                case SchemaObject.SPECIFIC_ROUTINE :
                    getCascadingReferencesTo(name, objectSet);
                    break;

                case SchemaObject.DOMAIN :
                    OrderedHashSet set = getReferencesTo(name);
                    Iterator       it  = set.iterator();

                    while (it.hasNext()) {
                        HsqlName ref = (HsqlName) it.next();

                        if (ref.type == SchemaObject.COLUMN) {
                            it.remove();
                        }
                    }

                    if (!set.isEmpty()) {
                        HsqlName objectName = (HsqlName) set.get(0);

                        throw Error.error(
                            ErrorCode.X_42502,
                            objectName.getSchemaQualifiedStatementName());
                    }
                    break;
            }

            if (objectSet.isEmpty()) {
                removeSchemaObject(name);

                return;
            }

            if (!cascade) {
                HsqlName objectName = (HsqlName) objectSet.get(0);

                throw Error.error(
                    ErrorCode.X_42502,
                    objectName.getSchemaQualifiedStatementName());
            }

            objectSet.add(name);
            removeSchemaObjects(objectSet);
        } finally {
            writeLock.unlock();
        }
    }

    public void removeSchemaObjects(OrderedHashSet set) {

        writeLock.lock();

        try {
            for (int i = 0; i < set.size(); i++) {
                HsqlName name = (HsqlName) set.get(i);

                removeSchemaObject(name);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void removeSchemaObject(HsqlName name) {

        writeLock.lock();

        try {
            Schema          schema = (Schema) schemaMap.get(name.schema.name);
            SchemaObject    object = null;
            SchemaObjectSet set    = null;

            switch (name.type) {

                case SchemaObject.SEQUENCE :
                    set    = schema.sequenceLookup;
                    object = set.getObject(name.name);
                    break;

                case SchemaObject.TABLE :
                case SchemaObject.VIEW : {
                    set    = schema.tableLookup;
                    object = set.getObject(name.name);

                    break;
                }
                case SchemaObject.COLUMN : {
                    Table table = (Table) getSchemaObject(name.parent);

                    if (table != null) {
                        object =
                            table.getColumn(table.getColumnIndex(name.name));
                    }

                    break;
                }
                case SchemaObject.CHARSET :
                    set    = schema.charsetLookup;
                    object = set.getObject(name.name);
                    break;

                case SchemaObject.COLLATION :
                    set    = schema.collationLookup;
                    object = set.getObject(name.name);
                    break;

                case SchemaObject.PROCEDURE : {
                    set = schema.procedureLookup;

                    RoutineSchema routine =
                        (RoutineSchema) set.getObject(name.name);

                    object = routine;

                    Routine[] specifics = routine.getSpecificRoutines();

                    for (int i = 0; i < specifics.length; i++) {
                        removeSchemaObject(specifics[i].getSpecificName());
                    }

                    break;
                }
                case SchemaObject.FUNCTION : {
                    set = schema.functionLookup;

                    RoutineSchema routine =
                        (RoutineSchema) set.getObject(name.name);

                    object = routine;

                    Routine[] specifics = routine.getSpecificRoutines();

                    for (int i = 0; i < specifics.length; i++) {
                        removeSchemaObject(specifics[i].getSpecificName());
                    }

                    break;
                }
                case SchemaObject.SPECIFIC_ROUTINE : {
                    set = schema.specificRoutineLookup;

                    Routine routine = (Routine) set.getObject(name.name);

                    object = routine;

                    routine.routineSchema.removeSpecificRoutine(routine);

                    if (routine.routineSchema.getSpecificRoutines().length
                            == 0) {
                        removeSchemaObject(routine.getName());
                    }

                    break;
                }
                case SchemaObject.DOMAIN :
                case SchemaObject.TYPE :
                    set    = schema.typeLookup;
                    object = set.getObject(name.name);
                    break;

                case SchemaObject.INDEX :
                    set = schema.indexLookup;
                    break;

                case SchemaObject.CONSTRAINT : {
                    set = schema.constraintLookup;

                    if (name.parent.type == SchemaObject.TABLE) {
                        Table table =
                            (Table) schema.tableList.get(name.parent.name);

                        object = table.getConstraint(name.name);

                        table.removeConstraint(name.name);
                    } else if (name.parent.type == SchemaObject.DOMAIN) {
                        Type type = (Type) schema.typeLookup.getObject(
                            name.parent.name);

                        object =
                            type.userTypeModifier.getConstraint(name.name);

                        type.userTypeModifier.removeConstraint(name.name);
                    }

                    break;
                }
                case SchemaObject.TRIGGER : {
                    set = schema.triggerLookup;

                    Table table =
                        (Table) schema.tableList.get(name.parent.name);

                    object = table.getTrigger(name.name);

                    if (object != null) {
                        table.removeTrigger((TriggerDef) object);
                    }

                    break;
                }
                default :
                    throw Error.runtimeError(ErrorCode.U_S0500,
                                             "SchemaManager");
            }

            if (object != null) {
                database.getGranteeManager().removeDbObject(name);
                removeReferencesFrom(object);
            }

            if (set != null) {
                set.remove(name.name);
            }

            removeReferencesTo(name);
        } finally {
            writeLock.unlock();
        }
    }

    public void renameSchemaObject(HsqlName name, HsqlName newName) {

        writeLock.lock();

        try {
            if (name.schema != newName.schema) {
                throw Error.error(ErrorCode.X_42505, newName.schema.name);
            }

            checkObjectIsReferenced(name);

            Schema          schema = (Schema) schemaMap.get(name.schema.name);
            SchemaObjectSet set    = getSchemaObjectSet(schema, name.type);

            set.rename(name, newName);
        } finally {
            writeLock.unlock();
        }
    }

    public void replaceReferences(SchemaObject oldObject,
                                  SchemaObject newObject) {

        writeLock.lock();

        try {
            removeReferencesFrom(oldObject);
            addReferencesFrom(newObject);
        } finally {
            writeLock.unlock();
        }
    }

    public String[] getSQLArray() {

        readLock.lock();

        try {
            OrderedHashSet resolved   = new OrderedHashSet();
            OrderedHashSet unresolved = new OrderedHashSet();
            HsqlArrayList  list       = new HsqlArrayList();
            Iterator       schemas    = schemaMap.values().iterator();

            schemas = schemaMap.values().iterator();

            while (schemas.hasNext()) {
                Schema schema = (Schema) schemas.next();

                if (SqlInvariants.isSystemSchemaName(schema.getName().name)) {
                    continue;
                }

                if (SqlInvariants.isLobsSchemaName(schema.getName().name)) {
                    continue;
                }

                list.add(schema.getSQL());
                schema.addSimpleObjects(unresolved);
            }

            while (true) {
                Iterator it = unresolved.iterator();

                if (!it.hasNext()) {
                    break;
                }

                OrderedHashSet newResolved = new OrderedHashSet();

                SchemaObjectSet.addAllSQL(resolved, unresolved, list, it,
                                          newResolved);
                unresolved.removeAll(newResolved);

                if (newResolved.size() == 0) {
                    break;
                }
            }

            schemas = schemaMap.values().iterator();

            while (schemas.hasNext()) {
                Schema schema = (Schema) schemas.next();

                if (SqlInvariants.isLobsSchemaName(schema.getName().name)) {
                    continue;
                }

                if (SqlInvariants.isSystemSchemaName(schema.getName().name)) {
                    continue;
                }

                list.addAll(schema.getSQLArray(resolved, unresolved));
            }

            while (true) {
                Iterator it = unresolved.iterator();

                if (!it.hasNext()) {
                    break;
                }

                OrderedHashSet newResolved = new OrderedHashSet();

                SchemaObjectSet.addAllSQL(resolved, unresolved, list, it,
                                          newResolved);
                unresolved.removeAll(newResolved);

                if (newResolved.size() == 0) {
                    break;
                }
            }

            Iterator it = unresolved.iterator();

            while (it.hasNext()) {
                SchemaObject object = (SchemaObject) it.next();

                if (object instanceof Routine) {
                    list.add(((Routine) object).getSQLDeclaration());
                }
            }

            it = unresolved.iterator();

            while (it.hasNext()) {
                SchemaObject object = (SchemaObject) it.next();

                if (object instanceof Routine) {
                    list.add(((Routine) object).getSQLAlter());
                } else {
                    list.add(object.getSQL());
                }
            }

            schemas = schemaMap.values().iterator();

            while (schemas.hasNext()) {
                Schema schema = (Schema) schemas.next();

                if (SqlInvariants.isLobsSchemaName(schema.getName().name)) {
                    continue;
                }

                if (SqlInvariants.isSystemSchemaName(schema.getName().name)) {
                    continue;
                }

                String[] t = schema.getTriggerSQL();

                if (t.length > 0) {
                    list.add(Schema.getSetSchemaSQL(schema.getName()));
                    list.addAll(t);
                }
            }

            schemas = schemaMap.values().iterator();

            while (schemas.hasNext()) {
                Schema schema = (Schema) schemas.next();

                list.addAll(schema.getSequenceRestartSQL());
            }

            if (defaultSchemaHsqlName != null) {
                StringBuffer sb = new StringBuffer();

                sb.append(Tokens.T_SET).append(' ').append(Tokens.T_DATABASE);
                sb.append(' ').append(Tokens.T_DEFAULT).append(' ');
                sb.append(Tokens.T_INITIAL).append(' ').append(
                    Tokens.T_SCHEMA);
                sb.append(' ').append(defaultSchemaHsqlName.statementName);
                list.add(sb.toString());
            }

            String[] array = new String[list.size()];

            list.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public String[] getTablePropsSQL(boolean withHeader) {

        readLock.lock();

        try {
            HsqlArrayList tableList = getAllTables(false);
            HsqlArrayList list      = new HsqlArrayList();

            for (int i = 0; i < tableList.size(); i++) {
                Table t = (Table) tableList.get(i);

                if (t.isText()) {
                    String[] ddl = t.getSQLForTextSource(withHeader);

                    list.addAll(ddl);
                }

                String ddl = t.getSQLForReadOnly();

                if (ddl != null) {
                    list.add(ddl);
                }

                if (t.isCached()) {
                    ddl = t.getSQLForClustered();

                    if (ddl != null) {
                        list.add(ddl);
                    }
                }
            }

            String[] array = new String[list.size()];

            list.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public String[] getTableSpaceSQL() {

        readLock.lock();

        try {
            HsqlArrayList tableList = getAllTables(false);
            HsqlArrayList list      = new HsqlArrayList();

            for (int i = 0; i < tableList.size(); i++) {
                Table t = (Table) tableList.get(i);

                if (t.isCached()) {
                    String ddl = t.getSQLForTableSpace();

                    if (ddl != null) {
                        list.add(ddl);
                    }
                }
            }

            String[] array = new String[list.size()];

            list.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public String[] getIndexRootsSQL() {

        readLock.lock();

        try {
            Session       sysSession = database.sessionManager.getSysSession();
            long[][]      rootsArray = getIndexRoots(sysSession);
            HsqlArrayList tableList  = getAllTables(true);
            HsqlArrayList list       = new HsqlArrayList();

            for (int i = 0; i < rootsArray.length; i++) {
                Table table = (Table) tableList.get(i);

                if (rootsArray[i] != null && rootsArray[i].length > 0
                        && rootsArray[i][0] != -1) {
                    String ddl = table.getIndexRootsSQL(rootsArray[i]);

                    list.add(ddl);
                }
            }

            String[] array = new String[list.size()];

            list.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public String[] getCommentsArray() {

        readLock.lock();

        try {
            HsqlArrayList tableList = getAllTables(false);
            HsqlArrayList list      = new HsqlArrayList();
            StringBuffer  sb        = new StringBuffer();

            for (int i = 0; i < tableList.size(); i++) {
                Table table = (Table) tableList.get(i);

                if (table.getTableType() == Table.INFO_SCHEMA_TABLE) {
                    continue;
                }

                int colCount = table.getColumnCount();

                for (int j = 0; j < colCount; j++) {
                    ColumnSchema column = table.getColumn(j);

                    if (column.getName().comment == null) {
                        continue;
                    }

                    sb.setLength(0);
                    sb.append(Tokens.T_COMMENT).append(' ').append(
                        Tokens.T_ON);
                    sb.append(' ').append(Tokens.T_COLUMN).append(' ');
                    sb.append(
                        table.getName().getSchemaQualifiedStatementName());
                    sb.append('.').append(column.getName().statementName);
                    sb.append(' ').append(Tokens.T_IS).append(' ');
                    sb.append(
                        StringConverter.toQuotedString(
                            column.getName().comment, '\'', true));
                    list.add(sb.toString());
                }

                if (table.getName().comment == null) {
                    continue;
                }

                sb.setLength(0);
                sb.append(Tokens.T_COMMENT).append(' ').append(Tokens.T_ON);
                sb.append(' ').append(Tokens.T_TABLE).append(' ');
                sb.append(table.getName().getSchemaQualifiedStatementName());
                sb.append(' ').append(Tokens.T_IS).append(' ');
                sb.append(
                    StringConverter.toQuotedString(
                        table.getName().comment, '\'', true));
                list.add(sb.toString());
            }

            Iterator it = databaseObjectIterator(SchemaObject.ROUTINE);

            while (it.hasNext()) {
                SchemaObject object = (SchemaObject) it.next();

                if (object.getName().comment == null) {
                    continue;
                }

                sb.setLength(0);
                sb.append(Tokens.T_COMMENT).append(' ').append(Tokens.T_ON);
                sb.append(' ').append(Tokens.T_ROUTINE).append(' ');
                sb.append(object.getName().getSchemaQualifiedStatementName());
                sb.append(' ').append(Tokens.T_IS).append(' ');
                sb.append(
                    StringConverter.toQuotedString(
                        object.getName().comment, '\'', true));
                list.add(sb.toString());
            }

            String[] array = new String[list.size()];

            list.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    long[][] tempIndexRoots;

    public void setTempIndexRoots(long[][] roots) {
        tempIndexRoots = roots;
    }

    public long[][] getIndexRoots(Session session) {

        readLock.lock();

        try {
            if (tempIndexRoots != null) {
                long[][] roots = tempIndexRoots;

                tempIndexRoots = null;

                return roots;
            }

            HsqlArrayList allTables = getAllTables(true);
            HsqlArrayList list      = new HsqlArrayList();

            for (int i = 0, size = allTables.size(); i < size; i++) {
                Table t = (Table) allTables.get(i);

                if (t.getTableType() == TableBase.CACHED_TABLE) {
                    long[] roots = t.getIndexRootsArray();

                    list.add(roots);
                } else {
                    list.add(null);
                }
            }

            long[][] array = new long[list.size()][];

            list.toArray(array);

            return array;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * called after the completion of defrag
     */
    public void setIndexRoots(long[][] roots) {

        readLock.lock();

        try {
            HsqlArrayList allTables =
                database.schemaManager.getAllTables(true);

            for (int i = 0, size = allTables.size(); i < size; i++) {
                Table t = (Table) allTables.get(i);

                if (t.getTableType() == TableBase.CACHED_TABLE) {
                    long[] rootsArray = roots[i];

                    if (rootsArray != null) {
                        t.setIndexRoots(rootsArray);
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    public void setDefaultTableType(int type) {
        defaultTableType = type;
    }

    public int getDefaultTableType() {
        return defaultTableType;
    }

    public void createSystemTables() {

        dualTable = TableUtil.newSingleColumnTable(database,
                SqlInvariants.DUAL_TABLE_HSQLNAME, TableBase.SYSTEM_TABLE,
                SqlInvariants.DUAL_COLUMN_HSQLNAME, Type.SQL_VARCHAR);

        dualTable.insertSys(database.sessionManager.getSysSession(),
                            dualTable.getRowStore(null), new Object[]{ "X" });
        dualTable.setDataReadOnly(true);

        Type[] columnTypes = new Type[] {
            Type.SQL_BIGINT, Type.SQL_BIGINT, Type.SQL_BIGINT,
            TypeInvariants.SQL_IDENTIFIER, TypeInvariants.SQL_IDENTIFIER,
            Type.SQL_BOOLEAN
        };
        HsqlName       tableName = database.nameManager.getSubqueryTableName();
        HashMappedList columnList = new HashMappedList();

        for (int i = 0; i < columnTypes.length; i++) {
            HsqlName name = database.nameManager.getAutoColumnName(i + 1);
            ColumnSchema column = new ColumnSchema(name, columnTypes[i], true,
                                                   false, null);

            columnList.add(name.name, column);
        }

        dataChangeTable = new TableDerived(database, tableName,
                                           TableBase.CHANGE_SET_TABLE,
                                           columnTypes, columnList,
                                           new int[]{ 0 });

        dataChangeTable.createIndexForColumns(null, new int[]{ 1 });
    }
}
