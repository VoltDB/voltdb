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
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.MultiValueHashMap;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.WrapperIterator;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.Type;

/**
 * Manages all SCHEMA related database objects
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version  1.9.0
 * @since 1.8.0
 */
public class SchemaManager {

    Database          database;
    HsqlName          defaultSchemaHsqlName;
    HashMappedList    schemaMap        = new HashMappedList();
    MultiValueHashMap referenceMap     = new MultiValueHashMap();
    int               defaultTableType = TableBase.MEMORY_TABLE;

    SchemaManager(Database database) {

        this.database         = database;
        defaultSchemaHsqlName = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;

        Schema schema =
            new Schema(SqlInvariants.INFORMATION_SCHEMA_HSQLNAME,
                       SqlInvariants.INFORMATION_SCHEMA_HSQLNAME.owner);

        schemaMap.put(schema.name.name, schema);

        try {
            schema.typeLookup.add(SqlInvariants.CARDINAL_NUMBER);
            schema.typeLookup.add(SqlInvariants.YES_OR_NO);
            schema.typeLookup.add(SqlInvariants.CHARACTER_DATA);
            schema.typeLookup.add(SqlInvariants.SQL_IDENTIFIER);
            schema.typeLookup.add(SqlInvariants.TIME_STAMP);
            schema.charsetLookup.add(SqlInvariants.SQL_TEXT);
            schema.charsetLookup.add(SqlInvariants.SQL_IDENTIFIER_CHARSET);
            schema.charsetLookup.add(SqlInvariants.SQL_CHARACTER);
        } catch (HsqlException e) {}
    }

    // pre-defined

    public HsqlName getSQLJSchemaHsqlName() {
        return SqlInvariants.SQLJ_SCHEMA_HSQLNAME;
    }

    // SCHEMA management
    void createPublicSchema() {

        HsqlName name = database.nameManager.newHsqlName(null,
            SqlInvariants.PUBLIC_SCHEMA, SchemaObject.SCHEMA);
        Schema schema = new Schema(name,
                                   database.getGranteeManager().getDBARole());

        defaultSchemaHsqlName = schema.name;

        schemaMap.put(schema.name.name, schema);
    }

    /**
     * Creates a schema belonging to the given grantee.
     */
    void createSchema(HsqlName name, Grantee owner) {

        SqlInvariants.checkSchemaNameNotSystem(name.name);

        Schema schema = new Schema(name, owner);

        schemaMap.add(name.name, schema);
    }

    void dropSchema(String name, boolean cascade) {

        Schema schema = (Schema) schemaMap.get(name);

        if (schema == null) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        if (cascade) {
            OrderedHashSet externalReferences = new OrderedHashSet();

            getCascadingSchemaReferences(schema.getName(), externalReferences);
            removeSchemaObjects(externalReferences);
        } else {
            if (!schema.isEmpty()) {
                throw Error.error(ErrorCode.X_2B000);
            }
        }

        Iterator tableIterator =
            schema.schemaObjectIterator(SchemaObject.TABLE);

        while (tableIterator.hasNext()) {
            Table table = ((Table) tableIterator.next());

            database.getGranteeManager().removeDbObject(table.getName());
            table.releaseTriggers();
            database.persistentStoreCollection.releaseStore(table);
        }

        Iterator sequenceIterator =
            schema.schemaObjectIterator(SchemaObject.SEQUENCE);

        while (sequenceIterator.hasNext()) {
            NumberSequence sequence =
                ((NumberSequence) sequenceIterator.next());

            database.getGranteeManager().removeDbObject(sequence.getName());
        }

        schema.clearStructures();
        schemaMap.remove(name);

        if (defaultSchemaHsqlName.name.equals(name)) {
            HsqlName hsqlName = database.nameManager.newHsqlName(name, false,
                SchemaObject.SCHEMA);

            schema = new Schema(hsqlName,
                                database.getGranteeManager().getDBARole());
            defaultSchemaHsqlName = schema.name;

            schemaMap.put(schema.name.name, schema);
        }

        // these are called last and in this particular order
        database.getUserManager().removeSchemaReference(name);
        database.getSessionManager().removeSchemaReference(schema);
    }

    void renameSchema(HsqlName name, HsqlName newName) {

        Schema schema = (Schema) schemaMap.get(name.name);
        Schema exists = (Schema) schemaMap.get(newName.name);

        if (schema == null) {
            throw Error.error(ErrorCode.X_42501, name.name);
        }

        if (exists != null) {
            throw Error.error(ErrorCode.X_42504, newName.name);
        }

        SqlInvariants.checkSchemaNameNotSystem(newName.name);
        schema.name.rename(newName);

        int index = schemaMap.getIndex(name);

        schemaMap.set(index, newName.name, schema);
    }

    void clearStructures() {

        Iterator it = schemaMap.values().iterator();

        while (it.hasNext()) {
            Schema schema = (Schema) it.next();

            schema.clearStructures();
        }
    }

    public Iterator allSchemaNameIterator() {
        return schemaMap.keySet().iterator();
    }

    HsqlName getUserSchemaHsqlName(String name) {

        Schema schema = (Schema) schemaMap.get(name);

        if (schema == null) {
            throw Error.error(ErrorCode.X_3F000, name);
        }

        if (schema.getName() == SqlInvariants.INFORMATION_SCHEMA_HSQLNAME) {
            throw Error.error(ErrorCode.X_3F000, name);
        }

        return schema.name;
    }

    public Grantee toSchemaOwner(String name) {

        // Note that INFORMATION_SCHEMA and DEFINITION_SCHEMA aren't in the
        // backing map.
        // This may not be the most elegant solution, but it is the safest
        // (without doing a code review for implications of adding
        // them to the map).
        if (SqlInvariants.INFORMATION_SCHEMA_HSQLNAME.name.equals(name)) {
            return SqlInvariants.INFORMATION_SCHEMA_HSQLNAME.owner;
        }

        Schema schema = (Schema) schemaMap.get(name);

        return schema == null ? null
                              : schema.owner;
    }

    public HsqlName getDefaultSchemaHsqlName() {
        return defaultSchemaHsqlName;
    }

    public void setDefaultSchemaHsqlName(HsqlName name) {
        defaultSchemaHsqlName = name;
    }

    boolean schemaExists(String name) {
        return SqlInvariants.INFORMATION_SCHEMA.equals(name)
               || schemaMap.containsKey(name);
    }

    public HsqlName findSchemaHsqlName(String name) {

        Schema schema = ((Schema) schemaMap.get(name));

        if (schema == null) {
            return null;
        }

        return schema.name;
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

        if (SqlInvariants.INFORMATION_SCHEMA.equals(name)) {
            return SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
        }

        Schema schema = ((Schema) schemaMap.get(name));

        if (schema == null) {
            throw Error.error(ErrorCode.X_3F000, name);
        }

        return schema.name;
    }

    /**
     * Same as above, but return string
     */
    public String getSchemaName(String name) {
        return getSchemaHsqlName(name).name;
    }

    /**
     * Iterator includes DEFINITION_SCHEMA
     */
    public Iterator fullSchemaNamesIterator() {
        return schemaMap.keySet().iterator();
    }

    public boolean isSystemSchema(String schema) {

        return SqlInvariants.INFORMATION_SCHEMA.equals(schema)
               || SqlInvariants.DEFINITION_SCHEMA.equals(schema)
               || SqlInvariants.SYSTEM_SCHEMA.equals(schema);
    }

    public boolean isLobsSchema(String schema) {
        return SqlInvariants.LOBS_SCHEMA.equals(schema);
    }

    /**
     * is a grantee the authorization of any schema
     */
    boolean isSchemaAuthorisation(Grantee grantee) {

        Iterator schemas = allSchemaNameIterator();

        while (schemas.hasNext()) {
            String schemaName = (String) schemas.next();

            if (grantee.equals(toSchemaOwner(schemaName))) {
                return true;
            }
        }

        return false;
    }

    /**
     * drop all schemas with the given authorisation
     */
    void dropSchemas(Grantee grantee, boolean cascade) {

        HsqlArrayList list = getSchemas(grantee);
        Iterator      it   = list.iterator();

        while (it.hasNext()) {
            Schema schema = (Schema) it.next();

            dropSchema(schema.name.name, cascade);
        }
    }

    HsqlArrayList getSchemas(Grantee grantee) {

        HsqlArrayList list = new HsqlArrayList();
        Iterator      it   = schemaMap.values().iterator();

        while (it.hasNext()) {
            Schema schema = (Schema) it.next();

            if (grantee.equals(schema.owner)) {
                list.add(schema);
            }
        }

        return list;
    }

    boolean hasSchemas(Grantee grantee) {

        Iterator it = schemaMap.values().iterator();

        while (it.hasNext()) {
            Schema schema = (Schema) it.next();

            if (grantee.equals(schema.owner)) {
                return true;
            }
        }

        return false;
    }

    /**
     *  Returns an HsqlArrayList containing references to all non-system
     *  tables and views. This includes all tables and views registered with
     *  this Database.
     */
    public HsqlArrayList getAllTables() {

        Iterator      schemas   = allSchemaNameIterator();
        HsqlArrayList alltables = new HsqlArrayList();

        while (schemas.hasNext()) {
            String         name    = (String) schemas.next();
            HashMappedList current = getTables(name);

            alltables.addAll(current.values());
        }

        return alltables;
    }

    public HashMappedList getTables(String schema) {

        Schema temp = (Schema) schemaMap.get(schema);

        return temp.tableList;
    }

    SchemaObjectSet getSchemaObjectSet(Schema schema, int type) {

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
        }

        return set;
    }

    void checkSchemaObjectNotExists(HsqlName name) {

        Schema          schema = (Schema) schemaMap.get(name.schema.name);
        SchemaObjectSet set    = getSchemaObjectSet(schema, name.type);

        set.checkAdd(name);
    }

    /**
     *  Returns the specified user-defined table or view visible within the
     *  context of the specified Session, or any system table of the given
     *  name. It excludes any temp tables created in other Sessions.
     *  Throws if the table does not exist in the context.
     */
    public Table getTable(Session session, String name, String schema) {

        Table t = null;

        if (schema == null) {
            t = findSessionTable(session, name, schema);
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
            throw Error.error(ErrorCode.X_42501, name);
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

        Schema schema = (Schema) schemaMap.get(schemaName);

        if (schema == null) {
            return null;
        }

        if (session != null) {
            Table table = session.getLocalTable(name);
            if (table != null) {
                return table;
            }
        }

        int i = schema.tableList.getIndex(name);

        if (i == -1) {
            return null;
        }

        return (Table) schema.tableList.get(i);
    }

    /**
     *  Returns the specified session context table.
     *  Returns null if the table does not exist in the context.
     */
    public Table findSessionTable(Session session, String name,
                                  String schemaName) {
        return session.findSessionTable(name);
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
    void dropTableOrView(Session session, Table table, boolean cascade) {

// ft - concurrent
        session.commit(false);

        if (table.isView()) {
            removeSchemaObject(table.getName(), cascade);
        } else {
            dropTable(session, table, cascade);
        }
    }

    void dropTable(Session session, Table table, boolean cascade) {

        Schema schema    = (Schema) schemaMap.get(table.getSchemaName().name);
        int    dropIndex = schema.tableList.getIndex(table.getName().name);
        OrderedHashSet externalConstraints =
            table.getDependentExternalConstraints();
        OrderedHashSet externalReferences = new OrderedHashSet();

        getCascadingReferences(table.getName(), externalReferences);

        if (!cascade) {
            for (int i = 0; i < externalConstraints.size(); i++) {
                Constraint c         = (Constraint) externalConstraints.get(i);
                HsqlName   tablename = c.getRef().getName();
                HsqlName   refname   = c.getRefName();

                if (c.getConstraintType() == Constraint.MAIN) {
                    throw Error.error(ErrorCode.X_42533,
                                      refname.schema.name + '.'
                                      + tablename.name + '.' + refname.name);
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

        TableWorks tw = new TableWorks(session, table);

        tableSet = tw.makeNewTables(tableSet, constraintNameSet, indexNameSet);

        tw.setNewTablesInSchema(tableSet);
        tw.updateConstraints(tableSet, constraintNameSet);
        removeSchemaObjects(externalReferences);
        removeReferencedObject(table.getName());
        schema.tableList.remove(dropIndex);
        database.getGranteeManager().removeDbObject(table.getName());
        schema.triggerLookup.removeParent(table.tableName);
        schema.indexLookup.removeParent(table.tableName);
        schema.constraintLookup.removeParent(table.tableName);
        table.releaseTriggers();
        database.persistentStoreCollection.releaseStore(table);
        recompileDependentObjects(tableSet);
    }

    void setTable(int index, Table table) {

        Schema schema = (Schema) schemaMap.get(table.getSchemaName().name);

        schema.tableList.set(index, table.getName().name, table);
    }

    /**
     *  Returns index of a table or view in the HashMappedList that
     *  contains the table objects for this Database.
     *
     * @param  table the Table object
     * @return  the index of the specified table or view, or -1 if not found
     */
    int getTableIndex(Table table) {

        Schema schema = (Schema) schemaMap.get(table.getSchemaName().name);

        if (schema == null) {
            return -1;
        }

        HsqlName name = table.getName();

        return schema.tableList.getIndex(name.name);
    }

    void recompileDependentObjects(OrderedHashSet tableSet) {

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < tableSet.size(); i++) {
            Table table = (Table) tableSet.get(i);

            set.addAll(getReferencingObjects(table.getName()));
        }

        Session session = database.sessionManager.getSysSession();

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);

            switch (name.type) {

                case SchemaObject.VIEW :
                case SchemaObject.CONSTRAINT :
                case SchemaObject.ASSERTION :
                    SchemaObject object = getSchemaObject(name);

                    object.compile(session);
                    break;
            }
        }
    }

    /**
     * After addition or removal of columns and indexes all views that
     * reference the table should be recompiled.
     */
    void recompileDependentObjects(Table table) {

        OrderedHashSet set     = getReferencingObjects(table.getName());
        Session        session = database.sessionManager.getSysSession();

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);

            switch (name.type) {

                case SchemaObject.VIEW :
                case SchemaObject.CONSTRAINT :
                case SchemaObject.ASSERTION :
                    SchemaObject object = getSchemaObject(name);

                    object.compile(session);
                    break;
            }
        }

        HsqlArrayList list = getAllTables();

        for (int i = 0; i < list.size(); i++) {
            Table t = (Table) list.get(i);

            t.updateConstraintPath();
        }
    }

    NumberSequence getSequence(String name, String schemaName, boolean raise) {

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
    }

    public Type getUserDefinedType(String name, String schemaName,
                                   boolean raise) {

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
    }

    public Type getDomain(String name, String schemaName, boolean raise) {

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
    }

    public Type getDistinctType(String name, String schemaName,
                                boolean raise) {

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
    }

    public SchemaObject getSchemaObject(String name, String schemaName,
                                        int type) {

        SchemaObject object = findSchemaObject(name, schemaName, type);

        if (object == null) {
            throw Error.error(SchemaObjectSet.getGetErrorCode(type), name);
        }

        return object;
    }

    public SchemaObject findSchemaObject(String name, String schemaName,
                                         int type) {

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
                return schema.sequenceLookup.getObject(name);

            case SchemaObject.CHARSET :
                if (name.equals("SQL_IDENTIFIER")) {
                    return SqlInvariants.SQL_IDENTIFIER_CHARSET;
                }

                if (name.equals("SQL_TEXT")) {
                    return SqlInvariants.SQL_TEXT;
                }

                if (name.equals("LATIN1")) {
                    return SqlInvariants.LATIN1;
                }

                if (name.equals("ASCII_GRAPHIC")) {
                    return SqlInvariants.ASCII_GRAPHIC;
                }

                return schema.charsetLookup.getObject(name);

            case SchemaObject.COLLATION :
                return schema.collationLookup.getObject(name);

            case SchemaObject.PROCEDURE :
                return schema.procedureLookup.getObject(name);

            case SchemaObject.FUNCTION :
                return schema.functionLookup.getObject(name);

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return schema.typeLookup.getObject(name);

            case SchemaObject.INDEX :
                set        = schema.indexLookup;
                objectName = set.getName(name);

                if (objectName == null) {
                    return null;
                }

                table = (Table) schema.tableList.get(objectName.parent.name);

                return table.getIndex(name);

            case SchemaObject.CONSTRAINT :
                set        = schema.constraintLookup;
                objectName = set.getName(name);

                if (objectName == null) {
                    return null;
                }

                table = (Table) schema.tableList.get(objectName.parent.name);

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

                table = (Table) schema.tableList.get(objectName.parent.name);

                return table.getTrigger(name);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SchemaManager");
        }
    }

    // INDEX management

    /**
     * Returns the table that has an index with the given name and schema.
     */
    Table findUserTableForIndex(Session session, String name,
                                String schemaName) {

        Schema   schema    = (Schema) schemaMap.get(schemaName);
        HsqlName indexName = schema.indexLookup.getName(name);

        if (indexName == null) {
            return null;
        }

        return findUserTable(session, indexName.parent.name, schemaName);
    }

    /**
     * Drops the index with the specified name.
     */
    void dropIndex(Session session, HsqlName name) {

        Table t = getTable(session, name.parent.name, name.parent.schema.name);
        TableWorks tw = new TableWorks(session, t);

        tw.dropIndex(name.name);
    }

    /**
     * Drops the index with the specified name.
     */
    void dropConstraint(Session session, HsqlName name, boolean cascade) {

        Table t = getTable(session, name.parent.name, name.parent.schema.name);
        TableWorks tw = new TableWorks(session, t);

        tw.dropConstraint(name.name, cascade);
    }

    void removeDependentObjects(HsqlName name) {

        Schema schema = (Schema) schemaMap.get(name.schema.name);

        schema.indexLookup.removeParent(name);
        schema.constraintLookup.removeParent(name);
        schema.triggerLookup.removeParent(name);
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

        // toDrop.schema may be null because it is not registerd
        Schema schema = (Schema) schemaMap.get(toDrop.getSchemaName().name);

        for (int i = 0; i < schema.tableList.size(); i++) {
            Table table = (Table) schema.tableList.get(i);

            for (int j = table.constraintList.length - 1; j >= 0; j--) {
                Table refTable = table.constraintList[j].getRef();

                if (toDrop == refTable) {
                    table.removeConstraint(j);
                }
            }
        }
    }

    public Iterator databaseObjectIterator(String schemaName, int type) {

        Schema schema = (Schema) schemaMap.get(schemaName);

        return schema.schemaObjectIterator(type);
    }

    public Iterator databaseObjectIterator(int type) {

        Iterator it      = schemaMap.values().iterator();
        Iterator objects = new WrapperIterator();

        while (it.hasNext()) {
            Schema temp = (Schema) it.next();

            objects = new WrapperIterator(objects,
                                          temp.schemaObjectIterator(type));
        }

        return objects;
    }

    // references
    private void addReferences(SchemaObject object) {

        OrderedHashSet set = object.getReferences();

        if (set == null) {
            return;
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = (HsqlName) set.get(i);

            if (referenced.type == SchemaObject.COLUMN) {
                referenceMap.put(referenced.parent, object.getName());
            } else {
                referenceMap.put(referenced, object.getName());
            }
        }
    }

    private void removeReferencedObject(HsqlName referenced) {
        referenceMap.remove(referenced);
    }

    private void removeReferencingObject(SchemaObject object) {

        OrderedHashSet set = object.getReferences();

        if (set == null) {
            return;
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName referenced = (HsqlName) set.get(i);

            referenceMap.remove(referenced, object.getName());
        }
    }

    OrderedHashSet getReferencingObjects(HsqlName object) {

        OrderedHashSet set = new OrderedHashSet();
        Iterator       it  = referenceMap.get(object);

        while (it.hasNext()) {
            HsqlName name = (HsqlName) it.next();

            set.add(name);
        }

        return set;
    }

    OrderedHashSet getReferencingObjects(HsqlName table, HsqlName column) {

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

        return set;
    }

    private boolean isReferenced(HsqlName object) {
        return referenceMap.containsKey(object);
    }

    //
    private void getCascadingReferences(HsqlName object, OrderedHashSet set) {

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

            getCascadingReferences(name, set);
        }
    }

    //
    private void getCascadingSchemaReferences(HsqlName schema,
            OrderedHashSet set) {

        Iterator mainIterator = referenceMap.keySet().iterator();

        while (mainIterator.hasNext()) {
            HsqlName name = (HsqlName) mainIterator.next();

            if (name.schema != schema) {
                continue;
            }

            getCascadingReferences(name, set);
        }

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);

            if (name.schema == schema) {
                set.remove(i);

                i--;
            }
        }
    }

    //
    HsqlName getSchemaObjectName(HsqlName schemaName, String name, int type,
                                 boolean raise) {

        Schema          schema = (Schema) schemaMap.get(schemaName.name);
        SchemaObjectSet set    = null;

        if (schema == null) {
            if (raise) {
                throw Error.error(SchemaObjectSet.getGetErrorCode(type));
            } else {
                return null;
            }
        }

        set = getSchemaObjectSet(schema, type);

        if (raise) {
            set.checkExists(name);
        }

        return set.getName(name);
    }

    SchemaObject getSchemaObject(HsqlName name) {

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

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return schema.typeLookup.getObject(name.name);

            case SchemaObject.TRIGGER : {
                name = schema.triggerLookup.getName(name.name);

                if (name == null) {
                    return null;
                }

                HsqlName tableName = name.parent;
                Table    table = (Table) schema.tableList.get(tableName.name);

                return table.getTrigger(name.name);
            }
            case SchemaObject.CONSTRAINT : {
                name = schema.constraintLookup.getName(name.name);

                if (name == null) {
                    return null;
                }

                HsqlName tableName = name.parent;
                Table    table = (Table) schema.tableList.get(tableName.name);

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
                Table    table = (Table) schema.tableList.get(tableName.name);

                return table.getIndex(name.name);
        }

        return null;
    }

    void checkColumnIsReferenced(HsqlName tableName, HsqlName name) {

        OrderedHashSet set = getReferencingObjects(tableName, name);

        if (!set.isEmpty()) {
            HsqlName objectName = (HsqlName) set.get(0);

            throw Error.error(ErrorCode.X_42502,
                              objectName.getSchemaQualifiedStatementName());
        }
    }

    void checkObjectIsReferenced(HsqlName name) {

        OrderedHashSet set     = getReferencingObjects(name);
        HsqlName       refName = null;

        for (int i = 0; i < set.size(); i++) {
            refName = (HsqlName) set.get(i);

            if (refName.parent != name) {
                break;
            }

            refName = null;
        }

        if (refName == null) {
            return;
        }

        throw Error.error(ErrorCode.X_42502,
                          refName.getSchemaQualifiedStatementName());
    }

    void addSchemaObject(SchemaObject object) {

        HsqlName        name   = object.getName();
        Schema          schema = (Schema) schemaMap.get(name.schema.name);
        SchemaObjectSet set    = getSchemaObjectSet(schema, name.type);

        switch (name.type) {

            case SchemaObject.PROCEDURE :
            case SchemaObject.FUNCTION :
                RoutineSchema routine =
                    (RoutineSchema) set.getObject(name.name);

                if (routine == null) {
                    routine = new RoutineSchema(name.type, name);

                    routine.addSpecificRoutine(database, (Routine) object);
                    set.add(routine);
                } else {
                    ((Routine) object).setName(routine.getName());
                    routine.addSpecificRoutine(database, (Routine) object);
                }

                addReferences(object);

                return;
        }

        set.add(object);
        addReferences(object);
    }

    void removeSchemaObject(HsqlName name, boolean cascade) {

        OrderedHashSet objectSet = new OrderedHashSet();

        switch (name.type) {

            case SchemaObject.SEQUENCE :
            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
            case SchemaObject.TYPE :
            case SchemaObject.CHARSET :
            case SchemaObject.COLLATION :
            case SchemaObject.PROCEDURE :
            case SchemaObject.FUNCTION :
                getCascadingReferences(name, objectSet);
                break;

            case SchemaObject.DOMAIN :
                break;
        }

        if (objectSet.isEmpty()) {
            removeSchemaObject(name);

            return;
        }

        if (!cascade) {
            HsqlName objectName = (HsqlName) objectSet.get(0);

            throw Error.error(ErrorCode.X_42502,
                              objectName.getSchemaQualifiedStatementName());
        }

        objectSet.add(name);
        removeSchemaObjects(objectSet);
    }

    void removeSchemaObjects(OrderedHashSet set) {

        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);

            removeSchemaObject(name);
        }
    }

    void removeSchemaObject(HsqlName name) {

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

                set.remove(name.name);

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

            case SchemaObject.PROCEDURE :
                set    = schema.procedureLookup;
                object = set.getObject(name.name);
                break;

            case SchemaObject.FUNCTION :
                set    = schema.functionLookup;
                object = set.getObject(name.name);
                break;

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
                    Type type =
                        (Type) schema.typeLookup.getObject(name.parent.name);

                    object = type.userTypeModifier.getConstraint(name.name);

                    type.userTypeModifier.removeConstraint(name.name);
                }

                break;
            }
            case SchemaObject.TRIGGER : {
                set = schema.triggerLookup;

                Table table = (Table) schema.tableList.get(name.parent.name);

                object = table.getTrigger(name.name);

                table.removeTrigger(name.name);

                break;
            }
        }

        if (object != null) {
            database.getGranteeManager().removeDbObject(object.getName());
            removeReferencingObject(object);
        }

        set.remove(name.name);
        removeReferencedObject(name);
    }

    void renameSchemaObject(HsqlName name, HsqlName newName) {

        if (name.schema != newName.schema) {
            throw Error.error(ErrorCode.X_42505, newName.schema.name);
        }

        checkObjectIsReferenced(name);

        Schema          schema = (Schema) schemaMap.get(name.schema.name);
        SchemaObjectSet set    = getSchemaObjectSet(schema, name.type);

        set.rename(name, newName);
    }

    public String[] getSQLArray() {

        OrderedHashSet resolved   = new OrderedHashSet();
        OrderedHashSet unresolved = new OrderedHashSet();
        HsqlArrayList  list       = new HsqlArrayList();
        Iterator       schemas    = schemaMap.values().iterator();

        while (schemas.hasNext()) {
            Schema schema = (Schema) schemas.next();

            if (isSystemSchema(schema.name.name)) {
                continue;
            }

            if (isLobsSchema(schema.name.name)) {
                continue;
            }

            list.addAll(schema.getSQLArray(resolved, unresolved));
        }

        while (true) {
            Iterator it = unresolved.iterator();

            if (!it.hasNext()) {
                break;
            }

            while (it.hasNext()) {
                SchemaObject   object     = (SchemaObject) it.next();
                OrderedHashSet references = object.getReferences();
                boolean        isResolved = true;

                for (int j = 0; j < references.size(); j++) {
                    HsqlName name = (HsqlName) references.get(j);

                    if (name.type == SchemaObject.COLUMN
                            || name.type == SchemaObject.CONSTRAINT) {
                        name = name.parent;
                    }

                    if (!resolved.contains(name)) {
                        isResolved = false;

                        break;
                    }
                }

                if (isResolved) {
                    if (object.getType() == SchemaObject.TABLE) {
                        list.addAll(((Table) object).getSQL(resolved,
                                                            unresolved));
                    } else {
                        list.add(object.getSQL());
                        resolved.add(object.getName());
                    }

                    it.remove();
                }
            }
        }

        schemas = schemaMap.values().iterator();

        while (schemas.hasNext()) {
            Schema schema = (Schema) schemas.next();

            if (database.schemaManager.isSystemSchema(schema.name.name)) {
                continue;
            }

            if (database.schemaManager.isLobsSchema(schema.name.name)) {

//                continue;
            }

            list.addAll(schema.getTriggerSQL());
            list.addAll(schema.getSequenceRestartSQL());
        }

        if (defaultSchemaHsqlName != null) {
            StringBuffer sb = new StringBuffer();

            sb.append(Tokens.T_SET).append(' ').append(Tokens.T_DATABASE);
            sb.append(' ').append(Tokens.T_DEFAULT).append(' ');
            sb.append(Tokens.T_INITIAL).append(' ').append(Tokens.T_SCHEMA);
            sb.append(' ').append(defaultSchemaHsqlName.statementName);
            list.add(sb.toString());
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public String[] getIndexRootsSQL() {

        Session       sysSession = database.sessionManager.getSysSession();
        HsqlArrayList tableList  = getAllTables();
        HsqlArrayList list       = new HsqlArrayList();

        for (int i = 0, tSize = tableList.size(); i < tSize; i++) {
            Table t = (Table) tableList.get(i);

            if (t.isIndexCached() && !t.isEmpty(sysSession)) {
                String ddl = ((Table) tableList.get(i)).getIndexRootsSQL();

                if (ddl != null) {
                    list.add(ddl);
                }
            }
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public void setDefaultTableType(int type) {
        defaultTableType = type;
    }

    int getDefaultTableType() {
        return defaultTableType;
    }

    /************************* Volt DB Extensions *************************/

    /**
     * If schemaName is null, return the default schema name, else return
     * the HsqlName object for the schema. If schemaName does not exist,
     * return the defaultName provided.
     * Not throwing the usual exception saves some throw-then-catch nonsense
     * in the usual session setup.
     */
    public HsqlName getSchemaHsqlNameNoThrow(String name, HsqlName defaultName) {

        if (name == null) {
            return defaultSchemaHsqlName;
        }

        if (SqlInvariants.INFORMATION_SCHEMA.equals(name)) {
            return SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
        }

        Schema schema = ((Schema) schemaMap.get(name));

        if (schema == null) {
            return defaultName;
        }
        return schema.name;
    }

    /**********************************************************************/
}
