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


package org.hsqldb_voltpatches.dbinfo;

import java.lang.reflect.Method;

import org.hsqldb_voltpatches.ColumnSchema;
import org.hsqldb_voltpatches.Constraint;
import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SqlInvariants;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.WrapperIterator;
import org.hsqldb_voltpatches.persist.HsqlProperties;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rights.GrantConstants;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.rights.GranteeManager;
import org.hsqldb_voltpatches.rights.Right;
import org.hsqldb_voltpatches.rights.User;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;

/* $Id: DatabaseInformationMain.java 3003 2009-06-09 23:42:23Z fredt $ */

// fredt@users - 1.7.2 - structural modifications to allow inheritance
// boucherb@users - 1.7.2 - 20020225
// - factored out all reusable code into DIXXX support classes
// - completed Fred's work on allowing inheritance
// boucherb@users - 1.7.2 - 20020304 - bug fixes, refinements, better java docs
// fredt@users - 1.8.0 - updated to report latest enhancements and changes
// boucherb@users 20051207 - patch 1.8.x initial JDBC 4.0 support work
// Revision 1.9  2006/07/12 11:36:59  boucherb
// - JDBC 4.0, Mustang b87: support for new DatabaseMetaData.getColumns() IS_AUTOINCREMENT result column
// - minor javadoc and code comment updates

/**
 * Provides definitions for a few of the SQL Standard Schemata views that are
 * supported by HSQLDB.<p>
 *
 * Provides definitions for some of HSQLDB's additional system view.
 *
 * The views supported in this class are exclusively those that are needed
 * to build the ResultSet objects that are returned by JDBC DatabaseMetaData
 * calls.<p>
 *
 * The definitions for the rest of SQL standard and HSQLDB specific system views
 * are provided by DatabaseInformationFull, which extends this class. <p>
 *
 * Produces a collection of views that form the system data dictionary. <p>
 *
 * Implementations use a group of arrays of equal size to store various
 * attributes or cached instances of system tables.<p>
 *
 * Two fixed static lists of reserved table names are kept in String[] and
 * HsqlName[] forms. These are shared by all implementations of
 * DatabaseInformtion.<p>
 *
 * Each implementation keeps a lookup set of names for those tables whose
 * contents are never cached (nonCachedTablesSet). <p>
 *
 * An instance of this class uses three lists named sysTablexxxx for caching
 * system tables.<p>
 *
 * sysTableSessionDependent indicates which tables contain data that is
 * dependent on the user rights of the User associatiod with the Session.<p>
 *
 * sysTableSessions contains the Session with whose rights each cached table
 * was built.<p>
 *
 * sysTables contains the cached tables.<p>
 *
 * At the time of instantiation, which is part of the Database.open() method
 * invocation, an empty table is created and placed in sysTables with calls to
 * generateTable(int) for each name in sysTableNames. Some of these
 * table entries may be null if an implementation does not produce them.<p>
 *
 * Calls to getSystemTable(String, Session) return a cached table if various
 * caching rules are met (see below), or it will delete all rows of the table
 * and rebuild the contents via generateTable(int).<p>
 *
 * generateTable(int) calls the appropriate single method for each table.
 * These methods either build and return an empty table (if sysTables
 * contains null for the table slot) or populate the table with up-to-date
 * rows. <p>
 *
 * When the setDirty() call is made externally, the internal isDirty flag
 * is set. This flag is used next time a call to
 * getSystemTable(String, Session) is made. <p>
 *
 * Rules for caching are applied as follows: <p>
 *
 * When a call to getSystemTable(String, Session) is made, if the isDirty flag
 * is true, then the contents of all cached tables are cleared and the
 * sysTableUsers slot for all tables is set to null. This also has the
 * effect of clearing the isDirty and isDirtyNextIdentity flags<p>
 *
 * if the isDirtyNextIdentity flag is true at this point, then the contents
 * of all next identity value dependent cached tables are cleared and the
 * sysTableUsers slot for these tables are set to null.  Currently,
 * the only member of this set is the SYSTEM_TABLES system table.
 *
 * If a table has non-cached contents, its contents are cleared and
 * rebuilt. <p>
 *
 * For the rest of the tables, if the sysTableSessions slot is null or if the
 * Session parameter is not the same as the Session object
 * in that slot, the table contents are cleared and rebuilt. <p>
 *
 * (fredt@users) <p>
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
class DatabaseInformationMain extends DatabaseInformation {

    static Type CARDINAL_NUMBER = SqlInvariants.CARDINAL_NUMBER;
    static Type YES_OR_NO       = SqlInvariants.YES_OR_NO;
    static Type CHARACTER_DATA  = SqlInvariants.CHARACTER_DATA;
    static Type SQL_IDENTIFIER  = SqlInvariants.SQL_IDENTIFIER;
    static Type TIME_STAMP      = SqlInvariants.TIME_STAMP;

    /** The HsqlNames of the system tables. */
    protected static final HsqlName[] sysTableHsqlNames;

    /** Current user for each cached system table */
    protected final HsqlName[] sysTableSessions =
        new HsqlName[sysTableNames.length];

    /** true if the contents of a cached system table depends on the session */
    protected static final boolean[] sysTableSessionDependent =
        new boolean[sysTableNames.length];

    /** cache of system tables */
    protected final Table[] sysTables = new Table[sysTableNames.length];

    /** Set: { names of system tables that are not to be cached } */
    protected static final HashSet nonCachedTablesSet;

    /**
     * The <code>Session</code> object under consideration in the current
     * executution context.
     */
    protected Session session;

    /** The table types HSQLDB supports. */
    protected static final String[] tableTypes = new String[] {
        "GLOBAL TEMPORARY", "SYSTEM TABLE", "TABLE", "VIEW"
    };

    /** Provides naming support. */
    protected DINameSpace ns;

    /** Provides SQL function/procedure reporting support. */
    protected DIProcedureInfo pi;

    static {
        nonCachedTablesSet = new HashSet();
        sysTableHsqlNames  = new HsqlName[sysTableNames.length];

        for (int i = 0; i < sysTableNames.length; i++) {
            sysTableHsqlNames[i] =
                HsqlNameManager.newInfoSchemaTableName(sysTableNames[i]);
            sysTableHsqlNames[i].schema =
                SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
            sysTableSessionDependent[i] = true;
        }

        // build the set of non-cached tables
        nonCachedTablesSet.add("SYSTEM_CACHEINFO");
        nonCachedTablesSet.add("SYSTEM_SESSIONINFO");
        nonCachedTablesSet.add("SYSTEM_SESSIONS");
        nonCachedTablesSet.add("SYSTEM_PROPERTIES");
        nonCachedTablesSet.add("SYSTEM_SEQUENCES");
    }

    /**
     * Constructs a table producer which provides system tables
     * for the specified <code>Database</code> object. <p>
     *
     * <b>Note:</b> before 1.7.2 Alpha N, it was important to observe that
     * by specifying an instance of this class or one of its descendents to
     * handle system table production, the new set of builtin permissions
     * and aliases would overwrite those of an existing database, meaning that
     * metadata reporting might have been rendered less secure if the same
     * database were then opened again using a lower numbered system table
     * producer instance (i.e. one in a 1.7.1 or earlier distribution).
     * As of 1.7.2 Alpha N, system-generated permissions and aliases are no
     * longer recorded in the checkpoint script, obseleting this issue.
     * Checkpointing of system-generated grants and aliases was removed
     * because their existence is very close to a core requirment for correct
     * operation and they are reintroduced to the system at each startup.
     * In a future release, it may even be an exception condition to attempt
     * to remove or alter system-generated grants and aliases,
     * respectvely. <p>
     *
     * @param db the <code>Database</code> object for which this object
     *      produces system tables
     */
    DatabaseInformationMain(Database db) {

        super(db);

        init();
    }

    protected final void addColumn(Table t, String name, Type type) {

        HsqlName     cn;
        ColumnSchema c;

        cn = database.nameManager.newInfoSchemaColumnName(name, t.getName());
        c  = new ColumnSchema(cn, type, true, false, null);

        t.addColumn(c);
    }

    /**
     * Retrieves an enumeration over all of the tables in this database.
     * This means all user tables, views, system tables, system views,
     * including temporary and text tables. <p>
     *
     * @return an enumeration over all of the tables in this database
     */
    protected final Iterator allTables() {

        return new WrapperIterator(
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE),
            new WrapperIterator(sysTables, true));
    }

    /**
     * Clears the contents of cached system tables and resets user slots
     * to null. <p>
     *
     */
    protected final void cacheClear() {

        int i = sysTables.length;

        while (i-- > 0) {
            Table t = sysTables[i];

            if (t != null) {
                t.clearAllData(session);
            }

            sysTableSessions[i] = null;
        }

        isDirty = false;
    }

    /**
     * Retrieves the system table corresponding to the specified
     * tableIndex value. <p>
     *
     * @param tableIndex int value identifying the system table to generate
     * @return the system table corresponding to the specified tableIndex value
     */
    protected Table generateTable(int tableIndex) {

        Table t = sysTables[tableIndex];

//        Please note that this class produces non-null tables for
//        just those absolutely essential to the JDBC 1 spec and the
//        HSQLDB core.  Also, all table producing methods except
//        SYSTEM_PROCEDURES() and SYSTEM_PROCEDURECOLUMNS() are declared final;
//        this class produces only an empty table for each, as per previous
//        DatabaseInformation implementations, whereas
//        DatabaseInformationFull produces comprehensive content for
//        them).
//
//        This break down of inheritance allows DatabaseInformation and
//        DatabaseInformationMain (this class) to be made as small as possible
//        while still meeting their mandates:
//
//        1.) DatabaseInformation prevents use of reserved system table names
//            for user tables and views, meaning that even under highly
//            constrained use cases where the notion of DatabaseMetaData can
//            be discarded (i.e. the engine operates in a distribution where
//            DatabaseInforationMain/Full and JDBCDatabaseMetaData have been
//            dropped from the JAR), it is still impossible to produce a
//            database which will be incompatible in terms of system table <=>
//            user table name clashes, if/when imported into a more
//            capable operating environment.
//
//        2.) DatabaseInformationMain builds on DatabaseInformation, providing
//            at minimum what is needed for comprehensive operation under
//            JDK 1.1/JDBC 1 and provides, at minimum, what was provided under
//            earlier implementations.
//
//        3.) descendents of DatabaseInformationMain (such as the current
//            DatabaseInformationFull) need not (and indeed: now cannot)
//            override most of the DatabaseInformationMain table producing
//            methods, as for the most part they are expected to be already
//            fully comprehensive, security aware and accessible to all users.
        switch (tableIndex) {

            case SYSTEM_ALLTYPEINFO :
                return SYSTEM_ALLTYPEINFO();

            case SYSTEM_BESTROWIDENTIFIER :
                return SYSTEM_BESTROWIDENTIFIER();

            case SYSTEM_COLUMNS :
                return SYSTEM_COLUMNS();

            case SYSTEM_CROSSREFERENCE :
                return SYSTEM_CROSSREFERENCE();

            case SYSTEM_INDEXINFO :
                return SYSTEM_INDEXINFO();

            case SYSTEM_PRIMARYKEYS :
                return SYSTEM_PRIMARYKEYS();

            case SYSTEM_PROCEDURECOLUMNS :
                return SYSTEM_PROCEDURECOLUMNS();

            case SYSTEM_PROCEDURES :
                return SYSTEM_PROCEDURES();

            case SYSTEM_SCHEMAS :
                return SYSTEM_SCHEMAS();

            case SYSTEM_TABLES :
                return SYSTEM_TABLES();

            case SYSTEM_TABLETYPES :
                return SYSTEM_TABLETYPES();

            case SYSTEM_TYPEINFO :
                return SYSTEM_TYPEINFO();

            case SYSTEM_USERS :
                return SYSTEM_USERS();

            case SYSTEM_SEQUENCES :
                return SYSTEM_SEQUENCES();

            case COLUMN_PRIVILEGES :
                return COLUMN_PRIVILEGES();

            case SEQUENCES :
                return SEQUENCES();

            case TABLE_PRIVILEGES :
                return TABLE_PRIVILEGES();

            case INFORMATION_SCHEMA_CATALOG_NAME :
                return INFORMATION_SCHEMA_CATALOG_NAME();

            default :
                return null;
        }
    }

    /**
     * One time initialisation of instance attributes
     * at construction time. <p>
     *
     */
    protected final void init() {

        ns = new DINameSpace(database);
        pi = new DIProcedureInfo(ns);

        // flag the Session-dependent cached tables
        Table t;

        for (int i = 0; i < sysTables.length; i++) {
            t = sysTables[i] = generateTable(i);

            if (t != null) {
                t.setDataReadOnly(true);
            }
        }

        GranteeManager gm    = database.getGranteeManager();
        Right          right = new Right();

        right.set(GrantConstants.SELECT, null);

        for (int i = 0; i < sysTableHsqlNames.length; i++) {
            if (sysTables[i] != null) {
                gm.grantSystemToPublic(sysTables[i], right);
            }
        }

        right = Right.fullRights;

        gm.grantSystemToPublic(SqlInvariants.YES_OR_NO, right);
        gm.grantSystemToPublic(SqlInvariants.TIME_STAMP, right);
        gm.grantSystemToPublic(SqlInvariants.CARDINAL_NUMBER, right);
        gm.grantSystemToPublic(SqlInvariants.CHARACTER_DATA, right);
        gm.grantSystemToPublic(SqlInvariants.SQL_CHARACTER, right);
        gm.grantSystemToPublic(SqlInvariants.SQL_IDENTIFIER_CHARSET, right);
        gm.grantSystemToPublic(SqlInvariants.SQL_IDENTIFIER, right);
        gm.grantSystemToPublic(SqlInvariants.SQL_TEXT, right);
    }

    /**
     * Retrieves whether any form of SQL access is allowed against the
     * the specified table w.r.t the database access rights
     * assigned to current Session object's User. <p>
     *
     * @return true if the table is accessible, else false
     * @param table the table for which to check accessibility
     */
    protected final boolean isAccessibleTable(Table table) {
        return session.getGrantee().isAccessible(table);
    }

    /**
     * Creates a new primoidal system table with the specified name. <p>
     *
     * @return a new system table
     * @param name of the table
     */
    protected final Table createBlankTable(HsqlName name) {

        Table table = new Table(database, name, TableBase.SYSTEM_TABLE);

        return table;
    }

    /**
     * Retrieves the system <code>Table</code> object corresponding to
     * the given <code>name</code> and <code>session</code> arguments. <p>
     *
     * @param session the Session object requesting the table
     * @param name a String identifying the desired table
     *      database access error occurs
     * @return a system table corresponding to the <code>name</code> and
     *      <code>session</code> arguments
     */
    public final Table getSystemTable(Session session, String name) {

        Table t;
        int   tableIndex;

        // must come first...many methods depend on this being set properly
        this.session = session;

        if (!isSystemTable(name)) {
            return null;
        }

        tableIndex = getSysTableID(name);
        t          = sysTables[tableIndex];

        // fredt - any system table that is not supported will be null here
        if (t == null) {
            return t;
        }

        // At the time of opening the database, no content is needed
        // at present.  However, table structure is required at this
        // point to allow processing logged View defn's against system
        // tables.  Returning tables without content speeds the database
        // open phase under such cases.
        if (!withContent) {
            return t;
        }

        if (isDirty) {
            cacheClear();
        }

        HsqlName oldUser    = sysTableSessions[tableIndex];
        boolean  tableValid = oldUser != null;

        // user has changed and table is user-dependent
        if (session.getGrantee().getName() != oldUser
                && sysTableSessionDependent[tableIndex]) {
            tableValid = false;
        }

        if (nonCachedTablesSet.contains(name)) {
            tableValid = false;
        }

        // any valid cached table will be returned here
        if (tableValid) {
            return t;
        }

        // fredt - clear the contents of table and set new User
        t.clearAllData(session);

        sysTableSessions[tableIndex] = session.getGrantee().getName();

        // match and if found, generate.
        t = generateTable(tableIndex);

        // t will be null at this point if the implementation
        // does not support the particular table.
        //
        // send back what we found or generated
        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the optimal
     * set of visible columns that uniquely identifies a row
     * for each accessible table defined within this database. <p>
     *
     * Each row describes a single column of the best row indentifier column
     * set for a particular table.  Each row has the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * SCOPE          SMALLINT  scope of applicability
     * COLUMN_NAME    VARCHAR   simple name of the column
     * DATA_TYPE      SMALLINT  SQL data type from Types
     * TYPE_NAME      VARCHAR   canonical type name
     * COLUMN_SIZE    INTEGER   precision
     * BUFFER_LENGTH  INTEGER   transfer size in bytes, if definitely known
     * DECIMAL_DIGITS SMALLINT  scale  - fixed # of decimal digits
     * PSEUDO_COLUMN  SMALLINT  is this a pseudo column like an Oracle ROWID?
     * TABLE_CAT      VARCHAR   table catalog
     * TABLE_SCHEM    VARCHAR   simple name of table schema
     * TABLE_NAME     VARCHAR   simple table name
     * NULLABLE       SMALLINT  is column nullable?
     * IN_KEY         BOOLEAN   column belongs to a primary or alternate key?
     * </pre> <p>
     *
     * <b>Notes:</b><p>
     *
     * <code>JDBCDatabaseMetaData.getBestRowIdentifier</code> uses its
     * nullable parameter to filter the rows of this table in the following
     * manner: <p>
     *
     * If the nullable parameter is <code>false</code>, then rows are reported
     * only if, in addition to satisfying the other specified filter values,
     * the IN_KEY column value is TRUE. If the nullable parameter is
     * <code>true</code>, then the IN_KEY column value is ignored. <p>
     *
     * There is not yet infrastructure in place to make some of the ranking
     * descisions described below, and it is anticipated that mechanisms
     * upon which cost descisions could be based will change significantly over
     * the next few releases.  Hence, in the interest of simplicity and of not
     * making overly complex dependency on features that will almost certainly
     * change significantly in the near future, the current implementation,
     * while perfectly adequate for all but the most demanding or exacting
     * purposes, is actually sub-optimal in the strictest sense. <p>
     *
     * A description of the current implementation follows: <p>
     *
     * <b>DEFINTIONS:</b>  <p>
     *
     * <b>Alternate key</b> <p>
     *
     *  <UL>
     *   <LI> An attribute of a table that, by virtue of its having a set of
     *        columns that are both the full set of columns participating in a
     *        unique constraint or index and are all not null, yeilds the same
     *        selectability characteristic that would obtained by declaring a
     *        primary key on those same columns.
     *  </UL> <p>
     *
     * <b>Column set performance ranking</b> <p>
     *
     *  <UL>
     *  <LI> The ranking of the expected average performance w.r.t a subset of
     *       a table's columns used to select and/or compare rows, as taken in
     *       relation to all other distinct candidate subsets under
     *       consideration. This can be estimated by comparing each cadidate
     *       subset in terms of total column count, relative peformance of
     *       comparisons amongst the domains of the columns and differences
     *       in other costs involved in the execution plans generated using
     *       each subset under consideration for row selection/comparison.
     *  </UL> <p>
     *
     *
     * <b>Rules:</b> <p>
     *
     * Given the above definitions, the rules currently in effect for reporting
     * best row identifier are as follows, in order of precedence: <p>
     *
     * <OL>
     * <LI> if the table under consideration has a primary key contraint, then
     *      the columns of the primary key are reported, with no consideration
     *      given to the column set performance ranking over the set of
     *      candidate keys. Each row has its IN_KEY column set to TRUE.
     *
     * <LI> if 1.) does not hold, then if there exits one or more alternate
     *      keys, then the columns of the alternate key with the lowest column
     *      count are reported, with no consideration given to the column set
     *      performance ranking over the set of candidate keys. If there
     *      exists a tie for lowest column count, then the columns of the
     *      first such key encountered are reported.
     *      Each row has its IN_KEY column set to TRUE.
     *
     * <LI> if both 1.) and 2.) do not hold, then, if possible, a unique
     *      contraint/index is selected from the set of unique
     *      contraints/indices containing at least one column having
     *      a not null constraint, with no consideration given to the
     *      column set performance ranking over the set of all such
     *      candidate column sets. If there exists a tie for lowest non-zero
     *      count of columns having a not null constraint, then the columns
     *      of the first such encountered candidate set are reported. Each
     *      row has its IN_KEY column set to FALSE. <p>
     *
     * <LI> Finally, if the set of candidate column sets in 3.) is the empty,
     *      then no column set is reported for the table under consideration.
     * </OL> <p>
     *
     * The scope reported for a best row identifier column set is determined
     * thus: <p>
     *
     * <OL>
     * <LI> if the database containing the table under consideration is in
     *      read-only mode or the table under consideration is GLOBAL TEMPORARY
     *      (a TEMP or TEMP TEXT table, in HSQLDB parlance), then the scope
     *      is reported as
     *      <code>java.sql.DatabaseMetaData.bestRowSession</code>.
     *
     * <LI> if 1.) does not hold, then the scope is reported as
     *      <code>java.sql.DatabaseMetaData.bestRowTemporary</code>.
     * </OL> <p>
     *
     * @return a <code>Table</code> object describing the optimal
     * set of visible columns that uniquely identifies a row
     * for each accessible table defined within this database
     */
    final Table SYSTEM_BESTROWIDENTIFIER() {

        Table t = sysTables[SYSTEM_BESTROWIDENTIFIER];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_BESTROWIDENTIFIER]);

            addColumn(t, "SCOPE", Type.SQL_SMALLINT);            // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);         // not null
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);        // not null
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);           // not null
            addColumn(t, "COLUMN_SIZE", Type.SQL_INTEGER);
            addColumn(t, "BUFFER_LENGTH", Type.SQL_INTEGER);
            addColumn(t, "DECIMAL_DIGITS", Type.SQL_SMALLINT);
            addColumn(t, "PSEUDO_COLUMN", Type.SQL_SMALLINT);    // not null
            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);          // not null
            addColumn(t, "NULLABLE", Type.SQL_SMALLINT);         // not null
            addColumn(t, "IN_KEY", Type.SQL_BOOLEAN);            // not null

            // order: SCOPE
            // for unique:  TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME
            // false PK, as TABLE_CAT and/or TABLE_SCHEM may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_BESTROWIDENTIFIER].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 8, 9, 10, 1
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        Integer scope;           // { temp, transaction, session }
        Integer pseudo;

        //-------------------------------------------
        // required for restriction of results via
        // DatabaseMetaData filter parameters, but
        // not actually required to be included in
        // DatabaseMetaData.getBestRowIdentifier()
        // result set
        //-------------------------------------------
        String  tableCatalog;    // table calalog
        String  tableSchema;     // table schema
        String  tableName;       // table name
        Boolean inKey;           // column participates in PK or AK?

        //-------------------------------------------

        /**
         * @todo -  Maybe include: - backing index (constraint) name?
         *       - column sequence in index (constraint)?
         */
        //-------------------------------------------
        // Intermediate holders
        Iterator       tables;
        Table          table;
        DITableInfo    ti;
        int[]          cols;
        Object[]       row;
        HsqlProperties p;

        // Column number mappings
        final int iscope          = 0;
        final int icolumn_name    = 1;
        final int idata_type      = 2;
        final int itype_name      = 3;
        final int icolumn_size    = 4;
        final int ibuffer_length  = 5;
        final int idecimal_digits = 6;
        final int ipseudo_column  = 7;
        final int itable_cat      = 8;
        final int itable_schem    = 9;
        final int itable_name     = 10;
        final int inullable       = 11;
        final int iinKey          = 12;

        // Initialization
        ti = new DITableInfo();
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            /** @todo - requires access to the actual columns */
            if (table.isView() || !isAccessibleTable(table)) {
                continue;
            }

            cols = table.getBestRowIdentifiers();

            if (cols == null) {
                continue;
            }

            ti.setTable(table);

            inKey = ValuePool.getBoolean(table.isBestRowIdentifiersStrict());
            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;
            tableName    = table.getName().name;

            Type[] types = table.getColumnTypes();

            scope  = ti.getBRIScope();
            pseudo = ti.getBRIPseudo();

            for (int i = 0; i < cols.length; i++) {
                ColumnSchema column = table.getColumn(i);

                row                  = t.getEmptyRowData();
                row[iscope]          = scope;
                row[icolumn_name]    = column.getName().name;
                row[idata_type] = ValuePool.getInt(types[i].getJDBCTypeCode());
                row[itype_name]      = types[i].getNameString();
                row[icolumn_size]    = types[i].getJDBCPrecision();
                row[ibuffer_length]  = null;
                row[idecimal_digits] = types[i].getJDBCScale();
                row[ipseudo_column]  = pseudo;
                row[itable_cat]      = tableCatalog;
                row[itable_schem]    = tableSchema;
                row[itable_name]     = tableName;
                row[inullable]       = column.getNullability();
                row[iinKey]          = inKey;

                t.insertSys(store, row);
            }
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the
     * visible columns of all accessible tables defined
     * within this database.<p>
     *
     * Each row is a column description with the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TABLE_CAT         VARCHAR   table catalog
     * TABLE_SCHEM       VARCHAR   table schema
     * TABLE_NAME        VARCHAR   table name
     * COLUMN_NAME       VARCHAR   column name
     * DATA_TYPE         SMALLINT  SQL type from DITypes
     * TYPE_NAME         VARCHAR   canonical type name
     * COLUMN_SIZE       INTEGER   column size (length/precision)
     * BUFFER_LENGTH     INTEGER   transfer size in bytes, if definitely known
     * DECIMAL_DIGITS    INTEGER   # of fractional digits (scale)
     * NUM_PREC_RADIX    INTEGER   Radix
     * NULLABLE          INTEGER   is NULL allowed? (from DatabaseMetaData)
     * REMARKS           VARCHAR   comment describing column
     * COLUMN_DEF        VARCHAR   default value (possibly expression) for the
     *                             column, which should be interpreted as a
     *                             string when the value is enclosed in quotes
     *                             (may be <code>null</code>)
     * SQL_DATA_TYPE     VARCHAR   type code as expected in the SQL CLI SQLDA
     * SQL_DATETIME_SUB  INTEGER   the SQL CLI subtype for DATETIME types
     * CHAR_OCTET_LENGTH INTEGER   for char types, max # of bytes in column
     * ORDINAL_POSITION  INTEGER   1-based index of column in table
     * IS_NULLABLE       VARCHAR   is column nullable? ("YES"|"NO"|""}
     * SCOPE_CATLOG      VARCHAR   catalog of REF attribute scope table
     * SCOPE_SCHEMA      VARCHAR   schema of REF attribute scope table
     * SCOPE_TABLE       VARCHAR   name of REF attribute scope table
     * SOURCE_DATA_TYPE  VARCHAR   source type of REF attribute
     * TYPE_SUB          INTEGER   HSQLDB data subtype code
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the
     *        visible columns of all accessible
     *        tables defined within this database.<p>
     */
    final Table SYSTEM_COLUMNS() {

        Table t = sysTables[SYSTEM_COLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_COLUMNS]);

            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);              // 0
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);            // 1
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);             // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);            // not null
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);           // not null
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);              // not null
            addColumn(t, "COLUMN_SIZE", Type.SQL_INTEGER);          // 6
            addColumn(t, "BUFFER_LENGTH", Type.SQL_INTEGER);        // 7
            addColumn(t, "DECIMAL_DIGITS", Type.SQL_INTEGER);       // 8
            addColumn(t, "NUM_PREC_RADIX", Type.SQL_INTEGER);       // 9
            addColumn(t, "NULLABLE", Type.SQL_INTEGER);             // not null
            addColumn(t, "REMARKS", CHARACTER_DATA);                // 11
            addColumn(t, "COLUMN_DEF", CHARACTER_DATA);             // 12
            addColumn(t, "SQL_DATA_TYPE", Type.SQL_INTEGER);        // 13
            addColumn(t, "SQL_DATETIME_SUB", Type.SQL_INTEGER);     // 14
            addColumn(t, "CHAR_OCTET_LENGTH", Type.SQL_INTEGER);    // 15
            addColumn(t, "ORDINAL_POSITION", Type.SQL_INTEGER);     // not null
            addColumn(t, "IS_NULLABLE", YES_OR_NO);                 // not null
            addColumn(t, "SCOPE_CATLOG", SQL_IDENTIFIER);           // 18
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);           // 19
            addColumn(t, "SCOPE_TABLE", SQL_IDENTIFIER);            // 20
            addColumn(t, "SOURCE_DATA_TYPE", SQL_IDENTIFIER);       // 21

            // ----------------------------------------------------------------
            // JDBC 4.0 - added Mustang b86
            // ----------------------------------------------------------------
            addColumn(t, "IS_AUTOINCREMENT", Type.SQL_BOOLEAN);     // not null

            // ----------------------------------------------------------------
            // HSQLDB-specific
            // ----------------------------------------------------------------
            addColumn(t, "TYPE_SUB", Type.SQL_INTEGER);             // not null

            // order: TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
            // added for unique: TABLE_CAT
            // false PK, as TABLE_SCHEM and/or TABLE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_COLUMNS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 16
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        String tableCatalog;
        String tableSchema;
        String tableName;

        // intermediate holders
        int         columnCount;
        Iterator    tables;
        Table       table;
        Object[]    row;
        DITableInfo ti;

        // column number mappings
        final int itable_cat         = 0;
        final int itable_schem       = 1;
        final int itable_name        = 2;
        final int icolumn_name       = 3;
        final int idata_type         = 4;
        final int itype_name         = 5;
        final int icolumn_size       = 6;
        final int ibuffer_length     = 7;
        final int idecimal_digits    = 8;
        final int inum_prec_radix    = 9;
        final int inullable          = 10;
        final int iremark            = 11;
        final int icolumn_def        = 12;
        final int isql_data_type     = 13;
        final int isql_datetime_sub  = 14;
        final int ichar_octet_length = 15;
        final int iordinal_position  = 16;
        final int iis_nullable       = 17;
        final int iscope_cat         = 18;
        final int iscope_schem       = 19;
        final int iscope_table       = 20;
        final int isrc_data_type     = 21;

        // JDBC 4.0
        final int iis_autoinc = 22;

        // HSQLDB-specific
        final int itype_sub = 23;

        // Initialization
        tables = allTables();
        ti     = new DITableInfo();

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            /** @todo - requires access to the actual columns */
            if (!isAccessibleTable(table)) {
                continue;
            }

            ti.setTable(table);

            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;
            tableName    = ti.getName();
            columnCount  = table.getColumnCount();

            Type[] types = table.getColumnTypes();

            for (int i = 0; i < columnCount; i++) {
                ColumnSchema column = table.getColumn(i);

                row = t.getEmptyRowData();

                //
                row[itable_cat]         = tableCatalog;
                row[itable_schem]       = tableSchema;
                row[itable_name]        = tableName;
                row[icolumn_name]       = column.getName().name;
                row[idata_type]         = types[i].getJDBCTypeCode();
                row[itype_name]         = types[i].getNameString();
                row[icolumn_size]       = types[i].getJDBCPrecision();
                row[ibuffer_length]     = null;
                row[idecimal_digits]    = types[i].getJDBCScale();
                row[inum_prec_radix]    = ti.getColPrecRadix(i);
                row[inullable]          = column.getNullability();
                row[iremark]            = ti.getColRemarks(i);
                row[icolumn_def]        = column.getDefaultSQL();
                row[isql_data_type]     = types[i].getJDBCTypeCode();
                row[isql_datetime_sub]  = ti.getColSqlDateTimeSub(i);
                row[ichar_octet_length] = ti.getColCharOctLen(i);
                row[iordinal_position]  = ValuePool.getInt(i + 1);
                row[iis_nullable]       = ti.getColIsNullable(i);

                // JDBC 4.0
                row[iis_autoinc] = ti.getColIsIdentity(i);

                // HSQLDB-specific
                row[itype_sub] = ti.getColDataTypeSub(i);

                t.insertSys(store, row);
            }
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing, for each
     * accessible referencing and referenced table, how the referencing
     * tables import, for the purposes of referential integrity,
     * the columns of the referenced tables.<p>
     *
     * Each row is a foreign key column description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * PKTABLE_CAT   VARCHAR   referenced table catalog
     * PKTABLE_SCHEM VARCHAR   referenced table schema
     * PKTABLE_NAME  VARCHAR   referenced table name
     * PKCOLUMN_NAME VARCHAR   referenced column name
     * FKTABLE_CAT   VARCHAR   referencing table catalog
     * FKTABLE_SCHEM VARCHAR   referencing table schema
     * FKTABLE_NAME  VARCHAR   referencing table name
     * FKCOLUMN_NAME VARCHAR   referencing column
     * KEY_SEQ       SMALLINT  sequence number within foreign key
     * UPDATE_RULE   SMALLINT
     *    { Cascade | Set Null | Set Default | Restrict (No Action)}?
     * DELETE_RULE   SMALLINT
     *    { Cascade | Set Null | Set Default | Restrict (No Action)}?
     * FK_NAME       VARCHAR   foreign key constraint name
     * PK_NAME       VARCHAR   primary key or unique constraint name
     * DEFERRABILITY SMALLINT
     *    { initially deferred | initially immediate | not deferrable }
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing how accessible tables
     *      import other accessible tables' primary key and/or unique
     *      constraint columns
     */
    final Table SYSTEM_CROSSREFERENCE() {

        Table t = sysTables[SYSTEM_CROSSREFERENCE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_CROSSREFERENCE]);

            addColumn(t, "PKTABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "PKTABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "PKTABLE_NAME", SQL_IDENTIFIER);        // not null
            addColumn(t, "PKCOLUMN_NAME", SQL_IDENTIFIER);       // not null
            addColumn(t, "FKTABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "FKTABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "FKTABLE_NAME", SQL_IDENTIFIER);        // not null
            addColumn(t, "FKCOLUMN_NAME", SQL_IDENTIFIER);       // not null
            addColumn(t, "KEY_SEQ", Type.SQL_SMALLINT);          // not null
            addColumn(t, "UPDATE_RULE", Type.SQL_SMALLINT);      // not null
            addColumn(t, "DELETE_RULE", Type.SQL_SMALLINT);      // not null
            addColumn(t, "FK_NAME", SQL_IDENTIFIER);
            addColumn(t, "PK_NAME", SQL_IDENTIFIER);
            addColumn(t, "DEFERRABILITY", Type.SQL_SMALLINT);    // not null

            // order: FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ
            // added for unique: FK_NAME
            // false PK, as FKTABLE_CAT, FKTABLE_SCHEM and/or FK_NAME
            // may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_CROSSREFERENCE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                4, 5, 6, 8, 11
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        String  pkTableCatalog;
        String  pkTableSchema;
        String  pkTableName;
        String  pkColumnName;
        String  fkTableCatalog;
        String  fkTableSchema;
        String  fkTableName;
        String  fkColumnName;
        Integer keySequence;
        Integer updateRule;
        Integer deleteRule;
        String  fkName;
        String  pkName;
        Integer deferrability;

        // Intermediate holders
        Iterator      tables;
        Table         table;
        Table         fkTable;
        Table         pkTable;
        int           columnCount;
        int[]         mainCols;
        int[]         refCols;
        Constraint[]  constraints;
        Constraint    constraint;
        int           constraintCount;
        HsqlArrayList fkConstraintsList;
        Object[]      row;
        DITableInfo   pkInfo;
        DITableInfo   fkInfo;

        // column number mappings
        final int ipk_table_cat   = 0;
        final int ipk_table_schem = 1;
        final int ipk_table_name  = 2;
        final int ipk_column_name = 3;
        final int ifk_table_cat   = 4;
        final int ifk_table_schem = 5;
        final int ifk_table_name  = 6;
        final int ifk_column_name = 7;
        final int ikey_seq        = 8;
        final int iupdate_rule    = 9;
        final int idelete_rule    = 10;
        final int ifk_name        = 11;
        final int ipk_name        = 12;
        final int ideferrability  = 13;

        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        pkInfo = new DITableInfo();
        fkInfo = new DITableInfo();

        // We must consider all the constraints in all the user tables, since
        // this is where reference relationships are recorded.  However, we
        // are only concerned with Constraint.FOREIGN_KEY constraints here
        // because their corresponing Constraint.MAIN entries are essentially
        // duplicate data recorded in the referenced rather than the
        // referencing table.  Also, we skip constraints where either
        // the referenced, referencing or both tables are not accessible
        // relative to the session of the calling context
        fkConstraintsList = new HsqlArrayList();

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView() || !isAccessibleTable(table)) {
                continue;
            }

            constraints     = table.getConstraints();
            constraintCount = constraints.length;

            for (int i = 0; i < constraintCount; i++) {
                constraint = (Constraint) constraints[i];

                if (constraint.getConstraintType() == Constraint.FOREIGN_KEY
                        && isAccessibleTable(constraint.getRef())) {
                    fkConstraintsList.add(constraint);
                }
            }
        }

        // Now that we have all of the desired constraints, we need to
        // process them, generating one row in our ouput table for each
        // imported/exported column pair of each constraint.
        // Do it.
        for (int i = 0; i < fkConstraintsList.size(); i++) {
            constraint = (Constraint) fkConstraintsList.get(i);
            pkTable    = constraint.getMain();

            pkInfo.setTable(pkTable);

            pkTableName = pkInfo.getName();
            fkTable     = constraint.getRef();

            fkInfo.setTable(fkTable);

            fkTableName    = fkInfo.getName();
            pkTableCatalog = pkTable.getCatalogName().name;
            pkTableSchema  = pkTable.getSchemaName().name;
            fkTableCatalog = fkTable.getCatalogName().name;
            fkTableSchema  = fkTable.getSchemaName().name;
            mainCols       = constraint.getMainColumns();
            refCols        = constraint.getRefColumns();
            columnCount    = refCols.length;
            fkName         = constraint.getRefName().name;
            pkName         = constraint.getMainName().name;
            deferrability  = ValuePool.getInt(constraint.getDeferability());

            //pkName = constraint.getMainIndex().getName().name;
            deleteRule = ValuePool.getInt(constraint.getDeleteAction());
            updateRule = ValuePool.getInt(constraint.getUpdateAction());

            for (int j = 0; j < columnCount; j++) {
                keySequence          = ValuePool.getInt(j + 1);
                pkColumnName         = pkInfo.getColName(mainCols[j]);
                fkColumnName         = fkInfo.getColName(refCols[j]);
                row                  = t.getEmptyRowData();
                row[ipk_table_cat]   = pkTableCatalog;
                row[ipk_table_schem] = pkTableSchema;
                row[ipk_table_name]  = pkTableName;
                row[ipk_column_name] = pkColumnName;
                row[ifk_table_cat]   = fkTableCatalog;
                row[ifk_table_schem] = fkTableSchema;
                row[ifk_table_name]  = fkTableName;
                row[ifk_column_name] = fkColumnName;
                row[ikey_seq]        = keySequence;
                row[iupdate_rule]    = updateRule;
                row[idelete_rule]    = deleteRule;
                row[ifk_name]        = fkName;
                row[ipk_name]        = pkName;
                row[ideferrability]  = deferrability;

                t.insertSys(store, row);
            }
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the visible
     * <code>Index</code> objects for each accessible table defined
     * within this database.<p>
     *
     * Each row is an index column description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TABLE_CAT        VARCHAR   table's catalog
     * TABLE_SCHEM      VARCHAR   simple name of table's schema
     * TABLE_NAME       VARCHAR   simple name of the table using the index
     * NON_UNIQUE       BOOLEAN   can index values be non-unique?
     * INDEX_QUALIFIER  VARCHAR   catalog in which the index is defined
     * INDEX_NAME       VARCHAR   simple name of the index
     * TYPE             SMALLINT  index type: { Clustered | Hashed | Other }
     * ORDINAL_POSITION SMALLINT  column sequence number within index
     * COLUMN_NAME      VARCHAR   simple column name
     * ASC_OR_DESC      VARCHAR   col. sort sequence: {"A" (Asc) | "D" (Desc)}
     * CARDINALITY      INTEGER   # of unique values in index (not implemented)
     * PAGES            INTEGER   index page use (not implemented)
     * FILTER_CONDITION VARCHAR   filter condition, if any (not implemented)
     * // HSQLDB-extension
     * ROW_CARDINALITY  INTEGER   total # of rows in index (not implemented)
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the visible
     *        <code>Index</code> objects for each accessible
     *        table defined within this database.
     */
    final Table SYSTEM_INDEXINFO() {

        Table t = sysTables[SYSTEM_INDEXINFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_INDEXINFO]);

            // JDBC
            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);             // NOT NULL
            addColumn(t, "NON_UNIQUE", Type.SQL_BOOLEAN);           // NOT NULL
            addColumn(t, "INDEX_QUALIFIER", SQL_IDENTIFIER);
            addColumn(t, "INDEX_NAME", SQL_IDENTIFIER);
            addColumn(t, "TYPE", Type.SQL_SMALLINT);                // NOT NULL
            addColumn(t, "ORDINAL_POSITION", Type.SQL_SMALLINT);    // NOT NULL
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "ASC_OR_DESC", CHARACTER_DATA);
            addColumn(t, "CARDINALITY", Type.SQL_INTEGER);
            addColumn(t, "PAGES", Type.SQL_INTEGER);
            addColumn(t, "FILTER_CONDITION", CHARACTER_DATA);

            // HSQLDB extension
            addColumn(t, "ROW_CARDINALITY", Type.SQL_INTEGER);

            // order: NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
            // added for unique: INDEX_QUALIFIER, TABLE_NAME
            // false PK, as INDEX_QUALIFIER may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_INDEXINFO].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                3, 6, 5, 7, 4, 2, 1
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        String  tableCatalog;
        String  tableSchema;
        String  tableName;
        Boolean nonUnique;
        String  indexQualifier;
        String  indexName;
        Integer indexType;

        //Integer ordinalPosition;
        //String  columnName;
        //String  ascOrDesc;
        Integer cardinality;
        Integer pages;
        String  filterCondition;
        Integer rowCardinality;

        // Intermediate holders
        Iterator       tables;
        Table          table;
        int            indexCount;
        int[]          cols;
        int            col;
        int            colCount;
        Object[]       row;
        DITableInfo    ti;
        HsqlProperties p;

        // column number mappings
        final int itable_cat        = 0;
        final int itable_schem      = 1;
        final int itable_name       = 2;
        final int inon_unique       = 3;
        final int iindex_qualifier  = 4;
        final int iindex_name       = 5;
        final int itype             = 6;
        final int iordinal_position = 7;
        final int icolumn_name      = 8;
        final int iasc_or_desc      = 9;
        final int icardinality      = 10;
        final int ipages            = 11;
        final int ifilter_condition = 12;
        final int irow_cardinality  = 13;

        // Initialization
        ti = new DITableInfo();
        p  = database.getProperties();
        tables = p.isPropertyTrue("hsqldb.system_table_indexinfo")
                 ? allTables()
                 : database.schemaManager.databaseObjectIterator(
                     SchemaObject.TABLE);

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView() || !isAccessibleTable(table)) {
                continue;
            }

            ti.setTable(table);

            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;
            tableName    = ti.getName();

            // not supported yet
            filterCondition = null;

            // different cat for index not supported yet
            indexQualifier = tableCatalog;
            indexCount     = table.getIndexCount();

            // process all of the visible indices for this table
            for (int i = 0; i < indexCount; i++) {
                colCount = ti.getIndexVisibleColumns(i);

                if (colCount < 1) {
                    continue;
                }

                indexName      = ti.getIndexName(i);
                nonUnique      = ti.isIndexNonUnique(i);
                cardinality    = ti.getIndexCardinality(i);
                pages          = ValuePool.INTEGER_0;
                rowCardinality = ti.getIndexRowCardinality(i);
                cols           = ti.getIndexColumns(i);
                indexType      = ti.getIndexType(i);

                for (int k = 0; k < colCount; k++) {
                    col                    = cols[k];
                    row                    = t.getEmptyRowData();
                    row[itable_cat]        = tableCatalog;
                    row[itable_schem]      = tableSchema;
                    row[itable_name]       = tableName;
                    row[inon_unique]       = nonUnique;
                    row[iindex_qualifier]  = indexQualifier;
                    row[iindex_name]       = indexName;
                    row[itype]             = indexType;
                    row[iordinal_position] = ValuePool.getInt(k + 1);
                    row[icolumn_name]      = ti.getColName(col);
                    row[iasc_or_desc]      = ti.getIndexColDirection(i, col);
                    row[icardinality]      = cardinality;
                    row[ipages]            = pages;
                    row[irow_cardinality]  = rowCardinality;
                    row[ifilter_condition] = filterCondition;

                    t.insertSys(store, row);
                }
            }
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the visible
     * primary key columns of each accessible table defined within
     * this database. <p>
     *
     * Each row is a PRIMARY KEY column description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TABLE_CAT   VARCHAR   table catalog
     * TABLE_SCHEM VARCHAR   table schema
     * TABLE_NAME  VARCHAR   table name
     * COLUMN_NAME VARCHAR   column name
     * KEY_SEQ     SMALLINT  sequence number within primary key
     * PK_NAME     VARCHAR   primary key constraint name
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the visible
     *        primary key columns of each accessible table
     *        defined within this database.
     */
    final Table SYSTEM_PRIMARYKEYS() {

        Table t = sysTables[SYSTEM_PRIMARYKEYS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PRIMARYKEYS]);

            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);     // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "KEY_SEQ", Type.SQL_SMALLINT);     // not null
            addColumn(t, "PK_NAME", SQL_IDENTIFIER);

            // order: COLUMN_NAME
            // added for unique: TABLE_NAME, TABLE_SCHEM, TABLE_CAT
            // false PK, as  TABLE_SCHEM and/or TABLE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PRIMARYKEYS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                3, 2, 1, 0
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        String tableCatalog;
        String tableSchema;
        String tableName;

        //String  columnName;
        //Integer keySequence;
        String primaryKeyName;

        // Intermediate holders
        Iterator       tables;
        Table          table;
        Object[]       row;
        Constraint     constraint;
        int[]          cols;
        int            colCount;
        HsqlProperties p;

        // column number mappings
        final int itable_cat   = 0;
        final int itable_schem = 1;
        final int itable_name  = 2;
        final int icolumn_name = 3;
        final int ikey_seq     = 4;
        final int ipk_name     = 5;

        // Initialization
        p = database.getProperties();
        tables = p.isPropertyTrue("hsqldb.system_table_primarykeys")
                 ? allTables()
                 : database.schemaManager.databaseObjectIterator(
                     SchemaObject.TABLE);

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView() || !isAccessibleTable(table)
                    || !table.hasPrimaryKey()) {
                continue;
            }

            constraint     = table.getPrimaryConstraint();
            tableCatalog   = table.getCatalogName().name;
            tableSchema    = table.getSchemaName().name;
            tableName      = table.getName().name;
            primaryKeyName = constraint.getName().name;
            cols           = constraint.getMainColumns();
            colCount       = cols.length;

            for (int j = 0; j < colCount; j++) {
                row               = t.getEmptyRowData();
                row[itable_cat]   = tableCatalog;
                row[itable_schem] = tableSchema;
                row[itable_name]  = tableName;
                row[icolumn_name] = table.getColumn(cols[j]).getName().name;
                row[ikey_seq]     = ValuePool.getInt(j + 1);
                row[ipk_name]     = primaryKeyName;

                t.insertSys(store, row);
            }
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the
     * return, parameter and result columns of the accessible
     * routines defined within this database.<p>
     *
     * Each row is a procedure column description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * PROCEDURE_CAT   VARCHAR   routine catalog
     * PROCEDURE_SCHEM VARCHAR   routine schema
     * PROCEDURE_NAME  VARCHAR   routine name
     * COLUMN_NAME     VARCHAR   column/parameter name
     * COLUMN_TYPE     SMALLINT  kind of column/parameter
     * DATA_TYPE       SMALLINT  SQL type from DITypes
     * TYPE_NAME       VARCHAR   SQL type name
     * PRECISION       INTEGER   precision (length) of type
     * LENGTH          INTEGER   transfer size, in bytes, if definitely known
     *                           (roughly equivalent to BUFFER_SIZE for table
     *                           columns)
     * SCALE           SMALLINT  scale
     * RADIX           SMALLINT  radix
     * NULLABLE        SMALLINT  can column contain NULL?
     * REMARKS         VARCHAR   explanatory comment on column
     * // JDBC 4.0
     * COLUMN_DEF        VARCHAR The default column value.
     *                           The string NULL (not enclosed in quotes)
     *                           - If NULL was specified as the default value
     *                           TRUNCATE (not enclosed in quotes)
     *                           - If the specified default value cannot be
     *                           represented without truncation
     *                           NULL
     *                           - If a default value was not specified
     * SQL_DATA_TYPE     INTEGER CLI type list from SQL 2003 Table 37,
     *                           tables 6-9 Annex A1, and/or addendums in other
     *                           documents, such as:
     *                           SQL 2003 Part 9: Management of External Data (SQL/MED) : DATALINK
     *                           SQL 2003 Part 14: XML-Related Specifications (SQL/XML) : XML
     * SQL_DATETIME_SUB  INTEGER SQL 2003 CLI datetime/interval subcode.
     * CHAR_OCTET_LENGTH INTEGER The maximum length of binary and character
     *                           based columns.  For any other datatype the
     *                           returned value is a NULL
     * ORDINAL_POSITION  INTEGER The ordinal position, starting from 1, for the
     *                           input and output parameters for a procedure.
     *                           A value of 0 is returned if this row describes
     *                           the procedure's return value.
     * IS_NULLABLE       VARCHAR ISO rules are used to determinte the nulliblity
     *                           for a column.
     *
     *                           YES (enclosed in quotes)  --- if the column can include NULLs
     *                           NO  (enclosed in quotes)  --- if the column cannot include NULLs
     *                           empty string              --- if the nullability for the column is unknown
     *
     * SPECIFIC_NAME     VARCHAR The name which uniquely identifies this
     *                           procedure within its schema.
     *                           Typically (but not restricted to) a
     *                           fully qualified Java Method name and
     *                           signature
     * // HSQLDB extension
     * JDBC_SEQ          INTEGER The JDBC-specified order within
     *                           runs of PROCEDURE_SCHEM, PROCEDURE_NAME,
     *                           SPECIFIC_NAME, which is:
     *
     *                           return value (0), if any, first, followed
     *                           by the parameter descriptions in call order
     *                           (1..n1), followed by the result column
     *                           descriptions in column number order
     *                           (n1 + 1..n1 + n2)
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the
     *        return, parameter and result columns
     *        of the accessible routines defined
     *        within this database.
     */
    Table SYSTEM_PROCEDURECOLUMNS() {

        Table t = sysTables[SYSTEM_PROCEDURECOLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PROCEDURECOLUMNS]);

            // ----------------------------------------------------------------
            // required
            // ----------------------------------------------------------------
            addColumn(t, "PROCEDURE_CAT", SQL_IDENTIFIER);          // 0
            addColumn(t, "PROCEDURE_SCHEM", SQL_IDENTIFIER);        // 1
            addColumn(t, "PROCEDURE_NAME", SQL_IDENTIFIER);         // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);            // not null
            addColumn(t, "COLUMN_TYPE", Type.SQL_SMALLINT);         // not null
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);           // not null
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);              // not null
            addColumn(t, "PRECISION", Type.SQL_INTEGER);            // 7
            addColumn(t, "LENGTH", Type.SQL_INTEGER);               // 8
            addColumn(t, "SCALE", Type.SQL_SMALLINT);               // 9
            addColumn(t, "RADIX", Type.SQL_SMALLINT);               // 10
            addColumn(t, "NULLABLE", Type.SQL_SMALLINT);            // not null
            addColumn(t, "REMARKS", CHARACTER_DATA);                // 12

            // ----------------------------------------------------------------
            // JDBC 4.0
            // ----------------------------------------------------------------
            addColumn(t, "COLUMN_DEF", CHARACTER_DATA);             // 13
            addColumn(t, "SQL_DATA_TYPE", Type.SQL_INTEGER);        // 14
            addColumn(t, "SQL_DATETIME_SUB", Type.SQL_INTEGER);     // 15
            addColumn(t, "CHAR_OCTET_LENGTH", Type.SQL_INTEGER);    // 16
            addColumn(t, "ORDINAL_POSITION", Type.SQL_INTEGER);     // not null
            addColumn(t, "IS_NULLABLE", CHARACTER_DATA);            // not null
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);          // not null

            // ----------------------------------------------------------------
            // just for JDBC sort contract
            // ----------------------------------------------------------------
            addColumn(t, "JDBC_SEQ", Type.SQL_INTEGER);             // not null

            // ----------------------------------------------------------------
            // order: PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME, SEQ
            // added for unique: PROCEDURE_CAT
            // false PK, as PROCEDURE_SCHEM and/or PROCEDURE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PROCEDURECOLUMNS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                1, 2, 19, 20, 0
            }, false);

            return t;
        }

        // calculated column values
        String  procedureCatalog;
        String  procedureSchema;
        String  procedureName;
        String  columnName;
        Integer columnType;
        Integer dataType;
        String  dataTypeName;
        Integer precision;
        Integer length;
        Integer scale;
        Integer radix;
        Integer nullability;
        String  remark;

        // JDBC 4.0
        String  colDefault;
        Integer sqlDataType;
        Integer sqlDateTimeSub;
        Integer charOctetLength;
        Integer ordinalPosition;
        String  isNullable;
        String  specificName;

        // extended
        int jdbcSequence;

        // intermediate holders
        int           colCount;
        HsqlArrayList aliasList;
        Object[]      info;
        Method        method;
        Iterator      methods;
        Object[]      row;
        DITypeInfo    ti;

        // Initialization
        methods = ns.iterateAllAccessibleMethods(session, true);    // and aliases
        ti      = new DITypeInfo();

        // no such thing as identity or ignorecase return/parameter
        // procedure columns.  Future: may need to worry about this if
        // result columns are ever reported
        ti.setTypeSub(Types.TYPE_SUB_DEFAULT);

        // JDBC 4.0

        /**
         * @todo we do not yet support declarative p-column defaults.
         * In essence, the default value for a procedure column is NULL
         */
        colDefault       = null;
        procedureCatalog = database.getCatalogName().name;
        procedureSchema =
            database.schemaManager.getDefaultSchemaHsqlName().name;

        // Do it.
        while (methods.hasNext()) {
            info      = (Object[]) methods.next();
            method    = (Method) info[0];
            aliasList = (HsqlArrayList) info[1];

            pi.setMethod(method);

            specificName  = pi.getSpecificName();
            procedureName = pi.getFQN();
            colCount      = pi.getColCount();

            for (int i = 0; i < colCount; i++) {
                ti.setTypeCode(pi.getColTypeCode(i));

                columnName   = pi.getColName(i);
                columnType   = pi.getColUsage(i);
                dataType     = pi.getColJDBCDataType(i);
                dataTypeName = ti.getTypeName();
                precision    = ti.getPrecision();
                length       = pi.getColLen(i);
                scale        = ti.getDefaultScale();
                radix        = ti.getNumPrecRadix();
                nullability  = pi.getColNullability(i);
                remark       = pi.getColRemark(i);

                // JDBC 4.0
                //colDefault    = null;
                sqlDataType     = ti.getSqlDataType();
                sqlDateTimeSub  = ti.getSqlDateTimeSub();
                charOctetLength = ti.getCharOctLen();
                ordinalPosition = pi.getColOrdinalPosition(i);
                isNullable      = pi.getColIsNullable(i);

                // extended
                jdbcSequence = pi.getColSequence(i);

                addPColRows(t, aliasList, procedureCatalog, procedureSchema,
                            procedureName, columnName, columnType, dataType,
                            dataTypeName, precision, length, scale, radix,
                            nullability, remark, colDefault, sqlDataType,
                            sqlDateTimeSub, charOctetLength, ordinalPosition,
                            isNullable, specificName, jdbcSequence);
            }
        }

        return t;
    }

    /**
     * Inserts a set of procedure description rows into the <code>Table</code>
     * object specified by the <code>t</code> argument. <p>
     *
     * @param t the table into which the specified rows will eventually
     *      be inserted
     * @param l the list of procedure name aliases to which the specified column
     *      values apply
     * @param cat the procedure catalog name
     * @param schem the procedure schema name
     * @param pName the base (non-alias) procedure name
     * @param ip the procedure input parameter count
     * @param op the procedure output parameter count
     * @param rs the procedure result column count
     * @param remark a human-readable remark regarding the procedure
     * @param pType the procedure type code, indicating whether it is a
     *      function, procedure, or uncatagorized (i.e. returns
     *      a value, does not return a value, or it is unknown
     *      if it returns a value)
     * @param specificName the specific name of the procedure
     *      (typically but not limited to a
     *      fully qualified Java Method name and signature)
     * @param origin origin of the procedure, e.g.
     *      (["BUILTIN" | "USER DEFINED"] "ROUTINE" | "TRIGGER") | "ALIAS", etc.
     *
     */
    protected void addProcRows(Table t, HsqlArrayList l, String cat,
                               String schem, String pName, Integer ip,
                               Integer op, Integer rs, String remark,
                               Integer pType, String specificName,
                               String origin) {

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int icat          = 0;
        final int ischem        = 1;
        final int ipname        = 2;
        final int iinput_parms  = 3;
        final int ioutput_parms = 4;
        final int iresult_sets  = 5;
        final int iremark       = 6;
        final int iptype        = 7;
        final int isn           = 8;
        final int iporigin      = 9;
        Object[]  row           = t.getEmptyRowData();

        row[icat]          = cat;
        row[ischem]        = schem;
        row[ipname]        = pName;
        row[iinput_parms]  = ip;
        row[ioutput_parms] = op;
        row[iresult_sets]  = rs;
        row[iremark]       = remark;
        row[iptype]        = pType;
        row[iporigin]      = origin;
        row[isn]           = specificName;

        t.insertSys(store, row);

        if (l != null) {
            int size = l.size();

            for (int i = 0; i < size; i++) {
                row                = t.getEmptyRowData();
                pName              = (String) l.get(i);
                row[icat]          = cat;
                row[ischem]        = schem;
                row[ipname]        = pName;
                row[iinput_parms]  = ip;
                row[ioutput_parms] = op;
                row[iresult_sets]  = rs;
                row[iremark]       = remark;
                row[iptype]        = pType;
                row[iporigin]      = "ALIAS";
                row[isn]           = specificName;

                t.insertSys(store, row);
            }
        }
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * routines defined within this database.
     *
     * Each row is a procedure description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * PROCEDURE_CAT     VARCHAR   catalog in which routine is defined
     * PROCEDURE_SCHEM   VARCHAR   schema in which routine is defined
     * PROCEDURE_NAME    VARCHAR   simple routine identifier
     * NUM_INPUT_PARAMS  INTEGER   number of input parameters
     * NUM_OUTPUT_PARAMS INTEGER   number of output parameters
     * NUM_RESULT_SETS   INTEGER   number of result sets returned
     * REMARKS           VARCHAR   explanatory comment on the routine
     * PROCEDURE_TYPE    SMALLINT  { Unknown | No Result | Returns Result }
     * // JDBC 4.0
     * SPECIFIC_NAME     VARCHAR   The name which uniquely identifies this
     *                             procedure within its schema.
     *                             typically (but not restricted to) a
     *                             fully qualified Java Method name
     *                             and signature.
     * // HSQLDB extension
     * ORIGIN            VARCHAR   {ALIAS |
     *                             [BUILTIN | USER DEFINED] ROUTINE |
     *                             [BUILTIN | USER DEFINED] TRIGGER |
     *                              ...}
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the accessible
     *        routines defined within the this database
     */
    Table SYSTEM_PROCEDURES() {

        Table t = sysTables[SYSTEM_PROCEDURES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PROCEDURES]);

            // ----------------------------------------------------------------
            // required
            // ----------------------------------------------------------------
            addColumn(t, "PROCEDURE_CAT", SQL_IDENTIFIER);          // 0
            addColumn(t, "PROCEDURE_SCHEM", SQL_IDENTIFIER);        // 1
            addColumn(t, "PROCEDURE_NAME", SQL_IDENTIFIER);         // not null
            addColumn(t, "NUM_INPUT_PARAMS", Type.SQL_INTEGER);     // 3
            addColumn(t, "NUM_OUTPUT_PARAMS", Type.SQL_INTEGER);    // 4
            addColumn(t, "NUM_RESULT_SETS", Type.SQL_INTEGER);      // 5
            addColumn(t, "REMARKS", CHARACTER_DATA);                // 6

            // basically: function (returns result), procedure (no return value)
            // or unknown (say, a trigger callout routine)
            addColumn(t, "PROCEDURE_TYPE", Type.SQL_SMALLINT);      // not null

            // ----------------------------------------------------------------
            // JDBC 4.0
            // ----------------------------------------------------------------
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);          // not null

            // ----------------------------------------------------------------
            // extended
            // ----------------------------------------------------------------
            addColumn(t, "ORIGIN", CHARACTER_DATA);                 // not null

            // ----------------------------------------------------------------
            // order: PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME
            // added for uniqe: PROCEDURE_CAT
            // false PK, as PROCEDURE_SCHEM and/or PROCEDURE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PROCEDURES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                1, 2, 8, 0
            }, false);

            return t;
        }

        // calculated column values
        // ------------------------
        // required
        // ------------------------
        String  catalog;
        String  schema;
        String  procName;
        Integer numInputParams;
        Integer numOutputParams;
        Integer numResultSets;
        String  remarks;
        Integer procRType;

        // -------------------
        // extended
        // -------------------
        String procOrigin;
        String specificName;

        // intermediate holders
        String        alias;
        HsqlArrayList aliasList;
        Iterator      methods;
        Object[]      methodInfo;
        Method        method;
        String        methodOrigin;
        Object[]      row;

        // Initialization
        methods = ns.iterateAllAccessibleMethods(session, true);    //and aliases
        catalog = database.getCatalogName().name;
        schema  = database.schemaManager.getDefaultSchemaHsqlName().name;

        // Do it.
        while (methods.hasNext()) {
            methodInfo   = (Object[]) methods.next();
            method       = (Method) methodInfo[0];
            aliasList    = (HsqlArrayList) methodInfo[1];
            methodOrigin = (String) methodInfo[2];

            pi.setMethod(method);

            procName        = pi.getFQN();
            numInputParams  = pi.getInputParmCount();
            numOutputParams = pi.getOutputParmCount();
            numResultSets   = pi.getResultSetCount();
            remarks         = pi.getRemark();
            procRType       = pi.getResultType(methodOrigin);
            specificName    = pi.getSpecificName();
            procOrigin      = pi.getOrigin(methodOrigin);

            addProcRows(t, aliasList, catalog, schema, procName,
                        numInputParams, numOutputParams, numResultSets,
                        remarks, procRType, specificName, procOrigin);
        }

        return t;
    }

    /**
     * Inserts a set of procedure column description rows into the
     * <code>Table</code> specified by the <code>t</code> argument.
     *
     * <p>
     *
     * @param t the table in which the rows are to be inserted
     * @param l the list of procedure name aliases to which the specified
     *   column values apply
     * @param cat the procedure's catalog name
     * @param schem the procedure's schema name
     * @param pName the procedure's simple base (non-alias) name
     * @param cName the procedure column name
     * @param cType the column type (return, parameter, result)
     * @param dType the column's data type code
     * @param tName the column's canonical data type name
     * @param prec the column's precision
     * @param len the column's buffer length
     * @param scale the column's scale (decimal digits)
     * @param radix the column's numeric precision radix
     * @param nullability the column's java.sql.DatbaseMetaData nullabiliy code
     * @param remark a human-readable remark regarding the column
     * @param colDefault String
     * @param sqlDataType helper value to back JDBC contract sort order
     * @param sqlDateTimeSub Integer
     * @param charOctetLength Integer
     * @param ordinalPosition Integer
     * @param isNullable String
     * @param specificName the specific name of the procedure (typically but
     *   not limited to a fully qualified Java Method name and signature)
     * @param jdbcSequence int
     */
    protected void addPColRows(Table t, HsqlArrayList l, String cat,
                               String schem, String pName, String cName,
                               Integer cType, Integer dType, String tName,
                               Integer prec, Integer len, Integer scale,
                               Integer radix, Integer nullability,
                               String remark, String colDefault,
                               Integer sqlDataType, Integer sqlDateTimeSub,
                               Integer charOctetLength,
                               Integer ordinalPosition, String isNullable,
                               String specificName, int jdbcSequence) {

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int icat       = 0;
        final int ischem     = 1;
        final int iname      = 2;
        final int icol_name  = 3;
        final int icol_type  = 4;
        final int idata_type = 5;
        final int itype_name = 6;
        final int iprec      = 7;
        final int ilength    = 8;
        final int iscale     = 9;
        final int iradix     = 10;
        final int inullable  = 11;
        final int iremark    = 12;

        // JDBC 4.0
        final int icol_default      = 13;
        final int isql_data_type    = 14;
        final int isql_datetime_sub = 15;
        final int ichar_octet_len   = 16;
        final int iordinal_position = 17;
        final int iis_nullable      = 18;
        final int ispecific_name    = 19;

        // HSQLDB extension
        final int ijdbc_sequence = 20;

        // initialization
        Object[] row      = t.getEmptyRowData();
        Integer  sequence = ValuePool.getInt(jdbcSequence);

        // Do it.
        row[icat]       = cat;
        row[ischem]     = schem;
        row[iname]      = pName;
        row[icol_name]  = cName;
        row[icol_type]  = cType;
        row[idata_type] = dType;
        row[itype_name] = tName;
        row[iprec]      = prec;
        row[ilength]    = len;
        row[iscale]     = scale;
        row[iradix]     = radix;
        row[inullable]  = nullability;
        row[iremark]    = remark;

        // JDBC 4.0
        row[icol_default]      = colDefault;
        row[isql_data_type]    = sqlDataType;
        row[isql_datetime_sub] = sqlDateTimeSub;
        row[ichar_octet_len]   = charOctetLength;
        row[iordinal_position] = ordinalPosition;
        row[iis_nullable]      = isNullable;
        row[ispecific_name]    = specificName;

        // HSQLDB extension
        row[ijdbc_sequence] = sequence;

        t.insertSys(store, row);

        if (l != null) {
            int size = l.size();

            for (int i = 0; i < size; i++) {
                row             = t.getEmptyRowData();
                pName           = (String) l.get(i);
                row[icat]       = cat;
                row[ischem]     = schem;
                row[iname]      = pName;
                row[icol_name]  = cName;
                row[icol_type]  = cType;
                row[idata_type] = dType;
                row[itype_name] = tName;
                row[iprec]      = prec;
                row[ilength]    = len;
                row[iscale]     = scale;
                row[iradix]     = radix;
                row[inullable]  = nullability;
                row[iremark]    = remark;

                // JDBC 4.0
                row[icol_default]      = colDefault;
                row[isql_data_type]    = sqlDataType;
                row[isql_datetime_sub] = sqlDateTimeSub;
                row[ichar_octet_len]   = charOctetLength;
                row[iordinal_position] = ordinalPosition;
                row[iis_nullable]      = isNullable;
                row[ispecific_name]    = specificName;

                // HSQLDB extension
                row[ijdbc_sequence] = sequence;

                t.insertSys(store, row);
            }
        }
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible schemas
     * defined within this database. <p>
     *
     * Each row is a schema description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TABLE_SCHEM      VARCHAR   simple schema name
     * TABLE_CATALOG    VARCHAR   catalog in which schema is defined
     * IS_DEFAULT       BOOLEAN   is the schema the default for new sessions
     * </pre> <p>
     *
     * @return table containing information about schemas defined
     *      within this database
     */
    final Table SYSTEM_SCHEMAS() {

        Table t = sysTables[SYSTEM_SCHEMAS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SCHEMAS]);

            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);    // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "IS_DEFAULT", Type.SQL_BOOLEAN);

            // order: TABLE_SCHEM
            // true PK, as rows never have null TABLE_SCHEM
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_SCHEMAS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator        schemas;
        Object[]        row;

        // Initialization
        schemas = database.schemaManager.fullSchemaNamesIterator();

        String defschema =
            database.schemaManager.getDefaultSchemaHsqlName().name;

        // Do it.
        while (schemas.hasNext()) {
            row = t.getEmptyRowData();

            String schema = (String) schemas.next();

            row[0] = schema;
            row[1] = database.getCatalogName().name;
            row[2] = schema.equals(defschema) ? Boolean.TRUE
                                              : Boolean.FALSE;

            t.insertSys(store, row);
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * tables defined within this database. <p>
     *
     * Each row is a table description with the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TABLE_CAT                 VARCHAR   table catalog
     * TABLE_SCHEM               VARCHAR   table schema
     * TABLE_NAME                VARCHAR   table name
     * TABLE_TYPE                VARCHAR   {"TABLE" | "VIEW" |
     *                                      "SYSTEM TABLE" | "GLOBAL TEMPORARY"}
     * REMARKS                   VARCHAR   comment on the table.
     * TYPE_CAT                  VARCHAR   table type catalog (not implemented).
     * TYPE_SCHEM                VARCHAR   table type schema (not implemented).
     * TYPE_NAME                 VARCHAR   table type name (not implemented).
     * SELF_REFERENCING_COL_NAME VARCHAR   designated "identifier" column of
     *                                     typed table (not implemented).
     * REF_GENERATION            VARCHAR   {"SYSTEM" | "USER" |
     *                                      "DERIVED" | NULL } (not implemented)
     * HSQLDB_TYPE               VARCHAR   HSQLDB-specific type:
     *                                     {"MEMORY" | "CACHED" | "TEXT" | ...}
     * READ_ONLY                 BOOLEAN   TRUE if table is read-only,
     *                                     else FALSE.
     * COMMIT_ACTION             VARCHAR   "PRESERVE" or "DELETE" for temp tables,
     *                                     else NULL
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the accessible
     *      tables defined within this database
     */
    final Table SYSTEM_TABLES() {

        Table t = sysTables[SYSTEM_TABLES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TABLES]);

            // -------------------------------------------------------------
            // required
            // -------------------------------------------------------------
            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);       // not null
            addColumn(t, "TABLE_TYPE", CHARACTER_DATA);       // not null
            addColumn(t, "REMARKS", CHARACTER_DATA);

            // -------------------------------------------------------------
            // JDBC 3.0
            // -------------------------------------------------------------
            addColumn(t, "TYPE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TYPE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "SELF_REFERENCING_COL_NAME", SQL_IDENTIFIER);
            addColumn(t, "REF_GENERATION", CHARACTER_DATA);

            // -------------------------------------------------------------
            // extended
            // ------------------------------------------------------------
            addColumn(t, "HSQLDB_TYPE", SQL_IDENTIFIER);
            addColumn(t, "READ_ONLY", Type.SQL_BOOLEAN);      // not null
            addColumn(t, "COMMIT_ACTION", CHARACTER_DATA);    // not null

            // ------------------------------------------------------------
            // order TABLE_TYPE, TABLE_SCHEM and TABLE_NAME
            // added for unique: TABLE_CAT
            // false PK, as TABLE_SCHEM and/or TABLE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TABLES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                3, 1, 2, 0
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // intermediate holders
        Iterator    tables;
        Table       table;
        Object[]    row;
        HsqlName    accessKey;
        DITableInfo ti;

        // column number mappings
        // JDBC 1
        final int itable_cat   = 0;
        final int itable_schem = 1;
        final int itable_name  = 2;
        final int itable_type  = 3;
        final int iremark      = 4;

        // JDBC 3.0
        final int itype_cat   = 5;
        final int itype_schem = 6;
        final int itype_name  = 7;
        final int isref_cname = 8;
        final int iref_gen    = 9;

        // hsqldb ext
        final int ihsqldb_type   = 10;
        final int iread_only     = 11;
        final int icommit_action = 12;

        // Initialization
        tables = allTables();
        ti     = new DITableInfo();

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (!isAccessibleTable(table)) {
                continue;
            }

            ti.setTable(table);

            row               = t.getEmptyRowData();
            row[itable_cat]   = database.getCatalogName().name;
            row[itable_schem] = table.getSchemaName().name;
            row[itable_name]  = ti.getName();
            row[itable_type]  = ti.getJDBCStandardType();
            row[iremark]      = ti.getRemark();
            row[ihsqldb_type] = ti.getHsqlType();
            row[iread_only]   = ti.isReadOnly();
            row[icommit_action] = table.isTemp()
                                  ? (table.onCommitPreserve() ? "PRESERVE"
                                                              : "DELETE")
                                  : null;

            t.insertSys(store, row);
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the table types
     * available in this database. <p>
     *
     * In general, the range of values that may be commonly encounted across
     * most DBMS implementations is: <p>
     *
     * <UL>
     *   <LI><FONT color='#FF00FF'>"TABLE"</FONT>
     *   <LI><FONT color='#FF00FF'>"VIEW"</FONT>
     *   <LI><FONT color='#FF00FF'>"SYSTEM TABLE"</FONT>
     *   <LI><FONT color='#FF00FF'>"GLOBAL TEMPORARY"</FONT>
     *   <LI><FONT color='#FF00FF'>"LOCAL TEMPORARY"</FONT>
     *   <LI><FONT color='#FF00FF'>"ALIAS"</FONT>
     *   <LI><FONT color='#FF00FF'>"SYNONYM"</FONT>
     * </UL> <p>
     *
     * As of HSQLDB 1.7.2, the engine supports and thus this method reports
     * only a subset of the range above: <p>
     *
     * <UL>
     *   <LI><FONT color='#FF00FF'>"TABLE"</FONT>
     *    (HSQLDB MEMORY, CACHED and TEXT tables)
     *   <LI><FONT color='#FF00FF'>"VIEW"</FONT>  (Views)
     *   <LI><FONT color='#FF00FF'>"SYSTEM TABLE"</FONT>
     *    (The tables generated by this object)
     *   <LI><FONT color='#FF00FF'>"GLOBAL TEMPORARY"</FONT>
     *    (HSQLDB TEMP and TEMP TEXT tables)
     * </UL> <p>
     *
     * @return a <code>Table</code> object describing the table types
     *        available in this database
     */
    Table SYSTEM_TABLETYPES() {

        Table t = sysTables[SYSTEM_TABLETYPES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TABLETYPES]);

            addColumn(t, "TABLE_TYPE", SQL_IDENTIFIER);    // not null

            // order: TABLE_TYPE
            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TABLETYPES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Object[]        row;

        for (int i = 0; i < tableTypes.length; i++) {
            row    = t.getEmptyRowData();
            row[0] = tableTypes[i];

            t.insertSys(store, row);
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the
     * result expected by the JDBC DatabaseMetaData interface implementation
     * for system-defined SQL types supported as table columns.
     *
     * <pre class="SqlCodeExample">
     * TYPE_NAME          VARCHAR   the canonical name for DDL statements.
     * DATA_TYPE          SMALLINT  data type code from DITypes.
     * PRECISION          INTEGER   max column size.
     *                              number => max precision.
     *                              character => max characters.
     *                              datetime => max chars incl. frac. component.
     * LITERAL_PREFIX     VARCHAR   char(s) prefixing literal of this type.
     * LITERAL_SUFFIX     VARCHAR   char(s) terminating literal of this type.
     * CREATE_PARAMS      VARCHAR   Localized syntax-order list of domain
     *                              create parameter keywords.
     *                              - for human consumption only
     * NULLABLE           SMALLINT  {No Nulls | Nullable | Unknown}
     * CASE_SENSITIVE     BOOLEAN   case-sensitive in collations/comparisons?
     * SEARCHABLE         SMALLINT  {None | Char (Only WHERE .. LIKE) |
     *                               Basic (Except WHERE .. LIKE) |
     *                               Searchable (All forms)}
     * UNSIGNED_ATTRIBUTE BOOLEAN   {TRUE  (unsigned) | FALSE (signed) |
     *                               NULL (non-numeric or not applicable)}
     * FIXED_PREC_SCALE   BOOLEAN   {TRUE (fixed) | FALSE (variable) |
     *                               NULL (non-numeric or not applicable)}
     * AUTO_INCREMENT     BOOLEAN   automatic unique value generated for
     *                              inserts and updates when no value or
     *                              NULL specified?
     * LOCAL_TYPE_NAME    VARCHAR   localized name of data type;
     *                              - NULL if not supported.
     *                              - for human consuption only
     * MINIMUM_SCALE      SMALLINT  minimum scale supported.
     * MAXIMUM_SCALE      SMALLINT  maximum scale supported.
     * SQL_DATA_TYPE      INTEGER   value expected in SQL CLI SQL_DESC_TYPE
     *                              field of the SQLDA.
     * SQL_DATETIME_SUB   INTEGER   SQL CLI datetime/interval subcode.
     * NUM_PREC_RADIX     INTEGER   numeric base w.r.t # of digits reported in
     *                              PRECISION column (typically 10).
     * TYPE_SUB           INTEGER   From DITypes:
     *                              {TYPE_SUB_DEFAULT | TYPE_SUB_IDENTITY |
     *                               TYPE_SUB_IGNORECASE}
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the
     *      system-defined SQL types supported as table columns
     */
    final Table SYSTEM_TYPEINFO() {

        Table t = sysTables[SYSTEM_TYPEINFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TYPEINFO]);

            //-------------------------------------------
            // required by JDBC:
            // ------------------------------------------
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);
            addColumn(t, "PRECISION", Type.SQL_INTEGER);
            addColumn(t, "LITERAL_PREFIX", CHARACTER_DATA);
            addColumn(t, "LITERAL_SUFFIX", CHARACTER_DATA);
            addColumn(t, "CREATE_PARAMS", CHARACTER_DATA);
            addColumn(t, "NULLABLE", Type.SQL_SMALLINT);
            addColumn(t, "CASE_SENSITIVE", Type.SQL_BOOLEAN);
            addColumn(t, "SEARCHABLE", Type.SQL_INTEGER);
            addColumn(t, "UNSIGNED_ATTRIBUTE", Type.SQL_BOOLEAN);
            addColumn(t, "FIXED_PREC_SCALE", Type.SQL_BOOLEAN);
            addColumn(t, "AUTO_INCREMENT", Type.SQL_BOOLEAN);
            addColumn(t, "LOCAL_TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "MINIMUM_SCALE", Type.SQL_SMALLINT);
            addColumn(t, "MAXIMUM_SCALE", Type.SQL_SMALLINT);
            addColumn(t, "SQL_DATA_TYPE", Type.SQL_INTEGER);
            addColumn(t, "SQL_DATETIME_SUB", Type.SQL_INTEGER);
            addColumn(t, "NUM_PREC_RADIX", Type.SQL_INTEGER);

            //-------------------------------------------
            // for JDBC sort contract:
            //-------------------------------------------
            addColumn(t, "TYPE_SUB", Type.SQL_INTEGER);

            // order: DATA_TYPE, TYPE_SUB
            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TYPEINFO].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                1, 18
            }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "select TYPE_NAME, DATA_TYPE, PRECISION, LITERAL_PREFIX,"
            + "LITERAL_SUFFIX, CREATE_PARAMS, NULLABLE, CASE_SENSITIVE,"
            + "SEARCHABLE,"
            + "UNSIGNED_ATTRIBUTE, FIXED_PREC_SCALE, AUTO_INCREMENT, LOCAL_TYPE_NAME, MINIMUM_SCALE, "
            + "MAXIMUM_SCALE, SQL_DATA_TYPE, SQL_DATETIME_SUB, NUM_PREC_RADIX, TYPE_SUB "
            + "from INFORMATION_SCHEMA.SYSTEM_ALLTYPEINFO  where AS_TAB_COL = true;");

        t.insertSys(store, rs);
        sys.close();

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing, in an extended
     * fashion, all of the system or formal specification SQL types known to
     * this database, including its level of support for them (which may
     * be no support at all) in various capacities. <p>
     *
     * <pre class="SqlCodeExample">
     * TYPE_NAME          VARCHAR   the canonical name used in DDL statements.
     * DATA_TYPE          SMALLINT  data type code from Types
     * PRECISION          INTEGER   max column size.
     *                              number => max. precision.
     *                              character => max characters.
     *                              datetime => max chars incl. frac. component.
     * LITERAL_PREFIX     VARCHAR   char(s) prefixing literal of this type.
     * LITERAL_SUFFIX     VARCHAR   char(s) terminating literal of this type.
     * CREATE_PARAMS      VARCHAR   Localized syntax-order list of domain
     *                              create parameter keywords.
     *                              - for human consumption only
     * NULLABLE           SMALLINT  { No Nulls | Nullable | Unknown }
     * CASE_SENSITIVE     BOOLEAN   case-sensitive in collations/comparisons?
     * SEARCHABLE         SMALLINT  { None | Char (Only WHERE .. LIKE) |
     *                                Basic (Except WHERE .. LIKE) |
     *                                Searchable (All forms) }
     * UNSIGNED_ATTRIBUTE BOOLEAN   { TRUE  (unsigned) | FALSE (signed) |
     *                                NULL (non-numeric or not applicable) }
     * FIXED_PREC_SCALE   BOOLEAN   { TRUE (fixed) | FALSE (variable) |
     *                                NULL (non-numeric or not applicable) }
     * AUTO_INCREMENT     BOOLEAN   automatic unique value generated for
     *                              inserts and updates when no value or
     *                              NULL specified?
     * LOCAL_TYPE_NAME    VARCHAR   Localized name of data type;
     *                              - NULL => not supported (no resource avail).
     *                              - for human consumption only
     * MINIMUM_SCALE      SMALLINT  minimum scale supported.
     * MAXIMUM_SCALE      SMALLINT  maximum scale supported.
     * SQL_DATA_TYPE      INTEGER   value expected in SQL CLI SQL_DESC_TYPE
     *                              field of the SQLDA.
     * SQL_DATETIME_SUB   INTEGER   SQL CLI datetime/interval subcode
     * NUM_PREC_RADIX     INTEGER   numeric base w.r.t # of digits reported
     *                              in PRECISION column (typically 10)
     * INTERVAL_PRECISION INTEGER   interval leading precision (not implemented)
     * AS_TAB_COL         BOOLEAN   type supported as table column?
     * AS_PROC_COL        BOOLEAN   type supported as procedure column?
     * MAX_PREC_ACT       BIGINT    like PRECISION unless value would be
     *                              truncated using INTEGER
     * MIN_SCALE_ACT      INTEGER   like MINIMUM_SCALE unless value would be
     *                              truncated using SMALLINT
     * MAX_SCALE_ACT      INTEGER   like MAXIMUM_SCALE unless value would be
     *                              truncated using SMALLINT
     * COL_ST_CLS_NAME    VARCHAR   Java Class FQN of in-memory representation
     * COL_ST_IS_SUP      BOOLEAN   is COL_ST_CLS_NAME supported under the
     *                              hosting JVM and engine build option?
     * STD_MAP_CLS_NAME   VARCHAR   Java class FQN of standard JDBC mapping
     * STD_MAP_IS_SUP     BOOLEAN   Is STD_MAP_CLS_NAME supported under the
     *                              hosting JVM?
     * CST_MAP_CLS_NAME   VARCHAR   Java class FQN of HSQLDB-provided JDBC
     *                              interface representation
     * CST_MAP_IS_SUP     BOOLEAN   is CST_MAP_CLS_NAME supported under the
     *                              hosting JVM and engine build option?
     * MCOL_JDBC          INTEGER   maximum character octet length representable
     *                              via JDBC interface
     * MCOL_ACT           BIGINT    like MCOL_JDBC unless value would be
     *                              truncated using INTEGER
     * DEF_OR_FIXED_SCALE INTEGER   default or fixed scale for numeric types
     * REMARKS            VARCHAR   localized comment on the data type
     * TYPE_SUB           INTEGER   From Types:
     *                              {TYPE_SUB_DEFAULT | TYPE_SUB_IGNORECASE}
     *                              deprecated: TYPE_SUB_IDENTITY
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing all of the
     *        standard SQL types known to this database
     */
    final Table SYSTEM_ALLTYPEINFO() {

        Table t = sysTables[SYSTEM_ALLTYPEINFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_ALLTYPEINFO]);

            //-------------------------------------------
            // same as SYSTEM_TYPEINFO:
            // ------------------------------------------
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);
            addColumn(t, "PRECISION", Type.SQL_INTEGER);
            addColumn(t, "LITERAL_PREFIX", CHARACTER_DATA);
            addColumn(t, "LITERAL_SUFFIX", CHARACTER_DATA);
            addColumn(t, "CREATE_PARAMS", CHARACTER_DATA);
            addColumn(t, "NULLABLE", Type.SQL_SMALLINT);
            addColumn(t, "CASE_SENSITIVE", Type.SQL_BOOLEAN);
            addColumn(t, "SEARCHABLE", Type.SQL_SMALLINT);
            addColumn(t, "UNSIGNED_ATTRIBUTE", Type.SQL_BOOLEAN);
            addColumn(t, "FIXED_PREC_SCALE", Type.SQL_BOOLEAN);
            addColumn(t, "AUTO_INCREMENT", Type.SQL_BOOLEAN);
            addColumn(t, "LOCAL_TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "MINIMUM_SCALE", Type.SQL_SMALLINT);
            addColumn(t, "MAXIMUM_SCALE", Type.SQL_SMALLINT);
            addColumn(t, "SQL_DATA_TYPE", Type.SQL_INTEGER);
            addColumn(t, "SQL_DATETIME_SUB", Type.SQL_INTEGER);
            addColumn(t, "NUM_PREC_RADIX", Type.SQL_INTEGER);

            //-------------------------------------------
            // SQL CLI / ODBC - not in JDBC spec
            // ------------------------------------------
            addColumn(t, "INTERVAL_PRECISION", Type.SQL_INTEGER);

            //-------------------------------------------
            // extended:
            //-------------------------------------------
            // level of support
            //-------------------------------------------
            addColumn(t, "AS_TAB_COL", Type.SQL_BOOLEAN);

            // for instance, some executable methods take Connection
            // or return non-serializable Object such as ResultSet, neither
            // of which maps to a supported table column type but which
            // we show as JAVA_OBJECT in SYSTEM_PROCEDURECOLUMNS.
            // Also, triggers take Object[] row, which we show as ARRAY
            // presently, although STRUCT would probably be better in the
            // future, as the row can actually contain mixed data types.
            addColumn(t, "AS_PROC_COL", Type.SQL_BOOLEAN);

            //-------------------------------------------
            // actual values for attributes that cannot be represented
            // within the limitations of the SQL CLI / JDBC interface
            //-------------------------------------------
            addColumn(t, "MAX_PREC_ACT", Type.SQL_BIGINT);
            addColumn(t, "MIN_SCALE_ACT", Type.SQL_INTEGER);
            addColumn(t, "MAX_SCALE_ACT", Type.SQL_INTEGER);

            //-------------------------------------------
            // how do we store this internally as a column value?
            //-------------------------------------------
            addColumn(t, "COL_ST_CLS_NAME", SQL_IDENTIFIER);
            addColumn(t, "COL_ST_IS_SUP", Type.SQL_BOOLEAN);

            //-------------------------------------------
            // what is the standard Java mapping for the type?
            //-------------------------------------------
            addColumn(t, "STD_MAP_CLS_NAME", SQL_IDENTIFIER);
            addColumn(t, "STD_MAP_IS_SUP", Type.SQL_BOOLEAN);

            //-------------------------------------------
            // what, if any, custom mapping do we provide?
            // (under the current build options and hosting VM)
            //-------------------------------------------
            addColumn(t, "CST_MAP_CLS_NAME", SQL_IDENTIFIER);
            addColumn(t, "CST_MAP_IS_SUP", Type.SQL_BOOLEAN);

            //-------------------------------------------
            // what is the max representable and actual
            // character octet length, if applicable?
            //-------------------------------------------
            addColumn(t, "MCOL_JDBC", Type.SQL_INTEGER);
            addColumn(t, "MCOL_ACT", Type.SQL_BIGINT);

            //-------------------------------------------
            // what is the default or fixed scale, if applicable?
            //-------------------------------------------
            addColumn(t, "DEF_OR_FIXED_SCALE", Type.SQL_INTEGER);

            //-------------------------------------------
            // Any type-specific, localized remarks can go here
            //-------------------------------------------
            addColumn(t, "REMARKS", CHARACTER_DATA);

            //-------------------------------------------
            // required for JDBC sort contract:
            //-------------------------------------------
            addColumn(t, "TYPE_SUB", Type.SQL_INTEGER);

            // order:  DATA_TYPE, TYPE_SUB
            // true primary key
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_ALLTYPEINFO].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                1, 34
            }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Object[]        row;
        int             type;
        DITypeInfo      ti;

        //-----------------------------------------
        // Same as SYSTEM_TYPEINFO
        //-----------------------------------------
        final int itype_name          = 0;
        final int idata_type          = 1;
        final int iprecision          = 2;
        final int iliteral_prefix     = 3;
        final int iliteral_suffix     = 4;
        final int icreate_params      = 5;
        final int inullable           = 6;
        final int icase_sensitive     = 7;
        final int isearchable         = 8;
        final int iunsigned_attribute = 9;
        final int ifixed_prec_scale   = 10;
        final int iauto_increment     = 11;
        final int ilocal_type_name    = 12;
        final int iminimum_scale      = 13;
        final int imaximum_scale      = 14;
        final int isql_data_type      = 15;
        final int isql_datetime_sub   = 16;
        final int inum_prec_radix     = 17;

        //------------------------------------------
        // Extensions
        //------------------------------------------
        // not in JDBC, but in SQL CLI SQLDA / ODBC
        //------------------------------------------
        final int iinterval_precision = 18;

        //------------------------------------------
        // HSQLDB/Java-specific:
        //------------------------------------------
        final int iis_sup_as_tcol = 19;
        final int iis_sup_as_pcol = 20;

        //------------------------------------------
        final int imax_prec_or_len_act = 21;
        final int imin_scale_actual    = 22;
        final int imax_scale_actual    = 23;

        //------------------------------------------
        final int ics_cls_name         = 24;
        final int ics_cls_is_supported = 25;

        //------------------------------------------
        final int ism_cls_name         = 26;
        final int ism_cls_is_supported = 27;

        //------------------------------------------
        final int icm_cls_name         = 28;
        final int icm_cls_is_supported = 29;

        //------------------------------------------
        final int imax_char_oct_len_jdbc = 30;
        final int imax_char_oct_len_act  = 31;

        //------------------------------------------
        final int idef_or_fixed_scale = 32;

        //------------------------------------------
        final int iremarks = 33;

        //------------------------------------------
        final int itype_sub = 34;

        ti = new DITypeInfo();

        for (int i = 0; i < Types.ALL_TYPES.length; i++) {
            ti.setTypeCode(Types.ALL_TYPES[i][0]);
            ti.setTypeSub(Types.ALL_TYPES[i][1]);

            row                      = t.getEmptyRowData();
            row[itype_name]          = ti.getTypeName();
            row[idata_type]          = ti.getDataType();
            row[iprecision]          = ti.getPrecision();
            row[iliteral_prefix]     = ti.getLiteralPrefix();
            row[iliteral_suffix]     = ti.getLiteralSuffix();
            row[icreate_params]      = ti.getCreateParams();
            row[inullable]           = ti.getNullability();
            row[icase_sensitive]     = ti.isCaseSensitive();
            row[isearchable]         = ti.getSearchability();
            row[iunsigned_attribute] = ti.isUnsignedAttribute();
            row[ifixed_prec_scale]   = ti.isFixedPrecisionScale();
            row[iauto_increment]     = ti.isAutoIncrement();
            row[ilocal_type_name]    = ti.getLocalName();
            row[iminimum_scale]      = ti.getMinScale();
            row[imaximum_scale]      = ti.getMaxScale();
            row[isql_data_type]      = ti.getSqlDataType();
            row[isql_datetime_sub]   = ti.getSqlDateTimeSub();
            row[inum_prec_radix]     = ti.getNumPrecRadix();

            //------------------------------------------
            row[iinterval_precision] = ti.getIntervalPrecision();

            //------------------------------------------
            row[iis_sup_as_tcol] = ti.isSupportedAsTCol();
            row[iis_sup_as_pcol] = ti.isSupportedAsPCol();

            //------------------------------------------
            row[imax_prec_or_len_act] = ti.getPrecisionAct();
            row[imin_scale_actual]    = ti.getMinScaleAct();
            row[imax_scale_actual]    = ti.getMaxScaleAct();

            //------------------------------------------
            row[ics_cls_name]         = ti.getColStClsName();
            row[ics_cls_is_supported] = ti.isColStClsSupported();

            //------------------------------------------
            row[ism_cls_name]         = ti.getStdMapClsName();
            row[ism_cls_is_supported] = ti.isStdMapClsSupported();

            //------------------------------------------
            row[icm_cls_name] = ti.getCstMapClsName();

            try {
                if (row[icm_cls_name] != null) {
                    ns.classForName((String) row[icm_cls_name]);

                    row[icm_cls_is_supported] = Boolean.TRUE;
                }
            } catch (Exception e) {
                row[icm_cls_is_supported] = Boolean.FALSE;
            }

            //------------------------------------------
            row[imax_char_oct_len_jdbc] = ti.getCharOctLen();
            row[imax_char_oct_len_act]  = ti.getCharOctLenAct();

            //------------------------------------------
            row[idef_or_fixed_scale] = ti.getDefaultScale();

            //------------------------------------------
            row[iremarks] = ti.getRemarks();

            //------------------------------------------
            row[itype_sub] = ti.getDataTypeSub();

            t.insertSys(store, row);
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the
     * visible <code>Users</code> defined within this database.
     * @return table containing information about the users defined within
     *      this database
     */
    Table SYSTEM_USERS() {

        Table t = sysTables[SYSTEM_USERS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_USERS]);

            addColumn(t, "USER_NAME", SQL_IDENTIFIER);
            addColumn(t, "ADMIN", Type.SQL_BOOLEAN);
            addColumn(t, "INITIAL_SCHEMA", SQL_IDENTIFIER);

            // order: USER
            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_USERS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Intermediate holders
        HsqlArrayList users;
        User          user;
        Object[]      row;
        HsqlName      initialSchema;

        // Initialization
        users = database.getUserManager().listVisibleUsers(session);

        // Do it.
        for (int i = 0; i < users.size(); i++) {
            row           = t.getEmptyRowData();
            user          = (User) users.get(i);
            initialSchema = user.getInitialSchema();
            row[0]        = user.getNameString();
            row[1]        = ValuePool.getBoolean(user.isAdmin());
            row[2]        = ((initialSchema == null) ? null
                                                     : initialSchema.name);

            t.insertSys(store, row);
        }

        return t;
    }

// -----------------------------------------------------------------------------
// SQL SCHEMATA VIEWS
// limited to views used in JDBC DatabaseMetaData

    /**
     * Retrieves a <code>Table</code> object describing the visible
     * access rights for all visible columns of all accessible
     * tables defined within this database.<p>
     *
     * Each row is a column privilege description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * TABLE_CAT    VARCHAR   table catalog
     * TABLE_SCHEM  VARCHAR   table schema
     * TABLE_NAME   VARCHAR   table name
     * COLUMN_NAME  VARCHAR   column name
     * GRANTOR      VARCHAR   grantor of access
     * GRANTEE      VARCHAR   grantee of access
     * PRIVILEGE    VARCHAR   name of access
     * IS_GRANTABLE VARCHAR   grantable?: "YES" - grant to others, else "NO"
     * </pre>
     *
     * <b>Note:</b> From 1.9.0, HSQLDB supports column level
     * privileges. <p>
     *
     * @return a <code>Table</code> object describing the visible
     *        access rights for all visible columns of
     *        all accessible tables defined within this
     *        database
     */
    final Table COLUMN_PRIVILEGES() {

        Table t = sysTables[COLUMN_PRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMN_PRIVILEGES]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);        // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);       // not null
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    // not null
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMN_PRIVILEGES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                2, 3, 4, 5, 6, 1, 0
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

// calculated column values
        String  tableCatalog;
        String  tableSchema;
        String  tableName;
        Grantee granteeObject;

// intermediate holders
        User     user;
        Iterator tables;
        Table    table;
        Object[] row;

// column number mappings
        final int grantor        = 0;
        final int grantee        = 1;
        final int table_catalog  = 2;
        final int table_schema   = 3;
        final int table_name     = 4;
        final int column_name    = 5;
        final int privilege_type = 6;
        final int is_grantable   = 7;

        // enumerations
        OrderedHashSet grantees =
            session.getGrantee().getGranteeAndAllRolesWithPublic();

// Initialization
        tables = allTables();

        while (tables.hasNext()) {
            table        = (Table) tables.next();
            tableName    = table.getName().name;
            tableCatalog = database.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;

            for (int i = 0; i < grantees.size(); i++) {
                granteeObject = (Grantee) grantees.get(i);

                OrderedHashSet rights =
                    granteeObject.getAllDirectPrivileges(table);
                OrderedHashSet grants =
                    granteeObject.getAllGrantedPrivileges(table);

                if (!grants.isEmpty()) {
                    grants.addAll(rights);

                    rights = grants;
                }

                for (int j = 0; j < rights.size(); j++) {
                    Right right          = (Right) rights.get(j);
                    Right grantableRight = right.getGrantableRights();

                    for (int k = 0; k < Right.privilegeTypes.length; k++) {
                        OrderedHashSet columnList =
                            right.getColumnsForPrivilege(
                                table, Right.privilegeTypes[k]);
                        OrderedHashSet grantableList =
                            grantableRight.getColumnsForPrivilege(table,
                                Right.privilegeTypes[k]);

                        for (int l = 0; l < columnList.size(); l++) {
                            HsqlName fullName = ((HsqlName) columnList.get(l));

                            row                 = t.getEmptyRowData();
                            row[grantor] = right.getGrantor().getName().name;
                            row[grantee] = right.getGrantee().getName().name;
                            row[table_catalog]  = tableCatalog;
                            row[table_schema]   = tableSchema;
                            row[table_name]     = tableName;
                            row[column_name]    = fullName.name;
                            row[privilege_type] = Right.privilegeNames[k];
                            row[is_grantable] =
                                right.getGrantee() == table.getOwner()
                                || grantableList.contains(fullName) ? "YES"
                                                                    : "NO";

                            try {
                                t.insertSys(store, row);
                            } catch (HsqlException e) {}
                        }
                    }
                }
            }
        }

        return t;
    }

    /**
     * The SEQUENCES view has one row for each external sequence
     * generator. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     *
     *      SEQUENCE_CATALOG     VARCHAR NULL,
     *      SEQUENCE_SCHEMA      VARCHAR NULL,
     *      SEQUENCE_NAME        VARCHAR NOT NULL,
     *      DATA_TYPE            CHARACTER_DATA
     *      DATA_TYPE            CHARACTER_DATA
     *      NUMERIC_PRECISION    CARDINAL_NUMBER
     *      NUMERIC_PRECISION_RADIX CARDINAL_NUMBER
     *      NUMERIC_SCALE        CARDINAL_NUMBER
     *      MAXIMUM_VALUE        VARCHAR NOT NULL,
     *      MINIMUM_VALUE        VARCHAR NOT NULL,
     *      INCREMENT            VARCHAR NOT NULL,
     *      CYCLE_OPTION         VARCHAR {'YES', 'NO'},
     *      START_WITH           VARCHAR NOT NULL,
     *      DECLARED_DATA_TYPE   CHARACTER_DATA
     *      DECLARED_NUMERIC_PRECISION CARDINAL_NUMBER
     *      DECLARED_NUMERIC_SCLAE     CARDINAL_NUMBER
     *
     * </pre>
     *
     * <b>DESCRIPTION:</b><p>
     *
     * <ol>
     * <li> The values of SEQUENCE_CATALOG, SEQUENCE_SCHEMA, and
     *      SEQUENCE_NAME are the catalog name, unqualified schema name,
     *      and qualified identifier, respectively, of the sequence generator
     *      being described. <p>
     *
     * <li> The values of SEQUENCE_CATALOG, SEQUENCE_SCHEMA, SEQUENCE_NAME, and
     *      DTD_IDENTIFIER are the values of OBJECT_CATALOG, OBJECT_SCHEMA,
     *      OBJECT_NAME, and DTD_IDENTIFIER, respectively, of the row in
     *      DATA_TYPE_DESCRIPTOR (not yet implemented) that describes the data
     *      type of the sequence generator. <p>
     *
     * <li> The values of MAXIMUM_VALUE, MINIMUM_VALUE, and INCREMENT are the
     *      character representations of maximum value, minimum value,
     *      and increment, respectively, of the sequence generator being
     *      described. <p>
     *
     * <li> The values of CYCLE_OPTION have the following meanings: <p>
     *
     *      <table border cellpadding="3">
     *          <tr>
     *              <td nowrap>YES</td>
     *              <td nowrap>The cycle option of the sequence generator
     *                         is CYCLE.</td>
     *          <tr>
     *              <td nowrap>NO</td>
     *              <td nowrap>The cycle option of the sequence generator is
     *                         NO CYCLE.</td>
     *          </tr>
     *      </table> <p>
     *
     * <li> The value of START_WITH is HSQLDB-specific (not in the SQL 200n
     *      spec).  <p>
     *
     *      It is the character representation of the START WITH value. <p>
     *
     * <li> The value of NEXT_VALUE is HSQLDB-specific (not in the SQL 200n)<p>
     *      This is the character representation of the value that
     *      would be generated by NEXT VALUE FOR when this sequence
     *      is materialized in an SQL statement. <p>
     * </ol>
     *
     * @return Table
     */
    final Table SEQUENCES() {

        Table t = sysTables[SEQUENCES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SEQUENCES]);

            addColumn(t, "SEQUENCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "MAXIMUM_VALUE", CHARACTER_DATA);
            addColumn(t, "MINIMUM_VALUE", CHARACTER_DATA);
            addColumn(t, "INCREMENT", CHARACTER_DATA);
            addColumn(t, "CYCLE_OPTION", YES_OR_NO);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCLAE", CARDINAL_NUMBER);

            // HSQLDB-specific
            addColumn(t, "START_WITH", CHARACTER_DATA);
            addColumn(t, "NEXT_VALUE", CHARACTER_DATA);

            // order SEQUENCE_CATALOG, SEQUENCE_SCHEMA, SEQUENCE_NAME
            // false PK, as CATALOG may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SEQUENCES].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        //
        final int sequence_catalog           = 0;
        final int sequence_schema            = 1;
        final int sequence_name              = 2;
        final int data_type                  = 3;
        final int numeric_precision          = 4;
        final int numeric_precision_radix    = 5;
        final int numeric_scale              = 6;
        final int maximum_value              = 7;
        final int minimum_value              = 8;
        final int increment                  = 9;
        final int cycle_option               = 10;
        final int declared_data_type         = 11;
        final int declared_numeric_precision = 12;
        final int declared_numeric_scale     = 13;
        final int start_with                 = 14;
        final int next_value                 = 15;

        //
        Iterator       it;
        Object[]       row;
        NumberSequence sequence;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SEQUENCE);

        while (it.hasNext()) {
            sequence = (NumberSequence) it.next();

            if (!session.getGrantee().isAccessible(sequence)) {
                continue;
            }

            row = t.getEmptyRowData();

            NumberType type = (NumberType) sequence.getDataType();
            int radix =
                (type.typeCode == Types.SQL_NUMERIC || type.typeCode == Types
                    .SQL_DECIMAL) ? 10
                                  : 2;

            row[sequence_catalog] = database.getCatalogName().name;
            row[sequence_schema]  = sequence.getSchemaName().name;
            row[sequence_name]    = sequence.getName().name;
            row[data_type]        = sequence.getDataType().getFullNameString();
            row[numeric_precision] =
                ValuePool.getInt((int) type.getPrecision());
            row[numeric_precision_radix]    = ValuePool.getInt(radix);
            row[numeric_scale]              = ValuePool.INTEGER_0;
            row[maximum_value] = String.valueOf(sequence.getMaxValue());
            row[minimum_value] = String.valueOf(sequence.getMinValue());
            row[increment] = String.valueOf(sequence.getIncrement());
            row[cycle_option]               = sequence.isCycle() ? "YES"
                                                                 : "NO";
            row[declared_data_type]         = row[data_type];
            row[declared_numeric_precision] = row[numeric_precision];
            row[declared_numeric_scale]     = row[declared_numeric_scale];
            row[start_with] = String.valueOf(sequence.getStartValue());
            row[next_value]                 = String.valueOf(sequence.peek());

            t.insertSys(store, row);
        }

        return t;
    }

    final Table SYSTEM_SEQUENCES() {

        Table t = sysTables[SYSTEM_SEQUENCES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SEQUENCES]);

            addColumn(t, "SEQUENCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "MAXIMUM_VALUE", CHARACTER_DATA);
            addColumn(t, "MINIMUM_VALUE", CHARACTER_DATA);
            addColumn(t, "INCREMENT", CHARACTER_DATA);
            addColumn(t, "CYCLE_OPTION", YES_OR_NO);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCLAE", CARDINAL_NUMBER);

            // HSQLDB-specific
            addColumn(t, "START_WITH", CHARACTER_DATA);
            addColumn(t, "NEXT_VALUE", CHARACTER_DATA);

            // order SEQUENCE_CATALOG, SEQUENCE_SCHEMA, SEQUENCE_NAME
            // false PK, as CATALOG may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_SEQUENCES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        //
        final int sequence_catalog           = 0;
        final int sequence_schema            = 1;
        final int sequence_name              = 2;
        final int data_type                  = 3;
        final int numeric_precision          = 4;
        final int numeric_precision_radix    = 5;
        final int numeric_scale              = 6;
        final int maximum_value              = 7;
        final int minimum_value              = 8;
        final int increment                  = 9;
        final int cycle_option               = 10;
        final int declared_data_type         = 11;
        final int declared_numeric_precision = 12;
        final int declared_numeric_scale     = 13;
        final int start_with                 = 14;
        final int next_value                 = 15;

        //
        Iterator       it;
        Object[]       row;
        NumberSequence sequence;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SEQUENCE);

        while (it.hasNext()) {
            sequence = (NumberSequence) it.next();

            if (!session.getGrantee().isAccessible(sequence)) {
                continue;
            }

            row = t.getEmptyRowData();

            NumberType type = (NumberType) sequence.getDataType();
            int radix =
                (type.typeCode == Types.SQL_NUMERIC || type.typeCode == Types
                    .SQL_DECIMAL) ? 10
                                  : 2;

            row[sequence_catalog] = database.getCatalogName().name;
            row[sequence_schema]  = sequence.getSchemaName().name;
            row[sequence_name]    = sequence.getName().name;
            row[data_type]        = sequence.getDataType().getFullNameString();
            row[numeric_precision] =
                ValuePool.getInt((int) type.getPrecision());
            row[numeric_precision_radix]    = ValuePool.getInt(radix);
            row[numeric_scale]              = ValuePool.INTEGER_0;
            row[maximum_value] = String.valueOf(sequence.getMaxValue());
            row[minimum_value] = String.valueOf(sequence.getMinValue());
            row[increment] = String.valueOf(sequence.getIncrement());
            row[cycle_option]               = sequence.isCycle() ? "YES"
                                                                 : "NO";
            row[declared_data_type]         = row[data_type];
            row[declared_numeric_precision] = row[numeric_precision];
            row[declared_numeric_scale]     = row[declared_numeric_scale];
            row[start_with] = String.valueOf(sequence.getStartValue());
            row[next_value]                 = String.valueOf(sequence.peek());

            t.insertSys(store, row);
        }

        return t;
    }

/*
    WHERE ( GRANTEE IN ( 'PUBLIC', CURRENT_USER )
    OR GRANTEE IN ( SELECT ROLE_NAME FROM ENABLED_ROLES )
    OR GRANTOR = CURRENT_USER
    OR GRANTOR IN ( SELECT ROLE_NAME FROM ENABLED_ROLES ) )

*/

/**
     * The TABLE_PRIVILEGES view has one row for each visible access
     * right for each accessible table definied within this database. <p>
     *
     * Each row is a table privilege description with the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * GRANTOR      VARCHAR   grantor of access
     * GRANTEE      VARCHAR   grantee of access
     * TABLE_CATALOG    VARCHAR   table catalog
     * TABLE_SCHEMA  VARCHAR   table schema
     * TABLE_NAME   VARCHAR   table name
     * PRIVILEGE_TYPE    VARCHAR   { "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "REFERENCES" | "TRIGGER" }
     * IS_GRANTABLE VARCHAR   { "YES" | "NO" }
     * WITH_HIERARCHY   { "YES" | "NO" }
     * </pre>
     *
     * @return a <code>Table</code> object describing the visible
     *        access rights for each accessible table
     *        defined within this database
     */
    final Table TABLE_PRIVILEGES() {

        Table t = sysTables[TABLE_PRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TABLE_PRIVILEGES]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);        // not null
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    // not null
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           // not null
            addColumn(t, "WITH_HIERARCHY", YES_OR_NO);

            //
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SEQUENCES].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        String  tableCatalog;
        String  tableSchema;
        String  tableName;
        Grantee granteeObject;
        String  privilege;

        // intermediate holders
        Iterator tables;
        Table    table;
        Object[] row;

        // column number mappings
        final int grantor        = 0;
        final int grantee        = 1;
        final int table_catalog  = 2;
        final int table_schema   = 3;
        final int table_name     = 4;
        final int privilege_type = 5;
        final int is_grantable   = 6;
        final int with_hierarchy = 7;
        OrderedHashSet grantees =
            session.getGrantee().getGranteeAndAllRolesWithPublic();

        tables = allTables();

        while (tables.hasNext()) {
            table        = (Table) tables.next();
            tableName    = table.getName().name;
            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;

            for (int i = 0; i < grantees.size(); i++) {
                granteeObject = (Grantee) grantees.get(i);

                OrderedHashSet rights =
                    granteeObject.getAllDirectPrivileges(table);
                OrderedHashSet grants =
                    granteeObject.getAllGrantedPrivileges(table);

                if (!grants.isEmpty()) {
                    grants.addAll(rights);

                    rights = grants;
                }

                for (int j = 0; j < rights.size(); j++) {
                    Right right          = (Right) rights.get(j);
                    Right grantableRight = right.getGrantableRights();

                    for (int k = 0; k < Right.privilegeTypes.length; k++) {
                        if (!right.canAccess(Right.privilegeTypes[k])) {
                            continue;
                        }

                        privilege           = Right.privilegeNames[k];
                        row                 = t.getEmptyRowData();
                        row[grantor] = right.getGrantor().getName().name;
                        row[grantee] = right.getGrantee().getName().name;
                        row[table_catalog]  = tableCatalog;
                        row[table_schema]   = tableSchema;
                        row[table_name]     = tableName;
                        row[privilege_type] = privilege;
                        row[is_grantable] =
                            right.getGrantee() == table.getOwner()
                            || grantableRight.canAccess(
                                Right.privilegeTypes[k]) ? "YES"
                                                         : "NO";
                        row[with_hierarchy] = "NO";

                        try {
                            t.insertSys(store, row);
                        } catch (HsqlException e) {}
                    }
                }
            }
        }

        return t;
    }

    Table TABLES() {

        Table t = sysTables[TABLES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TABLES]);

            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_TYPE", CHARACTER_DATA);
            addColumn(t, "SELF_REFERENCING_COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "REFERENCE_GENERATION", CHARACTER_DATA);
            addColumn(t, "USER_DEFINED_TYPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_INSERTABLE_INTO", YES_OR_NO);
            addColumn(t, "IS_TYPED", YES_OR_NO);
            addColumn(t, "COMMIT_ACTION", CHARACTER_DATA);

            //
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TABLES].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2,
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // intermediate holders
        Iterator  tables;
        Table     table;
        Object[]  row;
        final int table_catalog                = 0;
        final int table_schema                 = 1;
        final int table_name                   = 2;
        final int table_type                   = 3;
        final int self_referencing_column_name = 4;
        final int reference_generation         = 5;
        final int user_defined_type_catalog    = 6;
        final int user_defined_type_schema     = 7;
        final int user_defined_type_name       = 8;
        final int is_insertable_into           = 9;
        final int is_typed                     = 10;
        final int commit_action                = 11;

        // Initialization
        tables = allTables();

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (!isAccessibleTable(table)) {
                continue;
            }

            row                = t.getEmptyRowData();
            row[table_catalog] = database.getCatalogName().name;
            row[table_schema]  = table.getSchemaName().name;
            row[table_name]    = table.getName().name;

            switch (table.getTableType()) {

                case TableBase.SYSTEM_TABLE :
                case TableBase.VIEW_TABLE :
                    row[table_type]         = "VIEW";
                    row[is_insertable_into] = "NO";
                    break;

                case TableBase.TEMP_TABLE :
                case TableBase.TEMP_TEXT_TABLE :
                    row[table_type]         = "GLOBAL TEMPORARY";
                    row[is_insertable_into] = "YES";
                    break;

                default :
                    row[table_type]         = "BASE TABLE";
                    row[is_insertable_into] = table.isWritable() ? "YES"
                                                                 : "NO";
                    break;
            }

            row[self_referencing_column_name] = null;
            row[reference_generation]         = null;
            row[user_defined_type_catalog]    = null;
            row[user_defined_type_schema]     = null;
            row[user_defined_type_name]       = null;
            row[is_typed]                     = "NO";
            row[commit_action] = table.isTemp()
                                 ? (table.onCommitPreserve() ? "PRESERVE"
                                                             : "DELETE")
                                 : null;

            t.insertSys(store, row);
        }

        return t;
    }

// -----------------------------------------------------------------------------
// SQL SCHEMATA BASE TABLE

    /**
     * Retrieves a <code>Table</code> object naming the accessible catalogs
     * defined within this database. <p>
     *
     * Each row is a catalog name description with the following column: <p>
     *
     * <pre class="SqlCodeExample">
     * TABLE_CAT   VARCHAR   catalog name
     * </pre> <p>
     *
     * @return a <code>Table</code> object naming the accessible
     *        catalogs defined within this database
     */
    final Table INFORMATION_SCHEMA_CATALOG_NAME() {

        Table t = sysTables[INFORMATION_SCHEMA_CATALOG_NAME];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[INFORMATION_SCHEMA_CATALOG_NAME]);

            addColumn(t, "CATALOG_NAME", SQL_IDENTIFIER);    // not null

            // order:  TABLE_CAT
            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[INFORMATION_SCHEMA_CATALOG_NAME].name,
                false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Object[]        row   = t.getEmptyRowData();

        row[0] = database.getCatalogName().name;

        t.insertSys(store, row);

        return t;
    }
}
