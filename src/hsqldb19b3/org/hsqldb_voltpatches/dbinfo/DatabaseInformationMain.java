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


package org.hsqldb_voltpatches.dbinfo;

import org.hsqldb_voltpatches.ColumnSchema;
import org.hsqldb_voltpatches.Constraint;
import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.Routine;
import org.hsqldb_voltpatches.RoutineSchema;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SqlInvariants;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.TypeInvariants;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.WrapperIterator;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;
import org.hsqldb_voltpatches.persist.HsqlProperties;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.rights.GrantConstants;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.rights.GranteeManager;
import org.hsqldb_voltpatches.rights.Right;
import org.hsqldb_voltpatches.rights.User;
import org.hsqldb_voltpatches.types.Charset;
import org.hsqldb_voltpatches.types.DateTimeType;
import org.hsqldb_voltpatches.types.IntervalType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/* $Id: DatabaseInformationMain.java 5342 2014-01-25 17:51:22Z fredt $ */

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
 * The views supported in this class are mainly those that are needed
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
 * Calls to setStore(String, Session) return a cached table if various
 * caching rules are met (see below), or it will delete all rows of the table
 * and rebuild the contents via generateTable(int).<p>
 *
 * generateTable(int) calls the appropriate single method for each table.
 * These methods either build and return an empty table (if sysTables
 * contains null for the table slot) or populate the table with up-to-date
 * rows. <p>
 *
 * Rules for caching are applied as follows: <p>
 *
 * If a table has non-cached contents, its contents are cleared and
 * rebuilt. <p>
 *
 * For the rest of the tables, if the table has not been built for the Session
 * object or it is out of date, the table contents are cleared and rebuilt. <p>
 *
 * (fredt@users) <p>
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.2
 * @since 1.7.2
 */
class DatabaseInformationMain extends DatabaseInformation {

    static Type CARDINAL_NUMBER = TypeInvariants.CARDINAL_NUMBER;
    static Type YES_OR_NO       = TypeInvariants.YES_OR_NO;
    static Type CHARACTER_DATA  = TypeInvariants.CHARACTER_DATA;
    static Type SQL_IDENTIFIER  = TypeInvariants.SQL_IDENTIFIER;
    static Type TIME_STAMP      = TypeInvariants.TIME_STAMP;

    /** The HsqlNames of the system tables. */
    protected static final HsqlName[] sysTableHsqlNames;

    /** true if the contents of a cached system table depends on the session */
    protected static final boolean[] sysTableSessionDependent =
        new boolean[sysTableNames.length];

    /** Set: { names of system tables that are not to be cached } */
    protected static final HashSet nonCachedTablesSet;

    /** The table types HSQLDB supports. */
    protected static final String[] tableTypes = new String[] {
        "GLOBAL TEMPORARY", "SYSTEM TABLE", "TABLE", "VIEW"
    };

    /** Provides naming support. */
    static {
        synchronized (DatabaseInformationMain.class) {
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
    }

    /** cache of system tables */
    protected final Table[] sysTables = new Table[sysTableNames.length];

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

        Session session = db.sessionManager.getSysSession();

        init(session);
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
    protected final void cacheClear(Session session) {

        int i = sysTables.length;

        while (i-- > 0) {
            Table t = sysTables[i];

            if (t != null) {
                t.clearAllData(session);
            }
        }
    }

    /**
     * Retrieves the system table corresponding to the specified
     * tableIndex value. <p>
     *
     * @param tableIndex int value identifying the system table to generate
     * @return the system table corresponding to the specified tableIndex value
     */
    protected Table generateTable(Session session, PersistentStore store,
                                  int tableIndex) {

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

            case SYSTEM_BESTROWIDENTIFIER :
                return SYSTEM_BESTROWIDENTIFIER(session, store);

            case SYSTEM_COLUMNS :
                return SYSTEM_COLUMNS(session, store);

            case SYSTEM_CONNECTION_PROPERTIES :
                return SYSTEM_CONNECTION_PROPERTIES(session, store);

            case SYSTEM_CROSSREFERENCE :
                return SYSTEM_CROSSREFERENCE(session, store);

            case SYSTEM_INDEXINFO :
                return SYSTEM_INDEXINFO(session, store);

            case SYSTEM_PRIMARYKEYS :
                return SYSTEM_PRIMARYKEYS(session, store);

            case SYSTEM_PROCEDURECOLUMNS :
                return SYSTEM_PROCEDURECOLUMNS(session, store);

            case SYSTEM_PROCEDURES :
                return SYSTEM_PROCEDURES(session, store);

            case SYSTEM_SCHEMAS :
                return SYSTEM_SCHEMAS(session, store);

            case SYSTEM_SEQUENCES :
                return SYSTEM_SEQUENCES(session, store);

            case SYSTEM_TABLES :
                return SYSTEM_TABLES(session, store);

            case SYSTEM_TABLETYPES :
                return SYSTEM_TABLETYPES(session, store);

            case SYSTEM_TYPEINFO :
                return SYSTEM_TYPEINFO(session, store);

            case SYSTEM_USERS :
                return SYSTEM_USERS(session, store);

            case SYSTEM_UDTS :
                return SYSTEM_UDTS(session, store);

            case SYSTEM_VERSIONCOLUMNS :
                return SYSTEM_VERSIONCOLUMNS(session, store);

            case COLUMN_PRIVILEGES :
                return COLUMN_PRIVILEGES(session, store);

            case SEQUENCES :
                return SEQUENCES(session, store);

            case TABLE_PRIVILEGES :
                return TABLE_PRIVILEGES(session, store);

            case INFORMATION_SCHEMA_CATALOG_NAME :
                return INFORMATION_SCHEMA_CATALOG_NAME(session, store);

            default :
                return null;
        }
    }

    /**
     * One time initialisation of instance attributes
     * at construction time. <p>
     *
     */
    protected final void init(Session session) {

        // flag the Session-dependent cached tables
        Table t;

        for (int i = 0; i < sysTables.length; i++) {
            t = sysTables[i] = generateTable(session, null, i);

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

        gm.grantSystemToPublic(Charset.SQL_CHARACTER, right);
        gm.grantSystemToPublic(Charset.SQL_IDENTIFIER_CHARSET, right);
        gm.grantSystemToPublic(Charset.SQL_TEXT, right);
        gm.grantSystemToPublic(TypeInvariants.SQL_IDENTIFIER, right);
        gm.grantSystemToPublic(TypeInvariants.YES_OR_NO, right);
        gm.grantSystemToPublic(TypeInvariants.TIME_STAMP, right);
        gm.grantSystemToPublic(TypeInvariants.CARDINAL_NUMBER, right);
        gm.grantSystemToPublic(TypeInvariants.CHARACTER_DATA, right);
    }

    /**
     * Retrieves whether any form of SQL access is allowed against the
     * the specified table w.r.t the database access rights
     * assigned to current Session object's User. <p>
     *
     * @return true if the table is accessible, else false
     * @param table the table for which to check accessibility
     */
    protected final boolean isAccessibleTable(Session session, Table table) {
        return session.getGrantee().isAccessible(table);
    }

    /**
     * Creates a new primoidal system table with the specified name. <p>
     *
     * @return a new system table
     * @param name of the table
     */
    protected final Table createBlankTable(HsqlName name) {

        Table table = new Table(database, name, TableBase.INFO_SCHEMA_TABLE);

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

        if (!isSystemTable(name)) {
            return null;
        }

        tableIndex = getSysTableID(name);
        t          = sysTables[tableIndex];

        // fredt - any system table that is not supported will be null here
        if (t == null) {
            return t;
        }

        // At the time of opening the database, no content is needed.
        // However, table structure is required at this
        // point to allow processing logged View defn's against system
        // tables
        if (!withContent) {
            return t;
        }

        return t;
    }

    public boolean isNonCachedTable(String name) {
        return nonCachedTablesSet.contains(name);
    }

    public final void setStore(Session session, Table table,
                               PersistentStore store) {

        long dbscts = database.schemaManager.getSchemaChangeTimestamp();

        if (store.getTimestamp() == dbscts
                && !isNonCachedTable(table.getName().name)) {
            return;
        }

        // fredt - clear the contents of table and generate
        store.removeAll();
        store.setTimestamp(dbscts);

        int tableIndex = getSysTableID(table.getName().name);

        generateTable(session, store, tableIndex);
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
    final Table SYSTEM_BESTROWIDENTIFIER(Session session,
                                         PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 8, 9, 10, 1
            }, false);

            return t;
        }

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

        boolean translateTTI = database.sqlTranslateTTI;

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            /** @todo - requires access to the actual columns */
            if (table.isView() || !isAccessibleTable(session, table)) {
                continue;
            }

            cols = table.getBestRowIdentifiers();

            if (cols == null) {
                continue;
            }

            ti.setTable(table);

            inKey        = table.isBestRowIdentifiersStrict() ? Boolean.TRUE
                                                              : Boolean.FALSE;
            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;
            tableName    = table.getName().name;

            Type[] types = table.getColumnTypes();

            scope  = ti.getBRIScope();
            pseudo = ti.getBRIPseudo();

            for (int i = 0; i < cols.length; i++) {
                ColumnSchema column = table.getColumn(i);
                Type         type   = types[i];

                if (translateTTI) {
                    if (type.isIntervalType()) {
                        type = ((IntervalType) type).getCharacterType();
                    } else if (type.isDateTimeTypeWithZone()) {
                        type = ((DateTimeType) type)
                            .getDateTimeTypeWithoutZone();
                    }
                }

                row                 = t.getEmptyRowData();
                row[iscope]         = scope;
                row[icolumn_name]   = column.getName().name;
                row[idata_type]     = ValuePool.getInt(type.getJDBCTypeCode());
                row[itype_name]     = type.getNameString();
                row[icolumn_size] = ValuePool.getInt(type.getJDBCPrecision());
                row[ibuffer_length] = null;
                row[idecimal_digits] = type.acceptsScale()
                                       ? ValuePool.getInt(type.getJDBCScale())
                                       : null;
                row[ipseudo_column] = pseudo;
                row[itable_cat]     = tableCatalog;
                row[itable_schem]   = tableSchema;
                row[itable_name]    = tableName;
                row[inullable] = ValuePool.getInt(column.getNullability());
                row[iinKey]         = inKey;

                t.insertSys(session, store, row);
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
     * CHAR_OCTET_LENGTH INTEGER   for char types, max # of chars/bytes in column
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
    final Table SYSTEM_COLUMNS(Session session, PersistentStore store) {

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
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);          // 18
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);           // 19
            addColumn(t, "SCOPE_TABLE", SQL_IDENTIFIER);            // 20
            addColumn(t, "SOURCE_DATA_TYPE", SQL_IDENTIFIER);       // 21

            // ----------------------------------------------------------------
            // JDBC 4.0 - added Mustang b86
            // ----------------------------------------------------------------
            addColumn(t, "IS_AUTOINCREMENT", YES_OR_NO);            // 22

            // ----------------------------------------------------------------
            // JDBC 4.1
            // ----------------------------------------------------------------
            addColumn(t, "IS_GENERATEDCOLUMN", YES_OR_NO);          // 23

            // order: TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
            // added for unique: TABLE_CAT
            // false PK, as TABLE_SCHEM and/or TABLE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_COLUMNS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 16
            }, false);

            return t;
        }

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
        final int isource_data_type  = 21;

        // JDBC 4.0
        final int iis_autoinc = 22;

        // JDBC 4.1
        final int iis_generated = 23;

        // Initialization
        tables = allTables();
        ti     = new DITableInfo();

        boolean translateTTI = database.sqlTranslateTTI;

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            /** @todo - requires access to the actual columns */
            if (!isAccessibleTable(session, table)) {
                continue;
            }

            ti.setTable(table);

            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;
            tableName    = table.getName().name;
            columnCount  = table.getColumnCount();

            for (int i = 0; i < columnCount; i++) {
                ColumnSchema column = table.getColumn(i);
                Type         type   = column.getDataType();

                if (translateTTI) {
                    if (type.isIntervalType()) {
                        type = ((IntervalType) type).getCharacterType();
                    } else if (type.isDateTimeTypeWithZone()) {
                        type = ((DateTimeType) type)
                            .getDateTimeTypeWithoutZone();
                    }
                }

                row = t.getEmptyRowData();

                //
                row[itable_cat]         = tableCatalog;
                row[itable_schem]       = tableSchema;
                row[itable_name]        = tableName;
                row[icolumn_name]       = column.getName().name;
                row[idata_type] = ValuePool.getInt(type.getJDBCTypeCode());
                row[itype_name]         = type.getNameString();
                row[icolumn_size]       = ValuePool.INTEGER_0;
                row[ichar_octet_length] = ValuePool.INTEGER_0;

                if (type.isArrayType()) {
                    row[itype_name] = type.getDefinition();
                }

                if (type.isCharacterType()) {
                    row[icolumn_size] =
                        ValuePool.getInt(type.getJDBCPrecision());

                    // this is length not octet_length, for character columns
                    row[ichar_octet_length] =
                        ValuePool.getInt(type.getJDBCPrecision());
                }

                if (type.isBinaryType()) {
                    row[icolumn_size] =
                        ValuePool.getInt(type.getJDBCPrecision());
                    row[ichar_octet_length] =
                        ValuePool.getInt(type.getJDBCPrecision());
                }

                if (type.isNumberType()) {
                    row[icolumn_size] = ValuePool.getInt(
                        ((NumberType) type).getNumericPrecisionInRadix());
                    row[inum_prec_radix] =
                        ValuePool.getInt(type.getPrecisionRadix());

                    if (type.isExactNumberType()) {
                        row[idecimal_digits] = ValuePool.getLong(type.scale);
                    }
                }

                if (type.isDateTimeType()) {
                    int size = (int) column.getDataType().displaySize();

                    row[icolumn_size] = ValuePool.getInt(size);
                    row[isql_datetime_sub] = ValuePool.getInt(
                        ((DateTimeType) type).getSqlDateTimeSub());
                }

                row[inullable] = ValuePool.getInt(column.getNullability());
                row[iremark]           = ti.getColRemarks(i);
                row[icolumn_def]       = column.getDefaultSQL();
                row[isql_data_type]    = ValuePool.getInt(type.typeCode);
                row[iordinal_position] = ValuePool.getInt(i + 1);
                row[iis_nullable]      = column.isNullable() ? "YES"
                                                             : "NO";

                if (type.isDistinctType()) {
                    row[isource_data_type] =
                        type.getName().getSchemaQualifiedStatementName();
                }

                // JDBC 4.0
                row[iis_autoinc]   = column.isIdentity() ? "YES"
                                                         : "NO";
                row[iis_generated] = column.isGenerated() ? "YES"
                                                          : "NO";

                t.insertSys(session, store, row);
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
    final Table SYSTEM_CROSSREFERENCE(Session session, PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[] {
                4, 5, 6, 8, 11
            }, false);

            return t;
        }

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

            if (table.isView() || !isAccessibleTable(session, table)) {
                continue;
            }

            constraints     = table.getConstraints();
            constraintCount = constraints.length;

            for (int i = 0; i < constraintCount; i++) {
                constraint = (Constraint) constraints[i];

                if (constraint.getConstraintType() == SchemaObject
                        .ConstraintTypes
                        .FOREIGN_KEY && isAccessibleTable(session, constraint
                            .getRef())) {
                    fkConstraintsList.add(constraint);
                }
            }
        }

        // Now that we have all of the desired constraints, we need to
        // process them, generating one row in our ouput table for each
        // imported/exported column pair of each constraint.
        // Do it.
        for (int i = 0; i < fkConstraintsList.size(); i++) {
            constraint     = (Constraint) fkConstraintsList.get(i);
            pkTable        = constraint.getMain();
            pkTableName    = pkTable.getName().name;
            fkTable        = constraint.getRef();
            fkTableName    = fkTable.getName().name;
            pkTableCatalog = pkTable.getCatalogName().name;
            pkTableSchema  = pkTable.getSchemaName().name;
            fkTableCatalog = fkTable.getCatalogName().name;
            fkTableSchema  = fkTable.getSchemaName().name;
            mainCols       = constraint.getMainColumns();
            refCols        = constraint.getRefColumns();
            columnCount    = refCols.length;
            fkName         = constraint.getRefName().name;
            pkName         = constraint.getUniqueName().name;
            deferrability  = ValuePool.getInt(constraint.getDeferability());

            //pkName = constraint.getMainIndex().getName().name;
            deleteRule = ValuePool.getInt(constraint.getDeleteAction());
            updateRule = ValuePool.getInt(constraint.getUpdateAction());

            for (int j = 0; j < columnCount; j++) {
                keySequence          = ValuePool.getInt(j + 1);
                pkColumnName = pkTable.getColumn(mainCols[j]).getNameString();
                fkColumnName = fkTable.getColumn(refCols[j]).getNameString();
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

                t.insertSys(session, store, row);
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
    final Table SYSTEM_INDEXINFO(Session session, PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 7, 8
            }, false);

            return t;
        }

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
        Iterator tables;
        Table    table;
        int      indexCount;
        int[]    cols;
        int      col;
        int      colCount;
        Object[] row;

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
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView() || !isAccessibleTable(session, table)) {
                continue;
            }

            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;
            tableName    = table.getName().name;

            // not supported yet
            filterCondition = null;

            // different cat for index not supported yet
            indexQualifier = tableCatalog;
            indexCount     = table.getIndexCount();

            // process all of the visible indices for this table
            for (int i = 0; i < indexCount; i++) {
                Index index = table.getIndex(i);

                colCount = table.getIndex(i).getColumnCount();

                if (colCount < 1) {
                    continue;
                }

                indexName      = index.getName().name;
                nonUnique      = index.isUnique() ? Boolean.FALSE
                                                  : Boolean.TRUE;
                cardinality    = null;
                pages          = ValuePool.INTEGER_0;
                rowCardinality = null;
                cols           = index.getColumns();
                indexType      = ValuePool.getInt(3);

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
                    row[icolumn_name] =
                        table.getColumn(cols[k]).getName().name;
                    row[iasc_or_desc]      = "A";
                    row[icardinality]      = cardinality;
                    row[ipages]            = pages;
                    row[irow_cardinality]  = rowCardinality;
                    row[ifilter_condition] = filterCondition;

                    t.insertSys(session, store, row);
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
    final Table SYSTEM_PRIMARYKEYS(Session session, PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[] {
                3, 2, 1, 0
            }, false);

            return t;
        }

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
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView() || !isAccessibleTable(session, table)
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

                t.insertSys(session, store, row);
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
    Table SYSTEM_PROCEDURECOLUMNS(Session session, PersistentStore store) {

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
            addColumn(t, "ORDINAL_POSITION", Type.SQL_INTEGER);     // 17
            addColumn(t, "IS_NULLABLE", CHARACTER_DATA);            // 18
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);          // 19

            // order: PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME, ORDINAL_POSITION
            //
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PROCEDURECOLUMNS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 19, 17
            }, false);

            return t;
        }

        // column number mappings
        final int specific_cat            = 0;
        final int specific_schem          = 1;
        final int procedure_name          = 2;
        final int parameter_name          = 3;
        final int parameter_mode          = 4;
        final int data_type_sql_id        = 5;
        final int data_type               = 6;
        final int numeric_precision       = 7;
        final int byte_length             = 8;
        final int numeric_scale           = 9;
        final int numeric_precision_radix = 10;
        final int nullable                = 11;
        final int remark                  = 12;
        final int default_val             = 13;
        final int sql_data_type           = 14;
        final int sql_datetime_sub        = 15;
        final int character_octet_length  = 16;
        final int ordinal_position        = 17;
        final int is_nullable             = 18;
        final int specific_name           = 19;

        // intermediate holders
        int           columnCount;
        Iterator      routines;
        RoutineSchema routineSchema;
        Routine       routine;
        Object[]      row;
        Type          type;

        // Initialization
        boolean translateTTI = database.sqlTranslateTTI;

        routines = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);

        while (routines.hasNext()) {
            routineSchema = (RoutineSchema) routines.next();

            if (!session.getGrantee().isAccessible(routineSchema)) {
                continue;
            }

            Routine[] specifics = routineSchema.getSpecificRoutines();

            for (int i = 0; i < specifics.length; i++) {
                routine     = specifics[i];
                columnCount = routine.getParameterCount();

                for (int j = 0; j < columnCount; j++) {
                    ColumnSchema column = routine.getParameter(j);

                    row  = t.getEmptyRowData();
                    type = column.getDataType();

                    if (translateTTI) {
                        if (type.isIntervalType()) {
                            type = ((IntervalType) type).getCharacterType();
                        } else if (type.isDateTimeTypeWithZone()) {
                            type = ((DateTimeType) type)
                                .getDateTimeTypeWithoutZone();
                        }
                    }

                    row[specific_cat]     = database.getCatalogName().name;
                    row[specific_schem]   = routine.getSchemaName().name;
                    row[specific_name]    = routine.getSpecificName().name;
                    row[procedure_name]   = routine.getName().name;
                    row[parameter_name]   = column.getName().name;
                    row[ordinal_position] = ValuePool.getInt(j + 1);
                    row[parameter_mode] =
                        ValuePool.getInt(column.getParameterMode());
                    row[data_type] = type.getFullNameString();
                    row[data_type_sql_id] =
                        ValuePool.getInt(type.getJDBCTypeCode());
                    row[numeric_precision]      = ValuePool.INTEGER_0;
                    row[character_octet_length] = ValuePool.INTEGER_0;

                    if (type.isCharacterType()) {
                        row[numeric_precision] =
                            ValuePool.getInt(type.getJDBCPrecision());

                        // this is length not octet_length, for character columns
                        row[character_octet_length] =
                            ValuePool.getInt(type.getJDBCPrecision());
                    }

                    if (type.isBinaryType()) {
                        row[numeric_precision] =
                            ValuePool.getInt(type.getJDBCPrecision());
                        row[character_octet_length] =
                            ValuePool.getInt(type.getJDBCPrecision());
                    }

                    if (type.isNumberType()) {
                        row[numeric_precision] = ValuePool.getInt(
                            ((NumberType) type).getNumericPrecisionInRadix());
                        row[numeric_precision_radix] =
                            ValuePool.getLong(type.getPrecisionRadix());

                        if (type.isExactNumberType()) {
                            row[numeric_scale] = ValuePool.getLong(type.scale);
                        }
                    }

                    if (type.isDateTimeType()) {
                        int size = (int) column.getDataType().displaySize();

                        row[numeric_precision] = ValuePool.getInt(size);
                    }

                    row[sql_data_type] =
                        ValuePool.getInt(column.getDataType().typeCode);
                    row[nullable] = ValuePool.getInt(column.getNullability());
                    row[is_nullable] = column.isNullable() ? "YES"
                                                           : "NO";

                    t.insertSys(session, store, row);
                }
            }
        }

        return t;
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
     * COL_4             INTEGER   unused
     * COL_5             INTEGER   unused
     * COL_6             INTEGER   unused
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
    Table SYSTEM_PROCEDURES(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_PROCEDURES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PROCEDURES]);

            // ----------------------------------------------------------------
            // required
            // ----------------------------------------------------------------
            addColumn(t, "PROCEDURE_CAT", SQL_IDENTIFIER);        // 0
            addColumn(t, "PROCEDURE_SCHEM", SQL_IDENTIFIER);      // 1
            addColumn(t, "PROCEDURE_NAME", SQL_IDENTIFIER);       // 2
            addColumn(t, "COL_4", Type.SQL_INTEGER);              // 3
            addColumn(t, "COL_5", Type.SQL_INTEGER);              // 4
            addColumn(t, "COL_6", Type.SQL_INTEGER);              // 5
            addColumn(t, "REMARKS", CHARACTER_DATA);              // 6

            // basically: function (returns result), procedure (no return value)
            // or unknown (say, a trigger callout routine)
            addColumn(t, "PROCEDURE_TYPE", Type.SQL_SMALLINT);    // 7

            // ----------------------------------------------------------------
            // JDBC 4.0
            // ----------------------------------------------------------------
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);        // 8

            // ----------------------------------------------------------------
            // order: PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PROCEDURES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 8
            }, false);

            return t;
        }

        //
        final int procedure_catalog = 0;
        final int procedure_schema  = 1;
        final int procedure_name    = 2;
        final int col_4             = 3;
        final int col_5             = 4;
        final int col_6             = 5;
        final int remarks           = 6;
        final int procedure_type    = 7;
        final int specific_name     = 8;

        //
        Iterator it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);

        while (it.hasNext()) {
            Routine  routine = (Routine) it.next();
            Object[] row     = t.getEmptyRowData();

            row[procedure_catalog] = row[procedure_catalog] =
                database.getCatalogName().name;
            row[procedure_schema] = routine.getSchemaName().name;
            row[procedure_name]   = routine.getName().name;
            row[remarks]          = routine.getName().comment;
            row[procedure_type] = routine.isProcedure() ? ValuePool.INTEGER_1
                                                        : ValuePool.INTEGER_2;
            row[specific_name]    = routine.getSpecificName().name;

            t.insertSys(session, store, row);
        }

        return t;
    }

    /**
     * getClientInfoProperties
     *
     * @return Result
     *
     * <li><b>NAME</b> String=> The name of the client info property<br>
     * <li><b>MAX_LEN</b> int=> The maximum length of the value for the property<br>
     * <li><b>DEFAULT_VALUE</b> String=> The default value of the property<br>
     * <li><b>DESCRIPTION</b> String=> A description of the property.  This will typically
     *                                                  contain information as to where this property is
     *                                                  stored in the database.
     */
    final Table SYSTEM_CONNECTION_PROPERTIES(Session session,
            PersistentStore store) {

        Table t = sysTables[SYSTEM_CONNECTION_PROPERTIES];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[SYSTEM_CONNECTION_PROPERTIES]);

            addColumn(t, "NAME", SQL_IDENTIFIER);
            addColumn(t, "MAX_LEN", Type.SQL_INTEGER);
            addColumn(t, "DEFAULT_VALUE", SQL_IDENTIFIER);    // not null
            addColumn(t, "DESCRIPTION", SQL_IDENTIFIER);      // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PRIMARYKEYS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

        Object[] row;

        // column number mappings
        final int iname          = 0;
        final int imax_len       = 1;
        final int idefault_value = 2;
        final int idescription   = 3;
        Iterator  it = HsqlDatabaseProperties.getPropertiesMetaIterator();

        while (it.hasNext()) {
            Object[] meta = (Object[]) it.next();
            int propType =
                ((Integer) meta[HsqlProperties.indexType]).intValue();

            if (propType == HsqlDatabaseProperties.FILE_PROPERTY) {
                if (HsqlDatabaseProperties.hsqldb_readonly.equals(
                        meta[HsqlProperties.indexName]) || HsqlDatabaseProperties
                            .hsqldb_files_readonly.equals(
                                meta[HsqlProperties.indexName])) {}
                else {
                    continue;
                }
            } else if (propType != HsqlDatabaseProperties.SQL_PROPERTY) {
                continue;
            }

            row = t.getEmptyRowData();

            Object def = meta[HsqlProperties.indexDefaultValue];

            row[iname]          = meta[HsqlProperties.indexName];
            row[imax_len]       = ValuePool.getInt(8);
            row[idefault_value] = def == null ? null
                                              : def.toString();
            row[idescription]   = "see HyperSQL guide";

            t.insertSys(session, store, row);
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
    protected void addProcRows(Session session, Table t, HsqlArrayList l,
                               String cat, String schem, String pName,
                               Integer ip, Integer op, Integer rs,
                               String remark, Integer pType,
                               String specificName, String origin) {

        PersistentStore store = t.getRowStore(session);

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

        t.insertSys(session, store, row);

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

                t.insertSys(session, store, row);
            }
        }
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
     */
    protected void addPColRows(Session session, Table t, HsqlArrayList l,
                               String cat, String schem, String pName,
                               String cName, Integer cType, Integer dType,
                               String tName, Integer prec, Integer len,
                               Integer scale, Integer radix,
                               Integer nullability, String remark,
                               String colDefault, Integer sqlDataType,
                               Integer sqlDateTimeSub,
                               Integer charOctetLength,
                               Integer ordinalPosition, String isNullable,
                               String specificName) {

        PersistentStore store = t.getRowStore(session);

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

        // initialization
        Object[] row = t.getEmptyRowData();

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

        t.insertSys(session, store, row);

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

                t.insertSys(session, store, row);
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
    final Table SYSTEM_SCHEMAS(Session session, PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

        Object[] row;

        // Initialization
        String[] schemas = database.schemaManager.getSchemaNamesArray();
        String defschema =
            database.schemaManager.getDefaultSchemaHsqlName().name;

        // Do it.
        for (int i = 0; i < schemas.length; i++) {
            row = t.getEmptyRowData();

            String schema = schemas[i];

            row[0] = schema;
            row[1] = database.getCatalogName().name;
            row[2] = schema.equals(defschema) ? Boolean.TRUE
                                              : Boolean.FALSE;

            t.insertSys(session, store, row);
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
    final Table SYSTEM_TABLES(Session session, PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[] {
                3, 1, 2, 0
            }, false);

            return t;
        }

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

        // hsqldb_voltpatches ext
        final int ihsqldb_type   = 10;
        final int iread_only     = 11;
        final int icommit_action = 12;

        // Initialization
        tables = allTables();
        ti     = new DITableInfo();

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (!isAccessibleTable(session, table)) {
                continue;
            }

            ti.setTable(table);

            row               = t.getEmptyRowData();
            row[itable_cat]   = database.getCatalogName().name;
            row[itable_schem] = table.getSchemaName().name;
            row[itable_name]  = table.getName().name;
            row[itable_type]  = ti.getJDBCStandardType();
            row[iremark]      = ti.getRemark();
            row[ihsqldb_type] = ti.getHsqlType();
            row[iread_only]   = table.isDataReadOnly() ? Boolean.TRUE
                                                       : Boolean.FALSE;
            row[icommit_action] = table.isTemp()
                                  ? (table.onCommitPreserve() ? "PRESERVE"
                                                              : "DELETE")
                                  : null;

            t.insertSys(session, store, row);
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
    Table SYSTEM_TABLETYPES(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_TABLETYPES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TABLETYPES]);

            addColumn(t, "TABLE_TYPE", SQL_IDENTIFIER);    // not null

            // order: TABLE_TYPE
            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TABLETYPES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

        Object[] row;

        for (int i = 0; i < tableTypes.length; i++) {
            row    = t.getEmptyRowData();
            row[0] = tableTypes[i];

            t.insertSys(session, store, row);
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
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the
     *      system-defined SQL types supported as table columns
     */
    final Table SYSTEM_TYPEINFO(Session session, PersistentStore store) {

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
            // SQL CLI / ODBC - not in JDBC spec
            // ------------------------------------------
            addColumn(t, "INTERVAL_PRECISION", Type.SQL_INTEGER);

            // order:  DATA_TYPE, TYPE_NAME
            // true primary key
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TYPEINFO].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                1, 0
            }, true);

            return t;
        }

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
        Object[]  row;
        Iterator  it           = Type.typeNames.keySet().iterator();
        boolean   translateTTI = database.sqlTranslateTTI;

        while (it.hasNext()) {
            String typeName = (String) it.next();
            int    typeCode = Type.typeNames.get(typeName);
            Type   type     = Type.getDefaultType(typeCode);

            if (type == null) {
                continue;
            }

            if (translateTTI) {
                if (type.isIntervalType()) {
                    type = ((IntervalType) type).getCharacterType();
                } else if (type.isDateTimeTypeWithZone()) {
                    type = ((DateTimeType) type).getDateTimeTypeWithoutZone();
                }
            }

            row             = t.getEmptyRowData();
            row[itype_name] = typeName;
            row[idata_type] = ValuePool.getInt(type.getJDBCTypeCode());

            long maxPrecision = type.getMaxPrecision();

            row[iprecision] = maxPrecision > Integer.MAX_VALUE
                              ? ValuePool.INTEGER_MAX
                              : ValuePool.getInt((int) maxPrecision);

            if (type.isBinaryType() || type.isCharacterType()
                    || type.isDateTimeType() || type.isIntervalType()) {
                row[iliteral_prefix] = "\'";
                row[iliteral_suffix] = "\'";
            }

            if (type.acceptsPrecision() && type.acceptsScale()) {
                row[icreate_params] = "PRECISION,SCALE";
            } else if (type.acceptsPrecision()) {
                row[icreate_params] = type.isNumberType() ? "PRECISION"
                                                          : "LENGTH";
            } else if (type.acceptsScale()) {
                row[icreate_params] = "SCALE";
            }

            row[inullable] = ValuePool.INTEGER_1;
            row[icase_sensitive] =
                type.isCharacterType()
                && type.getCollation().isCaseSensitive() ? Boolean.TRUE
                                                         : Boolean.FALSE;

            if (type.isLobType()) {
                row[isearchable] = ValuePool.INTEGER_0;
            } else if (type.isCharacterType()
                       || (type.isBinaryType() && !type.isBitType())) {
                row[isearchable] = ValuePool.getInt(3);
            } else {
                row[isearchable] = ValuePool.getInt(2);
            }

            row[iunsigned_attribute] = Boolean.FALSE;
            row[ifixed_prec_scale] =
                type.typeCode == Types.SQL_NUMERIC
                || type.typeCode == Types.SQL_DECIMAL ? Boolean.TRUE
                                                      : Boolean.FALSE;
            row[iauto_increment]   = type.isIntegralType() ? Boolean.TRUE
                                                           : Boolean.FALSE;
            row[ilocal_type_name]  = null;
            row[iminimum_scale]    = ValuePool.INTEGER_0;
            row[imaximum_scale]    = ValuePool.getInt(type.getMaxScale());
            row[isql_data_type]    = null;
            row[isql_datetime_sub] = null;
            row[inum_prec_radix] = ValuePool.getInt(type.getPrecisionRadix());

            //------------------------------------------
            if (type.isIntervalType()) {
                row[iinterval_precision] = null;
            }

            t.insertSys(session, store, row);
        }

        row             = t.getEmptyRowData();
        row[itype_name] = "DISTINCT";
        row[idata_type] = ValuePool.getInt(Types.DISTINCT);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * user-defined types defined in this database. <p>
     *
     * Schema-specific UDTs may have type JAVA_OBJECT, STRUCT, or DISTINCT.
     *
     * <P>Each row is a UDT descripion with the following columns:
     * <OL>
     *   <LI><B>TYPE_CAT</B> <code>VARCHAR</code> => the type's catalog
     *   <LI><B>TYPE_SCHEM</B> <code>VARCHAR</code> => type's schema
     *   <LI><B>TYPE_NAME</B> <code>VARCHAR</code> => type name
     *   <LI><B>CLASS_NAME</B> <code>VARCHAR</code> => Java class name
     *   <LI><B>DATA_TYPE</B> <code>VARCHAR</code> =>
     *         type value defined in <code>DITypes</code>;
     *         one of <code>JAVA_OBJECT</code>, <code>STRUCT</code>, or
     *        <code>DISTINCT</code>
     *   <LI><B>REMARKS</B> <code>VARCHAR</code> =>
     *          explanatory comment on the type
     *   <LI><B>BASE_TYPE</B><code>SMALLINT</code> =>
     *          type code of the source type of a DISTINCT type or the
     *          type that implements the user-generated reference type of the
     *          SELF_REFERENCING_COLUMN of a structured type as defined in
     *          DITypes (null if DATA_TYPE is not DISTINCT or not
     *          STRUCT with REFERENCE_GENERATION = USER_DEFINED)
     *
     * </OL> <p>
     *
     * @return a <code>Table</code> object describing the accessible
     *      user-defined types defined in this database
     */
    Table SYSTEM_UDTS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_UDTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_UDTS]);

            addColumn(t, "TYPE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TYPE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "CLASS_NAME", CHARACTER_DATA);
            addColumn(t, "DATA_TYPE", Type.SQL_INTEGER);
            addColumn(t, "REMARKS", CHARACTER_DATA);
            addColumn(t, "BASE_TYPE", Type.SQL_SMALLINT);

            //
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_UDTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, null, false);

            return t;
        }

        boolean translateTTI = database.sqlTranslateTTI;

        // column number mappings
        final int type_catalog = 0;
        final int type_schema  = 1;
        final int type_name    = 2;
        final int class_name   = 3;
        final int data_type    = 4;
        final int remarks      = 5;
        final int base_type    = 6;
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.TYPE);

        while (it.hasNext()) {
            Type distinct = (Type) it.next();

            if (!distinct.isDistinctType()) {
                continue;
            }

            Object[] data = t.getEmptyRowData();
            Type     type = distinct;

            if (translateTTI) {
                if (type.isIntervalType()) {
                    type = ((IntervalType) type).getCharacterType();
                } else if (type.isDateTimeTypeWithZone()) {
                    type = ((DateTimeType) type).getDateTimeTypeWithoutZone();
                }
            }

            data[type_catalog] = database.getCatalogName().name;
            data[type_schema]  = distinct.getSchemaName().name;
            data[type_name]    = distinct.getName().name;
            data[class_name]   = type.getJDBCClassName();
            data[data_type]    = ValuePool.getInt(Types.DISTINCT);
            data[remarks]      = null;
            data[base_type]    = ValuePool.getInt(type.getJDBCTypeCode());

            t.insertSys(session, store, data);
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the accessible
     * columns that are automatically updated when any value in a row
     * is updated. <p>
     *
     * Each row is a version column description with the following columns: <p>
     *
     * <OL>
     * <LI><B>SCOPE</B> <code>SMALLINT</code> => is not used
     * <LI><B>COLUMN_NAME</B> <code>VARCHAR</code> => column name
     * <LI><B>DATA_TYPE</B> <code>SMALLINT</code> =>
     *        SQL data type from java.sql.Types
     * <LI><B>TYPE_NAME</B> <code>SMALLINT</code> =>
     *       Data source dependent type name
     * <LI><B>COLUMN_SIZE</B> <code>INTEGER</code> => precision
     * <LI><B>BUFFER_LENGTH</B> <code>INTEGER</code> =>
     *        length of column value in bytes
     * <LI><B>DECIMAL_DIGITS</B> <code>SMALLINT</code> => scale
     * <LI><B>PSEUDO_COLUMN</B> <code>SMALLINT</code> =>
     *        is this a pseudo column like an Oracle <code>ROWID</code>:<BR>
     *        (as defined in <code>java.sql.DatabaseMetadata</code>)
     * <UL>
     *    <LI><code>versionColumnUnknown</code> - may or may not be
     *        pseudo column
     *    <LI><code>versionColumnNotPseudo</code> - is NOT a pseudo column
     *    <LI><code>versionColumnPseudo</code> - is a pseudo column
     * </UL>
     * </OL> <p>
     *
     * <B>Note:</B> Currently, the HSQLDB engine does not support version
     * columns, so an empty table is returned. <p>
     *
     * @return a <code>Table</code> object describing the columns
     *        that are automatically updated when any value
     *        in a row is updated
     */
    Table SYSTEM_VERSIONCOLUMNS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_VERSIONCOLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_VERSIONCOLUMNS]);

            // ----------------------------------------------------------------
            // required by DatabaseMetaData.getVersionColumns result set
            // ----------------------------------------------------------------
            addColumn(t, "SCOPE", Type.SQL_INTEGER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);         // not null
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);        // not null
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);           // not null
            addColumn(t, "COLUMN_SIZE", Type.SQL_SMALLINT);
            addColumn(t, "BUFFER_LENGTH", Type.SQL_INTEGER);
            addColumn(t, "DECIMAL_DIGITS", Type.SQL_SMALLINT);
            addColumn(t, "PSEUDO_COLUMN", Type.SQL_SMALLINT);    // not null

            // -----------------------------------------------------------------
            // required by DatabaseMetaData.getVersionColumns filter parameters
            // -----------------------------------------------------------------
            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);          // not null

            // -----------------------------------------------------------------
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_VERSIONCOLUMNS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, null, false);

            return t;
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the
     * visible <code>Users</code> defined within this database.
     * @return table containing information about the users defined within
     *      this database
     */
    Table SYSTEM_USERS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_USERS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_USERS]);

            addColumn(t, "USER_NAME", SQL_IDENTIFIER);
            addColumn(t, "ADMIN", Type.SQL_BOOLEAN);
            addColumn(t, "INITIAL_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "AUTHENTICATION", SQL_IDENTIFIER);
            addColumn(t, "PASSWORD_DIGEST", SQL_IDENTIFIER);

            // order: USER
            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_USERS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

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
            row[0]        = user.getName().getNameString();
            row[1]        = user.isAdmin() ? Boolean.TRUE
                                           : Boolean.FALSE;
            row[2]        = ((initialSchema == null) ? null
                                                     : initialSchema.name);
            row[3]        = user.isLocalOnly ? Tokens.T_LOCAL
                                             : user.isExternalOnly
                                               ? Tokens.T_EXTERNAL
                                               : Tokens.T_ANY;
            row[4] = user.getPasswordDigest();

            t.insertSys(session, store, row);
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
    final Table COLUMN_PRIVILEGES(Session session, PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[] {
                2, 3, 4, 5, 6, 1, 0
            }, false);

            return t;
        }

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
                                t.insertSys(session, store, row);
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
     *      DECLARED_NUMERIC_SCALE     CARDINAL_NUMBER
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
    final Table SEQUENCES(Session session, PersistentStore store) {

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
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);

            // HSQLDB-specific
            addColumn(t, "START_WITH", CHARACTER_DATA);
            addColumn(t, "NEXT_VALUE", CHARACTER_DATA);

            // order SEQUENCE_CATALOG, SEQUENCE_SCHEMA, SEQUENCE_NAME
            // false PK, as CATALOG may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SEQUENCES].name, false, SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

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

            t.insertSys(session, store, row);
        }

        return t;
    }

    final Table SYSTEM_SEQUENCES(Session session, PersistentStore store) {

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
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);

            // HSQLDB-specific
            addColumn(t, "START_WITH", CHARACTER_DATA);
            addColumn(t, "NEXT_VALUE", CHARACTER_DATA);

            // order SEQUENCE_CATALOG, SEQUENCE_SCHEMA, SEQUENCE_NAME
            // false PK, as CATALOG may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_SEQUENCES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

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

            t.insertSys(session, store, row);
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
    final Table TABLE_PRIVILEGES(Session session, PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

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
                        if (!right.canAccessFully(Right.privilegeTypes[k])) {
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
                            || grantableRight.canAccessFully(
                                Right.privilegeTypes[k]) ? "YES"
                                                         : "NO";
                        row[with_hierarchy] = "NO";

                        try {
                            t.insertSys(session, store, row);
                        } catch (HsqlException e) {}
                    }
                }
            }
        }

        return t;
    }

    Table TABLES(Session session, PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2,
            }, false);

            return t;
        }

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

            if (!isAccessibleTable(session, table)) {
                continue;
            }

            row                = t.getEmptyRowData();
            row[table_catalog] = database.getCatalogName().name;
            row[table_schema]  = table.getSchemaName().name;
            row[table_name]    = table.getName().name;

            switch (table.getTableType()) {

                case TableBase.INFO_SCHEMA_TABLE :
                case TableBase.VIEW_TABLE :
                    row[table_type] = "VIEW";
                    row[is_insertable_into] = table.isInsertable()
                                              ? Tokens.T_YES
                                              : Tokens.T_NO;
                    break;

                case TableBase.TEMP_TABLE :
                case TableBase.TEMP_TEXT_TABLE :
                    row[table_type]         = "GLOBAL TEMPORARY";
                    row[is_insertable_into] = "YES";
                    break;

                default :
                    row[table_type] = "BASE TABLE";
                    row[is_insertable_into] = table.isInsertable()
                                              ? Tokens.T_YES
                                              : Tokens.T_NO;
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

            t.insertSys(session, store, row);
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
    final Table INFORMATION_SCHEMA_CATALOG_NAME(Session session,
            PersistentStore store) {

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

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

        Object[] row = t.getEmptyRowData();

        row[0] = database.getCatalogName().name;

        t.insertSys(session, store, row);

        return t;
    }
}
