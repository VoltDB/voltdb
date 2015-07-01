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

import org.hsqldb_voltpatches.Collation;
import org.hsqldb_voltpatches.ColumnSchema;
import org.hsqldb_voltpatches.Constraint;
import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Expression;
import org.hsqldb_voltpatches.ExpressionColumn;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.Routine;
import org.hsqldb_voltpatches.RoutineSchema;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.SchemaObjectSet;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SqlInvariants;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TextTable;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.TriggerDef;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.View;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.FileUtil;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.lib.WrapperIterator;
import org.hsqldb_voltpatches.persist.DataFileCache;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;
import org.hsqldb_voltpatches.persist.HsqlProperties;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.persist.TextCache;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.rights.Right;
import org.hsqldb_voltpatches.scriptio.ScriptWriterBase;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.Charset;
import org.hsqldb_voltpatches.types.IntervalType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;

import java.io.InputStream;
import java.io.LineNumberReader;

import org.hsqldb_voltpatches.lib.LineGroupReader;

import java.io.InputStreamReader;

import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.types.CharacterType;

// fredt@users - 1.7.2 - structural modifications to allow inheritance
// boucherb@users - 1.7.2 - 20020225
// - factored out all reusable code into DIXXX support classes
// - completed Fred's work on allowing inheritance
// boucherb@users - 1.7.2 - 20020304 - bug fixes, refinements, better java docs
// fredt@users - 1.8.0 - updated to report latest enhancements and changes
// boucherb@users - 1.8.0 - 20050515 - further SQL 2003 metadata support
// boucherb@users 20051207 - patch 1.8.x initial JDBC 4.0 support work
// fredt@users - 1.9.0 - new tables + renaming + upgrade of some others to SQL/SCHEMATA
// Revision 1.12  2006/07/12 11:42:09  boucherb
//  - merging back remaining material overritten by Fred's type-system upgrades
//  - rework to use grantee (versus user) orientation for certain system table content
//  - update collation and character set reporting to correctly reflect SQL3 spec

/**
 * Provides definitions for most of the SQL Standard Schemata views that are
 * supported by HSQLDB.<p>
 *
 * Provides definitions for some of HSQLDB's additional system vies.
 *
 * The definitions for the rest of system vies are provided by
 * DatabaseInformationMain, which this class extends. <p>
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
final class DatabaseInformationFull
extends org.hsqldb_voltpatches.dbinfo.DatabaseInformationMain {

    final static HashMappedList statementMap;

    static {
        final String resourceFileName =
            "/org/hsqldb_voltpatches/resources/information-schema.sql";
        final String[] starters = new String[]{ "/*" };
        InputStream fis =
            DatabaseInformation.class.getResourceAsStream(resourceFileName);
        InputStreamReader reader = null;

        try {
            reader = new InputStreamReader(fis, "ISO-8859-1");
        } catch (Exception e) {}

        LineNumberReader lineReader = new LineNumberReader(reader);
        LineGroupReader  lg = new LineGroupReader(lineReader, starters);

        statementMap = lg.getAsMap();

        lg.close();
    }

    /**
     * Constructs a new DatabaseInformationFull instance. <p>
     *
     * @param db the database for which to produce system tables.
     */
    DatabaseInformationFull(Database db) {
        super(db);
    }

    /**
     * Retrieves the system table corresponding to the specified index. <p>
     *
     * @param tableIndex index identifying the system table to generate
     * @return the system table corresponding to the specified index
     */
    protected Table generateTable(int tableIndex) {

        switch (tableIndex) {

            case SYSTEM_UDTS :
                return SYSTEM_UDTS();

            case SYSTEM_VERSIONCOLUMNS :
                return SYSTEM_VERSIONCOLUMNS();

            // HSQLDB-specific
            case SYSTEM_CACHEINFO :
                return SYSTEM_CACHEINFO();

            case SYSTEM_SESSIONINFO :
                return SYSTEM_SESSIONINFO();

            case SYSTEM_PROPERTIES :
                return SYSTEM_PROPERTIES();

            case SYSTEM_SESSIONS :
                return SYSTEM_SESSIONS();

            case SYSTEM_TEXTTABLES :
                return SYSTEM_TEXTTABLES();

            // SQL views
            case ADMINISTRABLE_ROLE_AUTHORIZATIONS :
                return ADMINISTRABLE_ROLE_AUTHORIZATIONS();

            case APPLICABLE_ROLES :
                return APPLICABLE_ROLES();

            case ASSERTIONS :
                return ASSERTIONS();

            case AUTHORIZATIONS :
                return AUTHORIZATIONS();

            case CHARACTER_SETS :
                return CHARACTER_SETS();

            case CHECK_CONSTRAINT_ROUTINE_USAGE :
                return CHECK_CONSTRAINT_ROUTINE_USAGE();

            case CHECK_CONSTRAINTS :
                return CHECK_CONSTRAINTS();

            case COLLATIONS :
                return COLLATIONS();

            case COLUMN_COLUMN_USAGE :
                return COLUMN_COLUMN_USAGE();

            case COLUMN_DOMAIN_USAGE :
                return COLUMN_DOMAIN_USAGE();

            case COLUMN_UDT_USAGE :
                return COLUMN_UDT_USAGE();

            case CONSTRAINT_COLUMN_USAGE :
                return CONSTRAINT_COLUMN_USAGE();

            case CONSTRAINT_TABLE_USAGE :
                return CONSTRAINT_TABLE_USAGE();

            case COLUMNS :
                return COLUMNS();

            case DATA_TYPE_PRIVILEGES :
                return DATA_TYPE_PRIVILEGES();

            case DOMAIN_CONSTRAINTS :
                return DOMAIN_CONSTRAINTS();

            case DOMAINS :
                return DOMAINS();

            case ENABLED_ROLES :
                return ENABLED_ROLES();

            case JAR_JAR_USAGE :
                return JAR_JAR_USAGE();

            case JARS :
                return JARS();

            case KEY_COLUMN_USAGE :
                return KEY_COLUMN_USAGE();

            case METHOD_SPECIFICATIONS :
                return METHOD_SPECIFICATIONS();

            case MODULE_COLUMN_USAGE :
                return MODULE_COLUMN_USAGE();

            case MODULE_PRIVILEGES :
                return MODULE_PRIVILEGES();

            case MODULE_TABLE_USAGE :
                return MODULE_TABLE_USAGE();

            case MODULES :
                return MODULES();

            case PARAMETERS :
                return PARAMETERS();

            case REFERENTIAL_CONSTRAINTS :
                return REFERENTIAL_CONSTRAINTS();

            case ROLE_AUTHORIZATION_DESCRIPTORS :
                return ROLE_AUTHORIZATION_DESCRIPTORS();

            case ROLE_COLUMN_GRANTS :
                return ROLE_COLUMN_GRANTS();

            case ROLE_ROUTINE_GRANTS :
                return ROLE_ROUTINE_GRANTS();

            case ROLE_TABLE_GRANTS :
                return ROLE_TABLE_GRANTS();

            case ROLE_USAGE_GRANTS :
                return ROLE_USAGE_GRANTS();

            case ROLE_UDT_GRANTS :
                return ROLE_UDT_GRANTS();

            case ROUTINE_COLUMN_USAGE :
                return ROUTINE_COLUMN_USAGE();

            case ROUTINE_JAR_USAGE :
                return ROUTINE_JAR_USAGE();

            case ROUTINE_PRIVILEGES :
                return ROUTINE_PRIVILEGES();

            case ROUTINE_ROUTINE_USAGE :
                return ROUTINE_ROUTINE_USAGE();

            case ROUTINE_SEQUENCE_USAGE :
                return ROUTINE_SEQUENCE_USAGE();

            case ROUTINE_TABLE_USAGE :
                return ROUTINE_TABLE_USAGE();

            case ROUTINES :
                return ROUTINES();

            case SCHEMATA :
                return SCHEMATA();

            case SEQUENCES :
                return SEQUENCES();

            case SQL_FEATURES :
                return SQL_FEATURES();

            case SQL_IMPLEMENTATION_INFO :
                return SQL_IMPLEMENTATION_INFO();

            case SQL_PACKAGES :
                return SQL_PACKAGES();

            case SQL_PARTS :
                return SQL_PARTS();

            case SQL_SIZING :
                return SQL_SIZING();

            case SQL_SIZING_PROFILES :
                return SQL_SIZING_PROFILES();

            case TABLE_CONSTRAINTS :
                return TABLE_CONSTRAINTS();

            case TABLES :
                return TABLES();

            case TRANSLATIONS :
                return TRANSLATIONS();

            case TRIGGERED_UPDATE_COLUMNS :
                return TRIGGERED_UPDATE_COLUMNS();

            case TRIGGER_COLUMN_USAGE :
                return TRIGGER_COLUMN_USAGE();

            case TRIGGER_ROUTINE_USAGE :
                return TRIGGER_ROUTINE_USAGE();

            case TRIGGER_SEQUENCE_USAGE :
                return TRIGGER_SEQUENCE_USAGE();

            case TRIGGER_TABLE_USAGE :
                return TRIGGER_TABLE_USAGE();

            case TRIGGERS :
                return TRIGGERS();

            case USAGE_PRIVILEGES :
                return USAGE_PRIVILEGES();

            case USER_DEFINED_TYPES :
                return USER_DEFINED_TYPES();

            case VIEW_COLUMN_USAGE :
                return VIEW_COLUMN_USAGE();

            case VIEW_ROUTINE_USAGE :
                return VIEW_ROUTINE_USAGE();

            case VIEW_TABLE_USAGE :
                return VIEW_TABLE_USAGE();

            case VIEWS :
                return VIEWS();

            default :
                return super.generateTable(tableIndex);
        }
    }

    /**
     * Retrieves a <code>Table</code> object describing the current
     * state of all row caching objects for the accessible
     * tables defined within this database. <p>
     *
     * Currently, the row caching objects for which state is reported are: <p>
     *
     * <OL>
     * <LI> the system-wide <code>Cache</code> object used by CACHED tables.
     * <LI> any <code>TextCache</code> objects in use by [TEMP] TEXT tables.
     * </OL> <p>
     *
     * Each row is a cache object state description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * CACHE_FILE          CHARACTER_DATA   absolute path of cache data file
     * MAX_CACHE_SIZE      INTEGER   maximum allowable cached Row objects
     * MAX_CACHE_BYTE_SIZE INTEGER   maximum allowable size of cached Row objects
     * CACHE_LENGTH        INTEGER   number of data bytes currently cached
     * CACHE_SIZE          INTEGER   number of rows currently cached
     * FREE_BYTES          INTEGER   total bytes in available file allocation units
     * FREE_COUNT          INTEGER   total # of allocation units available
     * FREE_POS            INTEGER   largest file position allocated + 1
     * </pre> <p>
     *
     * <b>Notes:</b> <p>
     *
     * <code>TextCache</code> objects do not maintain a free list because
     * deleted rows are only marked deleted and never reused. As such, the
     * columns FREE_BYTES, SMALLEST_FREE_ITEM, LARGEST_FREE_ITEM, and
     * FREE_COUNT are always reported as zero for rows reporting on
     * <code>TextCache</code> objects. <p>
     *
     * Currently, CACHE_SIZE, FREE_BYTES, SMALLEST_FREE_ITEM, LARGEST_FREE_ITEM,
     * FREE_COUNT and FREE_POS are the only dynamically changing values.
     * All others are constant for the life of a cache object. In a future
     * release, other column values may also change over the life of a cache
     * object, as SQL syntax may eventually be introduced to allow runtime
     * modification of certain cache properties. <p>
     *
     * @return a description of the current state of all row caching
     *      objects associated with the accessible tables of the database
     */
    Table SYSTEM_CACHEINFO() {

        Table t = sysTables[SYSTEM_CACHEINFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_CACHEINFO]);

            addColumn(t, "CACHE_FILE", CHARACTER_DATA);          // not null
            addColumn(t, "MAX_CACHE_COUNT", CARDINAL_NUMBER);    // not null
            addColumn(t, "MAX_CACHE_BYTES", CARDINAL_NUMBER);    // not null
            addColumn(t, "CACHE_SIZE", CARDINAL_NUMBER);         // not null
            addColumn(t, "CACHE_BYTES", CARDINAL_NUMBER);        // not null
            addColumn(t, "FILE_FREE_BYTES", CARDINAL_NUMBER);    // not null
            addColumn(t, "FILE_FREE_COUNT", CARDINAL_NUMBER);    // not null
            addColumn(t, "FILE_FREE_POS", CARDINAL_NUMBER);      // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_CACHEINFO].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, true);

            return t;
        }

        // column number mappings
        final int icache_file      = 0;
        final int imax_cache_sz    = 1;
        final int imax_cache_bytes = 2;
        final int icache_size      = 3;
        final int icache_length    = 4;
        final int ifree_bytes      = 5;
        final int ifree_count      = 6;
        final int ifree_pos        = 7;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        DataFileCache   cache = null;
        Object[]        row;
        HashSet         cacheSet;
        Iterator        caches;
        Iterator        tables;
        Table           table;
        int             iFreeBytes;
        int             iLargestFreeItem;
        long            lSmallestFreeItem;

        // Initialization
        cacheSet = new HashSet();

        // dynamic system tables are never cached
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        while (tables.hasNext()) {
            table = (Table) tables.next();

            PersistentStore currentStore =
                database.persistentStoreCollection.getStore(t);

            if (session.getGrantee().isFullyAccessibleByRole(table)) {
                if (currentStore != null) {
                    cache = currentStore.getCache();
                }

                if (cache != null) {
                    cacheSet.add(cache);
                }
            }
        }

        caches = cacheSet.iterator();

        // Do it.
        while (caches.hasNext()) {
            cache = (DataFileCache) caches.next();
            row   = t.getEmptyRowData();
            row[icache_file] =
                FileUtil.getDefaultInstance().canonicalOrAbsolutePath(
                    cache.getFileName());
            row[imax_cache_sz]    = ValuePool.getInt(cache.capacity());
            row[imax_cache_bytes] = ValuePool.getLong(cache.bytesCapacity());
            row[icache_size] = ValuePool.getInt(cache.getCachedObjectCount());
            row[icache_length] =
                ValuePool.getLong(cache.getTotalCachedBlockSize());
            row[ifree_bytes] = ValuePool.getInt(cache.getTotalFreeBlockSize());
            row[ifree_count] = ValuePool.getInt(cache.getFreeBlockCount());
            row[ifree_pos]   = ValuePool.getLong(cache.getFileFreePos());

            t.insertSys(store, row);
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the capabilities
     * and operating parameter properties for the engine hosting this
     * database, as well as their applicability in terms of scope and
     * name space. <p>
     *
     * Reported properties include certain predefined <code>Database</code>
     * properties file values as well as certain database scope
     * attributes. <p>
     *
     * It is intended that all <code>Database</code> attributes and
     * properties that can be set via the database properties file,
     * JDBC connection properties or SQL SET/ALTER statements will
     * eventually be reported here or, where more applicable, in an
     * ANSI/ISO conforming feature info base table in the defintion
     * schema. <p>
     *
     * Currently, the database properties reported are: <p>
     *
     * <OL>
     *     <LI>hsqldb.cache_file_scale - the scaling factor used to translate data and index structure file pointers
     *     <LI>hsqldb.cache_scale - base-2 exponent scaling allowable cache row count
     *     <LI>hsqldb.cache_size_scale - base-2 exponent scaling allowable cache byte count
     *     <LI>hsqldb.cache_version -
     *     <LI>hsqldb.catalogs - whether to report the database catalog (database uri)
     *     <LI>hsqldb.compatible_version -
     *     <LI>hsqldb.files_readonly - whether the database is in files_readonly mode
     *     <LI>hsqldb.gc_interval - # new records forcing gc ({0|NULL}=>never)
     *     <LI>hsqldb.max_nio_scale - scale factor for cache nio mapped buffers
     *     <LI>hsqldb.nio_data_file - whether cache uses nio mapped buffers
     *     <LI>hsqldb.original_version -
     *     <LI>sql.enforce_strict_size - column length specifications enforced strictly (raise exception on overflow)?
     *     <LI>textdb.all_quoted - default policy regarding whether to quote all character field values
     *     <LI>textdb.cache_scale - base-2 exponent scaling allowable cache row count
     *     <LI>textdb.cache_size_scale - base-2 exponent scaling allowable cache byte count
     *     <LI>textdb.encoding - default TEXT table file encoding
     *     <LI>textdb.fs - default field separator
     *     <LI>textdb.vs - default varchar field separator
     *     <LI>textdb.lvs - default long varchar field separator
     *     <LI>textdb.ignore_first - default policy regarding whether to ignore the first line
     *     <LI>textdb.quoted - default policy regarding treatement character field values that _may_ require quoting
     *     <LI>IGNORECASE - create table VARCHAR_IGNORECASE?
     *     <LI>LOGSIZSE - # bytes to which REDO log grows before auto-checkpoint
     *     <LI>REFERENTIAL_INTEGITY - currently enforcing referential integrity?
     *     <LI>SCRIPTFORMAT - 0 : TEXT, 1 : BINARY, ...
     *     <LI>WRITEDELAY - does REDO log currently use buffered write strategy?
     * </OL> <p>
     *
     * @return table describing database and session operating parameters
     *      and capabilities
     */
    Table SYSTEM_PROPERTIES() {

        Table t = sysTables[SYSTEM_PROPERTIES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PROPERTIES]);

            addColumn(t, "PROPERTY_SCOPE", CHARACTER_DATA);
            addColumn(t, "PROPERTY_NAMESPACE", CHARACTER_DATA);
            addColumn(t, "PROPERTY_NAME", CHARACTER_DATA);
            addColumn(t, "PROPERTY_VALUE", CHARACTER_DATA);
            addColumn(t, "PROPERTY_CLASS", CHARACTER_DATA);

            // order PROPERTY_SCOPE, PROPERTY_NAMESPACE, PROPERTY_NAME
            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PROPERTIES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, true);

            return t;
        }

        // column number mappings
        final int iscope = 0;
        final int ins    = 1;
        final int iname  = 2;
        final int ivalue = 3;
        final int iclass = 4;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        String scope;
        String nameSpace;

        // intermediate holders
        Object[]               row;
        HsqlDatabaseProperties props;

        // First, we want the names and values for
        // all JDBC capabilities constants
        scope     = "SESSION";
        props     = database.getProperties();
        nameSpace = "database.properties";

        // boolean properties
        Iterator it = props.getUserDefinedPropertyData().iterator();

        while (it.hasNext()) {
            Object[] metaData = (Object[]) it.next();

            row         = t.getEmptyRowData();
            row[iscope] = scope;
            row[ins]    = nameSpace;
            row[iname]  = metaData[HsqlProperties.indexName];
            row[ivalue] = props.getProperty((String) row[iname]);
            row[iclass] = metaData[HsqlProperties.indexClass];

            t.insertSys(store, row);
        }

        row         = t.getEmptyRowData();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "SCRIPTFORMAT";

        try {
            row[ivalue] =
                ScriptWriterBase
                    .LIST_SCRIPT_FORMATS[database.logger.getScriptType()];
        } catch (Exception e) {}

        row[iclass] = "java.lang.String";

        t.insertSys(store, row);

        // write delay
        row         = t.getEmptyRowData();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "WRITE_DELAY";
        row[ivalue] = "" + database.logger.getWriteDelay();
        row[iclass] = "int";

        t.insertSys(store, row);

        // ignore case
        row         = t.getEmptyRowData();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "IGNORECASE";
        row[ivalue] = database.isIgnoreCase() ? "true"
                                              : "false";
        row[iclass] = "boolean";

        t.insertSys(store, row);

        // referential integrity
        row         = t.getEmptyRowData();
        row[iscope] = scope;
        row[ins]    = nameSpace;
        row[iname]  = "REFERENTIAL_INTEGRITY";
        row[ivalue] = database.isReferentialIntegrity() ? "true"
                                                        : "false";
        row[iclass] = "boolean";

        t.insertSys(store, row);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing attributes
     * for the calling session context.<p>
     *
     * The rows report the following {key,value} pairs:<p>
     *
     * <pre class="SqlCodeExample">
     * KEY (VARCHAR)       VALUE (VARCHAR)
     * ------------------- ---------------
     * SESSION_ID          the id of the calling session
     * AUTOCOMMIT          YES: session is in autocommit mode, else NO
     * USER                the name of user connected in the calling session
     * (was READ_ONLY)
     * SESSION_READONLY    TRUE: session is in read-only mode, else FALSE
     * (new)
     * DATABASE_READONLY   TRUE: database is in read-only mode, else FALSE
     * MAXROWS             the MAXROWS setting in the calling session
     * DATABASE            the name of the database
     * IDENTITY            the last identity value used by calling session
     * </pre>
     *
     * <b>Note:</b>  This table <em>may</em> become deprecated in a future
     * release, as the information it reports now duplicates information
     * reported in the newer SYSTEM_SESSIONS and SYSTEM_PROPERTIES
     * tables. <p>
     *
     * @return a <code>Table</code> object describing the
     *        attributes of the connection associated
     *        with the current execution context
     */
    Table SYSTEM_SESSIONINFO() {

        Table t = sysTables[SYSTEM_SESSIONINFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SESSIONINFO]);

            addColumn(t, "KEY", CHARACTER_DATA);      // not null
            addColumn(t, "VALUE", CHARACTER_DATA);    // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_SESSIONINFO].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Object[]        row;

        row    = t.getEmptyRowData();
        row[0] = "SESSION_ID";
        row[1] = String.valueOf(session.getId());

        t.insertSys(store, row);

        row    = t.getEmptyRowData();
        row[0] = "AUTOCOMMIT";
        row[1] = session.isAutoCommit() ? "TRUE"
                                        : "FALSE";

        t.insertSys(store, row);

        row    = t.getEmptyRowData();
        row[0] = "USER";
        row[1] = session.getUsername();

        t.insertSys(store, row);

        row    = t.getEmptyRowData();
        row[0] = "SESSION_READONLY";
        row[1] = session.isReadOnlyDefault() ? "TRUE"
                                             : "FALSE";

        t.insertSys(store, row);

        row    = t.getEmptyRowData();
        row[0] = "DATABASE_READONLY";
        row[1] = database.isReadOnly() ? "TRUE"
                                       : "FALSE";

        t.insertSys(store, row);

        // fredt - value set by SET MAXROWS in SQL, not Statement.setMaxRows()
        row    = t.getEmptyRowData();
        row[0] = "MAXROWS";
        row[1] = String.valueOf(session.getSQLMaxRows());

        t.insertSys(store, row);

        row    = t.getEmptyRowData();
        row[0] = "DATABASE";
        row[1] = database.getURI();

        t.insertSys(store, row);

        row    = t.getEmptyRowData();
        row[0] = "IDENTITY";
        row[1] = String.valueOf(session.getLastIdentity());

        t.insertSys(store, row);

        row    = t.getEmptyRowData();
        row[0] = "SCHEMA";
        row[1] = String.valueOf(session.getSchemaName(null));

        t.insertSys(store, row);

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing all visible
     * sessions. ADMIN users see *all* sessions
     * while non-admin users see only their own session.<p>
     *
     * Each row is a session state description with the following columns: <p>
     *
     * <pre class="SqlCodeExample">
     * SESSION_ID         INTEGER   session identifier
     * CONNECTED          TIMESTAMP time at which session was created
     * USER_NAME          VARCHAR   db user name of current session user
     * IS_ADMIN           BOOLEAN   is session user an admin user?
     * AUTOCOMMIT         BOOLEAN   is session in autocommit mode?
     * READONLY           BOOLEAN   is session in read-only mode?
     * MAXROWS            INTEGER   session's MAXROWS setting
     * LAST_IDENTITY      INTEGER   last identity value used by this session
     * TRANSACTION_SIZE   INTEGER   # of undo items in current transaction
     * SCHEMA             VARCHAR   current schema for session
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing all visible
     *      sessions
     */
    Table SYSTEM_SESSIONS() {

        Table t = sysTables[SYSTEM_SESSIONS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SESSIONS]);

            addColumn(t, "SESSION_ID", CARDINAL_NUMBER);
            addColumn(t, "CONNECTED", TIME_STAMP);
            addColumn(t, "USER_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_ADMIN", Type.SQL_BOOLEAN);
            addColumn(t, "AUTOCOMMIT", Type.SQL_BOOLEAN);
            addColumn(t, "READONLY", Type.SQL_BOOLEAN);
            addColumn(t, "MAXROWS", CARDINAL_NUMBER);

            // Note: some sessions may have a NULL LAST_IDENTITY value
            addColumn(t, "LAST_IDENTITY", CARDINAL_NUMBER);
            addColumn(t, "TRANSACTION_SIZE", CARDINAL_NUMBER);
            addColumn(t, "SCHEMA", SQL_IDENTIFIER);

            // order:  SESSION_ID
            // true primary key
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_SESSIONS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, true);

            return t;
        }

        // column number mappings
        final int isid      = 0;
        final int ict       = 1;
        final int iuname    = 2;
        final int iis_admin = 3;
        final int iautocmt  = 4;
        final int ireadonly = 5;
        final int imaxrows  = 6;
        final int ilast_id  = 7;
        final int it_size   = 8;
        final int it_schema = 9;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // intermediate holders
        Session[] sessions;
        Session   s;
        Object[]  row;

        // Initialisation
        sessions = ns.listVisibleSessions(session);

        // Do it.
        for (int i = 0; i < sessions.length; i++) {
            s              = sessions[i];
            row            = t.getEmptyRowData();
            row[isid]      = ValuePool.getLong(s.getId());
            row[ict]       = new TimestampData(s.getConnectTime() / 1000);
            row[iuname]    = s.getUsername();
            row[iis_admin] = ValuePool.getBoolean(s.isAdmin());
            row[iautocmt]  = ValuePool.getBoolean(s.isAutoCommit());
            row[ireadonly] = ValuePool.getBoolean(s.isReadOnlyDefault());
            row[imaxrows]  = ValuePool.getInt(s.getSQLMaxRows());
            row[ilast_id] =
                ValuePool.getLong(((Number) s.getLastIdentity()).longValue());
            row[it_size]   = ValuePool.getInt(s.getTransactionSize());
            row[it_schema] = s.getCurrentSchemaHsqlName().name;

            t.insertSys(store, row);
        }

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the TEXT TABLE objects
     * defined within this database. The table contains one row for each row
     * in the SYSTEM_TABLES table with a HSQLDB_TYPE of  TEXT . <p>
     *
     * Each row is a description of the attributes that defines its TEXT TABLE,
     * with the following columns:
     *
     * <pre class="SqlCodeExample">
     * TABLE_CAT                 VARCHAR   table's catalog name
     * TABLE_SCHEM               VARCHAR   table's simple schema name
     * TABLE_NAME                VARCHAR   table's simple name
     * DATA_SOURCE_DEFINITION    VARCHAR   the "spec" proption of the table's
     *                                     SET TABLE ... SOURCE DDL declaration
     * FILE_PATH                 VARCHAR   absolute file path.
     * FILE_ENCODING             VARCHAR   endcoding of table's text file
     * FIELD_SEPARATOR           VARCHAR   default field separator
     * VARCHAR_SEPARATOR         VARCAHR   varchar field separator
     * LONGVARCHAR_SEPARATOR     VARCHAR   longvarchar field separator
     * IS_IGNORE_FIRST           BOOLEAN   ignores first line of file?
     * IS_QUOTED                 BOOLEAN   fields are quoted if necessary?
     * IS_ALL_QUOTED             BOOLEAN   all fields are quoted?
     * IS_DESC                   BOOLEAN   read rows starting at end of file?
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the text attributes
     * of the accessible text tables defined within this database
     *
     */
    Table SYSTEM_TEXTTABLES() {

        Table t = sysTables[SYSTEM_TEXTTABLES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TEXTTABLES]);

            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "DATA_SOURCE_DEFINTION", CHARACTER_DATA);
            addColumn(t, "FILE_PATH", CHARACTER_DATA);
            addColumn(t, "FILE_ENCODING", CHARACTER_DATA);
            addColumn(t, "FIELD_SEPARATOR", CHARACTER_DATA);
            addColumn(t, "VARCHAR_SEPARATOR", CHARACTER_DATA);
            addColumn(t, "LONGVARCHAR_SEPARATOR", CHARACTER_DATA);
            addColumn(t, "IS_IGNORE_FIRST", Type.SQL_BOOLEAN);
            addColumn(t, "IS_ALL_QUOTED", Type.SQL_BOOLEAN);
            addColumn(t, "IS_QUOTED", Type.SQL_BOOLEAN);
            addColumn(t, "IS_DESC", Type.SQL_BOOLEAN);

            // ------------------------------------------------------------
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TEXTTABLES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2,
            }, false);

            return t;
        }

        // column number mappings
        final int itable_cat   = 0;
        final int itable_schem = 1;
        final int itable_name  = 2;
        final int idsd         = 3;
        final int ifile_path   = 4;
        final int ifile_enc    = 5;
        final int ifs          = 6;
        final int ivfs         = 7;
        final int ilvfs        = 8;
        final int iif          = 9;
        final int iiq          = 10;
        final int iiaq         = 11;
        final int iid          = 12;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // intermediate holders
        Iterator tables;
        Table    table;
        Object[] row;

        // Initialization
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            PersistentStore currentStore =
                database.persistentStoreCollection.getStore(t);

            if (!table.isText() || !isAccessibleTable(table)) {
                continue;
            }

            row               = t.getEmptyRowData();
            row[itable_cat]   = database.getCatalogName().name;
            row[itable_schem] = table.getSchemaName().name;
            row[itable_name]  = table.getName().name;
            row[idsd]         = ((TextTable) table).getDataSource();

            TextCache cache = (TextCache) currentStore.getCache();

            if (cache != null) {
                row[ifile_path] =
                    FileUtil.getDefaultInstance().canonicalOrAbsolutePath(
                        cache.getFileName());
                row[ifile_enc] = cache.stringEncoding;
                row[ifs]       = cache.fs;
                row[ivfs]      = cache.vs;
                row[ilvfs]     = cache.lvs;
                row[iif]       = ValuePool.getBoolean(cache.ignoreFirst);
                row[iiq]       = ValuePool.getBoolean(cache.isQuoted);
                row[iiaq]      = ValuePool.getBoolean(cache.isAllQuoted);
                row[iid] = ((TextTable) table).isDescDataSource()
                           ? Boolean.TRUE
                           : Boolean.FALSE;
            }

            t.insertSys(store, row);
        }

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
     * <B>Note:</B> Currently, neither the HSQLDB engine or the JDBC driver
     * support UDTs, so an empty table is returned. <p>
     *
     * @return a <code>Table</code> object describing the accessible
     *      user-defined types defined in this database
     */
    Table SYSTEM_UDTS() {

        Table t = sysTables[SYSTEM_UDTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_UDTS]);

            addColumn(t, "TYPE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TYPE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);     // not null
            addColumn(t, "CLASS_NAME", CHARACTER_DATA);    // not null
            addColumn(t, "DATA_TYPE", SQL_IDENTIFIER);     // not null
            addColumn(t, "REMARKS", CHARACTER_DATA);
            addColumn(t, "BASE_TYPE", Type.SQL_SMALLINT);

            //
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_UDTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, null, false);

            return t;
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
    Table SYSTEM_VERSIONCOLUMNS() {

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

            t.createPrimaryKey(name, null, false);

            return t;
        }

        return t;
    }

//------------------------------------------------------------------------------
// SQL SCHEMATA VIEWS

    /**
     * Returns roles that are grantable by an admin user, which means all the
     * roles
     *
     * @return Table
     */
    Table ADMINISTRABLE_ROLE_AUTHORIZATIONS() {

        Table t = sysTables[ADMINISTRABLE_ROLE_AUTHORIZATIONS];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[ADMINISTRABLE_ROLE_AUTHORIZATIONS]);

            addColumn(t, "GRANTEE", SQL_IDENTIFIER);
            addColumn(t, "ROLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_GRANTABLE", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ADMINISTRABLE_ROLE_AUTHORIZATIONS].name,
                false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        if (session.isAdmin()) {
            insertRoles(t, session.getGrantee(), true);
        }

        return t;
    }

    /**
     * APPLICABLE_ROLES<p>
     *
     * <b>Function</b><p>
     *
     * Identifies the applicable roles for the current user.<p>
     *
     * <b>Definition</b><p>
     *
     * <pre class="SqlCodeExample">
     * CREATE RECURSIVE VIEW APPLICABLE_ROLES ( GRANTEE, ROLE_NAME, IS_GRANTABLE ) AS
     *      ( ( SELECT GRANTEE, ROLE_NAME, IS_GRANTABLE
     *            FROM DEFINITION_SCHEMA.ROLE_AUTHORIZATION_DESCRIPTORS
     *           WHERE ( GRANTEE IN ( CURRENT_USER, 'PUBLIC' )
     *                OR GRANTEE IN ( SELECT ROLE_NAME
     *                                  FROM ENABLED_ROLES ) ) )
     *      UNION
     *      ( SELECT RAD.GRANTEE, RAD.ROLE_NAME, RAD.IS_GRANTABLE
     *          FROM DEFINITION_SCHEMA.ROLE_AUTHORIZATION_DESCRIPTORS RAD
     *          JOIN APPLICABLE_ROLES R
     *            ON RAD.GRANTEE = R.ROLE_NAME ) );
     *
     * GRANT SELECT ON TABLE APPLICABLE_ROLES
     *    TO PUBLIC WITH GRANT OPTION;
     * </pre>
     */
    Table APPLICABLE_ROLES() {

        Table t = sysTables[APPLICABLE_ROLES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[APPLICABLE_ROLES]);

            addColumn(t, "GRANTEE", SQL_IDENTIFIER);
            addColumn(t, "ROLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_GRANTABLE", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[APPLICABLE_ROLES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        insertRoles(t, session.getGrantee(), session.isAdmin());

        return t;
    }

    private void insertRoles(Table t, Grantee role, boolean isGrantable) {

        final int       grantee      = 0;
        final int       role_name    = 1;
        final int       is_grantable = 2;
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        if (isGrantable) {
            Set      roles = database.getGranteeManager().getRoleNames();
            Iterator it    = roles.iterator();

            while (it.hasNext()) {
                String   roleName = (String) it.next();
                Object[] row      = t.getEmptyRowData();

                row[grantee]      = role.getNameString();
                row[role_name]    = roleName;
                row[is_grantable] = "YES";

                t.insertSys(store, row);
            }
        } else {
            OrderedHashSet roles = role.getDirectRoles();

            for (int i = 0; i < roles.size(); i++) {
                String   roleName = (String) roles.get(i);
                Object[] row      = t.getEmptyRowData();

                row[grantee]      = role.getNameString();
                row[role_name]    = roleName;
                row[is_grantable] = Tokens.T_NO;

                t.insertSys(store, row);

                role = database.getGranteeManager().getRole(roleName);

                insertRoles(t, role, isGrantable);
            }
        }
    }

    Table ASSERTIONS() {

        Table t = sysTables[ASSERTIONS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ASSERTIONS]);

            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "IS_DEFERRABLE", YES_OR_NO);
            addColumn(t, "INITIALLY_DEFERRED", YES_OR_NO);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ASSERTIONS].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int is_deferrable      = 3;
        final int initially_deferred = 4;

        return t;
    }

    /**
     *  SYSTEM_AUTHORIZATIONS<p>
     *
     *  <b>Function</b><p>
     *
     *  The AUTHORIZATIONS table has one row for each &lt;role name&gt; and
     *  one row for each &lt;authorization identifier &gt; referenced in the
     *  Information Schema. These are the &lt;role name&gt;s and
     *  &lt;authorization identifier&gt;s that may grant privileges as well as
     *  those that may create a schema, or currently own a schema created
     *  through a &lt;schema definition&gt;. <p>
     *
     *  <b>Definition</b><p>
     *
     *  <pre class="SqlCodeExample">
     *  CREATE TABLE AUTHORIZATIONS (
     *       AUTHORIZATION_NAME INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *       AUTHORIZATION_TYPE INFORMATION_SCHEMA.CHARACTER_DATA
     *           CONSTRAINT AUTHORIZATIONS_AUTHORIZATION_TYPE_NOT_NULL
     *               NOT NULL
     *           CONSTRAINT AUTHORIZATIONS_AUTHORIZATION_TYPE_CHECK
     *               CHECK ( AUTHORIZATION_TYPE IN ( 'USER', 'ROLE' ) ),
     *           CONSTRAINT AUTHORIZATIONS_PRIMARY_KEY
     *               PRIMARY KEY (AUTHORIZATION_NAME)
     *       )
     *  </pre>
     *
     *  <b>Description</b><p>
     *
     *  <ol>
     *  <li> The values of AUTHORIZATION_TYPE have the following meanings:<p>
     *
     *  <table border cellpadding="3">
     *       <tr>
     *           <td nowrap>USER</td>
     *           <td nowrap>The value of AUTHORIZATION_NAME is a known
     *                      &lt;user identifier&gt;.</td>
     *       <tr>
     *       <tr>
     *           <td nowrap>NO</td>
     *           <td nowrap>The value of AUTHORIZATION_NAME is a &lt;role
     *                      name&gt; defined by a &lt;role definition&gt;.</td>
     *       <tr>
     *  </table> <p>
     *  </ol>
     *
     * @return Table
     */
    Table AUTHORIZATIONS() {

        Table t = sysTables[AUTHORIZATIONS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[AUTHORIZATIONS]);

            addColumn(t, "AUTHORIZATION_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "AUTHORIZATION_TYPE", SQL_IDENTIFIER);    // not null

            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[AUTHORIZATIONS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Intermediate holders
        Iterator grantees;
        Grantee  grantee;
        Object[] row;

        // initialization
        grantees = session.getGrantee().visibleGrantees().iterator();

        // Do it.
        while (grantees.hasNext()) {
            grantee = (Grantee) grantees.next();
            row     = t.getEmptyRowData();
            row[0]  = grantee.getNameString();
            row[1]  = grantee.isRole() ? "ROLE"
                                       : "USER";

            t.insertSys(store, row);
        }

        return t;
    }

    Table CHARACTER_SETS() {

        Table t = sysTables[CHARACTER_SETS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[CHARACTER_SETS]);

            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_REPERTOIRE", SQL_IDENTIFIER);
            addColumn(t, "FORM_OF_USE", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_COLLATE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_COLLATE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_COLLATE_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CHARACTER_SETS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        final int character_set_catalog   = 0;
        final int character_set_schema    = 1;
        final int character_set_name      = 2;
        final int character_repertoire    = 3;
        final int form_of_use             = 4;
        final int default_collate_catalog = 5;
        final int default_collate_schema  = 6;
        final int default_collate_name    = 7;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator it = database.schemaManager.databaseObjectIterator(
            SchemaObject.CHARSET);

        while (it.hasNext()) {
            Charset charset = (Charset) it.next();

            if (!session.getGrantee().isAccessible(charset)) {
                continue;
            }

            Object[] data = t.getEmptyRowData();

            data[character_set_catalog]   = database.getCatalogName().name;
            data[character_set_schema]    = charset.getSchemaName().name;
            data[character_set_name]      = charset.getName().name;
            data[character_repertoire]    = "UCS";
            data[form_of_use]             = "UTF16";
            data[default_collate_catalog] = data[character_set_catalog];

            if (charset.base == null) {
                data[default_collate_schema] = data[character_set_schema];
                data[default_collate_name]   = data[character_set_name];
            } else {
                data[default_collate_schema] = charset.base.schema.name;
                data[default_collate_name]   = charset.base.name;
            }

            t.insertSys(store, data);
        }

        return t;
    }

    /**
     * The CHECK_CONSTRAINT_ROUTINE_USAGE view has one row for each
     * SQL-invoked routine identified as the subject routine of either a
     * &lt;routine invocation&gt;, a &lt;method reference&gt;, a
     * &lt;method invocation&gt;, or a &lt;static method invocation&gt;
     * contained in an &lt;assertion definition&gt;, a &lt;domain
     * constraint&gt;, or a &lt;table constraint definition&gt;. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_CHECK_ROUTINE_USAGE (
     *      CONSTRAINT_CATALOG      VARCHAR NULL,
     *      CONSTRAINT_SCHEMA       VARCHAR NULL,
     *      CONSTRAINT_NAME         VARCHAR NOT NULL,
     *      SPECIFIC_CATALOG        VARCHAR NULL,
     *      SPECIFIC_SCHEMA         VARCHAR NULL,
     *      SPECIFIC_NAME           VARCHAR NOT NULL,
     *      UNIQUE( CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME,
     *              SPECIFIC_CATALOG, SPECIFIC_SCHEMA, SPECIFIC_NAME )
     * )
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The CHECK_ROUTINE_USAGE table has one row for each
     *      SQL-invoked routine R identified as the subject routine of either a
     *      &lt;routine invocation&gt;, a &lt;method reference&gt;, a &lt;method
     *      invocation&gt;, or a &lt;static method invocation&gt; contained in
     *      an &lt;assertion definition&gt; or in the &lt;check constraint
     *      definition&gt; contained in either a &lt;domain constraint&gt; or a
     *      &lt;table constraint definition&gt;. <p>
     *
     * <li> The values of CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, and
     *      CONSTRAINT_NAME are the catalog name, unqualified schema name, and
     *      qualified identifier, respectively, of the assertion or check
     *     constraint being described. <p>
     *
     * <li> The values of SPECIFIC_CATALOG, SPECIFIC_SCHEMA, and SPECIFIC_NAME
     *      are the catalog name, unqualified schema name, and qualified
     *      identifier, respectively, of the specific name of R. <p>
     *
     * </ol>
     *
     * @return Table
     */
    Table CHECK_CONSTRAINT_ROUTINE_USAGE() {

        Table t = sysTables[CHECK_CONSTRAINT_ROUTINE_USAGE];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[CHECK_CONSTRAINT_ROUTINE_USAGE]);

            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);      // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CHECK_CONSTRAINT_ROUTINE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        // column number mappings
        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int specific_catalog   = 3;
        final int specific_schema    = 4;
        final int specific_name      = 5;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        String constraintCatalog;
        String constraintSchema;
        String constraintName;
        String specificSchema;

        // Intermediate holders
        Iterator       tables;
        Table          table;
        Constraint[]   constraints;
        int            constraintCount;
        Constraint     constraint;
        OrderedHashSet collector;
        Iterator       iterator;
        OrderedHashSet methodSet;
        Method         method;
        Object[]       row;

        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        collector = new OrderedHashSet();

        while (tables.hasNext()) {
            collector.clear();

            table = (Table) tables.next();

            if (table.isView()
                    || !session.getGrantee().isFullyAccessibleByRole(table)) {
                continue;
            }

            constraints       = table.getConstraints();
            constraintCount   = constraints.length;
            constraintCatalog = database.getCatalogName().name;
            constraintSchema  = table.getSchemaName().name;
            specificSchema =
                database.schemaManager.getDefaultSchemaHsqlName().name;

            for (int i = 0; i < constraintCount; i++) {
                constraint = constraints[i];

                if (constraint.getConstraintType() != Constraint.CHECK) {
                    continue;
                }

                constraintName = constraint.getName().name;

                constraint.getCheckExpression().collectAllFunctionExpressions(
                    collector);

                methodSet = new OrderedHashSet();
                iterator  = collector.iterator();

                while (iterator.hasNext()) {
/*
                    Function expression = (Function) iterator.next();
                    String className =
                        expression.getMethod().getDeclaringClass().getName();
                    String schema =
                        database.schemaManager.getDefaultSchemaHsqlName().name;
                    SchemaObject object =
                        database.schemaManager.getSchemaObject(className,
                            schema, SchemaObject.FUNCTION);

                    if (!session.getGrantee().isAccessible(object)) {
                        continue;
                    }

                    methodSet.add(expression.getMethod());
*/
                }

                iterator = methodSet.iterator();

                while (iterator.hasNext()) {
                    method                  = (Method) iterator.next();
                    row                     = t.getEmptyRowData();
                    row[constraint_catalog] = constraintCatalog;
                    row[constraint_schema]  = constraintSchema;
                    row[constraint_name]    = constraintName;
                    row[specific_catalog]   = database.getCatalogName();
                    row[specific_schema]    = specificSchema;
                    row[specific_name] =
                        DINameSpace.getMethodSpecificName(method);

                    t.insertSys(store, row);
                }
            }
        }

        return t;
    }

    /**
     * The CHECK_CONSTRAINTS view has one row for each domain
     * constraint, table check constraint, and assertion. <p>
     *
     * <b>Definition:</b><p>
     *
     * <pre class="SqlCodeExample">
     *      CONSTRAINT_CATALOG  VARCHAR NULL,
     *      CONSTRAINT_SCHEMA   VARCHAR NULL,
     *      CONSTRAINT_NAME     VARCHAR NOT NULL,
     *      CHECK_CLAUSE        VARCHAR NOT NULL,
     * </pre>
     *
     * <b>Description:</b><p>
     *
     * <ol>
     * <li> A constraint is shown in this view if the authorization for the
     *      schema that contains the constraint is the current user or is a role
     *      assigned to the current user. <p>
     *
     * <li> The values of CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA and
     *      CONSTRAINT_NAME are the catalog name, unqualified schema name,
     *      and qualified identifier, respectively, of the constraint being
     *      described. <p>
     *
     * <li> Case: <p>
     *
     *      <table>
     *          <tr>
     *               <td valign="top" halign="left">a)</td>
     *               <td> If the character representation of the
     *                    &lt;search condition&gt; contained in the
     *                    &lt;check constraint definition&gt;,
     *                    &lt;domain constraint definition&gt;, or
     *                    &lt;assertion definition&gt; that defined
     *                    the check constraint being described can be
     *                    represented without truncation, then the
     *                    value of CHECK_CLAUSE is that character
     *                    representation. </td>
     *          </tr>
     *          <tr>
     *              <td align="top" halign="left">b)</td>
     *              <td>Otherwise, the value of CHECK_CLAUSE is the
     *                  null value.</td>
     *          </tr>
     *      </table>
     * </ol>
     *
     * @return Table
     */
    Table CHECK_CONSTRAINTS() {

        Table t = sysTables[CHECK_CONSTRAINTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[CHECK_CONSTRAINTS]);

            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "CHECK_CLAUSE", CHARACTER_DATA);       // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CHECK_CONSTRAINTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                2, 1, 0
            }, false);

            return t;
        }

        // column number mappings
        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int check_clause       = 3;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        // Intermediate holders
        Iterator     tables;
        Table        table;
        Constraint[] tableConstraints;
        int          constraintCount;
        Constraint   constraint;
        Object[]     row;

        //
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView()
                    || !session.getGrantee().isFullyAccessibleByRole(table)) {
                continue;
            }

            tableConstraints = table.getConstraints();
            constraintCount  = tableConstraints.length;

            for (int i = 0; i < constraintCount; i++) {
                constraint = tableConstraints[i];

                if (constraint.getConstraintType() != Constraint.CHECK) {
                    continue;
                }

                row                     = t.getEmptyRowData();
                row[constraint_catalog] = database.getCatalogName().name;
                row[constraint_schema]  = table.getSchemaName().name;
                row[constraint_name]    = constraint.getName().name;

                try {
                    row[check_clause] = constraint.getCheckSQL();
                } catch (Exception e) {}

                t.insertSys(store, row);
            }
        }

        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.DOMAIN);

        while (it.hasNext()) {
            Type domain = (Type) it.next();

            if (!domain.isDomainType()) {
                continue;
            }

            if (!session.getGrantee().isFullyAccessibleByRole(domain)) {
                continue;
            }

            tableConstraints = domain.userTypeModifier.getConstraints();
            constraintCount  = tableConstraints.length;

            for (int i = 0; i < constraintCount; i++) {
                constraint              = tableConstraints[i];
                row                     = t.getEmptyRowData();
                row[constraint_catalog] = database.getCatalogName().name;
                row[constraint_schema]  = domain.getSchemaName().name;
                row[constraint_name]    = constraint.getName().name;

                try {
                    row[check_clause] = constraint.getCheckSQL();
                } catch (Exception e) {}

                t.insertSys(store, row);
            }
        }

        return t;
    }

    /**
     * COLLATIONS<p>
     *
     * <b>Function<b><p>
     *
     * The COLLATIONS view has one row for each character collation
     * descriptor. <p>
     *
     * <b>Definition</b>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE COLLATIONS (
     *      COLLATION_CATALOG INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *      COLLATION_SCHEMA INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *      COLLATION_NAME INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *      PAD_ATTRIBUTE INFORMATION_SCHEMA.CHARACTER_DATA
     *          CONSTRAINT COLLATIONS_PAD_ATTRIBUTE_CHECK
     *              CHECK ( PAD_ATTRIBUTE IN
     *                  ( 'NO PAD', 'PAD SPACE' ) )
     * )
     * </pre>
     *
     * <b>Description</b><p>
     *
     * <ol>
     *      <li>The values of COLLATION_CATALOG, COLLATION_SCHEMA, and
     *          COLLATION_NAME are the catalog name, unqualified schema name,
     *          and qualified identifier, respectively, of the collation being
     *          described.<p>
     *
     *      <li>The values of PAD_ATTRIBUTE have the following meanings:<p>
     *
     *      <table border cellpadding="3">
     *          <tr>
     *              <td nowrap>NO PAD</td>
     *              <td nowrap>The collation being described has the NO PAD
     *                  characteristic.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>PAD</td>
     *              <td nowrap>The collation being described has the PAD SPACE
     *                         characteristic.</td>
     *          <tr>
     *      </table> <p>
     * </ol>
     *
     * @return Table
     */
    Table COLLATIONS() {

        Table t = sysTables[COLLATIONS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLLATIONS]);

            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);    // not null
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);      // not null
            addColumn(t, "PAD_ATTRIBUTE", CHARACTER_DATA);

            // false PK, as rows may have NULL COLLATION_CATALOG
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLLATIONS].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        // Column number mappings
        final int collation_catalog = 0;
        final int collation_schema  = 1;
        final int collation_name    = 2;
        final int pad_attribute     = 3;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Intermediate holders
        Iterator collations;
        String   collation;
        String   collationSchema = SqlInvariants.PUBLIC_SCHEMA;
        String   padAttribute    = "NO PAD";
        Object[] row;

        // Initialization
        collations = Collation.nameToJavaName.keySet().iterator();

        // Do it.
        while (collations.hasNext()) {
            row                    = t.getEmptyRowData();
            collation              = (String) collations.next();
            row[collation_catalog] = database.getCatalogName().name;
            row[collation_schema]  = collationSchema;
            row[collation_name]    = collation;
            row[pad_attribute]     = padAttribute;

            t.insertSys(store, row);
        }

        return t;
    }

    /**
     * For generated columns
     * <p>
     *
     * @return Table
     */
    Table COLUMN_COLUMN_USAGE() {

        Table t = sysTables[COLUMN_COLUMN_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMN_COLUMN_USAGE]);

            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "DEPENDENT_COLUMN", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMN_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4
            }, false);

            return t;
        }

        final int table_catalog    = 0;
        final int table_schema     = 1;
        final int table_name       = 2;
        final int column_name      = 3;
        final int dependent_column = 4;

        return t;
    }

    /**
     * Domains are shown if the authorization is the user or a role given to the
     * user.
     *
     * <p>
     *
     * @return Table
     */
    Table COLUMN_DOMAIN_USAGE() {

        Table t = sysTables[COLUMN_DOMAIN_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMN_DOMAIN_USAGE]);

            addColumn(t, "DOMAIN_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);     // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);    // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMN_DOMAIN_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, TABLE_CATALOG, "
            + "TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE DOMAIN_NAME IS NOT NULL;");

        t.insertSys(store, rs);
        sys.close();

        return t;
    }

    /**
     * UDT's are shown if the authorization is the user or a role given to the
     * user.
     *
     * <p>
     *
     * @return Table
     */
    Table COLUMN_UDT_USAGE() {

        Table t = sysTables[COLUMN_UDT_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMN_UDT_USAGE]);

            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);     // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);    // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMN_UDT_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT UDT_CATALOG, UDT_SCHEMA, UDT_NAME, TABLE_CATALOG, "
            + "TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE UDT_NAME IS NOT NULL;");

        t.insertSys(store, rs);
        sys.close();

        return t;
    }

    Table COLUMNS() {

        Table t = sysTables[COLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMNS]);

            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);           //0
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "ORDINAL_POSITION", CARDINAL_NUMBER);
            addColumn(t, "COLUMN_DEFAULT", CHARACTER_DATA);
            addColumn(t, "IS_NULLABLE", YES_OR_NO);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);      //10
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", CHARACTER_DATA);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);        //20
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_NAME", SQL_IDENTIFIER);
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_NAME", SQL_IDENTIFIER);              //30
            addColumn(t, "MAXIMUM_CARDINALITY", CARDINAL_NUMBER);    // NULL (only for array tyes)
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "IS_SELF_REFERENCING", YES_OR_NO);
            addColumn(t, "IS_IDENTITY", YES_OR_NO);
            addColumn(t, "IDENTITY_GENERATION", CHARACTER_DATA);     // ALLWAYS / BY DEFAULT
            addColumn(t, "IDENTITY_START", CHARACTER_DATA);
            addColumn(t, "IDENTITY_INCREMENT", CHARACTER_DATA);
            addColumn(t, "IDENTITY_MAXIMUM", CHARACTER_DATA);
            addColumn(t, "IDENTITY_MINIMUM", CHARACTER_DATA);
            addColumn(t, "IDENTITY_CYCLE", YES_OR_NO);               //40
            addColumn(t, "IS_GENERATED", CHARACTER_DATA);            // ALLWAYS / NEVER
            addColumn(t, "GENERATION_EXPRESSION", CHARACTER_DATA);
            addColumn(t, "IS_UPDATABLE", YES_OR_NO);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);

            // order: TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION
            // added for unique: TABLE_CAT
            // false PK, as TABLE_SCHEM and/or TABLE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMNS].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                3, 2, 1, 4
            }, false);

            return t;
        }

        // column number mappings
        final int table_cat                  = 0;
        final int table_schem                = 1;
        final int table_name                 = 2;
        final int column_name                = 3;
        final int ordinal_position           = 4;
        final int column_default             = 5;
        final int is_nullable                = 6;
        final int data_type                  = 7;
        final int character_maximum_length   = 8;
        final int character_octet_length     = 9;
        final int numeric_precision          = 10;
        final int numeric_precision_radix    = 11;
        final int numeric_scale              = 12;
        final int datetime_precision         = 13;
        final int interval_type              = 14;
        final int interval_precision         = 15;
        final int character_set_catalog      = 16;
        final int character_set_schema       = 17;
        final int character_set_name         = 18;
        final int collation_catalog          = 19;
        final int collation_schema           = 20;
        final int collation_name             = 21;
        final int domain_catalog             = 22;
        final int domain_schema              = 23;
        final int domain_name                = 24;
        final int udt_catalog                = 25;
        final int udt_schema                 = 26;
        final int udt_name                   = 27;
        final int scope_catalog              = 28;
        final int scope_schema               = 29;
        final int scope_name                 = 30;
        final int maximum_cardinality        = 31;
        final int dtd_identifier             = 32;
        final int is_self_referencing        = 33;
        final int is_identity                = 34;
        final int identity_generation        = 35;
        final int identity_start             = 36;
        final int identity_increment         = 37;
        final int identity_maximum           = 38;
        final int identity_minimum           = 39;
        final int identity_cycle             = 40;
        final int is_generated               = 41;
        final int generation_expression      = 42;
        final int is_updatable               = 43;
        final int declared_data_type         = 44;
        final int declared_numeric_precision = 45;
        final int declared_numeric_scale     = 46;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // intermediate holders
        int            columnCount;
        Iterator       tables;
        Table          table;
        Object[]       row;
        DITableInfo    ti;
        OrderedHashSet columnList;
        Type           type;

        // Initialization
        tables = allTables();
        ti     = new DITableInfo();

        while (tables.hasNext()) {
            table = (Table) tables.next();
            columnList =
                session.getGrantee().getColumnsForAllPrivileges(table);

            if (columnList.isEmpty()) {
                continue;
            }

            ti.setTable(table);

            columnCount = table.getColumnCount();

            for (int i = 0; i < columnCount; i++) {
                ColumnSchema column = table.getColumn(i);

                type = column.getDataType();

                if (!columnList.contains(column.getName())) {
                    continue;
                }

                row                   = t.getEmptyRowData();
                row[table_cat]        = table.getCatalogName().name;
                row[table_schem]      = table.getSchemaName().name;
                row[table_name]       = table.getName().name;
                row[column_name]      = column.getName().name;
                row[ordinal_position] = ValuePool.getInt(i + 1);
                row[column_default]   = column.getDefaultSQL();
                row[is_nullable]      = column.isNullable() ? "YES"
                                                            : "NO";
                row[data_type]        = type.getFullNameString();

                if (type.isCharacterType()) {
                    row[character_maximum_length] =
                        ValuePool.getLong(type.precision);
                    row[character_octet_length] =
                        ValuePool.getLong(type.precision * 2);
                    row[character_set_catalog] =
                        database.getCatalogName().name;
                    row[character_set_schema] =
                        ((CharacterType) type).getCharacterSet()
                            .getSchemaName().name;
                    row[character_set_name] =
                        ((CharacterType) type).getCharacterSet().getName()
                            .name;
                    row[collation_catalog] = database.getCatalogName().name;
                    row[collation_schema] =
                        ((CharacterType) type).getCollation().getSchemaName()
                            .name;
                    row[collation_name] =
                        ((CharacterType) type).getCollation().getName().name;
                }

                if (type.isBinaryType()) {
                    row[character_maximum_length] =
                        ValuePool.getLong(type.precision);
                    row[character_octet_length] =
                        ValuePool.getLong(type.precision);
                }

                if (type.isNumberType()) {
                    row[numeric_precision] = ValuePool.getLong(type.precision);
                    row[numeric_precision_radix] = ValuePool.getLong(
                        ((NumberType) type).getPrecisionRadix());
                    row[numeric_scale] = ValuePool.getLong(type.scale);
                }

                if (type.isDateTimeType()) {
                    row[datetime_precision] = ValuePool.getLong(type.scale);
                }

                if (type.isIntervalType()) {
                    row[interval_type] =
                        IntervalType.getQualifier(type.typeCode);
                    row[interval_precision] =
                        ValuePool.getLong(type.precision);
                    row[datetime_precision] = ValuePool.getLong(type.scale);
                }

                if (type.isDomainType()) {
                    row[domain_catalog] = database.getCatalogName().name;
                    row[domain_schema]  = type.getSchemaName().name;
                    row[domain_name]    = type.getName().name;
                }

                if (type.isDistinctType()) {
                    row[udt_catalog] = database.getCatalogName().name;
                    row[udt_schema]  = type.getSchemaName().name;
                    row[udt_name]    = type.getName().name;
                }

                row[scope_catalog]       = null;
                row[scope_schema]        = null;
                row[scope_name]          = null;
                row[maximum_cardinality] = null;
                row[dtd_identifier]      = null;
                row[is_self_referencing] = null;
                row[is_identity]         = column.isIdentity() ? "YES"
                                                               : "NO";

                if (column.isIdentity()) {
                    NumberSequence sequence = column.getIdentitySequence();

                    row[identity_generation] = sequence.isAlways() ? "ALWAYS"
                                                                   : "BY DEFAULT";
                    row[identity_start] =
                        Long.toString(sequence.getStartValue());
                    row[identity_increment] =
                        Long.toString(sequence.getIncrement());
                    row[identity_maximum] =
                        Long.toString(sequence.getMaxValue());
                    row[identity_minimum] =
                        Long.toString(sequence.getMinValue());
                    row[identity_cycle] = sequence.isCycle() ? "YES"
                                                             : "NO";
                }

                row[is_generated]          = "NEVER";
                row[generation_expression] = null;
                row[is_updatable]          = table.isWritable() ? "YES"
                                                                : "NO";
                row[declared_data_type]    = row[data_type];

                if (type.isNumberType()) {
                    row[declared_numeric_precision] = row[numeric_precision];
                    row[declared_numeric_scale]     = row[numeric_scale];
                }

                t.insertSys(store, row);
            }
        }

        return t;
    }

    /**
     * The CONSTRAINT_COLUMN_USAGE view has one row for each column identified by
     * a table constraint or assertion.<p>
     *
     * <b>Definition:</b><p>
     *
     *      TABLE_CATALOG       VARCHAR
     *      TABLE_SCHEMA        VARCHAR
     *      TABLE_NAME          VARCHAR
     *      COLUMN_NAME         VARCHAR
     *      CONSTRAINT_CATALOG  VARCHAR
     *      CONSTRAINT_SCHEMA   VARCHAR
     *      CONSTRAINT_NAME     VARCHAR
     *
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, and
     *      COLUMN_NAME are the catalog name, unqualified schema name,
     *      qualified identifier, and column name, respectively, of a column
     *      identified by a &lt;column reference&gt; explicitly or implicitly
     *      contained in the &lt;search condition&gt; of the constraint
     *      being described.
     *
     * <li> The values of CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, and
     *      CONSTRAINT_NAME are the catalog name, unqualified schema name,
     *      and qualified identifier, respectively, of the constraint being
     *      described. <p>
     *
     * </ol>
     *
     * @return Table
     */
    Table CONSTRAINT_COLUMN_USAGE() {

        Table t = sysTables[CONSTRAINT_COLUMN_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[CONSTRAINT_COLUMN_USAGE]);

            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);         // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);        // not null
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CONSTRAINT_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        // column number mappings
        final int table_catalog      = 0;
        final int table_schems       = 1;
        final int table_name         = 2;
        final int column_name        = 3;
        final int constraint_catalog = 4;
        final int constraint_schema  = 5;
        final int constraint_name    = 6;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        String constraintCatalog;
        String constraintSchema;
        String constraintName;

        // Intermediate holders
        Iterator     tables;
        Table        table;
        Constraint[] constraints;
        int          constraintCount;
        Constraint   constraint;
        Iterator     iterator;
        Object[]     row;

        // Initialization
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView()
                    || !session.getGrantee().isFullyAccessibleByRole(table)) {
                continue;
            }

            constraints       = table.getConstraints();
            constraintCount   = constraints.length;
            constraintCatalog = database.getCatalogName().name;
            constraintSchema  = table.getSchemaName().name;

            // process constraints
            for (int i = 0; i < constraintCount; i++) {
                constraint     = constraints[i];
                constraintName = constraint.getName().name;

                switch (constraint.getConstraintType()) {

                    case Constraint.CHECK : {
                        OrderedHashSet expressions =
                            constraint.getCheckColumnExpressions();

                        if (expressions == null) {
                            break;
                        }

                        iterator = expressions.iterator();

                        // calculate distinct column references
                        while (iterator.hasNext()) {
                            ExpressionColumn expr =
                                (ExpressionColumn) iterator.next();
                            HsqlName name = expr.getBaseColumnHsqlName();

                            if (name.type != SchemaObject.COLUMN) {
                                continue;
                            }

                            row = t.getEmptyRowData();
                            row[table_catalog] =
                                database.getCatalogName().name;
                            row[table_schems]       = name.schema.name;
                            row[table_name]         = name.parent.name;
                            row[column_name]        = name.name;
                            row[constraint_catalog] = constraintCatalog;
                            row[constraint_schema]  = constraintSchema;
                            row[constraint_name]    = constraintName;

                            try {
                                t.insertSys(store, row);
                            } catch (HsqlException e) {}
                        }

                        break;
                    }
                    case Constraint.UNIQUE :
                    case Constraint.PRIMARY_KEY :
                    case Constraint.FOREIGN_KEY : {
                        Table target = table;
                        int[] cols   = constraint.getMainColumns();

                        if (constraint.getConstraintType()
                                == Constraint.FOREIGN_KEY) {
                            target = constraint.getMain();
                        }

/*
                       checkme - it seems foreign key columns are not included
                       but columns of the referenced unique constraint are included

                        if (constraint.getType() == Constraint.FOREIGN_KEY) {
                            for (int j = 0; j < cols.length; j++) {
                                row = t.getEmptyRowData();

                                Table mainTable = constraint.getMain();

                                row[table_catalog] = database.getCatalog();
                                row[table_schems] =
                                    mainTable.getSchemaName().name;
                                row[table_name] = mainTable.getName().name;
                                row[column_name] = mainTable.getColumn(
                                    cols[j]).columnName.name;
                                row[constraint_catalog] = constraintCatalog;
                                row[constraint_schema]  = constraintSchema;
                                row[constraint_name]    = constraintName;

                                try {
                                    t.insertSys(row);
                                } catch (HsqlException e) {}
                            }

                            cols = constraint.getRefColumns();
                        }
*/
                        for (int j = 0; j < cols.length; j++) {
                            row = t.getEmptyRowData();
                            row[table_catalog] =
                                database.getCatalogName().name;
                            row[table_schems] = constraintSchema;
                            row[table_name]   = target.getName().name;
                            row[column_name] =
                                target.getColumn(cols[j]).getName().name;
                            row[constraint_catalog] = constraintCatalog;
                            row[constraint_schema]  = constraintSchema;
                            row[constraint_name]    = constraintName;

                            try {
                                t.insertSys(store, row);
                            } catch (HsqlException e) {}
                        }

                        //
                    }
                }
            }
        }

        return t;
    }

    /**
     * The CONSTRAINT_TABLE_USAGE view has one row for each table identified by a
     * &lt;table name&gt; simply contained in a &lt;table reference&gt;
     * contained in the &lt;search condition&gt; of a check constraint,
     * domain constraint, or assertion. It has one row for each table
     * containing / referenced by each PRIMARY KEY, UNIQUE and FOREIGN KEY
     * constraint<p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     *      CONSTRAINT_CATALOG      VARCHAR
     *      CONSTRAINT_SCHEMA       VARCHAR
     *      CONSTRAINT_NAME         VARCHAR
     *      TABLE_CATALOG           VARCHAR
     *      TABLE_SCHEMA            VARCHAR
     *      TABLE_NAME              VARCHAR
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The values of CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, and
     *      CONSTRAINT_NAME are the catalog name, unqualified schema name,
     *       and qualified identifier, respectively, of the constraint being
     *      described. <p>
     *
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, and TABLE_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of a table identified by a &lt;table name&gt;
     *      simply contained in a &lt;table reference&gt; contained in the
     *      *lt;search condition&gt; of the constraint being described, or
     *      its columns.
     * </ol>
     *
     * @return Table
     */
    Table CONSTRAINT_TABLE_USAGE() {

        Table t = sysTables[CONSTRAINT_TABLE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[CONSTRAINT_TABLE_USAGE]);

            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);         // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CONSTRAINT_TABLE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        //
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "select DISTINCT CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, "
            + "CONSTRAINT_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME "
            + "from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE");

        t.insertSys(store, rs);
        sys.close();

        return t;
    }

    Table DATA_TYPE_PRIVILEGES() {

        Table t = sysTables[DATA_TYPE_PRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[DATA_TYPE_PRIVILEGES]);

            addColumn(t, "OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "OBJECT_TYPE", SQL_IDENTIFIER);
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[DATA_TYPE_PRIVILEGES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        //
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*data_type_privileges*/");
        Result rs  = sys.executeDirectStatement(sql);

        t.insertSys(store, rs);
        sys.close();

        return t;
    }

    /**
     * a DEFINITION_SCHEMA table. Not in the INFORMATION_SCHEMA list
     */
/*
    Table DATA_TYPE_DESCRIPTOR() {

        Table t = sysTables[DATA_TYPE_DESCRIPTOR];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[DATA_TYPE_DESCRIPTOR]);

            addColumn(t, "OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_NAME", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_TYPE", CHARACTER_DATA);
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCLAE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "USER_DEFINED_TYPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "MAXIMUM_CARDINALITY", SQL_IDENTIFIER);
            t.createPrimaryKey(null, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        final int       object_catalog             = 0;
        final int       object_schema              = 1;
        final int       object_name                = 2;
        final int       object_type                = 3;
        final int       dtd_identifier             = 4;
        final int       data_type                  = 5;
        final int       character_set_catalog      = 6;
        final int       character_set_schema       = 7;
        final int       character_set_name         = 8;
        final int       character_maximum_length   = 9;
        final int       character_octet_length     = 10;
        final int       collation_catalog          = 11;
        final int       collation_schema           = 12;
        final int       collation_name             = 13;
        final int       numeric_precision          = 14;
        final int       numeric_precision_radix    = 15;
        final int       numeric_scale              = 16;
        final int       declared_data_type         = 17;
        final int       declared_numeric_precision = 18;
        final int       declared_numeric_scale     = 19;
        final int       datetime_precision         = 20;
        final int       interval_type              = 21;
        final int       interval_precision         = 22;
        final int       user_defined_type_catalog  = 23;
        final int       user_defined_type_schema   = 24;
        final int       user_defined_type_name     = 25;
        final int       scope_catalog              = 26;
        final int       scope_schema               = 27;
        final int       scope_name                 = 28;
        final int       maximum_cardinality        = 29;
        return t;
    }
*/

    /**
     *
     * @return Table
     */
    Table DOMAIN_CONSTRAINTS() {

        Table t = sysTables[DOMAIN_CONSTRAINTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[DOMAIN_CONSTRAINTS]);

            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "DOMAIN_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_DEFERRABLE", YES_OR_NO);
            addColumn(t, "INITIALLY_DEFERRED", YES_OR_NO);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[DOMAIN_CONSTRAINTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);

            return t;
        }

        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int domain_catalog     = 3;
        final int domain_schema      = 4;
        final int domain_name        = 5;
        final int is_deferrable      = 6;
        final int initially_deferred = 7;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        //
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.DOMAIN);

        while (it.hasNext()) {
            Type domain = (Type) it.next();

            if (!domain.isDomainType()) {
                continue;
            }

            if (!session.getGrantee().isFullyAccessibleByRole(domain)) {
                continue;
            }

            Constraint[] constraints =
                domain.userTypeModifier.getConstraints();

            for (int i = 0; i < constraints.length; i++) {
                Object[] data = t.getEmptyRowData();

                data[constraint_catalog] = data[domain_catalog] =
                    database.getCatalogName().name;
                data[constraint_schema] = data[domain_schema] =
                    domain.getSchemaName().name;
                data[constraint_name]    = constraints[i].getName().name;
                data[domain_name]        = domain.getName().name;
                data[is_deferrable]      = Tokens.T_NO;
                data[initially_deferred] = Tokens.T_NO;

                t.insertSys(store, data);
            }
        }

        return t;
    }

    /**
     * The DOMAINS view has one row for each domain. <p>
     *
     *
     * <pre class="SqlCodeExample">
     *
     * </pre>
     *
     * @return Table
     */
    Table DOMAINS() {

        Table t = sysTables[DOMAINS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[DOMAINS]);

            addColumn(t, "DOMAIN_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DOMAIN_DEFAULT", CHARACTER_DATA);
            addColumn(t, "MAXIMUM_CARDINALITY", SQL_IDENTIFIER);
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCLAE", CARDINAL_NUMBER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[DOMAINS].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);

            return t;
        }

        final int domain_catalog             = 0;
        final int domain_schema              = 1;
        final int domain_name                = 2;
        final int data_type                  = 3;
        final int character_maximum_length   = 4;
        final int character_octet_length     = 5;
        final int character_set_catalog      = 6;
        final int character_set_schema       = 7;
        final int character_set_name         = 8;
        final int collation_catalog          = 9;
        final int collation_schema           = 10;
        final int collation_name             = 11;
        final int numeric_precision          = 12;
        final int numeric_precision_radix    = 13;
        final int numeric_scale              = 14;
        final int datetime_precision         = 15;
        final int interval_type              = 16;
        final int interval_precision         = 17;
        final int domain_default             = 18;
        final int maximum_cardinality        = 19;
        final int dtd_identifier             = 20;
        final int declared_data_type         = 21;
        final int declared_numeric_precision = 22;
        final int declared_numeric_scale     = 23;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        //
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.DOMAIN);

        while (it.hasNext()) {
            Type domain = (Type) it.next();

            if (!domain.isDomainType()) {
                continue;
            }

            if (!session.getGrantee().isAccessible(domain)) {
                continue;
            }

            Object[] data = t.getEmptyRowData();

            data[domain_catalog] = database.getCatalogName().name;
            data[domain_schema]  = domain.getSchemaName().name;
            data[domain_name]    = domain.getName().name;
            data[data_type]      = domain.getFullNameString();

            if (domain.isCharacterType()) {
                data[character_maximum_length] =
                    ValuePool.getLong(domain.precision);
                data[character_octet_length] =
                    ValuePool.getLong(domain.precision * 2);
                data[character_set_catalog] = database.getCatalogName().name;
                data[character_set_schema] =
                    ((CharacterType) domain).getCharacterSet().getSchemaName()
                        .name;
                data[character_set_name] =
                    ((CharacterType) domain).getCharacterSet().getName().name;
                data[collation_catalog] = database.getCatalogName().name;
                data[collation_schema] =
                    ((CharacterType) domain).getCollation().getSchemaName()
                        .name;
                data[collation_name] =
                    ((CharacterType) domain).getCollation().getName().name;
            } else if (domain.isNumberType()) {
                data[numeric_precision] =
                    ValuePool.getLong(((NumberType) domain).getPrecision());
                data[declared_numeric_precision] = data[numeric_precision];

                if (domain.typeCode != Types.SQL_DOUBLE) {
                    data[numeric_scale] = ValuePool.getLong(domain.scale);
                    data[declared_numeric_scale] = data[numeric_scale];
                }

                data[numeric_precision_radix] = ValuePool.getLong(
                    ((NumberType) domain).getPrecisionRadix());
            } else if (domain.isBooleanType()) {}
            else if (domain.isDateTimeType()) {
                data[datetime_precision] = ValuePool.getLong(domain.scale);
            } else if (domain.isIntervalType()) {
                data[interval_precision] = ValuePool.getLong(domain.precision);
                data[interval_type]      = domain.getFullNameString();
                data[datetime_precision] = ValuePool.getLong(domain.scale);
            } else if (domain.isBinaryType()) {
                data[character_maximum_length] =
                    ValuePool.getLong(domain.precision);
                data[character_octet_length] =
                    ValuePool.getLong(domain.precision);
            } else if (domain.isBitType()) {
                data[character_maximum_length] =
                    ValuePool.getLong(domain.precision);
                data[character_octet_length] =
                    ValuePool.getLong(domain.precision);
            }

            Expression defaultExpression =
                domain.userTypeModifier.getDefaultClause();

            if (defaultExpression != null) {
                data[domain_default] = defaultExpression.getSQL();
            }

            t.insertSys(store, data);
        }

        return t;
    }

    /**
     * ENABLED_ROLES<p>
     *
     * <b>Function</b><p>
     *
     * Identify the enabled roles for the current SQL-session.<p>
     *
     * Definition<p>
     *
     * <pre class="SqlCodeExample">
     * CREATE RECURSIVE VIEW ENABLED_ROLES ( ROLE_NAME ) AS
     *      VALUES ( CURRENT_ROLE )
     *      UNION
     *      SELECT RAD.ROLE_NAME
     *        FROM DEFINITION_SCHEMA.ROLE_AUTHORIZATION_DESCRIPTORS RAD
     *        JOIN ENABLED_ROLES R
     *          ON RAD.GRANTEE = R.ROLE_NAME;
     *
     * GRANT SELECT ON TABLE ENABLED_ROLES
     *    TO PUBLIC WITH GRANT OPTION;
     * </pre>
     */
    Table ENABLED_ROLES() {

        Table t = sysTables[ENABLED_ROLES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ENABLED_ROLES]);

            addColumn(t, "ROLE_NAME", SQL_IDENTIFIER);

            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ENABLED_ROLES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Intermediate holders
        Iterator grantees;
        Grantee  grantee;
        Object[] row;

        // initialization
        grantees = session.getGrantee().getAllRoles().iterator();

        while (grantees.hasNext()) {
            grantee = (Grantee) grantees.next();
            row     = t.getEmptyRowData();
            row[0]  = grantee.getNameString();

            t.insertSys(store, row);
        }

        return t;
    }

    Table JAR_JAR_USAGE() {

        Table t = sysTables[JAR_JAR_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[JAR_JAR_USAGE]);

            addColumn(t, "PATH_JAR_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "PATH_JAR_SCHAMA", SQL_IDENTIFIER);
            addColumn(t, "PATH_JAR_NAME", SQL_IDENTIFIER);
            addColumn(t, "JAR_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "JAR_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "JAR_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[JAR_JAR_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        // column number mappings
        final int path_jar_catalog = 0;
        final int path_jar_schema  = 1;
        final int path_jar_name    = 2;
        final int jar_catalog      = 3;
        final int jar_schema       = 4;
        final int jar_name         = 5;

        //
        Iterator it;
        Object[] row;

        return t;
    }

    Table JARS() {

        Table t = sysTables[JARS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[JARS]);

            addColumn(t, "JAR_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "JAR_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "JAR_NAME", SQL_IDENTIFIER);
            addColumn(t, "JAR_PATH", CHARACTER_DATA);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[JARS].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3
            }, false);

            return t;
        }

        // column number mappings
        final int jar_catalog = 0;
        final int jar_schema  = 1;
        final int jar_name    = 2;
        final int jar_path    = 3;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator        it;
        Object[]        row;

        return t;
    }

    /**
     * Retrieves a <code>Table</code> object describing the
     * primary key and unique constraint columns of each accessible table
     * defined within this database. <p>
     *
     * Each row is a PRIMARY KEY or UNIQUE column description with the following
     * columns: <p>
     *
     * <pre class="SqlCodeExample">
     * CONSTRAINT_CATALOG              VARCHAR NULL,
     * CONSTRAINT_SCHEMA               VARCHAR NULL,
     * CONSTRAINT_NAME                 VARCHAR NOT NULL,
     * TABLE_CATALOG                   VARCHAR   table catalog
     * TABLE_SCHEMA                    VARCHAR   table schema
     * TABLE_NAME                      VARCHAR   table name
     * COLUMN_NAME                     VARCHAR   column name
     * ORDINAL_POSITION                INT
     * POSITION_IN_UNIQUE_CONSTRAINT   INT
     * </pre> <p>
     *
     * @return a <code>Table</code> object describing the visible
     *        primary key and unique columns of each accessible table
     *        defined within this database.
     */
    Table KEY_COLUMN_USAGE() {

        Table t = sysTables[KEY_COLUMN_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[KEY_COLUMN_USAGE]);

            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);                   // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);                        // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);                       // not null
            addColumn(t, "ORDINAL_POSITION", CARDINAL_NUMBER);                 // not null
            addColumn(t, "POSITION_IN_UNIQUE_CONSTRAINT", CARDINAL_NUMBER);    // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[KEY_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                2, 1, 0, 6, 7
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Intermediate holders
        Iterator tables;
        Object[] row;

        // column number mappings
        final int constraint_catalog            = 0;
        final int constraint_schema             = 1;
        final int constraint_name               = 2;
        final int table_catalog                 = 3;
        final int table_schema                  = 4;
        final int table_name                    = 5;
        final int column_name                   = 6;
        final int ordinal_position              = 7;
        final int position_in_unique_constraint = 8;

        // Initialization
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        while (tables.hasNext()) {
            Table  table        = (Table) tables.next();
            String tableCatalog = database.getCatalogName().name;
            String tableSchema  = table.getSchemaName().name;
            String tableName    = table.getName().name;

            /** @todo - requires access to the actual columns */
            if (table.isView() || !isAccessibleTable(table)) {
                continue;
            }

            Constraint[] constraints = table.getConstraints();

            for (int i = 0; i < constraints.length; i++) {
                Constraint constraint = constraints[i];

                if (constraint.getConstraintType() == Constraint.PRIMARY_KEY
                        || constraint.getConstraintType() == Constraint.UNIQUE
                        || constraint.getConstraintType()
                           == Constraint.FOREIGN_KEY) {
                    String constraintName = constraint.getName().name;
                    int[]  cols           = constraint.getMainColumns();
                    int[]  uniqueColMap   = null;

                    if (constraint.getConstraintType()
                            == Constraint.FOREIGN_KEY) {
                        Table uniqueConstTable = constraint.getMain();
                        Constraint uniqueConstraint =
                            uniqueConstTable.getConstraint(
                                constraint.getUniqueName().name);
                        int[] uniqueConstIndexes =
                            uniqueConstraint.getMainColumns();

                        uniqueColMap = new int[cols.length];

                        for (int j = 0; j < cols.length; j++) {
                            uniqueColMap[j] =
                                ArrayUtil.find(uniqueConstIndexes, cols[j]);
                        }

                        cols = constraint.getRefColumns();
                    }

                    for (int j = 0; j < cols.length; j++) {
                        row                     = t.getEmptyRowData();
                        row[constraint_catalog] = tableCatalog;
                        row[constraint_schema]  = tableSchema;
                        row[constraint_name]    = constraintName;
                        row[table_catalog]      = tableCatalog;
                        row[table_schema]       = tableSchema;
                        row[table_name]         = tableName;
                        row[column_name] =
                            table.getColumn(cols[j]).getName().name;
                        row[ordinal_position] = ValuePool.getInt(j + 1);

                        if (constraint.getConstraintType()
                                == Constraint.FOREIGN_KEY) {
                            row[position_in_unique_constraint] =
                                ValuePool.getInt(uniqueColMap[j] + 1);
                        }

                        t.insertSys(store, row);
                    }
                }
            }
        }

        return t;
    }

    Table METHOD_SPECIFICATION_PARAMETERS() {
        return null;
    }

    Table METHOD_SPECIFICATIONS() {
        return null;
    }

    Table MODULE_COLUMN_USAGE() {
        return null;
    }

    Table MODULE_PRIVILEGES() {
        return null;
    }

    Table MODULE_TABLE_USAGE() {
        return null;
    }

    Table MODULES() {
        return null;
    }

    Table PARAMETERS() {

        Table t = sysTables[PARAMETERS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[PARAMETERS]);

            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ORDINAL_POSITION", CARDINAL_NUMBER);
            addColumn(t, "IS_RESULT", YES_OR_NO);
            addColumn(t, "AS_LOCATOR", YES_OR_NO);
            addColumn(t, "PARAMETER_NAME", SQL_IDENTIFIER);

            //
            addColumn(t, "FROM_SQL_SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "FROM_SQL_SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "FROM_SQL_SPECIFIC_NAME", SQL_IDENTIFIER);

            //
            addColumn(t, "TO_SQL_SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TO_SQL_SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TO_SQL_SPECIFIC_NAME", SQL_IDENTIFIER);

            //
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", CHARACTER_DATA);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "MAXIMUM_CARDINALITY", CARDINAL_NUMBER);    // NULL
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[PARAMETERS].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2,
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int specific_cat               = 0;
        final int specific_schem             = 1;
        final int specific_name              = 2;
        final int ordinal_position           = 3;
        final int is_result                  = 4;
        final int as_locator                 = 5;
        final int parameter_name             = 6;
        final int from_specific_catalog      = 7;
        final int from_specific_schema       = 8;
        final int from_specific_name         = 9;
        final int to_specific_catalog        = 10;
        final int to_specific_schema         = 11;
        final int to_specific_name           = 12;
        final int data_type                  = 13;
        final int character_maximum_length   = 14;
        final int character_octet_length     = 15;
        final int character_set_catalog      = 16;
        final int character_set_schema       = 17;
        final int character_set_name         = 18;
        final int collation_catalog          = 19;
        final int collation_schema           = 20;
        final int collation_name             = 21;
        final int numeric_precision          = 22;
        final int numeric_precision_radix    = 23;
        final int numeric_scale              = 24;
        final int datetime_precision         = 25;
        final int interval_type              = 26;
        final int interval_precision         = 27;
        final int udt_catalog                = 28;
        final int udt_schema                 = 29;
        final int udt_name                   = 30;
        final int scope_catalog              = 31;
        final int scope_schema               = 32;
        final int scope_name                 = 33;
        final int maximum_cardinality        = 34;
        final int dtd_identifier             = 35;
        final int declared_data_type         = 36;
        final int declared_numeric_precision = 37;
        final int declared_numeric_scale     = 38;

        return t;
    }

    /**
     * <ol>
     * <li> A constraint is shown in this view if the user has table level
     * privilege of at lease one of the types, INSERT, UPDATE, DELETE,
     * REFERENCES or TRIGGER.
     * </ol>
     *
     * @return Table
     */
    Table REFERENTIAL_CONSTRAINTS() {

        Table t = sysTables[REFERENTIAL_CONSTRAINTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[REFERENTIAL_CONSTRAINTS]);

            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);              // not null
            addColumn(t, "UNIQUE_CONSTRAINT_CATALOG", SQL_IDENTIFIER);    // not null
            addColumn(t, "UNIQUE_CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UNIQUE_CONSTRAINT_NAME", SQL_IDENTIFIER);
            addColumn(t, "MATCH_OPTION", CHARACTER_DATA);                 // not null
            addColumn(t, "UPDATE_RULE", CHARACTER_DATA);                  // not null
            addColumn(t, "DELETE_RULE", CHARACTER_DATA);                  // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[REFERENTIAL_CONSTRAINTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2,
            }, false);

            return t;
        }

        // column number mappings
        final int constraint_catalog        = 0;
        final int constraint_schema         = 1;
        final int constraint_name           = 2;
        final int unique_constraint_catalog = 3;
        final int unique_constraint_schema  = 4;
        final int unique_constraint_name    = 5;
        final int match_option              = 6;
        final int update_rule               = 7;
        final int delete_rule               = 8;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator        tables;
        Table           table;
        Constraint[]    constraints;
        Constraint      constraint;
        Object[]        row;

        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView()
                    || !session.getGrantee().hasNonSelectTableRight(table)) {
                continue;
            }

            constraints = table.getConstraints();

            for (int i = 0; i < constraints.length; i++) {
                constraint = constraints[i];

                if (constraint.getConstraintType() != Constraint.FOREIGN_KEY) {
                    continue;
                }

                HsqlName uniqueName = constraint.getUniqueName();

                row                     = t.getEmptyRowData();
                row[constraint_catalog] = database.getCatalogName().name;
                row[constraint_schema]  = constraint.getSchemaName().name;
                row[constraint_name]    = constraint.getName().name;

                if (isAccessibleTable(constraint.getMain())) {
                    row[unique_constraint_catalog] =
                        database.getCatalogName().name;
                    row[unique_constraint_schema] = uniqueName.schema.name;
                    row[unique_constraint_name]   = uniqueName.name;
                }

                row[match_option] = Tokens.T_NONE;
                row[update_rule]  = constraint.getUpdateActionString();
                row[delete_rule]  = constraint.getDeleteActionString();

                t.insertSys(store, row);
            }
        }

        return t;
    }

    Table ROLE_COLUMN_GRANTS() {

        Table t = sysTables[ROLE_COLUMN_GRANTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_COLUMN_GRANTS]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);        // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);       // not null
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    // not null
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           // not null

            // order: COLUMN_NAME, PRIVILEGE
            // for unique: GRANTEE, GRANTOR, TABLE_NAME, TABLE_SCHEMA, TABLE_CAT
            // false PK, as TABLE_SCHEMA and/or TABLE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_COLUMN_GRANTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                5, 6, 1, 0, 4, 3, 2
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT GRANTOR, GRANTEE, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, PRIVILEGE_TYPE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES "
            + "JOIN INFORMATION_SCHEMA.APPLICABLE_ROLES ON GRANTEE = ROLE_NAME;");

        t.insertSys(store, rs);
        sys.close();

        return t;
    }

    Table ROLE_ROUTINE_GRANTS() {

        Table t = sysTables[ROLE_ROUTINE_GRANTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_ROUTINE_GRANTS]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);          // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);          // not null
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_ROUTINE_GRANTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT GRANTOR, GRANTEE, SPECIFIC_CATALOG, SPECIFIC_SCHEMA, "
            + "SPECIFIC_NAME, ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, "
            + "PRIVILEGE_TYPE, IS_GRANTABLE, 'NO' "
            + "FROM INFORMATION_SCHEMA.ROUTINE_PRIVILEGES "
            + "JOIN INFORMATION_SCHEMA.APPLICABLE_ROLES ON GRANTEE = ROLE_NAME;");

        t.insertSys(store, rs);
        sys.close();

        // Column number mappings
        final int grantor          = 0;
        final int grantee          = 1;
        final int table_name       = 2;
        final int specific_catalog = 3;
        final int specific_schema  = 4;
        final int specific_name    = 5;
        final int routine_catalog  = 6;
        final int routine_schema   = 7;
        final int routine_name     = 8;
        final int privilege_type   = 9;
        final int is_grantable     = 10;

        //
        return t;
    }

    Table ROLE_TABLE_GRANTS() {

        Table t = sysTables[ROLE_TABLE_GRANTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_TABLE_GRANTS]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);        // not null
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    // not null
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           // not null
            addColumn(t, "WITH_HIERARCHY", YES_OR_NO);

            // order:  TABLE_SCHEM, TABLE_NAME, and PRIVILEGE,
            // added for unique:  GRANTEE, GRANTOR,
            // false PK, as TABLE_SCHEM and/or TABLE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_TABLE_GRANTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                3, 4, 5, 0, 1
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT GRANTOR, GRANTEE, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, PRIVILEGE_TYPE, IS_GRANTABLE, 'NO' "
            + "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES "
            + "JOIN INFORMATION_SCHEMA.APPLICABLE_ROLES ON GRANTEE = ROLE_NAME;");

        t.insertSys(store, rs);
        sys.close();

        return t;
    }

    Table ROLE_UDT_GRANTS() {

        Table t = sysTables[ROLE_UDT_GRANTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_UDT_GRANTS]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);     // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);     // not null
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);     // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_TABLE_GRANTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, null, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int grantor        = 0;
        final int grantee        = 1;
        final int udt_catalog    = 2;
        final int udt_schema     = 3;
        final int udt_name       = 4;
        final int privilege_type = 5;
        final int is_grantable   = 6;

        return t;
    }

    Table ROLE_USAGE_GRANTS() {

        Table t = sysTables[ROLE_USAGE_GRANTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_USAGE_GRANTS]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);        // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);        // not null
            addColumn(t, "OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "OBJECT_TYPE", CHARACTER_DATA);    // not null
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);        // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_USAGE_GRANTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6, 7
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT GRANTOR, GRANTEE, OBJECT_CATALOG, OBJECT_SCHEMA, OBJECT_NAME, "
            + "OBJECT_TYPE, PRIVILEGE_TYPE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.USAGE_PRIVILEGES "
            + "JOIN INFORMATION_SCHEMA.APPLICABLE_ROLES ON GRANTEE = ROLE_NAME;");

        t.insertSys(store, rs);
        sys.close();

        return t;
    }

    Table ROUTINE_COLUMN_USAGE() {

        Table t = sysTables[ROUTINE_COLUMN_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_COLUMN_USAGE]);

            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                3, 4, 5, 0, 1, 2, 6, 7, 8, 9
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int routine_catalog  = 3;
        final int routine_schema   = 4;
        final int routine_name     = 5;
        final int table_catalog    = 6;
        final int table_schema     = 7;
        final int table_name       = 8;
        final int column_name      = 9;

        //
        Iterator it;
        Object[] row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);

        while (it.hasNext()) {
            RoutineSchema routine = (RoutineSchema) it.next();

            if (!session.getGrantee().isAccessible(routine)) {
                continue;
            }

            Routine[] specifics = routine.getSpecificRoutines();

            for (int m = 0; m < specifics.length; m++) {
                OrderedHashSet set = specifics[m].getReferences();

                for (int i = 0; i < set.size(); i++) {
                    HsqlName refName = (HsqlName) set.get(i);

                    if (refName.type != SchemaObject.COLUMN) {
                        continue;
                    }

                    if (!session.getGrantee().isAccessible(refName)) {
                        continue;
                    }

                    row = t.getEmptyRowData();

                    //
                    row[specific_catalog] = database.getCatalogName().name;
                    row[specific_schema]  = specifics[m].getSchemaName().name;
                    row[specific_name]    = specifics[m].getName().name;
                    row[routine_catalog]  = database.getCatalogName().name;
                    row[routine_schema]   = routine.getSchemaName().name;
                    row[routine_name]     = routine.getName().name;
                    row[table_catalog]    = database.getCatalogName().name;
                    row[table_schema]     = refName.parent.schema.name;
                    row[table_name]       = refName.parent.name;
                    row[column_name]      = refName.name;

                    try {
                        t.insertSys(store, row);
                    } catch (HsqlException e) {}
                }
            }
        }

        return t;
    }

    Table ROUTINE_PRIVILEGES() {

        Table t = sysTables[ROUTINE_PRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_PRIVILEGES]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           // not null
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);     // not null
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);      // not null
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    // not null
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           // not null

            //
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_PRIVILEGES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9
            }, false);

            return t;
        }

        // column number mappings
        final int grantor          = 0;
        final int grantee          = 1;
        final int specific_catalog = 2;
        final int specific_schema  = 3;
        final int specific_name    = 4;
        final int routine_catalog  = 5;
        final int routine_schema   = 6;
        final int routine_name     = 7;
        final int privilege_type   = 8;
        final int is_grantable     = 9;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // calculated column values
        Grantee granteeObject;
        String  privilege;

        // intermediate holders
        Iterator      routines;
        RoutineSchema routine;
        Object[]      row;
        OrderedHashSet grantees =
            session.getGrantee().getGranteeAndAllRolesWithPublic();

        routines = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);

        while (routines.hasNext()) {
            routine = (RoutineSchema) routines.next();

            for (int i = 0; i < grantees.size(); i++) {
                granteeObject = (Grantee) grantees.get(i);

                OrderedHashSet rights =
                    granteeObject.getAllDirectPrivileges(routine);
                OrderedHashSet grants =
                    granteeObject.getAllGrantedPrivileges(routine);

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

                        Routine[] specifics = routine.getSpecificRoutines();

                        for (int m = 0; m < specifics.length; m++) {
                            privilege = Right.privilegeNames[k];
                            row       = t.getEmptyRowData();

                            //
                            row[grantor] = right.getGrantor().getName().name;
                            row[grantee] = right.getGrantee().getName().name;
                            row[specific_catalog] =
                                database.getCatalogName().name;
                            row[specific_schema] =
                                specifics[m].getSchemaName().name;
                            row[specific_name] = specifics[m].getName().name;
                            row[routine_catalog] =
                                database.getCatalogName().name;
                            row[routine_schema] = routine.getSchemaName().name;
                            row[routine_name]   = routine.getName().name;
                            row[privilege_type] = privilege;
                            row[is_grantable] =
                                right.getGrantee() == routine.getOwner()
                                || grantableRight.canAccess(
                                    Right.privilegeTypes[k]) ? "YES"
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

    Table ROUTINE_JAR_USAGE() {

        Table t = sysTables[ROUTINE_JAR_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_JAR_USAGE]);

            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "JAR_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "JAR_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "JAR_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_JAR_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        // column number mappings
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int jar_catalog      = 3;
        final int jar_schema       = 4;
        final int jar_name         = 5;

        //
        Iterator        it;
        Object[]        row;
        PersistentStore store = database.persistentStoreCollection.getStore(t);

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);

        while (it.hasNext()) {
            RoutineSchema routine = (RoutineSchema) it.next();

            if (!session.getGrantee().isAccessible(routine)) {
                continue;
            }

            Routine[] specifics = routine.getSpecificRoutines();

            for (int m = 0; m < specifics.length; m++) {
                if (specifics[m].getLanguage() != Routine.LANGUAGE_JAVA) {
                    continue;
                }

                row                   = t.getEmptyRowData();
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = routine.getSchemaName().name;
                row[specific_name]    = routine.getName().name;
                row[jar_catalog]      = database.getCatalogName().name;
                row[jar_schema] =
                    database.schemaManager.getSQLJSchemaHsqlName();
                row[jar_name] = "CLASSPATH";

                t.insertSys(store, row);
            }
        }

        return t;
    }

    /**
     * needs to provide list of specific referenced routines
     */
    Table ROUTINE_ROUTINE_USAGE() {

        Table t = sysTables[ROUTINE_ROUTINE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_ROUTINE_USAGE]);

            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_ROUTINE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        // column number mappings
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int routine_catalog  = 3;
        final int routine_schema   = 4;
        final int routine_name     = 5;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator        it;
        Object[]        row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);

        while (it.hasNext()) {
            RoutineSchema routine = (RoutineSchema) it.next();

            if (!session.getGrantee().isAccessible(routine)) {
                continue;
            }

            Routine[] specifics = routine.getSpecificRoutines();

            for (int m = 0; m < specifics.length; m++) {
                OrderedHashSet set = specifics[m].getReferences();

                for (int i = 0; i < set.size(); i++) {
                    HsqlName refName = (HsqlName) set.get(i);

                    if (refName.type != SchemaObject.FUNCTION
                            && refName.type != SchemaObject.PROCEDURE) {
                        continue;
                    }

                    if (!session.getGrantee().isAccessible(refName)) {
                        continue;
                    }

                    row                   = t.getEmptyRowData();
                    row[specific_catalog] = database.getCatalogName().name;
                    row[specific_schema]  = specifics[m].getSchemaName().name;
                    row[specific_name]    = specifics[m].getName().name;
                    row[routine_catalog]  = database.getCatalogName().name;
                    row[routine_schema]   = refName.schema.name;
                    row[routine_name]     = refName.name;

                    try {
                        t.insertSys(store, row);
                    } catch (HsqlException e) {}
                }
            }
        }

        return t;
    }

    Table ROUTINE_SEQUENCE_USAGE() {

        Table t = sysTables[ROUTINE_SEQUENCE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_SEQUENCE_USAGE]);

            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_SEQUENCE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        // column number mappings
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int sequence_catalog = 3;
        final int sequence_schema  = 4;
        final int sequence_name    = 5;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator        it;
        Object[]        row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);

        while (it.hasNext()) {
            RoutineSchema routine = (RoutineSchema) it.next();

            if (!session.getGrantee().isAccessible(routine)) {
                continue;
            }

            Routine[] specifics = routine.getSpecificRoutines();

            for (int m = 0; m < specifics.length; m++) {
                OrderedHashSet set = specifics[m].getReferences();

                for (int i = 0; i < set.size(); i++) {
                    HsqlName refName = (HsqlName) set.get(i);

                    if (refName.type != SchemaObject.SEQUENCE) {
                        continue;
                    }

                    if (!session.getGrantee().isAccessible(refName)) {
                        continue;
                    }

                    row                   = t.getEmptyRowData();
                    row[specific_catalog] = database.getCatalogName().name;
                    row[specific_schema]  = specifics[m].getSchemaName().name;
                    row[specific_name]    = specifics[m].getName().name;
                    row[sequence_catalog] = database.getCatalogName().name;
                    row[sequence_schema]  = refName.schema.name;
                    row[sequence_name]    = refName.name;

                    try {
                        t.insertSys(store, row);
                    } catch (HsqlException e) {}
                }
            }
        }

        return t;
    }

    Table ROUTINE_TABLE_USAGE() {

        Table t = sysTables[ROUTINE_TABLE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_TABLE_USAGE]);

            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_TABLE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                3, 4, 5, 0, 1, 2, 6, 7, 8
            }, false);

            return t;
        }

        // column number mappings
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int routine_catalog  = 3;
        final int routine_schema   = 4;
        final int routine_name     = 5;
        final int table_catalog    = 6;
        final int table_schema     = 7;
        final int table_name       = 8;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator        it;
        Object[]        row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);

        while (it.hasNext()) {
            RoutineSchema routine = (RoutineSchema) it.next();

            if (!session.getGrantee().isAccessible(routine)) {
                continue;
            }

            Routine[] specifics = routine.getSpecificRoutines();

            for (int m = 0; m < specifics.length; m++) {
                OrderedHashSet set = specifics[m].getReferences();

                for (int i = 0; i < set.size(); i++) {
                    HsqlName refName = (HsqlName) set.get(i);

                    if (refName.type != SchemaObject.TABLE
                            && refName.type != SchemaObject.VIEW) {
                        continue;
                    }

                    if (!session.getGrantee().isAccessible(refName)) {
                        continue;
                    }

                    row                   = t.getEmptyRowData();
                    row[specific_catalog] = database.getCatalogName().name;
                    row[specific_schema]  = specifics[m].getSchemaName().name;
                    row[specific_name]    = specifics[m].getName().name;
                    row[routine_catalog]  = database.getCatalogName().name;
                    row[routine_schema]   = routine.getSchemaName().name;
                    row[routine_name]     = routine.getName().name;
                    row[table_catalog]    = database.getCatalogName().name;
                    row[table_schema]     = refName.schema.name;
                    row[table_name]       = refName.name;

                    try {
                        t.insertSys(store, row);
                    } catch (HsqlException e) {}
                }
            }
        }

        return t;
    }

    Table ROUTINES() {

        Table t = sysTables[ROUTINES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINES]);

            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_TYPE", CHARACTER_DATA);
            addColumn(t, "MODULE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "MODULE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "MODULE_NAME", SQL_IDENTIFIER);
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);          //
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);        //
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "TYPE_UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TYPE_UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TYPE_UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_NAME", SQL_IDENTIFIER);                //
            addColumn(t, "MAXIMUM_CARDINALITY", CARDINAL_NUMBER);      // NULL (only for array tyes)
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_BODY", CHARACTER_DATA);
            addColumn(t, "ROUTINE_DEFINITION", CHARACTER_DATA);
            addColumn(t, "EXTERNAL_NAME", CHARACTER_DATA);
            addColumn(t, "EXTERNAL_LANGUAGE", CHARACTER_DATA);
            addColumn(t, "PARAMETER_STYLE", CHARACTER_DATA);
            addColumn(t, "IS_DETERMINISTIC", YES_OR_NO);
            addColumn(t, "SQL_DATA_ACCESS", CHARACTER_DATA);
            addColumn(t, "IS_NULL_CALL", YES_OR_NO);
            addColumn(t, "SQL_PATH", CHARACTER_DATA);
            addColumn(t, "SCHEMA_LEVEL_ROUTINE", YES_OR_NO);           //
            addColumn(t, "MAX_DYNAMIC_RESULT_SETS", CARDINAL_NUMBER);
            addColumn(t, "IS_USER_DEFINED_CAST", YES_OR_NO);
            addColumn(t, "IS_IMPLICITLY_INVOCABLE", YES_OR_NO);
            addColumn(t, "SECURITY_TYPE", CHARACTER_DATA);
            addColumn(t, "TO_SQL_SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TO_SQL_SPECIFIC_SCHEMA", SQL_IDENTIFIER);    //
            addColumn(t, "TO_SQL_SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "AS_LOCATOR", YES_OR_NO);
            addColumn(t, "CREATED", TIME_STAMP);
            addColumn(t, "LAST_ALTERED", TIME_STAMP);
            addColumn(t, "NEW_SAVEPOINT_LEVEL", YES_OR_NO);
            addColumn(t, "IS_UDT_DEPENDENT", YES_OR_NO);
            addColumn(t, "RESULT_CAST_FROM_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "RESULT_CAST_AS_LOCATOR", YES_OR_NO);
            addColumn(t, "RESULT_CAST_CHAR_MAX_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_CHAR_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_CHAR_SET_CATALOG", CHARACTER_DATA);
            addColumn(t, "RESULT_CAST_CHAR_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_NUMERIC_RADIX", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "RESULT_CAST_INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_TYPE_UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_TYPE_UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_TYPE_UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_SCOPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_MAX_CARDINALITY", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_DTD_IDENTIFIER", CHARACTER_DATA);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_FROM_DECLARED_DATA_TYPE",
                      CHARACTER_DATA);
            addColumn(t, "RESULT_CAST_DECLARED_NUMERIC_PRECISION",
                      CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_DECLARED_NUMERIC_SCALE",
                      CARDINAL_NUMBER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINES].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                3, 4, 5, 0, 1, 2
            }, false);

            return t;
        }

        // column number mappings
        final int specific_catalog                       = 0;
        final int specific_schema                        = 1;
        final int specific_name                          = 2;
        final int routine_catalog                        = 3;
        final int routine_schema                         = 4;
        final int routine_name                           = 5;
        final int routine_type                           = 6;
        final int module_catalog                         = 7;
        final int module_schema                          = 8;
        final int module_name                            = 9;
        final int udt_catalog                            = 10;
        final int udt_schema                             = 11;
        final int udt_name                               = 12;
        final int data_type                              = 13;
        final int character_maximum_length               = 14;
        final int character_octet_length                 = 15;
        final int character_set_catalog                  = 16;
        final int character_set_schema                   = 17;
        final int character_set_name                     = 18;
        final int collation_catalog                      = 19;
        final int collation_schema                       = 20;
        final int collation_name                         = 21;
        final int numeric_precision                      = 22;
        final int numeric_precision_radix                = 23;
        final int numeric_scale                          = 24;
        final int datetime_precision                     = 25;
        final int interval_type                          = 26;
        final int interval_precision                     = 27;
        final int type_udt_catalog                       = 28;
        final int type_udt_schema                        = 29;
        final int type_udt_name                          = 30;
        final int scope_catalog                          = 31;
        final int scope_schema                           = 32;
        final int scope_name                             = 33;
        final int maximum_cardinality                    = 34;
        final int dtd_identifier                         = 35;
        final int routine_body                           = 36;
        final int routine_definition                     = 37;
        final int external_name                          = 38;
        final int external_language                      = 39;
        final int parameter_style                        = 40;
        final int is_deterministic                       = 41;
        final int sql_data_access                        = 42;
        final int is_null_call                           = 43;
        final int sql_path                               = 44;
        final int schema_level_routine                   = 45;
        final int max_dynamic_result_sets                = 46;
        final int is_user_defined_cast                   = 47;
        final int is_implicitly_invocable                = 48;
        final int security_type                          = 49;
        final int to_sql_specific_catalog                = 50;
        final int to_sql_specific_schema                 = 51;
        final int to_sql_specific_name                   = 52;
        final int as_locator                             = 53;
        final int created                                = 54;
        final int last_altered                           = 55;
        final int new_savepoint_level                    = 56;
        final int is_udt_dependent                       = 57;
        final int result_cast_from_data_type             = 58;
        final int result_cast_as_locator                 = 59;
        final int result_cast_char_max_length            = 60;
        final int result_cast_char_octet_length          = 61;
        final int result_cast_char_set_catalog           = 62;
        final int result_cast_char_set_schema            = 63;
        final int result_cast_character_set_name         = 64;
        final int result_cast_collation_catalog          = 65;
        final int result_cast_collation_schema           = 66;
        final int result_cast_collation_name             = 67;
        final int result_cast_numeric_precision          = 68;
        final int result_cast_numeric_radix              = 69;
        final int result_cast_numeric_scale              = 70;
        final int result_cast_datetime_precision         = 71;
        final int result_cast_interval_type              = 72;
        final int result_cast_interval_precision         = 73;
        final int result_cast_type_udt_catalog           = 74;
        final int result_cast_type_udt_schema            = 75;
        final int result_cast_type_udt_name              = 76;
        final int result_cast_scope_catalog              = 77;
        final int result_cast_scope_schema               = 78;
        final int result_cast_scope_name                 = 79;
        final int result_cast_max_cardinality            = 80;
        final int result_cast_dtd_identifier             = 81;
        final int declared_data_type                     = 82;
        final int declared_numeric_precision             = 83;
        final int declared_numeric_scale                 = 84;
        final int result_cast_from_declared_data_type    = 85;
        final int result_cast_declared_numeric_precision = 86;
        final int result_cast_declared_numeric_scale     = 87;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator        it;
        Object[]        row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);

        while (it.hasNext()) {
            RoutineSchema routine = (RoutineSchema) it.next();

            if (!session.getGrantee().isAccessible(routine)) {
                continue;
            }

            Routine[] specifics = routine.getSpecificRoutines();

            for (int m = 0; m < specifics.length; m++) {
                row = t.getEmptyRowData();

                Routine specific = specifics[m];
                Type    type     = specific.isProcedure() ? null
                                                          : specific
                                                              .getReturnType();

                //
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = specific.getSchemaName().name;
                row[specific_name]    = specific.getSpecificName().name;
                row[routine_catalog]  = database.getCatalogName().name;
                row[routine_schema]   = routine.getSchemaName().name;
                row[routine_name]     = specific.getName().name;
                row[routine_type]     = specific.isProcedure() ? "PROCEDURE"
                                                               : "FUNCTION";
                row[module_catalog]   = null;
                row[module_schema]    = null;
                row[module_name]      = null;
                row[udt_catalog]      = null;
                row[udt_schema]       = null;
                row[udt_name]         = null;
                row[data_type]        = type == null ? null
                                                     : type.getNameString();

                if (type != null && type.isCharacterType()) {
                    row[character_maximum_length] =
                        ValuePool.getLong(type.precision);
                    row[character_octet_length] =
                        ValuePool.getLong(type.precision * 2);
                    row[character_set_catalog] =
                        database.getCatalogName().name;
                    row[character_set_schema] =
                        ((CharacterType) type).getCharacterSet()
                            .getSchemaName().name;
                    row[character_set_name] =
                        ((CharacterType) type).getCharacterSet().getName()
                            .name;
                    row[collation_catalog] = database.getCatalogName().name;
                    row[collation_schema] =
                        ((CharacterType) type).getCollation().getSchemaName()
                            .name;
                    row[collation_name] =
                        ((CharacterType) type).getCollation().getName().name;
                }

                if (type != null && type.isNumberType()) {
                    row[numeric_precision] = ValuePool.getLong(type.precision);
                    row[numeric_precision_radix] = ValuePool.getLong(
                        ((NumberType) type).getPrecisionRadix());
                    row[numeric_scale] = ValuePool.getLong(type.precision);
                }

                if (type != null
                        && (type.isIntervalType() || type.isDateTimeType())) {
                    row[datetime_precision] = ValuePool.getLong(type.scale);
                }

                if (type != null && type.isIntervalType()) {
                    row[interval_type] =
                        IntervalType.getQualifier(type.typeCode);
                    row[interval_precision] =
                        ValuePool.getLong(type.precision);
                }

                row[type_udt_catalog]    = null;
                row[type_udt_schema]     = null;
                row[type_udt_name]       = null;
                row[scope_catalog]       = null;
                row[scope_schema]        = null;
                row[scope_name]          = null;
                row[maximum_cardinality] = null;
                row[dtd_identifier]      = null;    //**
                row[routine_body] = specific.getLanguage()
                                    == Routine.LANGUAGE_JAVA ? "EXTERNAL"
                                                             : "SQL";
                row[routine_definition] = specific.getSQL();
                row[external_name] =
                    specific.getLanguage() == Routine.LANGUAGE_JAVA
                    ? specific.getMethod().getName()
                    : null;
                row[external_language] = specific.getLanguage()
                                         == Routine.LANGUAGE_JAVA ? "JAVA"
                                                                  : null;
                row[parameter_style] = specific.getLanguage()
                                       == Routine.LANGUAGE_JAVA ? "JAVA"
                                                                : null;
                row[is_deterministic] = specific.isDeterministic() ? "YES"
                                                                   : "NO";
                row[sql_data_access]  = specific.getDataImpactString();
                row[is_null_call]     = type == null ? null
                                                     : specific.isNullInputOutput()
                                                       ? "YES"
                                                       : "NO";
                row[sql_path]                               = null;
                row[schema_level_routine]                   = "YES";
                row[max_dynamic_result_sets] = ValuePool.getLong(0);
                row[is_user_defined_cast] = type == null ? null
                                                         : "NO";
                row[is_implicitly_invocable]                = null;
                row[security_type]                          = "DEFINER";
                row[to_sql_specific_catalog]                = null;
                row[to_sql_specific_schema]                 = null;
                row[to_sql_specific_name]                   = null;
                row[as_locator] = type == null ? null
                                               : "NO";
                row[created]                                = null;
                row[last_altered]                           = null;
                row[new_savepoint_level]                    = "YES";
                row[is_udt_dependent]                       = null;
                row[result_cast_from_data_type]             = null;
                row[result_cast_as_locator]                 = null;
                row[result_cast_char_max_length]            = null;
                row[result_cast_char_octet_length]          = null;
                row[result_cast_char_set_catalog]           = null;
                row[result_cast_char_set_schema]            = null;
                row[result_cast_character_set_name]         = null;
                row[result_cast_collation_catalog]          = null;
                row[result_cast_collation_schema]           = null;
                row[result_cast_collation_name]             = null;
                row[result_cast_numeric_precision]          = null;
                row[result_cast_numeric_radix]              = null;
                row[result_cast_numeric_scale]              = null;
                row[result_cast_datetime_precision]         = null;
                row[result_cast_interval_type]              = null;
                row[result_cast_interval_precision]         = null;
                row[result_cast_type_udt_catalog]           = null;
                row[result_cast_type_udt_schema]            = null;
                row[result_cast_type_udt_name]              = null;
                row[result_cast_scope_catalog]              = null;
                row[result_cast_scope_schema]               = null;
                row[result_cast_scope_name]                 = null;
                row[result_cast_max_cardinality]            = null;
                row[result_cast_dtd_identifier]             = null;
                row[declared_data_type]                     = row[data_type];
                row[declared_numeric_precision] = row[numeric_precision];
                row[declared_numeric_scale] = row[numeric_scale];
                row[result_cast_from_declared_data_type]    = null;
                row[result_cast_declared_numeric_precision] = null;
                row[result_cast_declared_numeric_scale]     = null;

                t.insertSys(store, row);
            }
        }

        return t;
    }

    /**
     * SCHEMATA<p>
     *
     * <b>Function</b><p>
     *
     * The SCHEMATA view has one row for each accessible schema. <p>
     *
     * <b>Definition</b><p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SCHEMATA (
     *      CATALOG_NAME INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *      SCHEMA_NAME INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *      SCHEMA_OWNER INFORMATION_SCHEMA.SQL_IDENTIFIER
     *          CONSTRAINT SCHEMA_OWNER_NOT_NULL
     *              NOT NULL,
     *      DEFAULT_CHARACTER_SET_CATALOG INFORMATION_SCHEMA.SQL_IDENTIFIER
     *          CONSTRAINT DEFAULT_CHARACTER_SET_CATALOG_NOT_NULL
     *              NOT NULL,
     *      DEFAULT_CHARACTER_SET_SCHEMA INFORMATION_SCHEMA.SQL_IDENTIFIER
     *          CONSTRAINT DEFAULT_CHARACTER_SET_SCHEMA_NOT_NULL
     *              NOT NULL,
     *      DEFAULT_CHARACTER_SET_NAME INFORMATION_SCHEMA.SQL_IDENTIFIER
     *          CONSTRAINT DEFAULT_CHARACTER_SET_NAME_NOT_NULL
     *              NOT NULL,
     *      SQL_PATH INFORMATION_SCHEMA.CHARACTER_DATA,
     *
     *      CONSTRAINT SCHEMATA_PRIMARY_KEY
     *          PRIMARY KEY ( CATALOG_NAME, SCHEMA_NAME ),
     *      CONSTRAINT SCHEMATA_FOREIGN_KEY_AUTHORIZATIONS
     *          FOREIGN KEY ( SCHEMA_OWNER )
     *              REFERENCES AUTHORIZATIONS,
     *      CONSTRAINT SCHEMATA_FOREIGN_KEY_CATALOG_NAMES
     *          FOREIGN KEY ( CATALOG_NAME )
     *              REFERENCES CATALOG_NAMES
     *      )
     * </pre>
     *
     * <b>Description</b><p>
     *
     * <ol>
     *      <li>The value of CATALOG_NAME is the name of the catalog of the
     *          schema described by this row.<p>
     *
     *      <li>The value of SCHEMA_NAME is the unqualified schema name of
     *          the schema described by this row.<p>
     *
     *      <li>The values of SCHEMA_OWNER are the authorization identifiers
     *          that own the schemata.<p>
     *
     *      <li>The values of DEFAULT_CHARACTER_SET_CATALOG,
     *          DEFAULT_CHARACTER_SET_SCHEMA, and DEFAULT_CHARACTER_SET_NAME
     *          are the catalog name, unqualified schema name, and qualified
     *          identifier, respectively, of the default character set for
     *          columns and domains in the schemata.<p>
     *
     *      <li>Case:<p>
     *          <ul>
     *              <li>If &lt;schema path specification&gt; was specified in
     *                  the &lt;schema definition&gt; that defined the schema
     *                  described by this row and the character representation
     *                  of the &lt;schema path specification&gt; can be
     *                  represented without truncation, then the value of
     *                  SQL_PATH is that character representation.<p>
     *
     *              <li>Otherwise, the value of SQL_PATH is the null value.
     *         </ul>
     * </ol>
     *
     * @return Table
     */
    Table SCHEMATA() {

        Table t = sysTables[SCHEMATA];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SCHEMATA]);

            addColumn(t, "CATALOG_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCHEMA_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCHEMA_OWNER", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "SQL_PATH", CHARACTER_DATA);

            // order: CATALOG_NAME, SCHEMA_NAME
            // false PK, as rows may have NULL CATALOG_NAME
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SCHEMATA].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Intermediate holders
        Iterator schemas;
        String   schema;
        String   dcsSchema = SqlInvariants.INFORMATION_SCHEMA;
        String   dcsName   = ValuePool.getString("UTF16");
        String   sqlPath   = null;
        Grantee  user      = session.getGrantee();
        Object[] row;

        // column number mappings
        final int schema_catalog                = 0;
        final int schema_name                   = 1;
        final int schema_owner                  = 2;
        final int default_character_set_catalog = 3;
        final int default_character_set_schema  = 4;
        final int default_character_set_name    = 5;
        final int sql_path                      = 6;

        // Initialization
        schemas = database.schemaManager.fullSchemaNamesIterator();

        // Do it.
        while (schemas.hasNext()) {
            schema = (String) schemas.next();

            if (!user.hasSchemaUpdateOrGrantRights(schema)) {
                continue;
            }

            row                 = t.getEmptyRowData();
            row[schema_catalog] = database.getCatalogName().name;
            row[schema_name]    = schema;
            row[schema_owner] =
                database.schemaManager.toSchemaOwner(schema).getNameString();
            row[default_character_set_catalog] =
                database.getCatalogName().name;
            row[default_character_set_schema] = dcsSchema;
            row[default_character_set_name]   = dcsName;
            row[sql_path]                     = sqlPath;

            t.insertSys(store, row);
        }

        return t;
    }

    Table SQL_FEATURES() {

        Table t = sysTables[SQL_FEATURES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_FEATURES]);

            addColumn(t, "FEATURE_ID", CHARACTER_DATA);
            addColumn(t, "FEATURE_NAME", CHARACTER_DATA);
            addColumn(t, "SUB_FEATURE_ID", CHARACTER_DATA);
            addColumn(t, "SUB_FEATURE_NAME", CHARACTER_DATA);
            addColumn(t, "IS_SUPPORTED", YES_OR_NO);
            addColumn(t, "IS_VERIFIED_BY", CHARACTER_DATA);
            addColumn(t, "COMMENTS", CHARACTER_DATA);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_FEATURES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 2
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*sql_features*/");
        Result rs = sys.executeDirectStatement(sql);

        t.insertSys(store, rs);

        return t;
    }

    Table SQL_IMPLEMENTATION_INFO() {

        Table t = sysTables[SQL_IMPLEMENTATION_INFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_IMPLEMENTATION_INFO]);

            addColumn(t, "IMPLEMENTATION_INFO_ID", CHARACTER_DATA);
            addColumn(t, "IMPLEMENTATION_INFO_NAME", CHARACTER_DATA);
            addColumn(t, "INTEGER_VALUE", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_VALUE", CHARACTER_DATA);
            addColumn(t, "COMMENTS", CHARACTER_DATA);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_IMPLEMENTATION_INFO].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());

/*
        Result rs = sys.executeDirectStatement(
            "VALUES "
            + ";");

        t.insertSys(store, rs);
*/
        return t;
    }

    Table SQL_PACKAGES() {

        Table t = sysTables[SQL_PACKAGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_PACKAGES]);

            addColumn(t, "ID", CHARACTER_DATA);
            addColumn(t, "NAME", CHARACTER_DATA);
            addColumn(t, "IS_SUPPORTED", YES_OR_NO);
            addColumn(t, "IS_VERIFIED_BY", CHARACTER_DATA);
            addColumn(t, "COMMENTS", CHARACTER_DATA);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_PACKAGES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*sql_packages*/");
        Result rs = sys.executeDirectStatement(sql);

        t.insertSys(store, rs);

        return t;
    }

    Table SQL_PARTS() {

        Table t = sysTables[SQL_PARTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_PARTS]);

            addColumn(t, "PART", CHARACTER_DATA);
            addColumn(t, "NAME", CHARACTER_DATA);
            addColumn(t, "IS_SUPPORTED", YES_OR_NO);
            addColumn(t, "IS_VERIFIED_BY", CHARACTER_DATA);
            addColumn(t, "COMMENTS", CHARACTER_DATA);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_PARTS].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*sql_parts*/");
        Result rs = sys.executeDirectStatement(sql);


        t.insertSys(store, rs);

        return t;
    }

    Table SQL_SIZING() {

        Table t = sysTables[SQL_SIZING];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_SIZING]);

            addColumn(t, "SIZING_ID", CARDINAL_NUMBER);
            addColumn(t, "SIZING_NAME", CHARACTER_DATA);
            addColumn(t, "SUPPORTED_VALUE", CARDINAL_NUMBER);
            addColumn(t, "COMMENTS", CHARACTER_DATA);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_SIZING].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());

        String sql = (String) statementMap.get("/*sql_sizing*/");
        Result rs = sys.executeDirectStatement(sql);

        t.insertSys(store, rs);

        return t;
    }

    Table SQL_SIZING_PROFILES() {

        Table t = sysTables[SQL_SIZING_PROFILES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_SIZING_PROFILES]);

            addColumn(t, "SIZING_ID", CARDINAL_NUMBER);
            addColumn(t, "SIZING_NAME", CHARACTER_DATA);
            addColumn(t, "PROFILE_ID", CARDINAL_NUMBER);
            addColumn(t, "PROFILE_NAME", CHARACTER_DATA);
            addColumn(t, "REQUIRED_VALUE", CARDINAL_NUMBER);
            addColumn(t, "COMMENTS", CHARACTER_DATA);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_SIZING_PROFILES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[]{ 0 }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());

        /*
                Result rs = sys.executeDirectStatement(
                    "VALUES "
                    + ";");

                t.insertSys(store, rs);
        */
        return t;
    }

    /**
     * The TABLE_CONSTRAINTS table has one row for each table constraint
     * associated with a table.  <p>
     *
     * It effectively contains a representation of the table constraint
     * descriptors. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_TABLE_CONSTRAINTS (
     *      CONSTRAINT_CATALOG      VARCHAR NULL,
     *      CONSTRAINT_SCHEMA       VARCHAR NULL,
     *      CONSTRAINT_NAME         VARCHAR NOT NULL,
     *      CONSTRAINT_TYPE         VARCHAR NOT NULL,
     *      TABLE_CATALOG           VARCHAR NULL,
     *      TABLE_SCHEMA            VARCHAR NULL,
     *      TABLE_NAME              VARCHAR NOT NULL,
     *      IS_DEFERRABLE           VARCHAR NOT NULL,
     *      INITIALLY_DEFERRED      VARCHAR NOT NULL,
     *
     *      CHECK ( CONSTRAINT_TYPE IN
     *                      ( 'UNIQUE', 'PRIMARY KEY',
     *                        'FOREIGN KEY', 'CHECK' ) ),
     *
     *      CHECK ( ( IS_DEFERRABLE, INITIALLY_DEFERRED ) IN
     *              ( VALUES ( 'NO',  'NO'  ),
     *                       ( 'YES', 'NO'  ),
     *                       ( 'YES', 'YES' ) ) )
     * )
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The values of CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, and
     *      CONSTRAINT_NAME are the catalog name, unqualified schema
     *      name, and qualified identifier, respectively, of the
     *      constraint being described. If the &lt;table constraint
     *      definition&gt; or &lt;add table constraint definition&gt;
     *      that defined the constraint did not specify a
     *      &lt;constraint name&gt;, then the values of CONSTRAINT_CATALOG,
     *      CONSTRAINT_SCHEMA, and CONSTRAINT_NAME are
     *      implementation-defined. <p>
     *
     * <li> The values of CONSTRAINT_TYPE have the following meanings: <p>
     *  <table border cellpadding="3">
     *  <tr>
     *      <td nowrap>FOREIGN KEY</td>
     *      <td nowrap>The constraint being described is a
     *                 foreign key constraint.</td>
     *  </tr>
     *  <tr>
     *      <td nowrap>UNIQUE</td>
     *      <td nowrap>The constraint being described is a
     *                 unique constraint.</td>
     *  </tr>
     *  <tr>
     *      <td nowrap>PRIMARY KEY</td>
     *      <td nowrap>The constraint being described is a
     *                 primary key constraint.</td>
     *  </tr>
     *  <tr>
     *      <td nowrap>CHECK</td>
     *      <td nowrap>The constraint being described is a
     *                 check constraint.</td>
     *  </tr>
     * </table> <p>
     *
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, and TABLE_NAME are
     *      the catalog name, the unqualified schema name, and the
     *      qualified identifier of the name of the table to which the
     *      table constraint being described applies. <p>
     *
     * <li> The values of IS_DEFERRABLE have the following meanings: <p>
     *
     *  <table>
     *      <tr>
     *          <td nowrap>YES</td>
     *          <td nowrap>The table constraint is deferrable.</td>
     *      </tr>
     *      <tr>
     *          <td nowrap>NO</td>
     *          <td nowrap>The table constraint is not deferrable.</td>
     *      </tr>
     *  </table> <p>
     *
     * <li> The values of INITIALLY_DEFERRED have the following meanings: <p>
     *
     *  <table>
     *      <tr>
     *          <td nowrap>YES</td>
     *          <td nowrap>The table constraint is initially deferred.</td>
     *      </tr>
     *      <tr>
     *          <td nowrap>NO</td>
     *          <td nowrap>The table constraint is initially immediate.</td>
     *      </tr>
     *  </table> <p>
     * </ol>
     *
     * @return Table
     */
    Table TABLE_CONSTRAINTS() {

        Table t = sysTables[TABLE_CONSTRAINTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TABLE_CONSTRAINTS]);

            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "CONSTRAINT_TYPE", CHARACTER_DATA);    // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);         // not null
            addColumn(t, "IS_DEFERRABLE", YES_OR_NO);           // not null
            addColumn(t, "INITIALLY_DEFERRED", YES_OR_NO);      // not null

            // false PK, as CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA,
            // TABLE_CATALOG and/or TABLE_SCHEMA may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TABLE_CONSTRAINTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Intermediate holders
        Iterator     tables;
        Table        table;
        Constraint[] constraints;
        int          constraintCount;
        Constraint   constraint;
        String       cat;
        String       schem;
        Object[]     row;

        // column number mappings
        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int constraint_type    = 3;
        final int table_catalog      = 4;
        final int table_schema       = 5;
        final int table_name         = 6;
        final int is_deferable       = 7;
        final int initially_deferred = 8;

        // initialization
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        table = null;    // else compiler complains

        // do it
        while (tables.hasNext()) {
            table = (Table) tables.next();

            /** @todo - requires table level INSERT or UPDATE or DELETE or REFERENCES (not SELECT) right */
            if (table.isView() || !isAccessibleTable(table)) {
                continue;
            }

            constraints     = table.getConstraints();
            constraintCount = constraints.length;

            for (int i = 0; i < constraintCount; i++) {
                constraint = constraints[i];
                row        = t.getEmptyRowData();

                switch (constraint.getConstraintType()) {

                    case Constraint.CHECK : {
                        row[constraint_type] = "CHECK";

                        break;
                    }
                    case Constraint.UNIQUE : {
                        row[constraint_type] = "UNIQUE";

                        break;
                    }
                    case Constraint.FOREIGN_KEY : {
                        row[constraint_type] = "FOREIGN KEY";
                        table                = constraint.getRef();

                        break;
                    }
                    case Constraint.PRIMARY_KEY : {
                        row[constraint_type] = "PRIMARY KEY";

                        break;
                    }
                    case Constraint.MAIN :
                    default : {
                        continue;
                    }
                }

                cat                     = database.getCatalogName().name;
                schem                   = table.getSchemaName().name;
                row[constraint_catalog] = cat;
                row[constraint_schema]  = schem;
                row[constraint_name]    = constraint.getName().name;
                row[table_catalog]      = cat;
                row[table_schema]       = schem;
                row[table_name]         = table.getName().name;
                row[is_deferable]       = Tokens.T_NO;
                row[initially_deferred] = Tokens.T_NO;

                t.insertSys(store, row);
            }
        }

        return t;
    }

    Table TRANSLATIONS() {

        Table t = sysTables[TRANSLATIONS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRANSLATIONS]);

            addColumn(t, "TRANSLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "SOURCE_CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SOURCE_CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SOURCE_CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "TARGET_CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TARGET_CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TARGET_CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_SOURCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_SOURCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_SOURCE_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRANSLATIONS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        return t;
    }

    Table TRIGGER_COLUMN_USAGE() {

        Table t = sysTables[TRIGGER_COLUMN_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGER_COLUMN_USAGE]);

            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);      // not null
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);     // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGER_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int trigger_catalog = 0;
        final int trigger_schema  = 1;
        final int trigger_name    = 2;
        final int table_catalog   = 3;
        final int table_schema    = 4;
        final int table_name      = 5;
        final int column_name     = 6;
        Iterator  it;
        Object[]  row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);

        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();

            if (!session.getGrantee().isAccessible(trigger)) {
                continue;
            }

            OrderedHashSet set = trigger.getReferences();

            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);

                if (refName.type != SchemaObject.COLUMN) {
                    continue;
                }

                if (!session.getGrantee().isAccessible(refName)) {
                    continue;
                }

                row = t.getEmptyRowData();

                //
                row[trigger_catalog] = database.getCatalogName().name;
                row[trigger_schema]  = trigger.getSchemaName().name;
                row[trigger_name]    = trigger.getName().name;
                row[table_catalog]   = database.getCatalogName().name;
                row[table_schema]    = refName.parent.schema.name;
                row[table_name]      = refName.parent.name;
                row[column_name]     = refName.name;

                try {
                    t.insertSys(store, row);
                } catch (HsqlException e) {}
            }
        }

        // Initialization
        return t;
    }

    Table TRIGGER_ROUTINE_USAGE() {

        Table t = sysTables[TRIGGER_ROUTINE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGER_ROUTINE_USAGE]);

            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);     // not null
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);    // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGER_ROUTINE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int trigger_catalog  = 0;
        final int trigger_schema   = 1;
        final int trigger_name     = 2;
        final int specific_catalog = 3;
        final int specific_schema  = 4;
        final int specific_name    = 5;
        Iterator  it;
        Object[]  row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);

        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();

            if (!session.getGrantee().isAccessible(trigger)) {
                continue;
            }

            OrderedHashSet set = trigger.getReferences();

            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);

                if (refName.type != SchemaObject.FUNCTION
                        && refName.type != SchemaObject.PROCEDURE) {
                    continue;
                }

                if (!session.getGrantee().isAccessible(refName)) {
                    continue;
                }

                row                   = t.getEmptyRowData();
                row[trigger_catalog]  = database.getCatalogName().name;
                row[trigger_schema]   = trigger.getSchemaName().name;
                row[trigger_name]     = trigger.getName().name;
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = refName.schema.name;
                row[specific_name]    = refName.name;

                try {
                    t.insertSys(store, row);
                } catch (HsqlException e) {}
            }
        }

        return t;
    }

    Table TRIGGER_SEQUENCE_USAGE() {

        Table t = sysTables[TRIGGER_SEQUENCE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGER_SEQUENCE_USAGE]);

            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);     // not null
            addColumn(t, "SEQUENCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_NAME", SQL_IDENTIFIER);    // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGER_SEQUENCE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int trigger_catalog  = 0;
        final int trigger_schema   = 1;
        final int trigger_name     = 2;
        final int sequence_catalog = 3;
        final int sequence_schema  = 4;
        final int sequence_name    = 5;
        Iterator  it;
        Object[]  row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);

        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();

            if (!session.getGrantee().isAccessible(trigger)) {
                continue;
            }

            OrderedHashSet set = trigger.getReferences();

            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);

                if (refName.type != SchemaObject.SEQUENCE) {
                    continue;
                }

                if (!session.getGrantee().isAccessible(refName)) {
                    continue;
                }

                row                   = t.getEmptyRowData();
                row[trigger_catalog]  = database.getCatalogName().name;
                row[trigger_schema]   = trigger.getSchemaName().name;
                row[trigger_name]     = trigger.getName().name;
                row[sequence_catalog] = database.getCatalogName().name;
                row[sequence_schema]  = refName.schema.name;
                row[sequence_name]    = refName.name;

                try {
                    t.insertSys(store, row);
                } catch (HsqlException e) {}
            }
        }

        // Initialization
        return t;
    }

    Table TRIGGER_TABLE_USAGE() {

        Table t = sysTables[TRIGGER_TABLE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGER_TABLE_USAGE]);

            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);      // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGER_TABLE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int trigger_catalog = 0;
        final int trigger_schema  = 1;
        final int trigger_name    = 2;
        final int table_catalog   = 3;
        final int table_schema    = 4;
        final int table_name      = 5;
        Iterator  it;
        Object[]  row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);

        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();

            if (!session.getGrantee().isAccessible(trigger)) {
                continue;
            }

            OrderedHashSet set = trigger.getReferences();

            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);

                if (refName.type != SchemaObject.TABLE
                        && refName.type != SchemaObject.VIEW) {
                    continue;
                }

                if (!session.getGrantee().isAccessible(refName)) {
                    continue;
                }

                row                  = t.getEmptyRowData();
                row[trigger_catalog] = database.getCatalogName().name;
                row[trigger_schema]  = trigger.getSchemaName().name;
                row[trigger_name]    = trigger.getName().name;
                row[table_catalog]   = database.getCatalogName().name;
                row[table_schema]    = refName.schema.name;
                row[table_name]      = refName.name;

                try {
                    t.insertSys(store, row);
                } catch (HsqlException e) {}
            }
        }

        // Initialization
        return t;
    }

    Table TRIGGERS() {

        Table t = sysTables[TRIGGERS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGERS]);

            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);
            addColumn(t, "EVENT_MANIPULATION", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_TABLE", SQL_IDENTIFIER);
            addColumn(t, "ACTION_ORDER", CHARACTER_DATA);
            addColumn(t, "ACTION_CONDITION", CHARACTER_DATA);
            addColumn(t, "ACTION_STATEMENT", CHARACTER_DATA);
            addColumn(t, "ACTION_ORIENTATION", CHARACTER_DATA);
            addColumn(t, "ACTION_TIMING", CHARACTER_DATA);
            addColumn(t, "ACTION_REFERENCE_OLD_TABLE", SQL_IDENTIFIER);
            addColumn(t, "ACTION_REFERENCE_NEW_TABLE", SQL_IDENTIFIER);
            addColumn(t, "ACTION_REFERENCE_OLD_ROW", SQL_IDENTIFIER);
            addColumn(t, "ACTION_REFERENCE_NEW_ROW", SQL_IDENTIFIER);
            addColumn(t, "CREATED", TIME_STAMP);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGERS].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int trigger_catalog            = 0;
        final int trigger_schema             = 1;
        final int trigger_name               = 2;
        final int event_manipulation         = 3;
        final int event_object_catalog       = 4;
        final int event_object_schema        = 5;
        final int event_object_table         = 6;
        final int action_order               = 7;
        final int action_condition           = 8;
        final int action_statement           = 9;
        final int action_orientation         = 10;
        final int action_timing              = 11;
        final int action_reference_old_table = 12;
        final int action_reference_new_table = 13;
        final int action_reference_old_row   = 14;
        final int action_reference_new_row   = 15;
        final int created                    = 16;
        Iterator  it;
        Object[]  row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);

        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();

            if (!session.getGrantee().isAccessible(trigger)) {
                continue;
            }

            row                       = t.getEmptyRowData();
            row[trigger_catalog]      = database.getCatalogName().name;
            row[trigger_schema]       = trigger.getSchemaName().name;
            row[trigger_name]         = trigger.getName().name;
            row[event_manipulation]   = trigger.getEventTypeString();
            row[event_object_catalog] = database.getCatalogName().name;
            row[event_object_schema] = trigger.getTable().getSchemaName().name;
            row[event_object_table]   = trigger.getTable().getName().name;
            row[action_order] =
                trigger.getTable().getTriggerIndex(trigger.getName().name);
            row[action_condition]   = trigger.getConditionSQL();
            row[action_statement]   = trigger.getProcedureSQL();
            row[action_orientation] = trigger.getActionOrientationString();
            row[action_timing]      = trigger.getActionTimingString();
            row[action_reference_old_table] =
                trigger.getOldTransitionTableName();
            row[action_reference_new_table] =
                trigger.getNewTransitionTableName();
            row[action_reference_old_row] = trigger.getOldTransitionRowName();
            row[action_reference_new_row] = trigger.getNewTransitionRowName();
            row[created]                  = null;

            t.insertSys(store, row);
        }

        // Initialization
        return t;
    }

    Table TRIGGERED_UPDATE_COLUMNS() {

        Table t = sysTables[TRIGGERED_UPDATE_COLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGERED_UPDATE_COLUMNS]);

            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);            // not null
            addColumn(t, "EVENT_OBJECT_CATALOG", SQL_IDENTIFIER);    // not null
            addColumn(t, "EVENT_OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_TABLE", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_COLUMN", SQL_IDENTIFIER);     // not null

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGERED_UPDATE_COLUMNS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // column number mappings
        final int trigger_catalog      = 0;
        final int trigger_schema       = 1;
        final int trigger_name         = 2;
        final int event_object_catalog = 3;
        final int event_object_schema  = 4;
        final int event_object_table   = 5;
        final int event_object_column  = 6;
        Iterator  it;
        Object[]  row;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);

        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();

            if (!session.getGrantee().isAccessible(trigger)) {
                continue;
            }

            int[] colIndexes = trigger.getUpdateColumnIndexes();

            if (colIndexes == null) {
                continue;
            }

            for (int i = 0; i < colIndexes.length; i++) {
                ColumnSchema column =
                    trigger.getTable().getColumn(colIndexes[i]);

                row                       = t.getEmptyRowData();
                row[trigger_catalog]      = database.getCatalogName().name;
                row[trigger_schema]       = trigger.getSchemaName().name;
                row[trigger_name]         = trigger.getName().name;
                row[event_object_catalog] = database.getCatalogName().name;
                row[event_object_schema] =
                    trigger.getTable().getSchemaName().name;
                row[event_object_table]  = trigger.getTable().getName().name;
                row[event_object_column] = column.getNameString();

                t.insertSys(store, row);
            }
        }

        // Initialization
        return t;
    }

    Table UDT_PRIVILEGES() {

        Table t = sysTables[UDT_PRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[UDT_PRIVILEGES]);

            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[UDT_PRIVILEGES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4
            }, false);

            return t;
        }

        return t;
    }

    /**
     * The USAGE_PRIVILEGES view has one row for each usage privilege
     * descriptor. <p>
     *
     * It effectively contains a representation of the usage privilege
     * descriptors. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_USAGE_PRIVILEGES (
     *      GRANTOR         VARCHAR NOT NULL,
     *      GRANTEE         VARCHAR NOT NULL,
     *      OBJECT_CATALOG  VARCHAR NULL,
     *      OBJECT_SCHEMA   VARCHAR NULL,
     *      OBJECT_NAME     VARCHAR NOT NULL,
     *      OBJECT_TYPE     VARCHAR NOT NULL
     *
     *          CHECK ( OBJECT_TYPE IN (
     *                      'DOMAIN',
     *                      'CHARACTER SET',
     *                      'COLLATION',
     *                      'TRANSLATION',
     *                      'SEQUENCE' ) ),
     *
     *      IS_GRANTABLE    VARCHAR NOT NULL
     *
     *          CHECK ( IS_GRANTABLE IN ( 'YES', 'NO' ) ),
     *
     *      UNIQUE( GRANTOR, GRANTEE, OBJECT_CATALOG,
     *              OBJECT_SCHEMA, OBJECT_NAME, OBJECT_TYPE )
     * )
     * </pre>
     *
     * <b>Description:</b><p>
     *
     * <ol>
     * <li> The value of GRANTOR is the &lt;authorization identifier&gt; of the
     *      user or role who granted usage privileges on the object of the type
     *      identified by OBJECT_TYPE that is identified by OBJECT_CATALOG,
     *      OBJECT_SCHEMA, and OBJECT_NAME, to the user or role identified by the
     *      value of GRANTEE forthe usage privilege being described. <p>
     *
     * <li> The value of GRANTEE is the &lt;authorization identifier&gt; of some
     *      user or role, or PUBLIC to indicate all users, to whom the usage
     *      privilege being described is granted. <p>
     *
     * <li> The values of OBJECT_CATALOG, OBJECT_SCHEMA, and OBJECT_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the object to which the privilege applies. <p>
     *
     * <li> The values of OBJECT_TYPE have the following meanings: <p>
     *
     *      <table border cellpadding="3">
     *          <tr>
     *              <td nowrap>DOMAIN</td>
     *              <td nowrap>The object to which the privilege applies is
     *                         a domain.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>CHARACTER SET</td>
     *              <td nowrap>The object to which the privilege applies is a
     *                         character set.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>COLLATION</td>
     *              <td nowrap>The object to which the privilege applies is a
     *                         collation.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>TRANSLATION</td>
     *              <td nowrap>The object to which the privilege applies is a
     *                         transliteration.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>SEQUENCE</td>
     *              <td nowrap>The object to which the privilege applies is a
     *                         sequence generator.</td>
     *          <tr>
     *      </table> <p>
     *
     * <li> The values of IS_GRANTABLE have the following meanings: <p>
     *
     *      <table border cellpadding="3">
     *          <tr>
     *              <td nowrap>YES</td>
     *              <td nowrap>The privilege being described was granted
     *                         WITH GRANT OPTION and is thus grantable.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>NO</td>
     *              <td nowrap>The privilege being described was not granted
     *                  WITH GRANT OPTION and is thus not grantable.</td>
     *          <tr>
     *      </table> <p>
     * <ol>
     *
     * @return Table
     */
    Table USAGE_PRIVILEGES() {

        Table t = sysTables[USAGE_PRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[USAGE_PRIVILEGES]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);        // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);        // not null
            addColumn(t, "OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "OBJECT_TYPE", CHARACTER_DATA);    // not null
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);        // not null

            // order: COLUMN_NAME, PRIVILEGE
            // for unique: GRANTEE, GRANTOR, TABLE_NAME, TABLE_SCHEM, TABLE_CAT
            // false PK, as TABLE_SCHEM and/or TABLE_CAT may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[USAGE_PRIVILEGES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6, 7
            }, false);

            return t;
        }

        //
        Object[] row;

        //
        final int       grantor        = 0;
        final int       grantee        = 1;
        final int       object_catalog = 2;
        final int       object_schema  = 3;
        final int       object_name    = 4;
        final int       object_type    = 5;
        final int       privilege_type = 6;
        final int       is_grantable   = 7;
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator objects =
            new WrapperIterator(database.schemaManager
                .databaseObjectIterator(SchemaObject.SEQUENCE), database
                .schemaManager.databaseObjectIterator(SchemaObject.COLLATION));

        objects = new WrapperIterator(
            objects,
            database.schemaManager.databaseObjectIterator(
                SchemaObject.CHARSET));
        objects = new WrapperIterator(
            objects,
            database.schemaManager.databaseObjectIterator(
                SchemaObject.DOMAIN));

/*
        objects = new WrapperIterator(
            objects,
            database.schemaManager.databaseObjectIterator(SchemaObject.TYPE));
*/
        OrderedHashSet grantees =
            session.getGrantee().getGranteeAndAllRolesWithPublic();

        while (objects.hasNext()) {
            SchemaObject object = (SchemaObject) objects.next();

            for (int i = 0; i < grantees.size(); i++) {
                Grantee granteeObject = (Grantee) grantees.get(i);
                OrderedHashSet rights =
                    granteeObject.getAllDirectPrivileges(object);
                OrderedHashSet grants =
                    granteeObject.getAllGrantedPrivileges(object);

                if (!grants.isEmpty()) {
                    grants.addAll(rights);

                    rights = grants;
                }

                for (int j = 0; j < rights.size(); j++) {
                    Right right          = (Right) rights.get(j);
                    Right grantableRight = right.getGrantableRights();

                    row                 = t.getEmptyRowData();
                    row[grantor]        = right.getGrantor().getName().name;
                    row[grantee]        = right.getGrantee().getName().name;
                    row[object_catalog] = database.getCatalogName().name;
                    row[object_schema]  = object.getSchemaName().name;
                    row[object_name]    = object.getName().name;
                    row[object_type] =
                        SchemaObjectSet.getName(object.getName().type);
                    row[privilege_type] = Tokens.T_USAGE;
                    row[is_grantable] =
                        right.getGrantee() == object.getOwner()
                        || grantableRight.isFull() ? Tokens.T_YES
                                                   : Tokens.T_NO;;

                    try {
                        t.insertSys(store, row);
                    } catch (HsqlException e) {}
                }
            }
        }

        return t;
    }

    Table USER_DEFINED_TYPES() {

        Table t = sysTables[USER_DEFINED_TYPES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[USER_DEFINED_TYPES]);

            addColumn(t, "USER_DEFINED_TYPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_CATEGORY", SQL_IDENTIFIER);
            addColumn(t, "IS_INSTANTIABLE", YES_OR_NO);
            addColumn(t, "IS_FINAL", YES_OR_NO);
            addColumn(t, "ORDERING_FORM", SQL_IDENTIFIER);
            addColumn(t, "ORDERING_CATEGORY", SQL_IDENTIFIER);
            addColumn(t, "ORDERING_ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ORDERING_ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ORDERING_ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "REFERENCE_TYPE", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "SOURCE_DTD_IDENTIFIER", CHARACTER_DATA);
            addColumn(t, "REF_DTD_IDENTIFIER", CHARACTER_DATA);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "EXTERNAL_NAME", CHARACTER_DATA);
            addColumn(t, "EXTERNAL_LANGUAGE", CHARACTER_DATA);
            addColumn(t, "JAVA_INTERFACE", CHARACTER_DATA);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[USER_DEFINED_TYPES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        final int       user_defined_type_catalog  = 0;
        final int       user_defined_type_schema   = 1;
        final int       user_defined_type_name     = 2;
        final int       user_defined_type_category = 3;
        final int       is_instantiable            = 4;
        final int       is_final                   = 5;
        final int       ordering_form              = 6;
        final int       ordering_category          = 7;
        final int       ordering_routine_catalog   = 8;
        final int       ordering_routine_schema    = 9;
        final int       ordering_routine_name      = 10;
        final int       reference_type             = 11;
        final int       data_type                  = 12;
        final int       character_maximum_length   = 13;
        final int       character_octet_length     = 14;
        final int       character_set_catalog      = 15;
        final int       character_set_schema       = 16;
        final int       character_set_name         = 17;
        final int       collation_catalog          = 18;
        final int       collation_schema           = 19;
        final int       collation_name             = 20;
        final int       numeric_precision          = 21;
        final int       numeric_precision_radix    = 22;
        final int       numeric_scale              = 23;
        final int       datetime_precision         = 24;
        final int       interval_type              = 25;
        final int       interval_precision         = 26;
        final int       source_dtd_identifier      = 27;
        final int       ref_dtd_identifier         = 28;
        final int       declared_data_type         = 29;
        final int       declared_numeric_precision = 30;
        final int       declared_numeric_scale     = 31;
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.DOMAIN);

        while (it.hasNext()) {
            Type distinct = (Type) it.next();

            if (!distinct.isDistinctType()) {
                continue;
            }

            Object[] data = t.getEmptyRowData();

            data[user_defined_type_catalog]  = database.getCatalogName().name;
            data[user_defined_type_schema]   = distinct.getSchemaName().name;
            data[user_defined_type_name]     = distinct.getName().name;
            data[data_type]                  = distinct.getFullNameString();
            data[declared_data_type]         = distinct.getFullNameString();
            data[user_defined_type_category] = "DISTINCT";
            data[is_instantiable]            = "YES";
            data[is_final]                   = "YES";
            data[ordering_form]              = "FULL";
            data[source_dtd_identifier]      = distinct.getFullNameString();

            if (distinct.isCharacterType()) {
                data[character_maximum_length] =
                    ValuePool.getLong(distinct.precision);
                data[character_octet_length] =
                    ValuePool.getLong(distinct.precision * 2);
                data[character_set_catalog] = database.getCatalogName().name;
                data[character_set_schema] =
                    ((CharacterType) distinct).getCharacterSet()
                        .getSchemaName().name;
                data[character_set_name] =
                    ((CharacterType) distinct).getCharacterSet().getName()
                        .name;
                data[collation_catalog] = database.getCatalogName().name;
                data[collation_schema] =
                    ((CharacterType) distinct).getCollation().getSchemaName()
                        .name;
                data[collation_name] =
                    ((CharacterType) distinct).getCollation().getName().name;
            } else if (distinct.isNumberType()) {
                data[numeric_precision] =
                    ValuePool.getLong(((NumberType) distinct).getPrecision());
                data[declared_numeric_precision] =
                    ValuePool.getLong(((NumberType) distinct).getPrecision());

                if (distinct.typeCode != Types.SQL_DOUBLE) {
                    data[numeric_scale] = ValuePool.getLong(distinct.scale);
                    data[declared_numeric_scale] =
                        ValuePool.getLong(distinct.scale);
                }

                data[numeric_precision_radix] = ValuePool.getLong(
                    ((NumberType) distinct).getPrecisionRadix());
            } else if (distinct.isBooleanType()) {}
            else if (distinct.isDateTimeType()) {
                data[datetime_precision] = ValuePool.getLong(distinct.scale);
            } else if (distinct.isIntervalType()) {
                data[interval_precision] =
                    ValuePool.getLong(distinct.precision);
                data[interval_type]      = distinct.getFullNameString();
                data[datetime_precision] = ValuePool.getLong(distinct.scale);
            } else if (distinct.isBinaryType()) {
                data[character_maximum_length] =
                    ValuePool.getLong(distinct.precision);
                data[character_octet_length] =
                    ValuePool.getLong(distinct.precision);
            } else if (distinct.isBitType()) {
                data[character_maximum_length] =
                    ValuePool.getLong(distinct.precision);
                data[character_octet_length] =
                    ValuePool.getLong(distinct.precision);
            }
        }

        return t;
    }

    /**
     * The VIEW_COLUMN_USAGE table has one row for each column of a
     * table that is explicitly or implicitly referenced in the
     * &lt;query expression&gt; of the view being described. <p>
     *
     * <b>Definition:</b> <p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_VIEW_COLUMN_USAGE (
     *      VIEW_CATALOG    VARCHAR NULL,
     *      VIEW_SCHEMA     VARCHAR NULL,
     *      VIEW_NAME       VARCHAR NOT NULL,
     *      TABLE_CATALOG   VARCHAR NULL,
     *      TABLE_SCHEMA    VARCHAR NULL,
     *      TABLE_NAME      VARCHAR NOT NULL,
     *      COLUMN_NAME     VARCHAR NOT NULL,
     *      UNIQUE ( VIEW_CATALOG, VIEW_SCHEMA, VIEW_NAME,
     *               TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,
     *               COLUMN_NAME )
     * )
     * </pre>
     *
     * <b>Description:</b> <p>
     *
     * <ol>
     * <li> The values of VIEW_CATALOG, VIEW_SCHEMA, and VIEW_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the view being described. <p>
     *
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, and
     *      COLUMN_NAME are the catalog name, unqualified schema name,
     *      qualified identifier, and column name, respectively, of a column
     *      of a table that is explicitly or implicitly referenced in the
     *      &lt;query expression&gt; of the view being described.
     * </ol>
     *
     * @return Table
     */
    Table VIEW_COLUMN_USAGE() {

        Table t = sysTables[VIEW_COLUMN_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[VIEW_COLUMN_USAGE]);

            addColumn(t, "VIEW_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "VIEW_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "VIEW_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[VIEW_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Calculated column values
        String viewCatalog;
        String viewSchema;
        String viewName;

        // Intermediate holders
        Iterator tables;
        View     view;
        Table    table;
        Object[] row;
        Iterator iterator;

        // Column number mappings
        final int view_catalog  = 0;
        final int view_schema   = 1;
        final int view_name     = 2;
        final int table_catalog = 3;
        final int table_schema  = 4;
        final int table_name    = 5;
        final int column_name   = 6;

        // Initialization
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView()
                    && session.getGrantee().isFullyAccessibleByRole(table)) {

                // $FALL-THROUGH$
            } else {
                continue;
            }

            viewCatalog = database.getCatalogName().name;
            viewSchema  = table.getSchemaName().name;
            viewName    = table.getName().name;
            view        = (View) table;

            OrderedHashSet references = view.getReferences();

            iterator = references.iterator();

            while (iterator.hasNext()) {
                HsqlName refName = (HsqlName) iterator.next();

                if (refName.type == SchemaObject.COLUMN) {
                    row                = t.getEmptyRowData();
                    row[view_catalog]  = viewCatalog;
                    row[view_schema]   = viewSchema;
                    row[view_name]     = viewName;
                    row[table_catalog] = viewCatalog;
                    row[table_schema]  = refName.parent.schema.name;
                    row[table_name]    = refName.parent.name;
                    row[column_name]   = refName.name;

                    try {
                        t.insertSys(store, row);
                    } catch (HsqlException e) {}
                }
            }
        }

        return t;
    }

    /**
     * The VIEW_ROUTINE_USAGE table has one row for each SQL-invoked
     * routine identified as the subject routine of either a &lt;routine
     * invocation&gt;, a &lt;method reference&gt;, a &lt;method invocation&gt;,
     * or a &lt;static method invocation&gt; contained in a &lt;view
     * definition&gt;. <p>
     *
     * <b>Definition</b><p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE VIEW_ROUTINE_USAGE (
     *      TABLE_CATALOG       VARCHAR NULL,
     *      TABLE_SCHEMA        VARCHAR NULL,
     *      TABLE_NAME          VARCHAR NOT NULL,
     *      SPECIFIC_CATALOG    VARCHAR NULL,
     *      SPECIFIC_SCHEMA     VARCHAR NULL,
     *      SPECIFIC_NAME       VARCHAR NOT NULL,
     *      UNIQUE( TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,
     *              SPECIFIC_CATALOG, SPECIFIC_SCHEMA,
     *              SPECIFIC_NAME )
     * )
     * </pre>
     *
     * <b>Description</b><p>
     *
     * <ol>
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, and TABLE_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the viewed table being described. <p>
     *
     * <li> The values of SPECIFIC_CATALOG, SPECIFIC_SCHEMA, and SPECIFIC_NAME are
     *      the catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the specific name of R. <p>
     * </ol>
     *
     * @return Table
     */
    Table VIEW_ROUTINE_USAGE() {

        Table t = sysTables[VIEW_ROUTINE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[VIEW_ROUTINE_USAGE]);

            addColumn(t, "VIEW_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "VIEW_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "VIEW_NAME", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[VIEW_ROUTINE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Intermediate holders
        Iterator tables;
        Table    table;
        Object[] row;

        // Column number mappings
        final int view_catalog     = 0;
        final int view_schema      = 1;
        final int view_name        = 2;
        final int specific_catalog = 3;
        final int specific_schema  = 4;
        final int specific_name    = 5;

        // Initialization
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView()
                    && session.getGrantee().isFullyAccessibleByRole(table)) {

                // $FALL-THROUGH$
            } else {
                continue;
            }

            OrderedHashSet set = table.getReferences();

            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);

                if (!session.getGrantee().isFullyAccessibleByRole(refName)) {
                    continue;
                }

                row                   = t.getEmptyRowData();
                row[view_catalog]     = database.getCatalogName().name;
                row[view_schema]      = table.getSchemaName().name;
                row[view_name]        = table.getName().name;
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = refName.schema.name;
                row[specific_name]    = refName.name;

                try {
                    t.insertSys(store, row);
                } catch (HsqlException e) {}
            }
        }

        return t;
    }

    /**
     * The VIEW_TABLE_USAGE table has one row for each table identified
     * by a &lt;table name&gt; simply contained in a &lt;table reference&gt;
     * that is contained in the &lt;query expression&gt; of a view. <p>
     *
     * <b>Definition</b><p>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE SYSTEM_VIEW_TABLE_USAGE (
     *      VIEW_CATALOG    VARCHAR NULL,
     *      VIEW_SCHEMA     VARCHAR NULL,
     *      VIEW_NAME       VARCHAR NULL,
     *      TABLE_CATALOG   VARCHAR NULL,
     *      TABLE_SCHEMA    VARCHAR NULL,
     *      TABLE_NAME      VARCHAR NULL,
     *      UNIQUE( VIEW_CATALOG, VIEW_SCHEMA, VIEW_NAME,
     *              TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME )
     * )
     * </pre>
     *
     * <b>Description:</b><p>
     *
     * <ol>
     * <li> The values of VIEW_CATALOG, VIEW_SCHEMA, and VIEW_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of the view being described. <p>
     *
     * <li> The values of TABLE_CATALOG, TABLE_SCHEMA, and TABLE_NAME are the
     *      catalog name, unqualified schema name, and qualified identifier,
     *      respectively, of a table identified by a &lt;table name&gt;
     *      simply contained in a &lt;table reference&gt; that is contained in
     *      the &lt;query expression&gt; of the view being described.
     * </ol>
     *
     * @return Table
     */
    Table VIEW_TABLE_USAGE() {

        Table t = sysTables[VIEW_TABLE_USAGE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[VIEW_TABLE_USAGE]);

            addColumn(t, "VIEW_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "VIEW_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "VIEW_NAME", SQL_IDENTIFIER);     // not null
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);    // not null

            // false PK, as VIEW_CATALOG, VIEW_SCHEMA, TABLE_CATALOG, and/or
            // TABLE_SCHEMA may be NULL
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[VIEW_TABLE_USAGE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);

            return t;
        }

        // Column number mappings
        final int view_catalog  = 0;
        final int view_schema   = 1;
        final int view_name     = 2;
        final int table_catalog = 3;
        final int table_schema  = 4;
        final int table_name    = 5;

        //
        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator        tables;
        Table           table;
        Object[]        row;

        // Initialization
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        // Do it.
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView()
                    && session.getGrantee().isFullyAccessibleByRole(table)) {

                // $FALL-THROUGH$
            } else {
                continue;
            }

            OrderedHashSet references = table.getReferences();

            for (int i = 0; i < references.size(); i++) {
                HsqlName refName = (HsqlName) references.get(i);

                if (!session.getGrantee().isFullyAccessibleByRole(refName)) {
                    continue;
                }

                if (refName.type != SchemaObject.TABLE) {
                    continue;
                }

                row                = t.getEmptyRowData();
                row[view_catalog]  = database.getCatalogName().name;
                row[view_schema]   = table.getSchemaName().name;
                row[view_name]     = table.getName().name;
                row[table_catalog] = database.getCatalogName().name;
                row[table_schema]  = refName.schema.name;
                row[table_name]    = refName.name;

                try {
                    t.insertSys(store, row);
                } catch (HsqlException e) {}
            }
        }

        return t;
    }

    /**
     * The VIEWS view contains one row for each VIEW definition. <p>
     *
     * Each row is a description of the query expression that defines its view,
     * with the following columns:
     *
     * <pre class="SqlCodeExample">
     * TABLE_CATALOG    VARCHAR     name of view's defining catalog.
     * TABLE_SCHEMA     VARCHAR     name of view's defining schema.
     * TABLE_NAME       VARCHAR     the simple name of the view.
     * VIEW_DEFINITION  VARCHAR     the character representation of the
     *                              &lt;query expression&gt; contained in the
     *                              corresponding &lt;view descriptor&gt;.
     * CHECK_OPTION     VARCHAR     {"CASCADED" | "LOCAL" | "NONE"}
     * IS_UPDATABLE     VARCHAR     {"YES" | "NO"}
     * INSERTABLE_INTO VARCHAR      {"YES" | "NO"}
     * IS_TRIGGER_UPDATABLE        VARCHAR  {"YES" | "NO"}
     * IS_TRIGGER_DELETEABLE       VARCHAR  {"YES" | "NO"}
     * IS_TRIGGER_INSERTABLE_INTO  VARCHAR  {"YES" | "NO"}
     * </pre> <p>
     *
     * @return a tabular description of the text source of all
     *        <code>View</code> objects accessible to
     *        the user.
     */
    Table VIEWS() {

        Table t = sysTables[VIEWS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[VIEWS]);

            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);               // not null
            addColumn(t, "VIEW_DEFINITION", CHARACTER_DATA);          // not null
            addColumn(t, "CHECK_OPTION", CHARACTER_DATA);             // not null
            addColumn(t, "IS_UPDATABLE", YES_OR_NO);                  // not null
            addColumn(t, "INSERTABLE_INTO", YES_OR_NO);               // not null
            addColumn(t, "IS_TRIGGER_UPDATABLE", YES_OR_NO);          // not null
            addColumn(t, "IS_TRIGGER_DELETABLE", YES_OR_NO);          // not null
            addColumn(t, "IS_TRIGGER_INSERTABLE_INTO", YES_OR_NO);    // not null

            // order TABLE_NAME
            // added for unique: TABLE_SCHEMA, TABLE_CATALOG
            // false PK, as TABLE_SCHEMA and/or TABLE_CATALOG may be null
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[VIEWS].name, false, SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                1, 2, 0
            }, false);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);
        Iterator        tables;
        Table           table;
        Object[]        row;
        final int       table_catalog              = 0;
        final int       table_schema               = 1;
        final int       table_name                 = 2;
        final int       view_definition            = 3;
        final int       check_option               = 4;
        final int       is_updatable               = 5;
        final int       insertable_into            = 6;
        final int       is_trigger_updatable       = 7;
        final int       is_trigger_deletable       = 8;
        final int       is_trigger_insertable_into = 9;

        tables = allTables();

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if ((table.getSchemaName() != SqlInvariants
                    .INFORMATION_SCHEMA_HSQLNAME && !table
                        .isView()) || !isAccessibleTable(table)) {
                continue;
            }

            row                = t.getEmptyRowData();
            row[table_catalog] = database.getCatalogName().name;
            row[table_schema]  = table.getSchemaName().name;
            row[table_name]    = table.getName().name;

            String check = Tokens.T_NONE;

            if (table instanceof View) {
                if (session.getGrantee().isFullyAccessibleByRole(table)) {
                    row[view_definition] = ((View) table).getStatement();
                }

                switch (((View) table).getCheckOption()) {

                    case SchemaObject.ViewCheckModes.CHECK_NONE :
                        break;

                    case SchemaObject.ViewCheckModes.CHECK_LOCAL :
                        check = Tokens.T_LOCAL;
                        break;

                    case SchemaObject.ViewCheckModes.CHECK_CASCADE :
                        check = Tokens.T_CASCADED;
                        break;
                }
            }

            row[check_option]         = check;
            row[is_updatable]         = table.isUpdatable() ? Tokens.T_YES
                                                            : Tokens.T_NO;
            row[insertable_into]      = table.isInsertable() ? Tokens.T_YES
                                                             : Tokens.T_NO;
            row[is_trigger_updatable] = null;    // only applies to INSTEAD OF triggers
            row[is_trigger_deletable]       = null;
            row[is_trigger_insertable_into] = null;

            t.insertSys(store, row);
        }

        return t;
    }

//------------------------------------------------------------------------------
// SQL SCHEMATA BASE TABLES

    /**
     * ROLE_AUTHORIZATION_DESCRIPTORS<p>
     *
     * <b>Function</b><p>
     *
     * Contains a representation of the role authorization descriptors.<p>
     * <b>Definition</b>
     *
     * <pre class="SqlCodeExample">
     * CREATE TABLE ROLE_AUTHORIZATION_DESCRIPTORS (
     *      ROLE_NAME INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *      GRANTEE INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *      GRANTOR INFORMATION_SCHEMA.SQL_IDENTIFIER,
     *      IS_GRANTABLE INFORMATION_SCHEMA.CHARACTER_DATA
     *          CONSTRAINT ROLE_AUTHORIZATION_DESCRIPTORS_IS_GRANTABLE_CHECK
     *              CHECK ( IS_GRANTABLE IN
     *                  ( 'YES', 'NO' ) ),
     *          CONSTRAINT ROLE_AUTHORIZATION_DESCRIPTORS_PRIMARY_KEY
     *              PRIMARY KEY ( ROLE_NAME, GRANTEE ),
     *          CONSTRAINT ROLE_AUTHORIZATION_DESCRIPTORS_CHECK_ROLE_NAME
     *              CHECK ( ROLE_NAME IN
     *                  ( SELECT AUTHORIZATION_NAME
     *                      FROM AUTHORIZATIONS
     *                     WHERE AUTHORIZATION_TYPE = 'ROLE' ) ),
     *          CONSTRAINT ROLE_AUTHORIZATION_DESCRIPTORS_FOREIGN_KEY_AUTHORIZATIONS_GRANTOR
     *              FOREIGN KEY ( GRANTOR )
     *                  REFERENCES AUTHORIZATIONS,
     *          CONSTRAINT ROLE_AUTHORIZATION_DESCRIPTORS_FOREIGN_KEY_AUTHORIZATIONS_GRANTEE
     *              FOREIGN KEY ( GRANTEE )
     *                  REFERENCES AUTHORIZATIONS
     *      )
     * </pre>
     *
     * <b>Description</b><p>
     *
     * <ol>
     *      <li>The value of ROLE_NAME is the &lt;role name&gt; of some
     *          &lt;role granted&gt; by the &lt;grant role statement&gt; or
     *          the &lt;role name&gt; of a &lt;role definition&gt;. <p>
     *
     *      <li>The value of GRANTEE is an &lt;authorization identifier&gt;,
     *          possibly PUBLIC, or &lt;role name&gt; specified as a
     *          &lt;grantee&gt; contained in a &lt;grant role statement&gt;,
     *          or the &lt;authorization identifier&gt; of the current
     *          SQLsession when the &lt;role definition&gt; is executed. <p>
     *
     *      <li>The value of GRANTOR is the &lt;authorization identifier&gt;
     *          of the user or role who granted the role identified by
     *          ROLE_NAME to the user or role identified by the value of
     *          GRANTEE. <p>
     *
     *      <li>The values of IS_GRANTABLE have the following meanings:<p>
     *
     *      <table border cellpadding="3">
     *          <tr>
     *              <td nowrap>YES</td>
     *              <td nowrap>The described role is grantable.</td>
     *          <tr>
     *          <tr>
     *              <td nowrap>NO</td>
     *              <td nowrap>The described role is not grantable.</td>
     *          <tr>
     *      </table> <p>
     * </ol>
     *
     * @return Table
     */
    Table ROLE_AUTHORIZATION_DESCRIPTORS() {

        Table t = sysTables[ROLE_AUTHORIZATION_DESCRIPTORS];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[ROLE_AUTHORIZATION_DESCRIPTORS]);

            addColumn(t, "ROLE_NAME", SQL_IDENTIFIER);    // not null
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);      // not null
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);      // not null
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);      // not null

            // true PK
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_AUTHORIZATION_DESCRIPTORS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKey(name, new int[] {
                0, 1
            }, true);

            return t;
        }

        PersistentStore store = database.persistentStoreCollection.getStore(t);

        // Intermediate holders
        String   grantorName = SqlInvariants.SYSTEM_AUTHORIZATION_NAME;
        Iterator grantees;
        Grantee  granteeObject;
        String   granteeName;
        Iterator roles;
        String   roleName;
        String   isGrantable;
        Object[] row;

        // Column number mappings
        final int role_name    = 0;
        final int grantee      = 1;
        final int grantor      = 2;
        final int is_grantable = 3;

        // Initialization
        grantees = session.getGrantee().visibleGrantees().iterator();

        //
        while (grantees.hasNext()) {
            granteeObject = (Grantee) grantees.next();
            granteeName   = granteeObject.getNameString();
            roles         = granteeObject.getDirectRoles().iterator();
            isGrantable   = granteeObject.isAdmin() ? Tokens.T_YES
                                                    : Tokens.T_NO;;

            while (roles.hasNext()) {
                Grantee role = (Grantee) roles.next();

                row               = t.getEmptyRowData();
                row[role_name]    = role.getNameString();
                row[grantee]      = granteeName;
                row[grantor]      = grantorName;
                row[is_grantable] = isGrantable;

                t.insertSys(store, row);
            }
        }

        return t;
    }
}
