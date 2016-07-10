/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import java.lang.reflect.Constructor;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.dbinfo.DatabaseInformation;
import org.hsqldb_voltpatches.lib.FileAccess;
import org.hsqldb_voltpatches.lib.FileUtil;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;
import org.hsqldb_voltpatches.persist.HsqlProperties;
import org.hsqldb_voltpatches.persist.LobManager;
import org.hsqldb_voltpatches.persist.Logger;
import org.hsqldb_voltpatches.persist.PersistentStoreCollectionDatabase;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rights.GranteeManager;
import org.hsqldb_voltpatches.rights.User;
import org.hsqldb_voltpatches.rights.UserManager;
import org.hsqldb_voltpatches.types.Type;

// fredt@users 20020130 - patch 476694 by velichko - transaction savepoints
// additions to different parts to support savepoint transactions
// fredt@users 20020215 - patch 1.7.0 - new HsqlProperties class
// support use of properties from database.properties file
// fredt@users 20020218 - patch 1.7.0 - DEFAULT keyword
// support for default values for table columns
// fredt@users 20020305 - patch 1.7.0 - restructuring
// some methods move to Table.java, some removed
// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP) - restructuring
// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP) - error trapping
// boucherb@users 20020130 - patch 1.7.0 - use lookup for speed
// idents listed in alpha-order for easy check of stats...
// fredt@users 20020420 - patch523880 by leptipre@users - VIEW support
// boucherb@users - doc 1.7.0 - added javadoc comments
// tony_lai@users 20020820 - patch 595073 - duplicated exception msg
// tony_lai@users 20020820 - changes to shutdown compact to save memory
// boucherb@users 20020828 - allow reconnect to local db that has shutdown
// fredt@users 20020912 - patch 1.7.1 by fredt - drop duplicate name triggers
// fredt@users 20021112 - patch 1.7.2 by Nitin Chauhan - use of switch
// rewrite of the majority of multiple if(){}else if(){} chains with switch()
// boucherb@users 20020310 - class loader update for JDK 1.1 compliance
// fredt@users 20030401 - patch 1.7.2 by akede@users - data files readonly
// fredt@users 20030401 - patch 1.7.2 by Brendan Ryan - data files in Jar
// boucherb@users 20030405 - removed 1.7.2 lint - updated JavaDocs
// boucherb@users 20030425 - DDL methods are moved to DatabaseCommandInterpreter.java
// boucherb@users - fredt@users 200305..200307 - patch 1.7.2 - DatabaseManager upgrade
// loosecannon1@users - patch 1.7.2 - properties on the JDBC URL
// oj@openoffice.org - changed to file access api

/**
 *  Database is the root class for HSQL Database Engine database. <p>
 *
 *  It holds the data structures that form an HSQLDB database instance.
 *
 * Modified significantly from the Hypersonic original in successive
 * HSQLDB versions.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public class Database {

    int    databaseID;
    String databaseType;
    String sName;

// loosecannon1@users 1.7.2 patch properties on the JDBC URL
    private HsqlProperties urlProperties;
    private String         sPath;
    DatabaseInformation    dbInfo;

    /** indicates the state of the database */
    private int   dbState;
    public Logger logger;

    /** true means that all tables are readonly. */
    boolean databaseReadOnly;

    /**
     * true means that all CACHED and TEXT tables are readonly.
     *  MEMORY tables are updatable but updates are not persisted.
     */
    private boolean filesReadOnly;

    /** true means filesReadOnly but CACHED and TEXT tables are disallowed */
    private boolean                filesInJar;
    public boolean                 sqlEnforceStrictSize;
    // BEGIN Cherry-picked code change from hsqldb-2.2.8
    public boolean                sqlConvertTruncate     = true;
    // END Cherry-picked code change from hsqldb-2.2.8
    private boolean                bIgnoreCase;
    private boolean                bReferentialIntegrity;
    private HsqlDatabaseProperties databaseProperties;
    private boolean                shutdownOnNoConnection;

    // schema invarient objects
    public UserManager     userManager;
    public GranteeManager  granteeManager;
    public HsqlNameManager nameManager;

    // session related objects
    public SessionManager     sessionManager;
    public TransactionManager txManager;
    public StatementManager   compiledStatementManager;

    // schema objects
    public SchemaManager schemaManager;

    //
    public PersistentStoreCollectionDatabase persistentStoreCollection;

    //
    // LobManager initialization is expensive,
    // annoying to avoid when setting breakpoints in the DDL parser,
    // and completely irrelevent to VoltDB.
    // So, it has been purged from this module but the
    // lobManager attribute has been left (null) for now
    // because it is still referenced in (dead) code in other
    // modules.
    // TODO: purge the LobManager class, this lobManager attribute,
    // and the remaining references to lobManager in other modules
    // and/or purge the (dead) code paths where they are found.
    public LobManager lobManager;
    public Collation  collation;

    //
    public static final int DATABASE_ONLINE       = 1;
    public static final int DATABASE_OPENING      = 4;
    public static final int DATABASE_CLOSING      = 8;
    public static final int DATABASE_SHUTDOWN     = 16;
    public static final int CLOSEMODE_IMMEDIATELY = -1;
    public static final int CLOSEMODE_NORMAL      = 0;
    public static final int CLOSEMODE_COMPACT     = 1;
    public static final int CLOSEMODE_SCRIPT      = 2;

    /**
     *  Constructs a new Database object.
     *
     * @param type is the type of the database: "mem:", "file:", "res:"
     * @param path is the given path to the database files
     * @param name is the combination of type and canonical path
     * @param props property overrides placed on the connect URL
     * @exception  HsqlException if the specified name and path
     *      combination is illegal or unavailable, or the database files the
     *      name and path resolves to are in use by another process
     */
    Database(String type, String path, String name, HsqlProperties props) {

        urlProperties = props;

        setState(Database.DATABASE_SHUTDOWN);

        sName        = name;
        databaseType = type;
        sPath        = path;

        if (databaseType == DatabaseURL.S_RES) {
            filesInJar    = true;
            filesReadOnly = true;
        }

// oj@openoffice.org - changed to file access api
        String fileaccess_class_name =
            urlProperties.getProperty("fileaccess_class_name");

        if (fileaccess_class_name != null) {
            String storagekey = urlProperties.getProperty("storage_key");

            try {
                Class zclass = Class.forName(fileaccess_class_name);
                Constructor constructor = zclass.getConstructor(new Class[]{
                    Object.class });

                fileaccess =
                    (FileAccess) constructor.newInstance(new Object[]{
                        storagekey });
                isStoredFileAccess = true;
            } catch (java.lang.ClassNotFoundException e) {
                System.out.println("ClassNotFoundException");
            } catch (java.lang.InstantiationException e) {
                System.out.println("InstantiationException");
            } catch (java.lang.IllegalAccessException e) {
                System.out.println("IllegalAccessException");
            } catch (Exception e) {
                System.out.println("Exception");
            }
        } else {
            fileaccess = FileUtil.getDefaultInstance();
        }

        shutdownOnNoConnection = urlProperties.getProperty("shutdown",
                "false").equals("true");
        logger                   = new Logger();
        compiledStatementManager = new StatementManager(this);
    }

    /**
     * Opens this database.  The database should be opened after construction.
     */
    synchronized void open() {

        if (!isShutdown()) {
            return;
        }

        reopen();
    }

    /**
     * Opens this database.  The database should be opened after construction.
     * or reopened by the close(int closemode) method during a
     * "shutdown compact". Closes the log if there is an error.
     */
    void reopen() {

        boolean isNew;

        setState(DATABASE_OPENING);

        try {
            databaseProperties = new HsqlDatabaseProperties(this);
            isNew = !DatabaseURL.isFileBasedDatabaseType(databaseType)
                    || !databaseProperties.checkFileExists();

            if (isNew && urlProperties.isPropertyTrue(
                    HsqlDatabaseProperties.url_ifexists)) {
                throw Error.error(ErrorCode.DATABASE_NOT_EXISTS, sName);
            }

            databaseProperties.load();
            databaseProperties.setURLProperties(urlProperties);
            compiledStatementManager.reset();

            nameManager    = new HsqlNameManager(this);
            granteeManager = new GranteeManager(this);
            userManager    = new UserManager(this);
            schemaManager  = new SchemaManager(this);
            persistentStoreCollection =
                new PersistentStoreCollectionDatabase();
            bReferentialIntegrity = true;
            sessionManager        = new SessionManager(this);
            txManager             = new TransactionManager(this);
            collation             = collation.getDefaultInstance();
            dbInfo = DatabaseInformation.newDatabaseInformation(this);

            databaseProperties.setDatabaseVariables();

            String version = databaseProperties.getProperty(
                HsqlDatabaseProperties.db_version);

            if (version.substring(0, 3).equals("1.7")) {
                schemaManager.createPublicSchema();
            }

            if (DatabaseURL.isFileBasedDatabaseType(databaseType)) {
                logger.openLog(this);
            }

            if (version.substring(0, 3).equals("1.7")
                    || version.substring(0, 5).equals("1.8.0")) {
                HsqlName name = schemaManager.findSchemaHsqlName(
                    SqlInvariants.PUBLIC_SCHEMA);

                if (name != null) {
                    schemaManager.setDefaultSchemaHsqlName(name);
                }
            }

            if (isNew) {
                String tableType = urlProperties.getProperty(
                    HsqlDatabaseProperties.hsqldb_default_table_type,
                    "MEMORY");

                if ("CACHED".equalsIgnoreCase(tableType)) {
                    schemaManager.setDefaultTableType(TableBase.CACHED_TABLE);
                }

                HsqlName name = nameManager.newHsqlName("SA", false,
                    SchemaObject.GRANTEE);

                userManager.createUser(name, "");

                Session session = sessionManager.getSysSession();

                granteeManager.grant(name.name,
                                     SqlInvariants.DBA_ADMIN_ROLE_NAME,
                                     granteeManager.getDBARole());
                logger.writeToLog(session,
                                  "CREATE USER SA PASSWORD \'\' ADMIN");
                schemaManager.createPublicSchema();
                logger.writeToLog(session,
                                  "CREATE SCHEMA PUBLIC AUTHORIZATION DBA");
                logger.writeToLog(
                    session, "SET DATABASE DEFAULT INITIAL SCHEMA PUBLIC");

                if (schemaManager.getDefaultTableType()
                        == Table.CACHED_TABLE) {
                    logger.writeToLog(
                        session, "SET DATABASE DEFAULT TABLE TYPE CACHED");
                }

                logger.synchLogForce();
            }

            dbInfo.setWithContent(true);
        } catch (Throwable e) {
            logger.closeLog(Database.CLOSEMODE_IMMEDIATELY);
            logger.releaseLock();
            setState(DATABASE_SHUTDOWN);
            clearStructures();
            DatabaseManager.removeDatabase(this);

            if (!(e instanceof HsqlException)) {
                e = Error.error(ErrorCode.GENERAL_ERROR, e.toString());
            }

            throw (HsqlException) e;
        }

        setState(DATABASE_ONLINE);
    }

    /**
     * Clears the data structuress, making them elligible for garbage collection.
     */
    void clearStructures() {

        if (schemaManager != null) {
            schemaManager.clearStructures();
        }

        granteeManager = null;
        userManager    = null;
        nameManager    = null;
        schemaManager  = null;
        sessionManager = null;
        dbInfo         = null;
    }

    /**
     *  Returns the database ID.
     */
    public int getDatabaseID() {
        return this.databaseID;
    }

    /**
     *  Returns the type of the database: "mem", "file", "res"
     */
    public String getType() {
        return databaseType;
    }

    /**
     *  Returns the path of the database
     */
    public String getPath() {
        return sPath;
    }

    public HsqlName getCatalogName() {
        return nameManager.getCatalogName();
    }

    /**
     *  Returns the database properties.
     */
    public HsqlDatabaseProperties getProperties() {
        return databaseProperties;
    }

    /**
     * Returns the SessionManager for the database.
     */
    public SessionManager getSessionManager() {
        return sessionManager;
    }

    public boolean isReadOnly() {
        return databaseReadOnly;
    }

    /**
     *  Returns true if database has been shut down, false otherwise
     */
    synchronized boolean isShutdown() {
        return dbState == DATABASE_SHUTDOWN;
    }

    /**
     *  Constructs a new Session that operates within (is connected to) the
     *  context of this Database object. <p>
     *
     *  If successful, the new Session object initially operates on behalf of
     *  the user specified by the supplied user name.
     *
     * Throws if username or password is invalid.
     */
    synchronized Session connect(String username, String password,
                                 int timeZoneSeconds) {

        if (username.equalsIgnoreCase("SA")) {
            username = "SA";
        }

        User user = userManager.getUser(username, password);
        Session session = sessionManager.newSession(this, user,
            databaseReadOnly, false, timeZoneSeconds);

        return session;
    }

    /**
     *  Puts this Database object in global read-only mode. After
     *  this call, all existing and future sessions are limited to read-only
     *  transactions. Any following attempts to update the state of the
     *  database will result in throwing an HsqlException.
     */
    public void setReadOnly() {
        databaseReadOnly = true;
        filesReadOnly    = true;
    }

    /**
     * After this call all CACHED and TEXT tables will be set to read-only
     * mode. Changes to MEMORY tables will NOT
     * be stored or updated in the script file. This mode is intended for
     * use with read-only media where data should not be persisted.
     */
    public void setFilesReadOnly() {
        filesReadOnly = true;
    }

    /**
     * Is this in filesReadOnly mode?
     */
    public boolean isFilesReadOnly() {
        return filesReadOnly;
    }

    /**
     * Is this in filesInJar mode?
     */
    public boolean isFilesInJar() {
        return filesInJar;
    }

    /**
     *  Returns the UserManager for this Database.
     */
    public UserManager getUserManager() {
        return userManager;
    }

    /**
     *  Returns the GranteeManager for this Database.
     */
    public GranteeManager getGranteeManager() {
        return granteeManager;
    }

    /**
     *  Sets the isReferentialIntegrity attribute.
     */
    public void setReferentialIntegrity(boolean ref) {
        bReferentialIntegrity = ref;
    }

    /**
     *  Is referential integrity currently enforced?
     */
    public boolean isReferentialIntegrity() {
        return bReferentialIntegrity;
    }

    /**
     * Sets the database to treat any new VARCHAR column declarations as
     * VARCHAR_IGNORECASE.
     */
    void setIgnoreCase(boolean b) {
        bIgnoreCase = b;
    }

    /**
     *  Does the database treat any new VARCHAR column declarations as
     * VARCHAR_IGNORECASE.
     */
    public boolean isIgnoreCase() {
        return bIgnoreCase;
    }

    /**
     *  Called by the garbage collector on this Databases object when garbage
     *  collection determines that there are no more references to it.
     */
    @Override
    protected void finalize() {

        if (getState() != DATABASE_ONLINE) {
            return;
        }

        try {
            close(CLOSEMODE_IMMEDIATELY);
        } catch (HsqlException e) {    // it's too late now
        }
    }

    void closeIfLast() {

        if (shutdownOnNoConnection && sessionManager.isEmpty()
                && dbState == this.DATABASE_ONLINE) {
            try {
                close(CLOSEMODE_NORMAL);
            } catch (HsqlException e) {}
        }
    }

    /**
     *  Closes this Database using the specified mode. <p>
     *
     * <ol>
     *  <LI> closemode -1 performs SHUTDOWN IMMEDIATELY, equivalent
     *       to a poweroff or crash.
     *
     *  <LI> closemode 0 performs a normal SHUTDOWN that
     *      checkpoints the database normally.
     *
     *  <LI> closemode 1 performs a shutdown compact that scripts
     *       out the contents of any CACHED tables to the log then
     *       deletes the existing *.data file that contains the data
     *       for all CACHED table before the normal checkpoint process
     *       which in turn creates a new, compact *.data file.
     * </ol>
     */
    public void close(int closemode) {

        HsqlException he = null;

        setState(DATABASE_CLOSING);
        sessionManager.closeAllSessions();
        sessionManager.clearAll();

        if (filesReadOnly) {
            closemode = CLOSEMODE_IMMEDIATELY;
        }

        /**
         * @todo  fredt - impact of possible error conditions in closing the log
         * should be investigated for the CLOSEMODE_COMPACT mode
         */
        logger.closeLog(closemode);

        try {
            if (closemode == CLOSEMODE_COMPACT) {
                clearStructures();
                reopen();
                setState(DATABASE_CLOSING);
                logger.closeLog(CLOSEMODE_NORMAL);
            }
        } catch (Throwable t) {
            if (t instanceof HsqlException) {
                he = (HsqlException) t;
            } else {
                he = Error.error(ErrorCode.GENERAL_ERROR, t.toString());
            }
        }

        logger.releaseLock();
        setState(DATABASE_SHUTDOWN);
        clearStructures();

        // fredt - this could change to avoid removing a db from the
        // DatabaseManager repository if there are pending getDatabase()
        // calls
        DatabaseManager.removeDatabase(this);

        if (he != null) {
            throw he;
        }
    }

    /**
     * Ensures system table producer's table cache, if it exists, is set dirty.
     * After this call up-to-date versions are generated in response to
     * system table requests. <p>
     *
     * Also resets all prepared statements if a change to database structure
     * can possibly affect any existing prepared statement's validity.<p>
     *
     * The argument is false if the change to the database structure does not
     * affect the prepared statement, such as when a new table is added.<p>
     *
     * The argument is typically true when a database object is dropped,
     * altered or a permission was revoked.
     *
     * @param  resetPrepared If true, reset all prepared statements.
     */
    public void setMetaDirty(boolean resetPrepared) {

        if (dbInfo != null) {
            dbInfo.setDirty();
        }

        if (resetPrepared) {
            compiledStatementManager.resetStatements();
        }
    }

    private synchronized void setState(int state) {
        dbState = state;
    }

    synchronized int getState() {
        return dbState;
    }

    String getStateString() {

        int state = getState();

        switch (state) {

            case DATABASE_CLOSING :
                return "DATABASE_CLOSING";

            case DATABASE_ONLINE :
                return "DATABASE_ONLINE";

            case DATABASE_OPENING :
                return "DATABASE_OPENING";

            case DATABASE_SHUTDOWN :
                return "DATABASE_SHUTDOWN";

            default :
                return "UNKNOWN";
        }
    }

    public String[] getSettingsSQL() {

        HsqlArrayList list = new HsqlArrayList();

        if (!getCatalogName().name.equals(
                HsqlNameManager.DEFAULT_CATALOG_NAME)) {
            String name = getCatalogName().statementName;

            list.add("ALTER CATALOG PUBLIC RENAME TO " + name);
        }

        if (collation.name != null) {
            String name = collation.getName().statementName;

            list.add("SET DATABASE COLLATION " + name);
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public String[] getPropertiesSQL() {

        HsqlArrayList list = new HsqlArrayList();

        if (schemaManager.getDefaultTableType() == TableBase.CACHED_TABLE) {
            list.add("SET DATABASE DEFAULT TABLE TYPE CACHED");
        }

        if (logger.hasLog()) {
            int     delay  = logger.getWriteDelay();
            boolean millis = delay < 1000;

            if (millis) {
                if (delay != 0 && delay < 20) {
                    delay = 20;
                }
            } else {
                delay /= 1000;
            }

            list.add("SET WRITE_DELAY " + delay + (millis ? " MILLIS"
                                                          : ""));
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    /**
     * Returns the schema and authorisation statements for the database.
     */
    public Result getScript(boolean indexRoots) {

        Result   r = Result.newSingleColumnResult("COMMAND", Type.SQL_VARCHAR);
        String[] list = getSettingsSQL();

        addRows(r, list);

        list = getGranteeManager().getSQL();

        addRows(r, list);

        // schemas and schema objects such as tables, sequences, etc.
        list = schemaManager.getSQLArray();

        addRows(r, list);

        // index roots
        if (indexRoots) {
            list = schemaManager.getIndexRootsSQL();

            addRows(r, list);
        }

        // user session start schema names
        list = getUserManager().getInitialSchemaSQL();

        addRows(r, list);

        // grantee rights
        list = getGranteeManager().getRightstSQL();

        addRows(r, list);

        list = getPropertiesSQL();

        addRows(r, list);

        return r;
    }

    private static void addRows(Result r, String[] sql) {

        if (sql == null) {
            return;
        }

        for (int i = 0; i < sql.length; i++) {
            String[] s = new String[1];

            s[0] = sql[i];

            r.initialiseNavigator().add(s);
        }
    }

// boucherb@users - 200403?? - patch 1.7.2 - metadata
//------------------------------------------------------------------------------

    /**
     * Retrieves the uri portion of this object's in-process JDBC url.
     *
     * @return the uri portion of this object's in-process JDBC url
     */
    public String getURI() {
        return sName;
    }

// oj@openoffice.org - changed to file access api
    public HsqlProperties getURLProperties() {
        return urlProperties;
    }

    private FileAccess fileaccess;
    private boolean    isStoredFileAccess;

    public FileAccess getFileAccess() {
        return fileaccess;
    }

    public boolean isStoredFileAccess() {
        return isStoredFileAccess;
    }

    String tempDirectoryPath;

    public String getTempDirectoryPath() {

        if (tempDirectoryPath == null) {
            if (databaseType == DatabaseURL.S_FILE) {
                String path = sPath + ".tmp";

                tempDirectoryPath = FileUtil.makeDirectories(path);
            } else {
                tempDirectoryPath = databaseProperties.getProperty(
                    HsqlDatabaseProperties.hsqldb_temp_directory);
            }
        }

        return tempDirectoryPath;
    }

    public int getResultMaxMemoryRows() {

        if (getTempDirectoryPath() == null) {
            return 0;
        }

        return databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_result_max_memory_rows, 0);
    }
}
