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

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.persist.TextCache;
import org.hsqldb_voltpatches.persist.TextFileReader;
import org.hsqldb_voltpatches.rowio.RowInputInterface;

/**
 * Subclass of Table to handle TEXT data source. <p>
 *
 * Extends Table to provide the notion of an SQL base table object whose
 * data is read from and written to a text format data file.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @version 2.3.0
 */
public class TextTable extends Table {

    String  dataSource  = "";
    boolean isReversed  = false;
    boolean isConnected = false;

//    TextCache cache;

    /**
     * Constructs a new TextTable from the given arguments.
     *
     * @param db the owning database
     * @param name the table's HsqlName
     * @param type code (normal or temp text table)
     */
    TextTable(Database db, HsqlNameManager.HsqlName name, int type) {

        super(db, name, type);

        isWithDataSource = true;
    }

    public boolean isConnected() {
        return isConnected;
    }

    /**
     * connects to the data source
     */
    public void connect(Session session) {
        connect(session, isReadOnly);
    }

    /**
     * connects to the data source
     */
    private void connect(Session session, boolean withReadOnlyData) {

        // Open new cache:
        if ((dataSource.length() == 0) || isConnected) {

            // nothing to do
            return;
        }

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);

        this.store = store;

        TextCache      cache  = null;
        TextFileReader reader = null;

        try {
            cache = (TextCache) database.logger.openTextFilePersistence(this,
                    dataSource, withReadOnlyData, isReversed);

            store.setCache(cache);

            reader = cache.getTextFileReader();

            // read and insert all the rows from the source file
            Row row     = null;
            long nextpos = 0;

            if (cache.isIgnoreFirstLine()) {
                nextpos += reader.readHeaderLine();
                cache.setHeaderInitialise(reader.getHeaderLine());
            }

            while (true) {
                RowInputInterface rowIn = reader.readObject(nextpos);

                if (rowIn == null) {
                    break;
                }

                row = (Row) store.get(rowIn);

                if (row == null) {
                    break;
                }

                Object[] data = row.getData();

                nextpos = (int) row.getPos() + row.getStorageSize();

                systemUpdateIdentityValue(data);
                enforceRowConstraints(session, data);
                store.indexRow(session, row);
            }
        } catch (Throwable t) {
            int linenumber = reader == null ? 0
                                            : reader.getLineNumber();

            clearAllData(session);

            if (cache != null) {
                database.logger.closeTextCache(this);
                store.release();
            }

            // everything is in order here.
            // At this point table should either have a valid (old) data
            // source and cache or have an empty source and null cache.
            throw Error.error(t, ErrorCode.TEXT_FILE, 0, new Object[] {
                new Integer(linenumber), t.toString()
            });
        }

        isConnected = true;
        isReadOnly  = withReadOnlyData;
    }

    /**
     * disconnects from the data source
     */
    public void disconnect() {

        this.store = null;

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);

        store.release();

        isConnected = false;
    }

    /**
     * This method does some of the work involved with managing the creation
     * and openning of the cache, the rest is done in Log.java and
     * TextCache.java.
     *
     * Better clarification of the role of the methods is needed.
     */
    private void openCache(Session session, String dataSourceNew,
                           boolean isReversedNew, boolean isReadOnlyNew) {

        String  dataSourceOld = dataSource;
        boolean isReversedOld = isReversed;
        boolean isReadOnlyOld = isReadOnly;

        if (dataSourceNew == null) {
            dataSourceNew = "";
        }

        disconnect();

        dataSource = dataSourceNew;
        isReversed = (isReversedNew && dataSource.length() > 0);

        try {
            connect(session, isReadOnlyNew);
        } catch (HsqlException e) {
            dataSource = dataSourceOld;
            isReversed = isReversedOld;

            connect(session, isReadOnlyOld);

            throw e;
        }
    }

    /**
     * High level command to assign a data source to the table definition.
     * Reassigns only if the data source or direction has changed.
     */
    void setDataSource(Session session, String dataSourceNew,
                                 boolean isReversedNew, boolean createFile) {

        if (getTableType() == Table.TEMP_TEXT_TABLE) {
            //
        } else {
            session.getGrantee().checkSchemaUpdateOrGrantRights(
                getSchemaName().name);
        }

        dataSourceNew = dataSourceNew.trim();

        //-- Open if descending, direction changed, file changed, or not connected currently
        if (isReversedNew || (isReversedNew != isReversed)
                || !dataSource.equals(dataSourceNew) || !isConnected) {
            openCache(session, dataSourceNew, isReversedNew, isReadOnly);
        }

        if (isReversed) {
            isReadOnly = true;
        }
    }

    public String getDataSource() {
        return dataSource;
    }

    public boolean isDescDataSource() {
        return isReversed;
    }

    public void setHeader(String header) {

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);
        TextCache cache = (TextCache) store.getCache();

        if (cache != null && cache.isIgnoreFirstLine()) {
            cache.setHeader(header);

            return;
        }

        throw Error.error(ErrorCode.TEXT_TABLE_HEADER);
    }

    private String getHeader() {

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);
        TextCache cache  = (TextCache) store.getCache();
        String    header = cache == null ? null
                                         : cache.getHeader();

        return header == null ? null
                              : StringConverter.toQuotedString(header, '\'',
                              true);
    }

    /**
     * Used by INSERT, DELETE, UPDATE operations. This class will return
     * a more appropriate message when there is no data source.
     */
    public void checkDataReadOnly() {

        if (dataSource.length() == 0) {
            throw Error.error(ErrorCode.TEXT_TABLE_UNKNOWN_DATA_SOURCE);
        }

        if (isDataReadOnly()) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }
    }

    public boolean isDataReadOnly() {
        return !isConnected() || super.isDataReadOnly()
               || store.getCache().isDataReadOnly();
    }

    public void setDataReadOnly(boolean value) {

        if (!value) {
            if (isReversed) {
                throw Error.error(ErrorCode.DATA_IS_READONLY);
            }

            if (database.isFilesReadOnly()) {
                throw Error.error(ErrorCode.DATABASE_IS_READONLY);
            }

            if (isConnected()) {
                store.getCache().close();
                store.getCache().open(value);
            }
        }

        isReadOnly = value;
    }

    /**
     * Adds commitPersistence() call
     */
    public void insertData(Session session, PersistentStore store,
                           Object[] data) {

        Row row = (Row) store.getNewCachedObject(session, data, false);

        store.indexRow(session, row);
        store.commitPersistence(row);
    }

    String getDataSourceDDL() {

        String dataSource = getDataSource();

        if (dataSource == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_SOURCE).append(' ').append('\'');
        sb.append(dataSource);
        sb.append('\'');

        return sb.toString();
    }

    /**
     * Generates the SET TABLE <tablename> SOURCE HEADER <string> statement for a
     * text table;
     */
    String getDataSourceHeader() {

        String header = getHeader();

        if (header == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_SOURCE).append(' ');
        sb.append(Tokens.T_HEADER).append(' ');
        sb.append(header);

        return sb.toString();
    }
}
