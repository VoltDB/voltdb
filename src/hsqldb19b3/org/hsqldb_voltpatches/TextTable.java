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

import org.hsqldb_voltpatches.lib.FileUtil;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.persist.TextCache;
import org.hsqldb_voltpatches.persist.DataFileCache;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.persist.PersistentStore;

// tony_lai@users 20020820 - patch 595099 - user define PK name

/**
 * Subclass of Table to handle TEXT data source. <p>
 *
 * Extends Table to provide the notion of an SQL base table object whose
 * data is read from and written to a text format data file.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @version 1.8.0
 */
public class TextTable extends org.hsqldb_voltpatches.Table {

    private String  dataSource  = "";
    private boolean isReversed  = false;
    private boolean isConnected = false;

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

        DataFileCache cache = null;

        try {
            cache = (TextCache) database.logger.openTextCache(this,
                    dataSource, withReadOnlyData, isReversed);

            store.setCache(cache);

            // read and insert all the rows from the source file
            Row row     = null;
            int nextpos = 0;

            if (((TextCache) cache).ignoreFirst) {
                nextpos += ((TextCache) cache).readHeaderLine();
            }

            while (true) {
                row = (Row) store.get(nextpos, false);

                if (row == null) {
                    break;
                }

                Object[] data = row.getData();

                nextpos = row.getPos() + row.getStorageSize();

                ((RowAVLDiskData) row).setNewNodes();
                systemUpdateIdentityValue(data);
                enforceRowConstraints(session, data);

                for (int i = 0; i < indexList.length; i++) {
                    indexList[i].insert(null, store, row);
                }
            }
        } catch (Exception e) {
            int linenumber = cache == null ? 0
                                           : ((TextCache) cache)
                                               .getLineNumber();

            clearAllData(session);

            if (cache != null) {
                database.logger.closeTextCache(this);
                store.release();
            }

            // everything is in order here.
            // At this point table should either have a valid (old) data
            // source and cache or have an empty source and null cache.
            throw Error.error(ErrorCode.TEXT_FILE, 0, new Object[] {
                new Integer(linenumber), e.getMessage()
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
    protected void setDataSource(Session session, String dataSourceNew,
                                 boolean isReversedNew, boolean createFile) {

        if (getTableType() == Table.TEMP_TEXT_TABLE) {
            ;
        } else {
            session.getGrantee().checkSchemaUpdateOrGrantRights(
                getSchemaName().name);
        }

        dataSourceNew = dataSourceNew.trim();

        if (createFile
                && FileUtil.getDefaultInstance().exists(dataSourceNew)) {
            throw Error.error(ErrorCode.TEXT_SOURCE_EXISTS, dataSourceNew);
        }

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

        if (cache != null && cache.ignoreFirst) {
            cache.setHeader(header);

            return;
        }

        throw Error.error(ErrorCode.TEXT_TABLE_HEADER);
    }

    public String getHeader() {

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);
        TextCache cache  = (TextCache) store.getCache();
        String    header = cache == null ? null
                                         : cache.getHeader();

        return header == null ? null
                              : StringConverter.toQuotedString(header, '\"',
                              true);
    }

    /**
     * Used by INSERT, DELETE, UPDATE operations. This class will return
     * a more appropriate message when there is no data source.
     */
    void checkDataReadOnly() {

        if (dataSource.length() == 0) {
            throw Error.error(ErrorCode.TEXT_TABLE_UNKNOWN_DATA_SOURCE);
        }

        if (isReadOnly) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }
    }

    public boolean isDataReadOnly() {
        return !isConnected() || super.isDataReadOnly();
    }

    public void setDataReadOnly(boolean value) {

        if (!value) {
            if (isReversed) {
                throw Error.error(ErrorCode.DATA_IS_READONLY);
            }

            if (database.isFilesReadOnly()) {
                throw Error.error(ErrorCode.DATABASE_IS_READONLY);
            }
        }

        openCache(null, dataSource, isReversed, value);

        isReadOnly = value;
    }

    boolean isIndexCached() {
        return false;
    }

    void setIndexRoots(String s) {

        // do nothing
    }

    String getDataSourceDDL() {

        String dataSource = getDataSource();

        if (dataSource == null) {
            return null;
        }

        boolean      isDesc = isDescDataSource();
        StringBuffer sb     = new StringBuffer(128);

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_SOURCE).append(' ').append('"');
        sb.append(dataSource);
        sb.append('"');

        if (isDesc) {
            sb.append(' ').append(Tokens.T_DESC);
        }

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

    /**
     * Adds commitPersistence() call
     */
    public void insertData(PersistentStore store, Object[] data) {

        Row row = (Row) store.getNewCachedObject(null, data);

        store.indexRow(null, row);
        store.commitPersistence(row);
    }
}
