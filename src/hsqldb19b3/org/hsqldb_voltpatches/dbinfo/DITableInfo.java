/* Copyright (c) 2001-2011, The HSQL Development Group
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

import java.util.Locale;

import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.TableBase;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.resources.ResourceBundleHandler;

/**
 * Provides extended information about HSQLDB tables and their
 * columns/indices. <p>
 *
 * Current version has been reduced in scope.<p>
 *
 * @author boucherb@users
 * @version 2.2.7
 * @since 1.7.2
 */
final class DITableInfo {

    // related to DatabaseMetaData
    int                bestRowTemporary   = 0;
    int                bestRowTransaction = 1;
    int                bestRowSession     = 2;
    int                bestRowUnknown     = 0;
    int                bestRowNotPseudo   = 1;
    static final short tableIndexOther    = 3;

    /** Used in buffer size and character octet length determinations. */
    private static final int HALF_MAX_INT = Integer.MAX_VALUE >>> 1;

    /** BundleHandler id for column remarks resource bundle. */
    private int hnd_column_remarks = -1;

    /** BundleHandler id for table remarks resource bundle. */
    private int hnd_table_remarks = -1;

    /** The Table object upon which this object is reporting. */
    private Table table;

    /**
     * Creates a new DITableInfo object with the default Locale and reporting
     * on no table.  It is absolutely essential the a valid Table object is
     * assigned to this object, using the setTable method, before any Table,
     * Column or Index oriented value retrieval methods are called; this class
     * contains no assertions or exception handling related to a null or
     * invalid table member attribute.
     */
    DITableInfo() {
        setupBundles();
    }

    /**
     * Sets the Locale for table and column remarks. <p>
     */
    void setupBundles() {

        Locale oldLocale;

        synchronized (ResourceBundleHandler.class) {
            oldLocale = ResourceBundleHandler.getLocale();

            ResourceBundleHandler.setLocale(Locale.getDefault());

            hnd_column_remarks =
                ResourceBundleHandler.getBundleHandle("info-column-remarks",
                    null);
            hnd_table_remarks =
                ResourceBundleHandler.getBundleHandle("info-table-remarks",
                    null);

            ResourceBundleHandler.setLocale(oldLocale);
        }
    }

    /**
     * Retrieves whether the best row identifier column is
     * a pseudo column, like an Oracle ROWID. <p>
     *
     * Currently, this always returns an Integer whose value is
     * DatabaseMetaData.bestRowNotPseudo, as HSQLDB does not support
     * pseudo columns such as ROWID. <p>
     *
     * @return whether the best row identifier column is
     * a pseudo column
     */
    Integer getBRIPseudo() {
        return ValuePool.getInt(bestRowNotPseudo);
    }

    /**
     * Retrieves the scope of the best row identifier. <p>
     *
     * This implements the rules described in
     * DatabaseInformationMain.SYSTEM_BESTROWIDENTIFIER. <p>
     *
     * @return the scope of the best row identifier
     */
    Integer getBRIScope() {
        return (table.isWritable()) ? ValuePool.getInt(bestRowTemporary)
                                    : ValuePool.getInt(bestRowSession);
    }

    /**
     * Retrieves the simple name of the specified column. <p>
     *
     * @param i zero-based column index
     * @return the simple name of the specified column.
     */
    String getColName(int i) {
        return table.getColumn(i).getName().name;
    }

    /**
     * Retrieves the remarks, if any, recorded against the specified
     * column. <p>
     *
     * @param i zero-based column index
     * @return the remarks recorded against the specified column.
     */
    String getColRemarks(int i) {

        String key;

        if (table.getTableType() != TableBase.INFO_SCHEMA_TABLE) {
            return table.getColumn(i).getName().comment;
        }

        key = getName() + "_" + getColName(i);

        return ResourceBundleHandler.getString(hnd_column_remarks, key);
    }

    /**
     * Retrieves the HSQLDB-specific type of the table. <p>
     *
     * @return the HSQLDB-specific type of the table
     */
    String getHsqlType() {

        switch (table.getTableType()) {

            case TableBase.MEMORY_TABLE :
            case TableBase.TEMP_TABLE :
            case TableBase.INFO_SCHEMA_TABLE :
                return "MEMORY";

            case TableBase.CACHED_TABLE :
                return "CACHED";

            case TableBase.TEMP_TEXT_TABLE :
            case TableBase.TEXT_TABLE :
                return "TEXT";

            case TableBase.VIEW_TABLE :
            default :
                return null;
        }
    }

    /**
     * Retrieves the simple name of the table. <p>
     *
     * @return the simple name of the table
     */
    String getName() {
        return table.getName().name;
    }

    /**
     * Retrieves the remarks (if any) recorded against the Table. <p>
     *
     * @return the remarks recorded against the Table
     */
    String getRemark() {

        return (table.getTableType() == TableBase.INFO_SCHEMA_TABLE)
               ? ResourceBundleHandler.getString(hnd_table_remarks, getName())
               : table.getName().comment;
    }

    /**
     * Retrieves the standard JDBC type of the table. <p>
     *
     * "TABLE" for user-defined tables, "VIEW" for user-defined views,
     * and so on.
     *
     * @return the standard JDBC type of the table
     */
    String getJDBCStandardType() {

        switch (table.getTableType()) {

            case TableBase.VIEW_TABLE :
                return "VIEW";

            case TableBase.TEMP_TABLE :
            case TableBase.TEMP_TEXT_TABLE :
                return "GLOBAL TEMPORARY";

            case TableBase.INFO_SCHEMA_TABLE :
                return "SYSTEM TABLE";

            default :
                if (table.getOwner().isSystem()) {
                    return "SYSTEM TABLE";
                }

                return "TABLE";
        }
    }

    /**
     * Retrieves the Table object on which this object is currently
     * reporting. <p>
     *
     * @return the Table object on which this object
     *    is currently reporting
     */
    Table getTable() {
        return this.table;
    }

    /**
     * Assigns the Table object on which this object is to report. <p>
     *
     * @param table the Table object on which this object is to report
     */
    void setTable(Table table) {
        this.table = table;
    }
}
