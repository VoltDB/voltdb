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


package org.hsqldb_voltpatches.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Hashtable;

/**
 * Base class for conversion from a different databases
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @version 1.7.0
 */
class TransferHelper {

    protected TransferDb db;
    protected Traceable  tracer;
    protected String     sSchema;
    protected JDBCTypes  JDBCT;
    private String       quote;

    TransferHelper() {

        db     = null;
        tracer = null;
        quote  = "\'";
        JDBCT  = new JDBCTypes();
    }

    TransferHelper(TransferDb database, Traceable t, String q) {

        db     = database;
        tracer = t;
        quote  = q;
        JDBCT  = new JDBCTypes();
    }

    void set(TransferDb database, Traceable t, String q) {

        db     = database;
        tracer = t;
        quote  = q;
    }

    String formatIdentifier(String id) {

        if (id == null) {
            return id;
        }

        if (id.equals("")) {
            return id;
        }

        if (!Character.isLetter(id.charAt(0)) || (id.indexOf(' ') != -1)) {
            return (quote + id + quote);
        }

        return id;
    }

    void setSchema(String _Schema) {
        sSchema = _Schema;
    }

    String formatName(String t) {

        String Name = "";

        if ((sSchema != null) && (sSchema.length() > 0)) {
            Name = sSchema + ".";
        }

        Name += formatIdentifier(t);

        return Name;
    }

    int convertFromType(int type) {
        return (type);
    }

    int convertToType(int type) {
        return (type);
    }

    Hashtable getSupportedTypes() {

        Hashtable hTypes = new Hashtable();

        if (db != null) {
            try {
                ResultSet result = db.meta.getTypeInfo();

                while (result.next()) {
                    Integer intobj = new Integer(result.getShort(2));

                    if (hTypes.get(intobj) == null) {
                        try {
                            hTypes.put(intobj,
                                       JDBCT.toString(result.getShort(2)));
                        } catch (Exception e) {}
                    }
                }

                result.close();
            } catch (SQLException e) {}
        }

        if (hTypes.isEmpty()) {
            hTypes = JDBCT.getHashtable();
        }

        return hTypes;
    }

    String fixupColumnDefRead(TransferTable t, ResultSetMetaData meta,
                              String columnType, ResultSet columnDesc,
                              int columnIndex) throws SQLException {
        return (columnType);
    }

    String fixupColumnDefWrite(TransferTable t, ResultSetMetaData meta,
                               String columnType, ResultSet columnDesc,
                               int columnIndex) throws SQLException {
        return (columnType);
    }

    boolean needTransferTransaction() {
        return (false);
    }

    Object convertColumnValue(Object value, int column, int type) {
        return (value);
    }

    void beginDataTransfer() {}

    void endDataTransfer() {}

    String fixupColumnDefRead(String aTableName, ResultSetMetaData meta,
                              String columnType, ResultSet columnDesc,
                              int columnIndex) throws SQLException {
        return columnType;
    }

    String fixupColumnDefWrite(String aTableName, ResultSetMetaData meta,
                               String columnType, ResultSet columnDesc,
                               int columnIndex) throws SQLException {
        return columnType;
    }
}
