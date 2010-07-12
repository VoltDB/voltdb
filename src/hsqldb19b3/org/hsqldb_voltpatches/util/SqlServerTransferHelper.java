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

import java.sql.Types;

// sqlbob@users 20020325 - patch 1.7.0 - reengineering

/**
 * Conversions from SQLServer7 databases
 *
 * @version 1.7.0
 */
class SqlServerTransferHelper extends TransferHelper {

    private boolean firstTinyintRow;
    private boolean firstSmallintRow;

    SqlServerTransferHelper() {
        super();
    }

    SqlServerTransferHelper(TransferDb database, Traceable t, String q) {
        super(database, t, q);
    }

    String formatTableName(String t) {

        if (t == null) {
            return t;
        }

        if (t.equals("")) {
            return t;
        }

        if (t.indexOf(' ') != -1) {
            return ("[" + t + "]");
        } else {
            return (formatIdentifier(t));
        }
    }

    int convertFromType(int type) {

        // MS SQL 7 specific problems (Northwind database)
        if (type == 11) {
            tracer.trace("Converted DATETIME (type 11) to TIMESTAMP");

            type = Types.TIMESTAMP;
        } else if (type == -9) {
            tracer.trace("Converted NVARCHAR (type -9) to VARCHAR");

            type = Types.VARCHAR;
        } else if (type == -8) {
            tracer.trace("Converted NCHAR (type -8) to VARCHAR");

            type = Types.VARCHAR;
        } else if (type == -10) {
            tracer.trace("Converted NTEXT (type -10) to VARCHAR");

            type = Types.VARCHAR;
        } else if (type == -1) {
            tracer.trace("Converted LONGTEXT (type -1) to LONGVARCHAR");

            type = Types.LONGVARCHAR;
        }

        return (type);
    }

    void beginTransfer() {
        firstSmallintRow = true;
        firstTinyintRow  = true;
    }

    Object convertColumnValue(Object value, int column, int type) {

        // solves a problem for MS SQL 7
        if ((type == Types.SMALLINT) && (value instanceof Integer)) {
            if (firstSmallintRow) {
                firstSmallintRow = false;

                tracer.trace("SMALLINT: Converted column " + column
                             + " Integer to Short");
            }

            value = new Short((short) ((Integer) value).intValue());
        } else if ((type == Types.TINYINT) && (value instanceof Integer)) {
            if (firstTinyintRow) {
                firstTinyintRow = false;

                tracer.trace("TINYINT: Converted column " + column
                             + " Integer to Byte");
            }

            value = new Byte((byte) ((Integer) value).intValue());
        }

        return (value);
    }
}
