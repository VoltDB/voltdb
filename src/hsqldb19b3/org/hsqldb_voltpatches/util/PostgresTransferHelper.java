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
import java.sql.Types;

// fredt@users 20020215 - patch 516309 by Nicolas Bazin - transfer PostgresSQL
// sqlbob@users 20020325 - patch 1.7.0 - reengineering

/**
 * Conversions from PostgresSQL databases
 *
 * @author Nichola Bazin
 * @version 1.7.0
 */
class PostgresTransferHelper extends TransferHelper {

    private final int PostgreSQL = 0;
    private final int HSQLDB     = 1;
    String[][]        Funcs      = {
        {
            "now()", "\'now\'"
        }
    };

    PostgresTransferHelper() {
        super();
    }

    PostgresTransferHelper(TransferDb database, Traceable t, String q) {
        super(database, t, q);
    }

    int convertToType(int type) {

        if (type == Types.DECIMAL) {
            type = Types.NUMERIC;

            tracer.trace("Converted DECIMAL to NUMERIC");
        }

        return (type);
    }

    String fixupColumnDefRead(TransferTable t, ResultSetMetaData meta,
                              String columnType, ResultSet columnDesc,
                              int columnIndex) throws SQLException {

        String SeqName   = new String("_" + columnDesc.getString(4) + "_seq");
        int    spaceleft = 31 - SeqName.length();

        if (t.Stmts.sDestTable.length() > spaceleft) {
            SeqName = t.Stmts.sDestTable.substring(0, spaceleft) + SeqName;
        } else {
            SeqName = t.Stmts.sDestTable + SeqName;
        }

        String CompareString = "nextval(\'\"" + SeqName + "\"\'";

        if (columnType.indexOf(CompareString) >= 0) {

            // We just found a increment
            columnType = "SERIAL";
        }

        for (int Idx = 0; Idx < Funcs.length; Idx++) {
            String PostgreSQL_func = Funcs[Idx][PostgreSQL];
            int    iStartPos       = columnType.indexOf(PostgreSQL_func);

            if (iStartPos >= 0) {
                String NewColumnType = columnType.substring(0, iStartPos);

                NewColumnType += Funcs[Idx][HSQLDB];
                NewColumnType +=
                    columnType.substring(iStartPos
                                         + PostgreSQL_func.length());
                columnType = NewColumnType;
            }
        }

        return (columnType);
    }

    String fixupColumnDefWrite(TransferTable t, ResultSetMetaData meta,
                               String columnType, ResultSet columnDesc,
                               int columnIndex) throws SQLException {

        if (columnType.equals("SERIAL")) {
            String SeqName = new String("_" + columnDesc.getString(4)
                                        + "_seq");
            int spaceleft = 31 - SeqName.length();

            if (t.Stmts.sDestTable.length() > spaceleft) {
                SeqName = t.Stmts.sDestTable.substring(0, spaceleft)
                          + SeqName;
            } else {
                SeqName = t.Stmts.sDestTable + SeqName;
            }

            String DropSequence = "DROP SEQUENCE " + SeqName + ";";

            t.Stmts.sDestDrop += DropSequence;
        }

        for (int Idx = 0; Idx < Funcs.length; Idx++) {
            String HSQLDB_func = Funcs[Idx][HSQLDB];
            int    iStartPos   = columnType.indexOf(HSQLDB_func);

            if (iStartPos >= 0) {
                String NewColumnType = columnType.substring(0, iStartPos);

                NewColumnType += Funcs[Idx][PostgreSQL];
                NewColumnType += columnType.substring(iStartPos
                                                      + HSQLDB_func.length());
                columnType = NewColumnType;
            }
        }

        return (columnType);
    }

    void beginDataTransfer() {

        try {
            db.setAutoCommit(false);
        } catch (Exception e) {}
    }

    void endDataTransfer() {

        try {
            db.commit();
            db.execute("VACUUM ANALYZE");
        } catch (Exception e) {}
    }
}
