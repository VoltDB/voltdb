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


package org.hsqldb_voltpatches.scriptio;

import java.io.IOException;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.rowio.RowOutputBinary;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;
import org.hsqldb_voltpatches.lib.DataOutputStream;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @since 1.8.0
 * @version 1.7.2
 */
class ScriptWriterBinary extends ScriptWriterBase {

    RowOutputInterface rowOut;

    ScriptWriterBinary(Database db, String file, boolean includeCached,
                       boolean newFile) {
        super(db, file, includeCached, newFile, false);
    }

    protected void initBuffers() {
        rowOut = new RowOutputBinary();
    }

    protected void writeSingleColumnResult(Result r) throws IOException {

        DataOutputStream dataOutput = new DataOutputStream(fileStreamOut);

        rowOut.reset();
        r.write(dataOutput, rowOut);
        dataOutput.flush();
    }

    // int : row size (0 if no more rows) ,
    // RowInput/OutputBinary : row (column values)
    protected void writeRow(Session session, Table t,
                            Object[] data) throws IOException {

        rowOut.reset();
        rowOut.writeRow(data, t.getColumnTypes());
        fileStreamOut.write(rowOut.getOutputStream().getBuffer(), 0,
                            rowOut.size());

        tableRowCount++;
    }

    // int : headersize (0 if no more tables), String : tablename, int : operation,
    protected void writeTableInit(Table t) throws IOException {

        tableRowCount = 0;

        rowOut.reset();
        rowOut.writeSize(0);
        rowOut.writeString(t.getName().name);
        rowOut.writeInt(INSERT_WITH_SCHEMA);
        rowOut.writeString(t.getSchemaName().name);
        rowOut.writeIntData(rowOut.size(), 0);
        fileStreamOut.write(rowOut.getOutputStream().getBuffer(), 0,
                            rowOut.size());
    }

    protected void writeTableTerm(Table t) throws IOException {

        rowOut.reset();
        rowOut.writeSize(0);
        rowOut.writeInt(this.tableRowCount);
        fileStreamOut.write(rowOut.getOutputStream().getBuffer(), 0,
                            rowOut.size());
    }

    protected void writeDataTerm() throws IOException {

        rowOut.reset();
        rowOut.writeSize(0);
        fileStreamOut.write(rowOut.getOutputStream().getBuffer(), 0,
                            rowOut.size());
    }

    public void writeLogStatement(Session session,
                                  String s) throws IOException {}

    protected void addSessionId(Session session) throws IOException {}

    public void writeDeleteStatement(Session session, Table table,
                                     Object[] ddata) throws IOException {}

    public void writeSequenceStatement(Session session,
                                       NumberSequence seq)
                                       throws IOException {}

    public void writeInsertStatement(Session session, Table table,
                                     Object[] data) throws IOException {}

    public void writeCommitStatement(Session session) throws IOException {}
}
