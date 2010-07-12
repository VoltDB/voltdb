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

import java.io.Serializable;
import java.util.Vector;

/**
 * Database Transfer Tool
 * @author Nicolas BAZIN, INGENICO
 * @version 1.7.0
 */
class DataAccessPoint implements Serializable {

    Traceable      tracer;
    TransferHelper helper;
    String         databaseToConvert;

    public DataAccessPoint() {

        tracer            = null;
        helper            = HelperFactory.getHelper("");
        databaseToConvert = "";
    }

    public DataAccessPoint(Traceable t) {

        tracer = t;
        helper = HelperFactory.getHelper("");

        helper.set(null, t, "\'");

        databaseToConvert = "";
    }

    boolean isConnected() {
        return false;
    }

    boolean getAutoCommit() throws DataAccessPointException {
        return false;
    }

    void commit() throws DataAccessPointException {}

    void rollback() throws DataAccessPointException {}

    void setAutoCommit(boolean flag) throws DataAccessPointException {}

    boolean execute(String statement) throws DataAccessPointException {
        return false;
    }

    TransferResultSet getData(String statement)
    throws DataAccessPointException {
        return null;
    }

    void putData(String statement, TransferResultSet r,
                 int iMaxRows) throws DataAccessPointException {}

    Vector getSchemas() throws DataAccessPointException {
        return new Vector();
    }

    Vector getCatalog() throws DataAccessPointException {
        return new Vector();
    }

    void setCatalog(String sCatalog) throws DataAccessPointException {}

    Vector getTables(String sCatalog,
                     String[] sSchemas) throws DataAccessPointException {
        return new Vector();
    }

    void getTableStructure(TransferTable SQLCommands,
                           DataAccessPoint Dest)
                           throws DataAccessPointException {
        throw new DataAccessPointException("Nothing to Parse");
    }

    void close() throws DataAccessPointException {}

    void beginDataTransfer() throws DataAccessPointException {

        try {
            helper.beginDataTransfer();
        } catch (Exception e) {
            throw new DataAccessPointException(e.getMessage());
        }
    }

    void endDataTransfer() throws DataAccessPointException {

        try {
            helper.endDataTransfer();
        } catch (Exception e) {
            throw new DataAccessPointException(e.getMessage());
        }
    }

    /**
     * @return Returns the helper.
     */
    public TransferHelper getHelper() {
        return helper;
    }
}
