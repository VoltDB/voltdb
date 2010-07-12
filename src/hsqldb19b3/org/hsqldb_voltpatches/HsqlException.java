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

import org.hsqldb_voltpatches.result.Result;

/**
 * Class encapsulating all exceptions that can be thrown within the engine.
 * Instances are used to create instances of java.sql.SQLException and returned
 * to JDBC callers.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class HsqlException extends RuntimeException {

    private String message;
    private String state;
    private int    code;
    private int    level;
    private int    statementGroup;
    private int    statementCode;

    //
    public final static HsqlException noDataCondition =
        Error.error(ErrorCode.N_02000);

    /**
     * @param message String
     * @param state XOPEN / SQL code for exception
     * @param code number code in HSQLDB
     */
    public HsqlException(String message, String state, int code) {

        this.message = message;
        this.state   = state;
        this.code    = code;
    }

    /**
     * @param r containing the members
     */
    public HsqlException(Result r) {

        this.message = r.getMainString();
        this.state   = r.getSubString();
        this.code    = r.getErrorCode();
    }

    public HsqlException(Throwable t, String errorState, int errorCode) {

        super(t);

        this.message = t.toString();
        this.state   = errorState;
        this.code    = errorCode;
    }

    /**
     * @return message
     */
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @return SQL State
     */
    public String getSQLState() {
        return state;
    }

    /**
     * @return vendor specific error code
     */
    public int getErrorCode() {
        return code;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    public int getStatementCode() {
        return statementCode;
    }

    public void setStatementType(int group, int code) {
        statementGroup = group;
        statementCode  = code;
    }

    public static class HsqlRuntimeMemoryError extends OutOfMemoryError {
        HsqlRuntimeMemoryError() {}
    }
}
