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


package org.hsqldb_voltpatches.result;

import org.hsqldb_voltpatches.StatementTypes;

/**
 * An enumeration of the request and response mode values used to communicate
 * between the client and the engine when sending Result objects back
 * and forth.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @since 1.9.0
 * @version 1.7.2
 */

// fredt@users - the constants from the SQL standards are used freely where a
// similar function is performed. The Result objects do not necessarily contain
// the same information as stated in SQL standard for CLI.
public interface ResultConstants {

    /** The offset at which HSQLDB API Result mode values start. */
    int HSQL_API_BASE = 0;

    /**
     * Not a result
     */
    int NONE = HSQL_API_BASE + 0;

    /**
     * Indicates that the Result object encapsulates an update
     * count response.
     */
    int UPDATECOUNT = HSQL_API_BASE + 1;

    /**
     * Indicates that the Result object encapsualtes an
     * error response.
     */
    int ERROR = HSQL_API_BASE + 2;

    /**
     * Indicates that the Result object encapsulates a result
     * set response containing data.
     */
    int DATA = HSQL_API_BASE + 3;

    /**
     * Indicates that the Result object encapsulates a response
     * that communicates the acknowlegement of newly allocated
     * Statement object in the form of its statementID
     * and metadata
     */
    int PREPARE_ACK = HSQL_API_BASE + 4;

    /**
     * Indicates that the Result object encapsulates a result
     * set for setting session attributes.
     */
    int SETSESSIONATTR = HSQL_API_BASE + 6;

    /**
     * Indicates that the Result object encapsulates a request
     * to get session attributes.
     */
    int GETSESSIONATTR = HSQL_API_BASE + 7;

    /**
     * Indicates that the Result object encapsulates a batch of statements
     */
    int BATCHEXECDIRECT = HSQL_API_BASE + 8;

    /**
     * Indicates that the Result object encapsulates a batch of prepared
     * statement parameter values
     */
    int BATCHEXECUTE = HSQL_API_BASE + 9;

    /**
     * Indicates that the Result object encapsulates a request to start a new
     * internal session for the connection
     */
    int RESETSESSION = HSQL_API_BASE + 10;

    /**
     * Indicates that the Result object encapsulates a response to a connection
     * attempt that was successful
     */
    int CONNECTACKNOWLEDGE = HSQL_API_BASE + 11;

    /**
     * Indicates that the Result object encapsulates a request to prepare
     * to commit as the first phase of a two-phase commit
     */
    int PREPARECOMMIT = HSQL_API_BASE + 12;

    /**
     * Indicates that the Result object encapsulates a request to return
     * some rows of data
     */
    int REQUESTDATA = HSQL_API_BASE + 13;

    /**
     * Indicates that the Result object encapsulates a set of data rows
     * without metadata
     */
    int DATAROWS = HSQL_API_BASE + 14;

    /**
     * Indicates that the Result object encapsulates a set of data rows
     * with metadata
     */
    int DATAHEAD = HSQL_API_BASE + 15;

    /**
     * Indicates that the Result object encapsulates a set of update counts
     * for a batch execution
     */
    int BATCHEXECRESPONSE = HSQL_API_BASE + 16;

    /**
     * Only for metadata, indicates that the metadata is for the parameters.
     */
    int PARAM_METADATA = HSQL_API_BASE + 17;

    /**
     * Common result type for all large object operations
     */
    int LARGE_OBJECT_OP = HSQL_API_BASE + 18;

    /**
     * Warning
     */
    int WARNING = HSQL_API_BASE + 19;

    /**
     * Indicates that Result encapsulates a request to establish a connection.
     */
    int CONNECT = HSQL_API_BASE + 31;

    /**
     * Indicates that Result encapsulates a request to terminate an
     * established connection.
     */
    int DISCONNECT = HSQL_API_BASE + 32;

    /**
     * Indicates that Result encapsulates a request to terminate an
     * SQL-transaction.
     */
    int ENDTRAN = HSQL_API_BASE + 33;

    /**
     * Indicates that Result encapsulates a request to execute a statement
     * directly.
     */
    int EXECDIRECT = HSQL_API_BASE + 34;

    /**
     * Indicates that Result encapsulates a request to execute a prepared
     * statement.
     */
    int EXECUTE = HSQL_API_BASE + 35;

    /**
     * Indicates that Result encapsulates a request to deallocate an
     * SQL-statement.
     */
    int FREESTMT = HSQL_API_BASE + 36;

    /**
     * Indicates that Result encapsulates a request to prepare a statement.
     */
    int PREPARE = HSQL_API_BASE + 37;

    /**
     * Indicates that Result encapsulates a request to set the value of an
     * SQL-connection attribute.
     */
    int SETCONNECTATTR = HSQL_API_BASE + 38;

    /**
     * Indicates that Result encapsulates a request to explicitly start an
     * SQL-transaction and set its characteristics.
     */
    int STARTTRAN = HSQL_API_BASE + 39;

    /**
     * Indicates that the Result encapsulates a request to close a result set
     */
    int CLOSE_RESULT = HSQL_API_BASE + 40;

    /**
     * Indicates that the Result encapsulates a request to update or insert into a result set
     */
    int UPDATE_RESULT = HSQL_API_BASE + 41;

    /**
     * Indicates that the Result encapsulates a simple value for internal use
     */
    int VALUE = HSQL_API_BASE + 42;


    /**
     * Indicates that the Result encapsulates a response to a procedure call via CallableStatement
     */
    int CALL_RESPONSE = HSQL_API_BASE + 43;

    int MODE_UPPER_LIMIT =  HSQL_API_BASE + 44;

//    /** The offset at which the standard SQL API Result mode values start. */
//    int SQL_API_BASE = 0x00010000;
//
//    /**
//     * Indicates that Result encapsulates a request to allocate an
//     * SQL-connection and assign a handle to it.
//     */
//    int SQLALLOCCONNECT     = SQL_API_BASE + 1;
//    /**
//     * Indicates that Result encapsulates a request to allocate an
//     * SQL-environment and assign a handle to it.
//     */
//    int SQLALLOCENV         = SQL_API_BASE + 2;
//
//    /**
//     * Indicates that Result encapsulates a request to allocate a resource
//     * and assign a handle to it.
//     */
//    int SQLALLOCHANDLE      = SQL_API_BASE + 1001;
//
//    /**
//     * Indicates that Result encapsulates a request to allocate an
//     * SQL-statement and assign a handle to it.
//     */
//    int SQLALLOCSTMT        = SQL_API_BASE + 3;
//
//    /**
//     * Indicates that Result encapsulates a request to describe a target
//     * specification or array of target specifications.
//     */
//    int SQLBINDCOL          = SQL_API_BASE + 4;
//
//    /**
//     * Indicates that Result encapsulates a request to describe a
//     * dynamic parameter specification and its value.
//     */
//    int SQLBINDPARAMETER    = SQL_API_BASE + 72;
//
//    /**
//     * Indicates that Result encapsulates a request to cancel execution of
//     * a CLI routine.
//     */
//    int SQLCANCEL           = SQL_API_BASE + 5;
//
//    /** Indicates that Result encapsulates a request to close a cursor. */
//    int SQLCLOSECURSOR      = SQL_API_BASE + 1003;
//
//    /**
//     * Indicates that Result encapsulates a request to get a
//     * column attribute.
//     */
//    int SQLCOLATTRIBUTE     = SQL_API_BASE + 6;
//
//    /**
//     * Indicates that Result encapsulates a request to return a result set that
//     * contains a list of the privileges held on the columns whose names adhere
//     * to the requested pattern or patterns within a single specified table
//     * stored in the Information Schema of the connected data source.
//     */
//    int SQLCOLUMNPRIVILEGES = SQL_API_BASE + 56;
//
//    /**
//     * Indicates that Result encapsulates a request to, based on the specified
//     * selection criteria, return a result set that contains information about
//     * columns of tables stored in the information schemas of the connected
//     * data source.
//     */
//    int SQLCOLUMNS          = SQL_API_BASE + 40;
//
//
//    /**
//     * Indicates that Result encapsulates a request to establish a connection.
//     */
//    int SQLCONNECT = SQL_API_BASE + 7;
//
//    /**
//     * Indicates that Result encapsulates a request to copy a CLI descriptor.
//     */
//    int SQLCOPYDESC         = SQL_API_BASE + 1004;
//
//    /**
//     * Indicates that Result encapsulates a request to get server name(s) that
//     * the application can connect to, along with description information,
//     * if available.
//     */
//    int SQLDATASOURCES      = SQL_API_BASE + 57;
//
//    /**
//     * Indicates that Result encapsulates a request to get column attributes.
//     */
//    int SQLDESCRIBECOL      = SQL_API_BASE + 8;
//
//
//    /**
//     * Indicates that Result encapsulates a request to terminate an
//     * established connection.
//     */
//    int SQLDISCONNECT = SQL_API_BASE + 9;
//
//    /**
//     * Indicates that Result encapsulates a request to terminate an
//     * SQL-transaction.
//     */
//    int SQLENDTRAN = SQL_API_BASE + 1005;
//
//    /**
//     * Indicates that Result encapsulates a request to return diagnostic
//     * information.
//     */
//    int SQLERROR            = SQL_API_BASE + 10;
//
//    /**
//     * Indicates that Result encapsulates a request to execute a statement
//     * directly.
//     */
//    int SQLEXECDIRECT = SQL_API_BASE + 11;
//
//    /**
//     * Indicates that Result encapsulates a request to execute a prepared
//     * statement.
//     */
//    int SQLEXECUTE = SQL_API_BASE + 12;
//
//    /**
//     * Indicates that Result encapsulates a request to fetch the next row of
//     * a cursor.
//     */
//
//  int SQLFETCH = SQL_API_BASE + 13;
//
//    /**
//     * Indicates that Result encapsulates a request to position a cursor on
//     * the specified row and retrieve values from that row.
//     */
//  int SQLFETCHSCROLL = SQL_API_BASE + 1021;
//    /**
//     * Indicates that Result encapsulates a request to return a result set
//     * that contains information about foreign keys either in or referencing
//     * a single specified table stored in the Information Schema of the
//     * connected data source.
//     */
//    int SQLFOREIGNKEYS      = SQL_API_BASE + 60;
//
//    /**
//     * Indicates that Result encapsulates a request to deallocate an
//     * SQL-connection.
//     */
//    int SQLFREECONNECT      = SQL_API_BASE + 14;
//
//    /**
//     * Indicates that Result encapsulates a request to deallocate an
//     * SQL-environment.
//     */
//    int SQLFREEENV          = SQL_API_BASE + 15;
//
//    /**
//     * Indicates that Result encapsulates a request to free a resource.
//     */
//    int SQLFREEHANDLE       = SQL_API_BASE + 1006;
//
//    /**
//     * Indicates that Result encapsulates a request to deallocate an
//     * SQL-statement.
//     */
//    int SQLFREESTMT = SQL_API_BASE + 16;
//
//    /**
//     * Indicates that Result encapsulates a request to get the value of an
//     * SQL-connection attribute.
//     */
//    int SQLGETCONNECTATTR   = SQL_API_BASE + 1007;
//
//    /**
//     * Indicates that Result encapsulates a request to get a cursor name.
//     */
//    int SQLGETCURSORNAME    = SQL_API_BASE + 17;
//
//    /**
//     * Indicates that Result encapsulates a request to retrieve a column value.
//     */
//    int SQLGETDATA          = SQL_API_BASE + 43;
//
//    /**
//     * Indicates that Result encapsulates a request to get a field from a CLI
//     * descriptor area.
//     */
//    int SQLGETDESCFIELD     = SQL_API_BASE + 1008;
//
//    /**
//     * Indicates that Result encapsulates a request to get commonly-used
//     * fields from a CLI descriptor area.
//     */
//    int SQLGETDESCREC       = SQL_API_BASE + 1009;
//
//    /**
//     * Indicates that Result encapsulates a request to get information from a
//     * CLI diagnostics area.
//     */
//    int SQLGETDIAGFIELD     = SQL_API_BASE + 1010;
//
//    /** Indicates that Result encapsulates a request to get commonly-used
//     * information from a CLI diagnostics area.
//     */
//    int SQLGETDIAGREC       = SQL_API_BASE + 1011;
//
//    /**
//     * Indicates that Result encapsulates a request to get the value of an
//     * SQL-environment attribute.
//     */
//    int SQLGETENVATTR       = SQL_API_BASE + 1012;
//
//    /**
//     * Indicates that Result encapsulates a request to get information
//     * about features supported by the CLI implementation.
//     */
//    int SQLGETFEATUREINFO   = SQL_API_BASE + 1027;
//
//    /**
//     * Indicates that Result encapsulates a request to determine whether a CLI
//     * routine is supported.
//     */
//    int SQLGETFUNCTIONS     = SQL_API_BASE + 44;
//
//    /**
//     * Indicates that Result encapsulates a request to get information about
//     * the implementation.
//     */
//    int SQLGETINFO          = SQL_API_BASE + 45;
//
//    /**
//     * Indicates that Result encapsulates a request to retrieve the length of
//     * the character or octet string value represented by a Large Object
//     * locator.
//     */
//    int SQLGETLENGTH        = SQL_API_BASE + 1022;
//
//    /**
//     * Indicates that Result encapsulates a request to retrieve the value of a
//     * dynamic output parameter.
//     */
//    int SQLGETPARAMDATA     = SQL_API_BASE + 1025;
//
//    /**
//     * Indicates that Result encapsulates a request to retrieve the starting
//     * position of a string value within another string value, where the second
//     * string value is represented by a Large Object locator.
//     */
//    int SQLGETPOSITION      = SQL_API_BASE + 1023;
//
//
//    /**
//     * Indicates that Result encapsulates a request to get information about
//     * general value specifications supported by the implementation.
//     */
//    int SQLGETSESSIONINFO = SQL_API_BASE + 1028;
//    /**
//     * Indicates that Result encapsulates a request to get the value of an
//     * SQL-statement attribute.
//     */
//    int SQLGETSTMTATTR      = SQL_API_BASE + 1014;
//
//    /**
//     * Indicates that Result encapsulates a request to either retrieve a
//     * portion of a character or octet string value that is represented by
//     * a Large Object locator or create a Large Object value at the server
//     * and retrieve a Large Object locator for that value.
//     */
//    int SQLGETSUBSTRING     = SQL_API_BASE + 1024;
//
//    /**
//     * Indicates that Result encapsulates a request to get information about
//     * one or all of the predefined data types supported by the implementation.
//     */
//    int SQLGETTYPEINFO      = SQL_API_BASE + 47;
//
//    /**
//     * Indicates that Result encapsulates a request to determine whether there
//     * are more result sets available on a statement handle and, if there are,
//     * initialize processing for those result sets.
//     */
//  int SQLMORERESULTS = SQL_API_BASE + 61;
//
//    /**
//     * Indicates that Result encapsulates a request to determine whether there
//     * are more result sets available on a statement handle and, if there are,
//     * initialize processing for the next result set on a separate statement
//     * handle.
//     */
//  int SQLNEXTRESULT = SQL_API_BASE + 73;
//    /**
//     * Indicates that Result encapsulates a request to get the number of
//     * result columns of a prepared or executed statement.
//     */
//    int SQLNUMRESULTCOLS = SQL_API_BASE + 18;
//    /**
//     * Indicates that Result encapsulates a request to process a deferred
//     * parameter value. For example, a streamed or locator identified
//     * parameter.
//     */
//    int SQLPARAMDATA        = SQL_API_BASE + 48;
//
//    /**
//     * Indicates that Result encapsulates a request to prepare a statement.
//     */
//    int SQLPREPARE = SQL_API_BASE + 19;
//
//    /**
//     * Indicates that Result encapsulates a request to return a result set that
//     * contains a list of the column names that comprise the primary key for a
//     * single specified table stored in the information schemas of the
//     * connected data source.
//     */
//    int SQLPRIMARYKEYS      = SQL_API_BASE + 65;
//
//    /**
//     * Indicates that Result encapsulates a request to provide a deferred
//     * parameter value. For example, a streamed or locator-identified
//     * parameter.
//     */
//    int SQLPUTDATA          = SQL_API_BASE + 49;
//
//    /**
//     * Indicates that Result encapsulates a request to get the row count of an
//     * executed statement.
//     */
//    int SQLROWCOUNT = SQL_API_BASE + 20;
//
//    /**
//     * Indicates that Result encapsulates a request to set the value of an
//     * SQL-connection attribute.
//     */
//    int SQLSETCONNECTATTR = SQL_API_BASE + 1016;
//
//
//    /** Indicates that Result encapsulates a request to set a cursor name. */
//    int SQLSETCURSORNAME    = SQL_API_BASE + 21;
//
//    /**
//     * Indicates that Result encapsulates a request to set a field in a CLI
//     * descriptor area.
//     */
//    int SQLSETDESCFIELD     = SQL_API_BASE + 1017;
//
//    /**
//     * Indicates that Result encapsulates a request to set commonly-used
//     * fields in a CLI descriptor area.
//     */
//    int SQLSETDESCREC       = SQL_API_BASE + 1018;
//
//
//    /**
//     * Indicates that Result encapsulates a request to set the value of an
//     * SQL-environment attribute.
//     */
//  int SQLSETENVATTR = SQL_API_BASE + 1019;
//    /** Indicates that Result encapsulates a request to set the value of an
//     * SQL-statement attribute.
//     */
//    int SQLSETSTMTATTR      = SQL_API_BASE + 1020;
//
//    /**
//     * Indicates that Result encapsulates a request to return a result set that
//     * contains a list of columns the combined values of which can uniquely
//     * identify any row within a single specified table described by the
//     * Information Schemas of the connected data source.
//     */
//    int SQLSPECIALCOLUMNS   = SQL_API_BASE + 52;
//
//
//    /**
//     * Indicates that Result encapsulates a request to explicitly start an
//     * SQL-transaction and set its characteristics.
//     */
//    int SQLSTARTTRAN = SQL_API_BASE + 74;
//
//    /**
//     * Indicates that Result encapsulates a request to return a result set that
//     * contains a list of the privileges held on the tables whose names adhere
//     * to the requested pattern(s) within tables described by the Information
//     * Schemas of the connected data source.
//     */
//    int SQLTABLES           = SQL_API_BASE + 54;
//
//    /**
//     * Indicates that Result encapsulates a request to, based on the specified
//     * selection criteria, return a result set that contains information about
//     * tables described by the Information Schema of the connected data source.
//     */
//    int SQLTABLEPRIVILEGES  = SQL_API_BASE + 70;
/*
 Codes for transaction termination:

 COMMIT 0
 ROLLBACK 1
 SAVEPOINT NAME ROLLBACK 2
 SAVEPOINT NAME RELEASE 4
 COMMIT AND CHAIN 6
 ROLLBACK AND CHAIN 7
 Implementation-defined termination type <0
 */
    int TX_COMMIT                  = 0;
    int TX_ROLLBACK                = 1;
    int TX_SAVEPOINT_NAME_ROLLBACK = 2;
    int TX_SAVEPOINT_NAME_RELEASE  = 4;
    int TX_COMMIT_AND_CHAIN        = 6;
    int TX_ROLLBACK_AND_CHAIN      = 7;

/* StatementType codes duplicated for cursor operations */

    int UPDATE_CURSOR = StatementTypes.UPDATE_CURSOR;
    int DELETE_CURSOR = StatementTypes.DELETE_CURSOR;
    int INSERT_CURSOR = StatementTypes.INSERT;
/* Environment attributes */

//#define SQL_ATTR_OUTPUT_NTS 10001

/* Connection attributes */

//#define SQL_ATTR_AUTO_IPD 10001
//#define SQL_ATTR_SAVEPOINT_NAME 10027
    int SQL_ATTR_SAVEPOINT_NAME = 10027;

// Batched execution constants:

    /** batch item failed */
    int EXECUTE_FAILED = -3;

    /**
     * Batch item succeeded but does not generate an update count,
     * for example a call having no return value
     */
    int SUCCESS_NO_INFO = -2;
/*
   SQL standard properties
   The operational sensitivity property (either SENSITIVE, INSENSITIVE, or ASENSITIVE).
   The operational scrollability property (either SCROLL or NO SCROLL).
   The operational holdability property (either WITH HOLD or WITHOUT HOLD).
   The operational returnability property (either WITH RETURN or WITHOUT RETURN).
*/

// data result properties - matching java.sql.ResultSet constants
    int TYPE_FORWARD_ONLY       = 1003;
    int TYPE_SCROLL_INSENSITIVE = 1004;
    int TYPE_SCROLL_SENSITIVE   = 1005;

    //
    int CONCUR_READ_ONLY = 1007;
    int CONCUR_UPDATABLE = 1008;

    //
    int HOLD_CURSORS_OVER_COMMIT = 1;
    int CLOSE_CURSORS_AT_COMMIT  = 2;

    /** Constants indicating generated key return behaviour */
    int RETURN_GENERATED_KEYS             = 1;     // matching java.sql.Statement constant
    int RETURN_NO_GENERATED_KEYS          = 2;     // matching java.sql.Statement constant
    int RETURN_GENERATED_KEYS_COL_NAMES   = 11;    // constant in HSQLDB only
    int RETURN_GENERATED_KEYS_COL_INDEXES = 21;    // constant in HSQLDB only
}
