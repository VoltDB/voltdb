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


package org.hsqldb_voltpatches.jdbc;

import java.sql.SQLException;
import java.sql.SQLWarning;

//#ifdef JAVA6
import java.sql.SQLDataException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;

//#endif JAVA6
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.result.Result;

/* $Id: Util.java 2947 2009-03-22 23:44:51Z fredt $ */

// boucherb@users - 20060523 - patch 1.9.0 - removed some unused imports
// Revision 1.16  2006/07/12 11:53:53  boucherb
//  - merging back remaining material overritten by Fred's type-system upgrades

/**
 * Provides driver constants and a gateway from internal HsqlExceptions to
 * external SQLExceptions.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class Util {

    static final void throwError(HsqlException e) throws SQLException {

//#ifdef JAVA6
        throw sqlException(e.getMessage(), e.getSQLState(), e.getErrorCode(),
                           e);

//#else
/*
        throw new SQLException(e.getMessage(), e.getSQLState(),
                               e.getErrorCode());
*/

//#endif JAVA6
    }

    static final void throwError(Result r) throws SQLException {

//#ifdef JAVA6
        throw sqlException(r.getMainString(), r.getSubString(),
                           r.getErrorCode(), r.getException());

//#else
/*
        throw new SQLException(r.getMainString(), r.getSubString(),
                               r.getErrorCode());
*/

//#endif JAVA6
    }

    public static final SQLException sqlException(HsqlException e) {

//#ifdef JAVA6
        return sqlException(e.getMessage(), e.getSQLState(), e.getErrorCode(),
                            e);

//#else
/*
        return new SQLException(e.getMessage(), e.getSQLState(),
                                e.getErrorCode());
*/

//#endif JAVA6
    }

    public static final SQLException sqlException(HsqlException e,
            Throwable cause) {

//#ifdef JAVA6
        return sqlException(e.getMessage(), e.getSQLState(), e.getErrorCode(),
                            cause);

//#else
/*
        return new SQLException(e.getMessage(), e.getSQLState(),
                                e.getErrorCode());
*/

//#endif JAVA6
    }

    public static final SQLException sqlException(int id) {
        return sqlException(Error.error(id));
    }

    public static final SQLException sqlExceptionSQL(int id) {
        return sqlException(Error.error(id));
    }

    public static final SQLException sqlException(int id, String message) {
        return sqlException(Error.error(id, message));
    }

    public static final SQLException sqlException(int id, String message,
            Exception cause) {
        return sqlException(Error.error(id, message), cause);
    }

    public static final SQLException sqlException(int id, int add) {
        return sqlException(Error.error(id, add));
    }

    static final SQLException sqlException(int id, int subId, Object[] add) {
        return sqlException(Error.error(id, subId, add));
    }

    static final SQLException notSupported() {
        return sqlException(Error.error(ErrorCode.X_0A000));
    }

    static SQLException notUpdatableColumn() {
        return sqlException(ErrorCode.X_0U000);
    }

    public static SQLException nullArgument() {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT);
    }

    static SQLException nullArgument(String name) {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name + ": null");
    }

    public static SQLException invalidArgument() {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT);
    }

    public static SQLException invalidArgument(String name) {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name);
    }

    public static SQLException outOfRangeArgument() {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT);
    }

    public static SQLException outOfRangeArgument(String name) {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name);
    }

    public static SQLException connectionClosedException() {
        return sqlException(ErrorCode.X_08003);
    }

    public static SQLWarning sqlWarning(Result r) {
        return new SQLWarning(r.getMainString(), r.getSubString(),
                              r.getErrorCode());
    }

    public static SQLException sqlException(Result r) {

//#ifdef JAVA6
        return new SQLException(r.getMainString(), r.getSubString(),
                                r.getErrorCode(), r.getException());

//#else
/*
        return new SQLException(r.getMainString(), r.getSubString(),
                                r.getErrorCode());
*/

//#endif JAVA6
    }

// TODO: Needs review.
//
//  Connection exception subclass may be an insufficient discriminator
//  regarding the choice of throwing transient or non-transient
//  connection exception.
//
// SQL 2003 Table 32  SQLSTATE class and subclass values
//
//  connection exception 08 (no subclass)                     000
//
//                     SQL-client unable to establish    001
//                     SQL-connection
//
//                     connection name in use            002
//
//                     connection does not exist         003
//
//                     SQL-server rejected establishment 004
//                     of SQL-connection
//
//                     connection failure                006
//
//                     transaction resolution unknown    007
// org.hsqldb_voltpatches.Trace - sql-error-messages
//
// 080=08000 socket creation error                             - better 08001 ?
// 085=08000 Unexpected exception when setting up TLS
//
// 001=08001 The database is already in use by another process - better 08002 ?
//
// 002=08003 Connection is closed
// 003=08003 Connection is broken
// 004=08003 The database is shutdown
// 094=08003 Database does not exists                          - better 08001 ?
//
//#ifdef JAVA6
    public static final SQLException sqlException(String msg, String sqlstate,
            int code, Throwable cause) {

        if (sqlstate.startsWith("08")) {
            if (!sqlstate.endsWith("003")) {

                // then, e.g. - the database may spuriously cease to be "in use"
                //              upon retry
                //            - the network configuration, server availability
                //              may change spuriously
                //            - keystore location/content may change spuriously
                return new SQLTransientConnectionException(msg, sqlstate,
                        code, cause);
            } else {

                // the database is (permanently) shut down or the connection is
                // (permanently) closed or broken
                return new SQLNonTransientConnectionException(msg, sqlstate,
                        code, cause);
            }
        } else if (sqlstate.startsWith("22")) {
            return new SQLDataException(msg, sqlstate, code, cause);
        } else if (sqlstate.startsWith("23")) {
            return new SQLIntegrityConstraintViolationException(msg, sqlstate,
                    code, cause);
        } else if (sqlstate.startsWith("28")) {
            return new SQLInvalidAuthorizationSpecException(msg, sqlstate,
                    code, cause);
        } else if (sqlstate.startsWith("42") || sqlstate.startsWith("37")
                   || sqlstate.startsWith("2A")) {

            // TODO:
            //
            // First, the overview section of java.sql.SQLSyntaxErrorException
            //
            // "...thrown when the SQLState class value is '<i>42</i>'"
            //
            // appears to be inaccurate or not in sync with the
            // SQL 2003 standard, 02 Foundation, Table 32, which states:
            //
            // Condition                               Class SubClass
            // syntax error or access rule violation -  42   (no subclass) 000
            //
            // SQL 2003 describes an Access Rule Violation as refering to
            // the case where, in the course of preparing or executing
            // an SQL statement, an Access Rule section pertaining
            // to one of the elements of the statement is violated.
            //
            // Further, section 13.4 Calls to an <externally-invoked-procedure>
            // lists:
            //
            // SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_NO_SUBCLASS:
            // constant SQLSTATE_TYPE :="42000";
            // SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_IN_DIRECT_STATEMENT_NO_SUBCLASS:
            // constant SQLSTATE_TYPE :="2A000";
            // SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION_IN_DYNAMIC_STATEMENT_NO_SUBCLASS:
            // constant SQLSTATE_TYPE :="37000";
            //
            // Strangely, SQLSTATEs "37000" and 2A000" are not mentioned
            // anywhere else in any of the SQL 2003 parts and are
            // conspicuously missing from 02 - Foundation, Table 32.
            //
            //  -----------------------------------
            ///
            // Our only Access Violation SQLSTATE so far is:
            //
            // Error.NOT_AUTHORIZED 255=42000 User not authorized for action '$$'
            //
            // Our syntax exceptions are apparently all sqlstate "37000"
            //
            // Clearly, we should differentiate between DIRECT and DYNAMIC
            // SQL forms.  And clearly, our current "37000" is "wrong" in
            // that we do not actually support dynamic SQL syntax, but
            // rather implement similar behaviour only through JDBC
            // Prepared and Callable statements.
            return new SQLSyntaxErrorException(msg, sqlstate, code, cause);
        } else if (sqlstate.startsWith("40")) {

            // TODO: our 40xxx exceptions are not currently used (correctly)
            //       for transaction rollback exceptions:
            //
            //       018=40001 Serialization failure
            //
            //       - currently used to indicate Java object serialization
            //         failures, which is just plain wrong.
            //
            //       019=40001 Transfer corrupted
            //
            //        - currently used to indicate IOExceptions related to
            //          PreparedStatement XXXStreamYYY operations and Result
            //          construction using RowInputBinary (e.g. when reading
            //          a result transmitted over the network), which is
            //          probably also just plain wrong.
            //
            // SQL 2003 02 - Foundation, Table 32 states:
            //
            // 40000  transaction rollback  - no subclass
            // 40001  transaction rollback  - (transaction) serialization failure
            // 40002  transaction rollback  - integrity constraint violation
            // 40003  transaction rollback  - statement completion unknown
            // 40004  transaction rollback  - triggered action exception
            //
            return new SQLTransactionRollbackException(msg, sqlstate, code,
                    cause);
        } else if (sqlstate.startsWith("0A")) {    // JSR 221 2005-12-14 prd
            return new SQLFeatureNotSupportedException(msg, sqlstate, code,
                    cause);
        } else {

            // TODO resolved:
            //
            // JSR 221 2005-12-14 prd
            //
            //   "Any SQLState class values which are currently not mapped to
            //    either a SQLNonTransientException or a SQLTransientException
            //    will result in a java.sql.SQLException being thrown."
            //
            return new SQLException(msg, sqlstate, code, cause);
        }
    }

//#endif JAVA6
// -----------------------------------------------------------------------------
// TODO:
// This is just reminder stuff to borrow from as error reporting is refined,
// better localized and made more standards-compliant.
//    static SQLException blobDirectUpdateByLocatorNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException blobInFreedStateException() {
//        return sqlException(ErrorCode.ASSERT_FAILED, "valid==true");
//    }
//
//    static SQLException blobInputMaxOctetLengthExceededException(long length) {
//        String msg = "Maximum Blob input octet length exceeded: "
//                   + length; //NOI18N
//
//        return sqlException(ErrorCode.INPUTSTREAM_ERROR, msg);
//    }
//
//    static SQLException blobInputStreamTransferCorruptedException(Exception e) {
//        // According to SQL 2003, error code 19 should not
//        // have sqlstate 40001, which is supposed to indicate a
//        // transaction rollback due to transaction serialization
//        // failure
//        return sqlException(ErrorCode.TRANSFER_CORRUPTED, String.valueOf(e));
//    }
//
//    static SQLException callableStatementOutAndInOutParametersNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException callableStatementParameterIndexNotFoundException(int index) {
//        //String msg = "Parameter index out of bounds: " + index; //NOI18N
//        return sqlException(Error.error(ErrorCode.COLUMN_NOT_FOUND, Integer.toString(index)));
//    }
//
//    static SQLException callableStatementParameterNameNotFoundException(String parameterName) {
//        return sqlException(Error.error(ErrorCode.COLUMN_NOT_FOUND, parameterName));
//    }
//
//    static SQLException characterInputStreamIOException(Exception e) {
//        return sqlException(Error.INPUTSTREAM_ERROR, String.valueOf(e));
//    }
//
//    static SQLException characterInputStreamTransferCorruptedException(Exception e) {
//        // According to SQL 2003, error code 19 should not
//        // have sqlstate 40001, which is supposed to indicate a
//        // transaction rollback due to transaction serialization
//        // failure
//        return sqlException(ErrorCode.TRANSFER_CORRUPTED, String.valueOf(e));
//    }
//
//    static SQLException characterOutputStreamIOException(Exception e) {
//        return sqlException(ErrorCode.GENERAL_IO_ERROR, String.valueOf(e));
//    }
//
//    static SQLException characterSequenceIndexArgumentOutOfBoundsException(String name, long value) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name + ": " + value);
//    }
//
//    static SQLException clientInfoNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED, "ClientInfo");
//    }
//
//    static SQLException clobDirectUpdateByLocatorNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException clobInFreedStateException() {
//        return sqlException(ErrorCode.ASSERT_FAILED, "valid==true");
//    }
//
//    static SQLException clobInputMaxCharacterLengthExceededException(long length) {
//        String msg = "Max Clob input character length exceeded: "
//                   + length; //NOI18N
//
//        return sqlException(ErrorCode.INPUTSTREAM_ERROR, msg);
//    }
//
//    static SQLException clobInputStreamTransferCorruptedException(Exception e) {
//        // According to SQL 2003, error code 19 should not
//        // have sqlstate 40001, which is supposed to indicate a
//        // transaction rollback due to transaction serialization
//        // failure
//        return sqlException(ErrorCode.TRANSFER_CORRUPTED, String.valueOf(e));
//    }
//
////    public static SQLException connectionClosedException() {
////        return sqlException(ErrorCode.CONNECTION_IS_CLOSED);
////    }
//
//
//    static SQLException connectionNativeSQLException(String sql, int index) {
//        boolean substitute = true;
//        String msg = Error.getMessage(ErrorCode.JDBC_CONNECTION_NATIVE_SQL,
//                                      substitute, new Object[]{
//                                      sql.substring(index)});
//
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, msg);
//    }
//
//    static SQLException connectionResetFailureException(Exception e) {
//        return sqlException(ErrorCode.GENERAL_ERROR, "Error resetting connection: "
//                                   + e.getMessage());
//    }
//
//    static SQLException deserializeToJavaObjectException(HsqlException e) {
//        // TODO:  This is wrong.
//        // According to SQL 2003, error code 18,
//        // sqlstate 40001 is supposed to indicate a
//        // transaction rollback due to
//        // transaction serialization failure
//        return sqlException(e);
//    }
//
//    public static SQLException driverConnectMalformedURLException(String url) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, "url: " + url);
//    }
//
//    public static SQLException driverConnectTimeoutException(long timeout) {
//        return sqlException(ErrorCode.GENERAL_ERROR,
//                           "Connect operation timed out after " + timeout + " ms.");
//    }
//
//
//    static SQLException illegalBestRowIdentifierScopeArgumentException(int scope) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT,
//                            Error.JDBC_ILLEGAL_BRI_SCOPE,
//                            new Object[]  {Integer.toString(scope)});
//    }
//
//    static SQLException illegalConnectionSubProtocolException(String protocol) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, "protocol: " + protocol);
//    }
//
//    static SQLException illegalHexadecimalCharacterSequenceArgumentException(String name, Exception e) {
//        return sqlException(ErrorCode.INVALID_CONVERSION, name + ": " + e);
//    }
//
//    static SQLException illegalNegativeIntegralArgumentException(String name, long value) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name + ": " + value);
//    }
//
//    static SQLException illegalNullArgumentException(String name) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name + ": null");
//    }
//
//    static SQLException illegalResultSetConcurrencyArgumentException(int concurrency) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, "concurrency: " + concurrency);
//    }
//
//    static SQLException illegalResultSetFetchDirectionArgumentException(int direction) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, "direction: " + direction);
//    }
//
//    static SQLException illegalResultSetHoldabilityArgumentException(int holdability) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, "holdability: " + holdability);
//    }
//
//    static SQLException illegalResultSetTypeArgumentException(int type) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, "type: " + type);
//    }
//
//    static SQLException illegalTableTypeArgumentException(int index, String type) {
//        String msg = "types[" + index + "]=>\"" + type + "\"";
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, msg);
//    }
//
//    static SQLException illegalTransactionIsolationLevelArgumentException(int level) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, "level: " + level);
//    }
//
//    static SQLException illegalUnicodeStreamLengthArgumentException(int length) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT,
//                "Odd length argument for pre-JDBC4 UTF16 encoded stream: "
//                  + length); //NOI18N
//    }
//
//    static SQLException invalidDateTimeEscapeException(Exception e) {
//        return sqlException(ErrorCode.INVALID_ESCAPE, e.getMessage());
//    }
//
//    static SQLException invalidNullInputStreamArgumentException() {
//        return sqlException(ErrorCode.error(ErrorCode.JDBC_INVALID_ARGUMENT,
//                Error.JDBC_NULL_STREAM));
//    }
//
//    static SQLException octetInputStreamInvalidCharacterEncodingException(Exception e) {
//        return sqlException(ErrorCode.INVALID_CHARACTER_ENCODING, String.valueOf(e));
//    }
//
//    static SQLException octetInputStreamTransferCorruptedException(Exception e) {
//        // According to SQL 2003, error code 19 should not
//        // have sqlstate 40001, which is supposed to indicate a
//        // transaction rollback due to transaction serialization
//        // failure
//        return sqlException(ErrorCode.TRANSFER_CORRUPTED, String.valueOf(e));
//    }
//
//    static SQLException octetSequenceIndexArgumentOutOfBoundsException(String name, long value) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name + ": " + value);
//    }
//
//    static SQLException parameterMetaDataParameterIndexNotFoundException(int index) {
//        // String msg = param + " is out of range"; //NOI18N
//        return sqlException(ErrorCode.COLUMN_NOT_FOUND, Integer.toString(index));
//    }
//
//    static SQLException preparedStatementFeatureNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException preparedStatementInitializationException(Exception e) {
//        return sqlException(ErrorCode.GENERAL_ERROR, e.toString());
//    }
//
//    static SQLException preparedStatementParameterIndexNotFoundException(int parameterIndex) {
//        return sqlException(ErrorCode.COLUMN_NOT_FOUND,
//                            Integer.toString(parameterIndex));
//    }
//
//    static SQLException resultSetClosedException() {
//        return sqlException(ErrorCode.JDBC_RESULTSET_IS_CLOSED);
//    }
//
//    static SQLException resultSetColumnIndexNotFoundException(int columnIndex) {
//        return sqlException(ErrorCode.COLUMN_NOT_FOUND,
//                Integer.toString(columnIndex));
//    }
//
//    static SQLException resultSetColumnNameNotFoundException(String columnName) {
//        return sqlException(Error.error(ErrorCode.COLUMN_NOT_FOUND, columnName));
//    }
//
//    static SQLWarning resultSetConcurrencyTranslationWarning(int requestedConcurrency,
//                                                             int translatedConcurrency) {
//        String requested  = toResultSetConcurrencyString(requestedConcurrency);
//        String translated = toResultSetConcurrencyString(translatedConcurrency);
//        String msg        = requested + " => " + translated;
//
//        return new SQLWarning(msg, "SOO10", Error.JDBC_INVALID_ARGUMENT);
//    }
//
//    static SQLException resultSetCursorNameNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException resultSetDataIsNotAvailableException() {
//        return sqlException(ErrorCode.NO_DATA_IS_AVAILABLE);
//    }
//
//    static SQLException resultSetFetchDirectionValueNotSupportedException(int direction) {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED,
//                            toResultSetFetchDirectionString(direction));
//    }
//
//    static SQLWarning resultSetHoldabilityTranslationWarning(int requestedHoldability, int translatedHoldability) {
//        String requested  = toResultSetHoldabilityString(requestedHoldability);
//        String translated = toResultSetHoldabilityString(translatedHoldability);
//        String msg        = requested + " => " + translated;
//
//        return new SQLWarning(msg, "SOO10", Error.JDBC_INVALID_ARGUMENT);
//    }
//
//
//    static SQLException resultSetHoldabilityValueNotSupportedException(int holdability) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, toResultSetHoldabilityString(holdability));
//    }
//
//    static SQLException resultSetIsForwardOnlyException() {
//        return sqlException(ErrorCode.RESULTSET_FORWARD_ONLY);
//    }
//
//    static SQLException resultSetMetaDataColumnIndexNotFoundException(int columnIndex) {
//        return Util.sqlException(ErrorCode.COLUMN_NOT_FOUND,
//                Integer.toString(columnIndex));
//    }
//
//    static SQLException resultSetMetaDataInitializationNullResultArgumentException() {
//        return sqlException(ErrorCode.GENERAL_ERROR,
//                Error.JDBC_NO_RESULT_SET, null);
//    }
//
//    static SQLException resultSetMetaDataInitializationNullResultSetArgumentException() {
//        return sqlException(ErrorCode.GENERAL_ERROR,
//                Error.JDBC_NO_RESULT_SET_METADATA, null);
//    }
//
//    static SQLException resultSetNotRefreshableException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException resultSetNotUpdateableException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLWarning resultSetTypeTranslationWarning(int requestedType, int translatedType) {
//        String requested  = toResultSetTypeString(requestedType);
//        String translated = toResultSetTypeString(translatedType);
//        String msg        = requested + " => " + translated;
//
//        return new SQLWarning(msg, "SOO10", Error.JDBC_INVALID_ARGUMENT);
//    }
//
//
//    static SQLException resultSetWasNotGeneratedByStatementExecutionException() {
//        String msg = "Expected but did not receive a result set"; // NOI18N
//
//        return sqlException(ErrorCode.UNEXPECTED_EXCEPTION, msg);
//    }
//
//    static SQLException resultSetWillNotBeGeneratedByExecuteQueryException() {
//        return sqlException(ErrorCode.JDBC_STATEMENT_NOT_RESULTSET);
//    }
//
//    static SQLException retrieveAutoGeneratedKeysFeatureNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException savepointIssuedOnDifferentConnectionException() {
//        return sqlException(ErrorCode.ASSERT_FAILED, "savepoint.connection==this");
//    }
//
//    static SQLException savepointNumericIdentifierNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException savepointRollbackInAutocommitException() {
//        return sqlException(ErrorCode.ASSERT_FAILED, "autocommit==false");
//    }
//
//    static SQLException savepointWrongObjectClassException(Class clazz) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, "savepoint: " + clazz);
//    }
//
//    static SQLException sqlxmlDirectUpdateByLocatorNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException sqlxmlInFreedStateException() {
//        return sqlException(ErrorCode.ASSERT_FAILED, "valid==true");
//    }
//
//    static SQLException sqlxmlParseException(Exception e) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, e.toString());
//    }
//
//    static SQLException sqlxmlParserInitializationException(Exception e) {
//        return sqlException(ErrorCode.GENERAL_ERROR, e.toString());
//    }
//
//    static SQLException statementClosedException() {
//        return sqlException(ErrorCode.STATEMENT_IS_CLOSED);
//    }
//
//    static SQLException statementGetMoreResultsWithCurrentResultSetHandlingNotSupportedException() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static int toObjectDataType(Object o) {
//        if (o == null) {
//            return Types.NULL;
//        }
//
//        if (o instanceof Binary) {
//            return Types.BINARY;
//        }  else            if (o instanceof JavaObject) {
//                return Types.OTHER;
//            }
//
//        try {
//            return org.hsqldb_voltpatches.Types.getTypeNr(o.getClass().getName());
//        }  catch (Exception e) {
//            return o instanceof Serializable ? Types.OTHER : Types.JAVA_OBJECT;
//        }
//    }
//
//    static String toResultSetConcurrencyString(int type) {
//        switch(type) {
//            case JDBCResultSet.CONCUR_READ_ONLY:
//            {
//                return "CONCUR_READ_ONLY";
//            }
//            JDBCResultSet.CONCUR_UPDATABLE:
//            {
//                return "CONCUR_UPDATABLE";
//            }
//            default:
//            {
//                return "CONCUR_UNKNOWN: " + type;
//            }
//        }
//    }
//
//    static String toResultSetFetchDirectionString(int direction) {
//        switch(direction) {
//            case JDBCResultSet.FETCH_FORWARD:
//            {
//                return "FETCH_FORWARD";
//            }
//            case JDBCResultSet.FETCH_REVERSE:
//            {
//                return "FETCH_REVERSE";
//            }
//            case JDBCResultSet.FETCH_UNKNOWN:
//            {
//                return "FETCH_UNKNOWN";
//            }
//            default:
//            {
//                return "direction: " + direction;
//            }
//        }
//    }
//
//    static String toResultSetHoldabilityString(int type) {
//        switch(type) {
//            case JDBCResultSet.CLOSE_CURSORS_AT_COMMIT:
//            {
//                return "CLOSE_CURSORS_AT_COMMIT";
//            }
//            case JDBCResultSet.HOLD_CURSORS_OVER_COMMIT:
//            {
//                return "HOLD_CURSORS_OVER_COMMIT";
//            }
//            default:
//            {
//                return "HOLDABILITY_UNKNOWN: " + type;
//            }
//        }
//    }
//
//
//    static String toResultSetTypeString(int type) {
//        switch(type) {
//            case JDBCResultSet.TYPE_FORWARD_ONLY:
//            {
//                return "TYPE_FORWARD_ONLY";
//            }
//            case JDBCResultSet.TYPE_SCROLL_INSENSITIVE:
//            {
//                return "TYPE_SCROLL_INSENSITIVE";
//            }
//            case JDBCResultSet.TYPE_SCROLL_SENSITIVE:
//            {
//                return "TYPE_SCROLL_SENSITIVE";
//            }
//            default:
//            {
//                return "TYPE_UNKNOWN: " + type;
//            }
//        }
//    }
//
//    static SQLException unsupportedDataType_ARRAY_Exception() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//
//    static SQLException unsupportedDataType_DATALINK_Exception() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException unsupportedDataType_DISTINCT_Exception() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException unsupportedDataType_REF_Exception() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException unsupportedDataType_ROWID_Exception() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException unsupportedDataType_SQLXML_Exception() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException unsupportedDataType_STRUCT_Exception() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//    static SQLException unsupportedDataTypes_STRUCT_AND_DISTINCT_Exception() {
//        return sqlException(ErrorCode.FUNCTION_NOT_SUPPORTED);
//    }
//
//
//    static SQLException unsupportedParameterValueConversionException(Object srcVal, int dstType) {
//        return unsupportedParameterValueConversionException(toObjectDataType(srcVal),
//                                                            srcVal,
//                                                            dstType);
//    }
//
//    static SQLException unsupportedParameterValueConversionException(int srcType, Object srcVal, int dstType) {
//        String msg = Types.getTypeString(srcType)
//                + " => "
//                + Types.getTypeString(dstType)
//                + " : "
//                + srcVal;
//
//        return sqlException(ErrorCode.INVALID_CONVERSION, msg);
//    }
//
//    static SQLException unsupportedResultSetValueConversionException(int srcType, Object srcVal, int dstType) {
//        String msg = Types.getTypeString(srcType)
//                + " => "
//                + Types.getTypeString(dstType)
//                + " : "
//                + srcVal;
//
//        return sqlException(ErrorCode.INVALID_CONVERSION, msg);
//    }
//
//    static SQLException updateCountResultInResultSetInitializationException() {
//        return sqlException(ErrorCode.ASSERT_FAILED, "result.mode != UPDATECOUNT");
//    }
//
//    static SQLException updateCountWasNotGeneratedByPreparedStatementExecutionException() {
//        String msg = "Expected but did not receive a row update count"; //NOI18N
//
//        return sqlException(ErrorCode.UNEXPECTED_EXCEPTION, msg);
//    }
//
//    static SQLException updateCountWasNotGeneratedByStatementExecutionException() {
//        return sqlException(ErrorCode.ASSERT_FAILED,
//                            Error.getMessage(ErrorCode.JDBC_STATEMENT_EXECUTE_UPDATE));
//    }
//
//    static SQLException updateCountWillNotBeGeneratedByExecuteUpdateException() {
//        return sqlException(ErrorCode.JDBC_STATEMENT_NOT_ROW_COUNT);
//    }
//
//    public static SQLException wrappedObjectNotFoundException(Class clazz) {
//        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, String.valueOf(clazz));
//    }
}
