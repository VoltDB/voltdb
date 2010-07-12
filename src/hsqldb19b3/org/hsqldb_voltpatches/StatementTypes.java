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

/*
 * Codes based on SQL Standards for different types of statement.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public interface StatementTypes {

    int ALLOCATE_CURSOR                  = 1;
    int ALLOCATE_DESCRIPTOR              = 2;
    int ALTER_DOMAIN                     = 3;
    int ALTER_ROUTINE                    = 17;
    int ALTER_SEQUENCE                   = 134;
    int ALTER_TYPE                       = 60;
    int ALTER_TABLE                      = 4;
    int ALTER_TRANSFORM                  = 127;
    int CREATE_ASSERTION                 = 6;
    int CALL                             = 7;
    int CREATE_CHARACTER_SET             = 8;
    int CLOSE_CURSOR                     = 9;
    int CREATE_COLLATION                 = 10;
    int COMMIT_WORK                      = 11;
    int CONNECT                          = 13;
    int DEALLOCATE_DESCRIPTOR            = 15;
    int DEALLOCATE_PREPARE               = 16;
    int DELETE_CURSOR                    = 18;
    int DELETE_WHERE                     = 19;
    int DESCRIBE                         = 20;
    int SELECT_DIRECT_SINGLE             = 21;    // identifier is SELECT
    int DISCONNECT                       = 22;
    int CREATE_DOMAIN                    = 23;
    int DROP_ASSERTION                   = 24;
    int DROP_CHARACTER_SET               = 25;
    int DROP_COLLATION                   = 26;
    int DROP_TYPE                        = 35;
    int DROP_DOMAIN                      = 27;
    int DROP_ROLE                        = 29;
    int DROP_ROUTINE                     = 30;
    int DROP_SCHEMA                      = 31;
    int DROP_SEQUENCE                    = 135;
    int DROP_TABLE                       = 32;
    int DROP_TRANSFORM                   = 116;
    int DROP_TRANSLATION                 = 33;
    int DROP_TRIGGER                     = 34;
    int DROP_CAST                        = 78;
    int DROP_ORDERING                    = 115;
    int DROP_VIEW                        = 36;
    int DYNAMIC_CLOSE                    = 37;
    int DYNAMIC_DELETE_CURSOR            = 38;
    int DYNAMIC_FETCH                    = 39;
    int DYNAMIC_OPEN                     = 40;
    int SELECT_CURSOR                    = 85;
    int SELECT_SINGLE_DYNAMIC            = 41;    // identifier is SELECT
    int DYNAMIC_UPDATE_CURSOR            = 42;
    int EXECUTE_IMMEDIATE                = 43;
    int EXECUTE                          = 44;
    int FETCH                            = 45;
    int FREE_LOCATOR                     = 98;
    int GET_DESCRIPTOR                   = 47;
    int HOLD_LOCATOR                     = 99;
    int GRANT                            = 48;
    int GRANT_ROLE                       = 49;
    int INSERT                           = 50;
    int MERGE                            = 128;
    int OPEN                             = 53;
    int PREPARABLE_DYNAMIC_DELETE_CURSOR = 54;
    int PREPARABLE_DYNAMIC_UPDATE_CURSOR = 55;
    int PREPARE                          = 56;
    int RELEASE_SAVEPOINT                = 57;
    int RETURN                           = 58;
    int REVOKE                           = 59;
    int REVOKE_ROLE                      = 129;
    int CREATE_ROLE                      = 61;
    int ROLLBACK_WORK                    = 62;
    int SAVEPOINT                        = 63;
    int CREATE_SCHEMA                    = 64;
    int CREATE_ROUTINE                   = 14;
    int SELECT_SINGLE                    = 65;    // identifier is SELECT
    int CREATE_SEQUENCE                  = 133;
    int SET_CATALOG                      = 66;
    int SET_CONNECTION                   = 67;
    int SET_CONSTRAINT                   = 68;
    int SET_DESCRIPTOR                   = 70;
    int SET_TIME_ZONE                    = 71;
    int SET_NAMES                        = 72;
    int SET_PATH                         = 69;
    int SET_ROLE                         = 73;
    int SET_SCHEMA                       = 74;
    int SET_SESSION_AUTHORIZATION        = 76;
    int SET_SESSION_CHARACTERISTICS      = 109;
    int SET_COLLATION                    = 136;
    int SET_TRANSFORM_GROUP              = 118;
    int SET_TRANSACTION                  = 75;
    int START_TRANSACTION                = 111;
    int CREATE_TABLE                     = 77;
    int CREATE_TRANSFORM                 = 117;
    int CREATE_TRANSLATION               = 79;
    int CREATE_TRIGGER                   = 80;
    int UPDATE_CURSOR                    = 81;
    int UPDATE_WHERE                     = 82;
    int CREATE_CAST                      = 52;
    int CREATE_TYPE                      = 83;
    int CREATE_ORDERING                  = 114;
    int CREATE_VIEW                      = 84;
    int ASSIGNMENT                       = 5;     // PSM
    int CASE                             = 86;
    int BEGIN_END                        = 12;
    int DROP_MODULE                      = 28;
    int FOR                              = 46;
    int IF                               = 88;
    int ITERATE                          = 102;
    int LEAVE                            = 89;
    int LOOP                             = 90;
    int RESIGNAL                         = 91;
    int REPEAT                           = 95;
    int SIGNAL                           = 92;
    int CREATE_MODULE                    = 51;
    int WHILE                            = 97;

    // hsqldb database
    int DATABASE_BACKUP     = 1001;
    int DATABASE_CHECKPOINT = 1002;
    int DATABASE_SHUTDOWN   = 1003;
    int DATABASE_SCRIPT     = 1004;

    // hsqldb database settings
    int SET_DATABASE_DEFAULT_INITIAL_SCHEMA    = 1011;
    int SET_DATABASE_DEFAULT_TABLE_TYPE        = 1012;
    int SET_DATABASE_FILES_BACKUP_INCREMENT    = 1013;
    int SET_DATABASE_FILES_CACHE_FILE_SCALE    = 1014;
    int SET_DATABASE_FILES_CACHE_SIZE          = 1015;
    int SET_DATABASE_FILES_DEFRAG              = 1016;
    int SET_DATABASE_FILES_EVENT_LOG           = 1017;
    int SET_DATABASE_FILES_LOCK                = 1018;
    int SET_DATABASE_FILES_LOG_SIZE            = 1019;
    int SET_DATABASE_FILES_NIO                 = 1021;
    int SET_DATABASE_FILES_READ_ONLY           = 1022;
    int SET_DATABASE_FILES_READ_ONLY_FILES     = 1023;
    int SET_DATABASE_FILES_SCRIPT_FORMAT       = 1024;
    int SET_DATABASE_FILES_WRITE_DELAY         = 1035;
    int SET_DATABASE_PROPERTY                  = 1026;
    int SET_DATABASE_RESULT_MEMORY_ROWS        = 1027;
    int SET_DATABASE_SCRIPT_FORMAT             = 1028;
    int SET_DATABASE_SQL_COLLATION             = 1029;
    int SET_DATABASE_SQL_IGNORECASE            = 1030;
    int SET_DATABASE_SQL_REFERENTIAL_INTEGRITY = 1031;
    int SET_DATABASE_SQL_STRICT_KEYWORDS       = 1032;
    int SET_DATABASE_SQL_STRICT_SIZE           = 1033;
    int SET_DATABASE_READ_ONLY                 = 1034;
    int SET_DATABASE_READ_ONLY_FILES           = 1035;
    int SET_DATABASE_TRANSACTION_CONTROL       = 1036;

    // hsqldb user settings
    int SET_USER_INITIAL_SCHEMA = 1042;
    int SET_USER_PASSWORD       = 1043;

    // hsqldb session
    int TRANSACTION_LOCK_TABLE         = 1044;
    int SET_SESSION_AUTOCOMMIT         = 1045;
    int SET_SESSION_RESULT_MAX_ROWS    = 1046;
    int SET_SESSION_RESULT_MEMORY_ROWS = 1047;
    int ROLLBACK_SAVEPOINT             = 1048;

    // hsqldb schema
    int CREATE_ALIAS       = 1051;
    int CREATE_INDEX       = 1052;
    int CREATE_USER        = 1053;
    int DECLARE_VARIABLE   = 1056;
    int DROP_COLUMN        = 1057;
    int DROP_INDEX         = 1058;
    int DROP_CONSTRAINT    = 1059;
    int DROP_USER          = 1062;
    int EXPLAIN_PLAN       = 1063;
    int RENAME_OBJECT      = 1064;
    int SET_TABLE_INDEX    = 1067;
    int SET_TABLE_READONLY = 1068;
    int SET_TABLE_SOURCE   = 1069;
    int SET_TABLE_TYPE     = 1070;

    // hsqldb sql implementation
    int CONDITION = 1101;                         // element of IF
    int HANDLER   = 1102;
    int DDL       = 1103;
    int CHECK     = 1104;

    // hsqldb groups
    int X_SQL_SCHEMA_DEFINITION   = 2001;
    int X_SQL_SCHEMA_MANIPULATION = 2002;
    int X_SQL_DATA                = 2003;
    int X_SQL_DATA_CHANGE         = 2004;
    int X_SQL_TRANSACTION         = 2005;
    int X_SQL_CONNECTION          = 2006;
    int X_SQL_CONTROL             = 2007;
    int X_SQL_SESSION             = 2008;
    int X_SQL_DIAGNOSTICS         = 2009;
    int X_SQL_DYNAMIC             = 2010;
    int X_HSQLDB_SESSION          = 2011;
    int X_HSQLDB_SETTING          = 2012;
    int X_HSQLDB_OPERATION        = 2013;
    int X_HSQLDB_TRANSACTION      = 2014;
    int X_DYNAMIC                 = 2015;

    // Expected types of Result returned for an SQL statement
    int RETURN_ANY    = 0;
    int RETURN_COUNT  = 1;
    int RETURN_RESULT = 2;
}
