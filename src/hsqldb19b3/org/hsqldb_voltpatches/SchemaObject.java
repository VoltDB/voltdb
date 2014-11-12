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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.lib.OrderedHashSet;

/**
 * SQL schema object interface
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public interface SchemaObject {

    int CATALOG    = 1;
    int SCHEMA     = 2;
    int TABLE      = 3;
    int VIEW       = 4;
    int CONSTRAINT = 5;
    int ASSERTION  = 6;
    int SEQUENCE   = 7;
    int TRIGGER    = 8;
    int COLUMN     = 9;
    int TRANSITION = 10;
    int GRANTEE    = 11;
    int TYPE       = 12;
    int DOMAIN     = 13;
    int CHARSET    = 14;
    int COLLATION  = 15;
    int FUNCTION   = 16;
    int PROCEDURE  = 17;
    int ROUTINE    = 18;
    int CURSOR     = 19;
    int INDEX      = 20;
    int LABEL      = 21;
    int VARIABLE   = 22;
    int PARAMETER  = 23;

    //
    SchemaObject[] emptyArray = new SchemaObject[]{};

    int getType();

    HsqlName getName();

    HsqlName getSchemaName();

    HsqlName getCatalogName();

    Grantee getOwner();

    OrderedHashSet getReferences();

    OrderedHashSet getComponents();

    void compile(Session session);

    String getSQL();

    interface ViewCheckModes {

        int CHECK_NONE    = 0;
        int CHECK_LOCAL   = 1;
        int CHECK_CASCADE = 2;
    }

    interface ParameterModes {

        byte PARAM_UNKNOWN = 0;    // java.sql.ParameterMetaData.parameterModeUnknown
        byte PARAM_IN    = 1;      // java.sql.ParameterMetaData.parameterModeIn
        byte PARAM_OUT   = 4;      // java.sql.ParameterMetaData.parameterModeInOut
        byte PARAM_INOUT = 2;      // java.sql.ParameterMetaData.parameterModeOut
    }

    interface Nullability {

        byte NO_NULLS         = 0;    // java.sql.ResultSetMetaData.columnNoNulls
        byte NULLABLE         = 1;    // java.sql.ResultSetMetaData.columnNullable
        byte NULLABLE_UNKNOWN = 2;    // java.sql.ResultSetMetaData.columnNullableUnknown
    }
}
