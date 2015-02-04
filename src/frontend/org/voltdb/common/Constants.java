/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.common;

import java.nio.charset.Charset;

public class Constants
{
    public static final Charset UTF8ENCODING = Charset.forName("UTF-8");
    public static final Charset US_ASCII_ENCODING = Charset.forName("US-ASCII");

    // ODBC Datetime Format
    // if you need microseconds, you'll have to change this code or
    //  export a bigint representing microseconds since an epoch
    public static final String ODBC_DATE_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSS";

    public static final int DEFAULT_PORT = 21212;

    // authentication handshake codes
    public static final byte AUTH_HANDSHAKE_VERSION = 2;
    public static final byte AUTH_SERVICE_NAME = 4;
    public static final byte AUTH_HANDSHAKE = 5;

    public static final String KERBEROS = "kerberos";

    // reasons a connection can fail
    public static final byte AUTHENTICATION_FAILURE = -1;
    public static final byte MAX_CONNECTIONS_LIMIT_ERROR = 1;
    public static final byte WIRE_PROTOCOL_TIMEOUT_ERROR = 2;
    public static final byte WIRE_PROTOCOL_FORMAT_ERROR = 3;
    public static final byte AUTHENTICATION_FAILURE_DUE_TO_REJOIN = 4;
    public static final byte EXPORT_DISABLED_REJECTION = 5;

    // from jdbc metadata generation
    public static final String JSON_PARTITION_PARAMETER = "partitionParameter";
    public static final String JSON_PARTITION_PARAMETER_TYPE = "partitionParameterType";
    public static final String JSON_SINGLE_PARTITION = "singlePartition";
    public static final String JSON_READ_ONLY = "readOnly";

    // The transaction id layout.
    static final long UNUSED_SIGN_BITS = 1;
    static final long SEQUENCE_BITS = 49;
    static final long PARTITIONID_BITS = 14;

    // maximum values for the txn id fields
    static final long SEQUENCE_MAX_VALUE = (1L << SEQUENCE_BITS) - 1L;
    static final int PARTITIONID_MAX_VALUE = (1 << PARTITIONID_BITS) - 1;

    // from MP Initiator
    public static final int MP_INIT_PID = PARTITIONID_MAX_VALUE;

    /** String that can be used to indicate NULL value in CSV files */
    public static final String CSV_NULL = "\\N";
    /** String that can be used to indicate NULL value in CSV files */
    public static final String QUOTED_CSV_NULL = "\"\\N\"";

    // default export group to use when no group name is provided
    public static final String DEFAULT_EXPORT_CONNECTOR_NAME = "default";
}
