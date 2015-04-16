/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb_voltpatches.rowio;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.hsqldb_voltpatches.HsqlDateTime;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Scanner;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.scriptio.ScriptReaderBase;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.BlobData;
import org.hsqldb_voltpatches.types.BlobDataID;
import org.hsqldb_voltpatches.types.ClobData;
import org.hsqldb_voltpatches.types.ClobDataID;
import org.hsqldb_voltpatches.types.DateTimeType;
import org.hsqldb_voltpatches.types.IntervalMonthData;
import org.hsqldb_voltpatches.types.IntervalSecondData;
import org.hsqldb_voltpatches.types.IntervalType;
import org.hsqldb_voltpatches.types.JavaObjectData;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.TimeData;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;

/**
 * Class for reading the data for a database row from the script file.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.1
 * @since 1.7.3
 */
public class RowInputTextLog extends RowInputBase
implements RowInputInterface {

    Scanner  scanner;
    String   tableName  = null;
    String   schemaName = null;
    int      statementType;
    Object   value;
    boolean  version18;
    boolean  noSeparators;
    Calendar tempCalDefault = new GregorianCalendar();

    public RowInputTextLog() {

        super(new byte[0]);

        scanner = new Scanner();
    }

    public RowInputTextLog(boolean version18) {

        super(new byte[0]);

        scanner        = new Scanner();
        this.version18 = version18;
    }

    public void setSource(String text) {

        scanner.reset(text);

        statementType = ScriptReaderBase.ANY_STATEMENT;

        scanner.scanNext();

        String s = scanner.getString();

        if (s.equals(Tokens.T_INSERT)) {
            statementType = ScriptReaderBase.INSERT_STATEMENT;

            scanner.scanNext();
            scanner.scanNext();

            tableName = scanner.getString();

            scanner.scanNext();
        } else if (s.equals(Tokens.T_DELETE)) {
            statementType = ScriptReaderBase.DELETE_STATEMENT;

            scanner.scanNext();
            scanner.scanNext();

            tableName = scanner.getString();
        } else if (s.equals(Tokens.T_COMMIT)) {
            statementType = ScriptReaderBase.COMMIT_STATEMENT;
        } else if (s.equals(Tokens.T_SET)) {
            scanner.scanNext();

            if (Tokens.T_SCHEMA.equals(scanner.getString())) {
                scanner.scanNext();

                schemaName    = scanner.getString();
                statementType = ScriptReaderBase.SET_SCHEMA_STATEMENT;
            }
        }
    }

    public int getStatementType() {
        return statementType;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    protected void readField() {

        readFieldPrefix();
        scanner.scanNext();

        value = scanner.getValue();
    }

    protected void readNumberField(Type type) {

        readFieldPrefix();
        scanner.scanNext();

        boolean minus = scanner.getTokenType() == Tokens.MINUS;

        if (minus) {
            scanner.scanNext();
        }

        value = scanner.getValue();

        if (minus) {
            try {
                value = ((NumberType) scanner.getDataType()).negate(value);
            } catch (HsqlException e) {}
        }
    }

    protected void readFieldPrefix() {

        if (!noSeparators) {
            scanner.scanNext();

            if (statementType == ScriptReaderBase.DELETE_STATEMENT) {
                scanner.scanNext();
                scanner.scanNext();
            }
        }
    }

    public String readString() throws IOException {

        readField();

        return (String) value;
    }

    public short readShort() throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "");
    }

    public int readInt() throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "");
    }

    public long readLong() throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "");
    }

    public int readType() throws IOException {
        return 0;
    }

    protected boolean readNull() {

        // Return null on each column read instead.
        return false;
    }

    protected String readChar(Type type) throws IOException {

        readField();

        return (String) value;
    }

    protected Integer readSmallint() throws IOException {

        readNumberField(Type.SQL_SMALLINT);

        return (Integer) value;
    }

    protected Integer readInteger() throws IOException {

        readNumberField(Type.SQL_INTEGER);

        if (value instanceof Long) {
            value = Type.SQL_INTEGER.convertToDefaultType(null, value);
        }

        return (Integer) value;
    }

    protected Long readBigint() throws IOException {

        readNumberField(Type.SQL_BIGINT);

        if (value == null) {
            return null;
        }

        if (value instanceof BigDecimal) {
            return (Long) Type.SQL_BIGINT.convertToDefaultType(null, value);
        }

        return ValuePool.getLong(((Number) value).longValue());
    }

    protected Double readReal() throws IOException {

        readNumberField(Type.SQL_DOUBLE);

        if (value == null) {
            return null;
        }

        if (scanner.scanSpecialIdentifier(Tokens.T_DIVIDE)) {
            scanner.scanNext();

            Object divisor = scanner.getValue();
            double i       = ((Number) divisor).doubleValue();

            if (i == 0) {
                if (((Number) value).doubleValue() == 1E0) {
                    i = Double.NEGATIVE_INFINITY;
                } else if (((Number) value).doubleValue() == -1E0) {
                    i = Double.POSITIVE_INFINITY;
                } else if (((Number) value).doubleValue() == 0E0) {
                    i = Double.NaN;
                } else {
                    throw Error.error(ErrorCode.X_42584);
                }
            } else {
                throw Error.error(ErrorCode.X_42584);
            }

            value = Double.valueOf(i);
        }

        return (Double) value;
    }

    protected BigDecimal readDecimal(Type type) throws IOException {

        readNumberField(type);

        if (value == null) {
            return null;
        }

        BigDecimal bd = (BigDecimal) type.convertToDefaultType(null, value);

        return bd;
    }

    protected TimeData readTime(Type type) throws IOException {

        readField();

        if (value == null) {
            return null;
        }

        if (version18) {
            java.sql.Time dateTime = java.sql.Time.valueOf((String) value);
            long millis =
                HsqlDateTime.convertMillisFromCalendar(tempCalDefault,
                    dateTime.getTime());

            millis = HsqlDateTime.getNormalisedTime(millis);

            return new TimeData((int) millis / 1000, 0, 0);
        }

        return scanner.newTime((String) value);
    }

    protected TimestampData readDate(Type type) throws IOException {

        readField();

        if (value == null) {
            return null;
        }

        if (version18) {
            java.sql.Date dateTime = java.sql.Date.valueOf((String) value);
            long millis =
                HsqlDateTime.convertMillisFromCalendar(tempCalDefault,
                    dateTime.getTime());

            millis = HsqlDateTime.getNormalisedDate(millis);

            return new TimestampData(millis / 1000);
        }

        return scanner.newDate((String) value);
    }

    protected TimestampData readTimestamp(Type type) throws IOException {

        readField();

        if (value == null) {
            return null;
        }

        if (version18) {
            java.sql.Timestamp dateTime =
                java.sql.Timestamp.valueOf((String) value);
            long millis =
                HsqlDateTime.convertMillisFromCalendar(tempCalDefault,
                    dateTime.getTime());
            int nanos = dateTime.getNanos();

            nanos = ((DateTimeType) type).normaliseFraction(nanos, type.scale);

            return new TimestampData(millis / 1000, nanos, 0);
        }

        return scanner.newTimestamp((String) value);
    }

    protected IntervalMonthData readYearMonthInterval(Type type)
    throws IOException {

        readField();

        if (value == null) {
            return null;
        }

        return (IntervalMonthData) scanner.newInterval((String) value,
                (IntervalType) type);
    }

    protected IntervalSecondData readDaySecondInterval(Type type)
    throws IOException {

        readField();

        if (value == null) {
            return null;
        }

        return (IntervalSecondData) scanner.newInterval((String) value,
                (IntervalType) type);
    }

    protected Boolean readBoole() throws IOException {

        readFieldPrefix();
        scanner.scanNext();

        String token = scanner.getString();

        value = null;

        if (token.equalsIgnoreCase(Tokens.T_TRUE)) {
            value = Boolean.TRUE;
        } else if (token.equalsIgnoreCase(Tokens.T_FALSE)) {
            value = Boolean.FALSE;
        }

        return (Boolean) value;
    }

    protected Object readOther() throws IOException {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanBinaryStringWithQuote();

        if (scanner.getTokenType() == Tokens.X_MALFORMED_BINARY_STRING) {
            throw Error.error(ErrorCode.X_42587);
        }

        value = scanner.getValue();

        return new JavaObjectData(((BinaryData) value).getBytes());
    }

    protected BinaryData readBit() throws IOException {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanBitStringWithQuote();

        if (scanner.getTokenType() == Tokens.X_MALFORMED_BIT_STRING) {
            throw Error.error(ErrorCode.X_42587);
        }

        value = scanner.getValue();

        return (BinaryData) value;
    }

    protected BinaryData readBinary() throws IOException {

        readFieldPrefix();

        if (scanner.scanNull()) {
            return null;
        }

        scanner.scanBinaryStringWithQuote();

        if (scanner.getTokenType() == Tokens.X_MALFORMED_BINARY_STRING) {
            throw Error.error(ErrorCode.X_42587);
        }

        value = scanner.getValue();

        return (BinaryData) value;
    }

    protected ClobData readClob() throws IOException {

        readNumberField(Type.SQL_BIGINT);

        if (value == null) {
            return null;
        }

        long id = ((Number) value).longValue();

        return new ClobDataID(id);
    }

    protected BlobData readBlob() throws IOException {

        readNumberField(Type.SQL_BIGINT);

        if (value == null) {
            return null;
        }

        long id = ((Number) value).longValue();

        return new BlobDataID(id);
    }

    protected Object[] readArray(Type type) throws IOException {

        type = type.collectionBaseType();

        readFieldPrefix();
        scanner.scanNext();

        String token = scanner.getString();

        value = null;

        if (token.equalsIgnoreCase(Tokens.T_NULL)) {
            return null;
        } else if (!token.equalsIgnoreCase(Tokens.T_ARRAY)) {
            throw Error.error(ErrorCode.X_42584);
        }

        scanner.scanNext();

        token = scanner.getString();

        if (!token.equalsIgnoreCase(Tokens.T_LEFTBRACKET)) {
            throw Error.error(ErrorCode.X_42584);
        }

        HsqlArrayList list = new HsqlArrayList();

        noSeparators = true;

        for (int i = 0; ; i++) {
            if (scanner.scanSpecialIdentifier(Tokens.T_RIGHTBRACKET)) {
                break;
            }

            if (i > 0) {
                if (!scanner.scanSpecialIdentifier(Tokens.T_COMMA)) {
                    throw Error.error(ErrorCode.X_42584);
                }
            }

            Object value = readData(type);

            list.add(value);
        }

        noSeparators = false;

        Object[] data = new Object[list.size()];

        list.toArray(data);

        return data;
    }
}
