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

package org.voltdb;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.common.Constants;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.SerializationHelper;

/**
 * The ordered set of parameters of the proper types that is passed into
 * a stored procedure OR a plan fragment.
 *
 */
public class ParameterSet implements JSONString {

    static final byte ARRAY = -99;

    static class OneParamInfo {
        Object value;
        byte[] encodedString;
        byte[][] encodedStringArray;
    }

    private final Object m_params[];

    /*
     * The same ParameterSet instance could be accessed by multiple threads to
     * serialize the parameters. These two member variables keeps the encoded
     * copy of strings and string arrays besides the decoded versions in
     * m_params. There should only be one thread that adds the encoded strings.
     * Once created, they are not mutated. So it should be safe to not
     * synchronize on them.
     */
    private final byte[][] m_encodedStrings;
    private final byte[][][] m_encodedStringArrays;
    // memoized serialized size (start assuming valid size for empty ParameterSet)
    private final int m_serializedSize;

    public static ParameterSet emptyParameterSet() {
        return fromArrayNoCopy();
    }

    public static ParameterSet fromArrayWithCopy(Object... params) {
        return fromArrayNoCopy(params.clone());
    }

    public static ParameterSet fromArrayNoCopy(Object... params) {
        byte[][][] encodedStringArrays = new byte[params.length][][];
        byte[][] encodedStrings = new byte[params.length][];

        int size = 2;

        for (int ii = 0; ii < params.length; ii++) {
            Object obj = params[ii];
            if ((obj == null) || (obj == JSONObject.NULL)) {
                size++;
                continue;
            }
            size += 1;//everything has a type even arrays and null
            Class<?> cls = obj.getClass();
            if (cls.isArray()) {

                if (obj instanceof byte[]) {
                    final byte[] b = (byte[]) obj;
                    size += 4 + b.length;
                    continue;
                }

                VoltType type;
                try {
                    type = VoltType.typeFromClass(cls.getComponentType());
                }
                catch (VoltTypeException e) {
                    obj = getAKosherArray((Object[]) obj);
                    cls = obj.getClass();
                    type = VoltType.typeFromClass(cls.getComponentType());
                }

                size +=  1 + 2;// component type, array length
                switch (type) {
                    case SMALLINT:
                        size += 2 * ((short[])obj).length;
                        break;
                    case INTEGER:
                        size += 4 * ((int[])obj).length;
                        break;
                    case BIGINT:
                        size += 8 * ((long[])obj).length;
                        break;
                    case FLOAT:
                        size += 8 * ((double[])obj).length;
                        break;
                    case STRING:
                        String strings[] = (String[]) obj;
                        byte arrayEncodedStrings[][] = new byte[strings.length][];
                        for (int zz = 0; zz < strings.length; zz++) {
                            if (strings[zz] == null) {
                                size += 4;
                            } else {
                                arrayEncodedStrings[zz] = strings[zz].getBytes(Constants.UTF8ENCODING);
                                size += 4 + arrayEncodedStrings[zz].length;
                            }
                        }
                        encodedStringArrays[ii] = arrayEncodedStrings;
                        break;
                    case TIMESTAMP:
                        size += 8 * ((TimestampType[])obj).length;
                        break;
                    case DECIMAL:
                        size += 16 * ((BigDecimal[])obj).length;
                        break;
                    case VOLTTABLE:
                        for (VoltTable vt : (VoltTable[]) obj) {
                            size += vt.getSerializedSize();
                        }
                        break;
                    case VARBINARY:
                        for (byte[] buf : (byte[][]) obj) {
                            size += 4; // length prefix
                            if (buf != null) {
                                size += buf.length;
                            }
                        }
                        break;
                    default:
                        throw new RuntimeException("FIXME: Unsupported type " + type);
                }
                continue;
            }

            // Handle NULL mappings not encoded by type.min_value convention
            if (obj == VoltType.NULL_TIMESTAMP) {
                size += 8;
                continue;
            }
            else if (obj == VoltType.NULL_STRING_OR_VARBINARY) {
                size += 4;
                continue;
            }
            else if (obj == VoltType.NULL_DECIMAL) {
                size += 16;
                continue;
            }

            VoltType type = VoltType.typeFromClass(cls);
            switch (type) {
                case TINYINT:
                    size++;
                    break;
                case SMALLINT:
                    size += 2;
                    break;
                case INTEGER:
                    size += 4;
                    break;
                case BIGINT:
                    size += 8;
                    break;
                case FLOAT:
                    size += 8;
                    break;
                case STRING:
                    byte encodedString[] = ((String)obj).getBytes(Constants.UTF8ENCODING);
                    size += 4 + encodedString.length;
                    encodedStrings[ii] = encodedString;
                    break;
                case TIMESTAMP:
                    size += 8;
                    break;
                case DECIMAL:
                    size += 16;
                    break;
                case VOLTTABLE:
                    size += ((VoltTable) obj).getSerializedSize();
                    break;
                default:
                    throw new RuntimeException("FIXME: Unsupported type " + type);
            }
        }

        return new ParameterSet(params, size, encodedStrings, encodedStringArrays);
    }

    public static ParameterSet fromJSONString(String json) throws JSONException, IOException {
        JSONArray jArray = new JSONArray(json);
        return fromJSONArray(jArray);
    }

    public static ParameterSet fromJSONArray(JSONArray paramArray) throws JSONException, IOException {
        Object[] params = new Object[paramArray.length()];
        for (int i = 0; i < paramArray.length(); i++) {
            params[i] = paramFromPossibleJSON(paramArray.get(i));
        }
        return fromArrayNoCopy(params);
    }

    public static ParameterSet fromByteBuffer(ByteBuffer buffer) throws IOException {
        int startPos = buffer.position();

        short count = buffer.getShort();
        Object[] params = new Object[count];
        byte[][] encodedStrings = null;
        byte[][][] encodedStringArrays = null;

        for (int i = 0; i < count; ++i) {
            OneParamInfo opi = readOneParameter(buffer);
            params[i] = opi.value;
            if (opi.encodedString != null) {
                if (encodedStrings == null) {
                    encodedStrings = new byte[count][];
                }
                encodedStrings[i] = opi.encodedString;
            }
            if (opi.encodedStringArray != null) {
                if (encodedStringArrays == null) {
                    encodedStringArrays = new byte[count][][];
                }
                encodedStringArrays[i] = opi.encodedStringArray;
            }
        }

        int size = buffer.position() - startPos;

        return new ParameterSet(params, size, encodedStrings, encodedStringArrays);
    }

    private ParameterSet(Object[] params, int serializedSize, byte[][] encodedStrings, byte[][][] encodedStringArrays) {
        m_params = params;
        m_serializedSize = serializedSize;
        m_encodedStrings = encodedStrings;
        m_encodedStringArrays = encodedStringArrays;
    }

    static Object limitType(Object o) {
        Class<?> ctype = o.getClass();
        if (ctype == Integer.class) {
            return ((Integer) o).longValue();
        }

        return o;
    }

    public Object getParam(int index) {
        return m_params[index];
    }

    /**
     * Returns a copy of the parameter array
     * @return
     */
    public Object[] toArray() {
        return m_params.clone();
    }

    public int size() {
        return m_params.length;
    }

    public int getSerializedSize() {
        assert(m_serializedSize >= 2);
        return m_serializedSize;
    }

    public ByteBuffer serialize() {
        return null;
    }

    /*
     * Get a single indexed parameter. No size limits are enforced.
     * Do not use for large strings or varbinary (> 1MB).
     */
    static Object getParameterAtIndex(int partitionIndex, ByteBuffer unserializedParams) throws IOException {
        int paramLen = unserializedParams.getShort();
        if (partitionIndex >= paramLen) {
            // error if caller desires out of bounds parameter
            throw new RuntimeException("Invalid partition parameter requested.");
        }
        for (int i = 0; i < partitionIndex; ++i) {
            readOneParameter(unserializedParams);
        }
        OneParamInfo opi = readOneParameter(unserializedParams);
        unserializedParams.rewind();
        return opi.value;
    }

    static Object getAKosherArray(Object[] array) {
        int tables = 0;
        int integers = 0;
        int strings = 0;
        int doubles = 0;
        int nulls = 0;

        // handle empty arrays (too bad this is ints...)
        if (array.length == 0)
            return new int[0];

        // A note on Object[] null handling.  For most object array types, nulls can be bassed in as 'null' and/or
        // the VoltType Null equivalent.  Note that Object[] containing Strings supports null and VoltType nulls
        // as array elements.  But any other Sigil type class (big decimal, timestamp, varbinary)
        // DO NOT support nulls or VoltType nulls in Object[] arrays.  Also note that currently we do not
        // support timestamp or varbinary in Object arrays.  Future work...

        // first pass counts value types
        for (int i = 0; i < array.length; i++) {
            if (array[i] instanceof VoltTable) tables++;
            else if (array[i] instanceof Double) doubles++;
            else if (array[i] instanceof Float) doubles++;
            else if (array[i] instanceof Byte) integers++;
            else if (array[i] instanceof Short) integers++;
            else if (array[i] instanceof Integer) integers++;
            else if (array[i] instanceof Long) integers++;
            else if (array[i] instanceof String) strings++;
            else if (array[i] == VoltType.NULL_STRING_OR_VARBINARY) nulls++;
            else if (null == array[i]) nulls++;  // Handle nulls in an Object array.  Note only support nulls in STRING type, later we'll reject all other null usage.
            else {
                String msg = String.format("Type %s not supported in parameter set arrays.",
                                        array[i].getClass().toString());
                throw new RuntimeException(msg);
            }
        }

        // validate and choose type

        if (tables > 0) {
            if ((integers + strings + doubles) > 0) {
                String msg = "Cannot mix tables and other types in parameter set arrays.";
                throw new RuntimeException(msg);
            }
            assert(tables == array.length);
            VoltTable[] retval = new VoltTable[tables];
            for (int i = 0; i < array.length; i++)
                retval[i] = (VoltTable) array[i];
            return retval;
        }

        // note: there can't be any tables past this point

        // Verify that we don't have all null values.
        if (nulls == array.length)
            throw new RuntimeException("Unable to determine type. Parameter set array contains all NULL values.");

        if (strings > 0) {
            if ((integers + doubles) > 0) {
                String msg = "Cannot mix strings and numbers in parameter set arrays.";
                throw new RuntimeException(msg);
            }
            assert((strings + nulls) == array.length);
            String[] retval = new String[array.length];
            for (int i = 0; i < array.length; i++)
            {
                if (array[i] == VoltType.NULL_STRING_OR_VARBINARY) {
                    array[i] = null;
                }
                retval[i] = (String) array[i];
            }
            return retval;
        }

        // note: there can't be any strings or nulls past this point

        if (nulls > 0) {
            String msg = "Cannot mix numerics and nulls in parameter set arrays.";
            throw new RuntimeException(msg);
        }

        if (doubles > 0) {
            // note, ok to mix integers and doubles
            assert((doubles + integers) == array.length);
            double[] retval = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                Number numberval = (Number) array[i];
                retval[i] = numberval.doubleValue();
            }
            return retval;
        }

        // all ints from here out

        assert(integers == array.length);
        long[] retval = new long[array.length];
        for (int i = 0; i < array.length; i++) {
            Number numberval = (Number) array[i];
            retval[i] = numberval.longValue();
        }
        return retval;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("ParameterSet:");
        for (int i = 0; i < m_params.length; ++i) {
            b.append(",param[" + i + "]=" + (m_params[i] == null ? "NULL"
                    : m_params[i].toString() + "(" + m_params[i].getClass().getName() + ")"));
        }
        return new String(b);
    }

    @Override
    public String toJSONString() {
        JSONStringer js = new JSONStringer();
        try {
            js.array();
            for (Object o : m_params) {
                js.value(o);
            }
            js.endArray();
        }
        catch (JSONException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to serialize a parameter set to JSON.", e);
        }
        return js.toString();
    }

    static Object paramFromPossibleJSON(Object value) throws JSONException, IOException {
        if (value instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) value;
            return VoltTable.fromJSONObject(jsonObj);
        }
        if (value instanceof JSONArray) {
            JSONArray array = (JSONArray) value;
            Object[] retval = new Object[array.length()];
            for (int i = 0; i < array.length(); i++) {
                Object valueAtIndex = array.get(i);
                retval[i] = paramFromPossibleJSON(valueAtIndex);
            }
            return retval;
        }
        return value;
    }

    static private OneParamInfo readOneParameter(ByteBuffer in)
            throws IOException {
        Object value;
        int len;
        byte[] encodedString = null;
        byte[][] encodedStringArray = null;

        byte nextTypeByte = in.get();
        if (nextTypeByte == ARRAY) {
            VoltType nextType = null;
            byte etype = in.get();
            try {
                nextType = VoltType.get(etype);
            } catch (AssertionError ae) {
                throw new RuntimeException("ParameterSet doesn't support type " + etype);
            }
            if (nextType == null) {
                value = null;
            }
            else if (nextType == VoltType.STRING) {
                encodedStringArray = (byte[][]) SerializationHelper.readArray(byte[].class, in);
                String[] sval = new String[encodedStringArray.length];
                for (int i = 0; i < encodedStringArray.length; ++i) {
                    if (encodedStringArray[i] == null) {
                        sval[i] = null;
                    }
                    else {
                        sval[i] = new String(encodedStringArray[i], Constants.UTF8ENCODING);
                    }
                }
                value = sval;
            }
            else {
                value = SerializationHelper.readArray(nextType.classFromType(), in);
            }
        }
        else {
            VoltType nextType;
            try {
                nextType = VoltType.get(nextTypeByte);
            } catch (AssertionError ae) {
                throw new RuntimeException("ParameterSet doesn't support type " + nextTypeByte);
            }
            switch (nextType) {
                case NULL:
                    value = null;
                    break;
                case TINYINT:
                    value = in.get();
                    break;
                case SMALLINT:
                    value = in.getShort();
                    break;
                case INTEGER:
                    value = in.getInt();
                    break;
                case BIGINT:
                    value = in.getLong();
                    break;
                case FLOAT:
                    value = in.getDouble();
                    break;
                case STRING:
                    len = in.getInt();
                    if (len == VoltType.NULL_STRING_LENGTH) {
                        value = VoltType.NULL_STRING_OR_VARBINARY;
                    }
                    else {
                        encodedString = new byte[len];
                        in.get(encodedString);
                        value = new String(encodedString, Constants.UTF8ENCODING);
                    }
                    break;
                case VARBINARY:
                    len = in.getInt();
                    if (len == VoltType.NULL_STRING_LENGTH) {
                        value = VoltType.NULL_STRING_OR_VARBINARY;
                    }
                    else {
                        encodedString = new byte[len];
                        in.get(encodedString);
                        value = encodedString;
                    }
                    break;
                case TIMESTAMP:
                    final long micros = in.getLong();
                    value = new TimestampType(micros);
                    break;
                case VOLTTABLE:
                    final int tableSize = in.getInt();
                    byte[] tableBytes = new byte[tableSize];
                    in.get(tableBytes);
                    value = PrivateVoltTableFactory.createVoltTableFromBuffer(ByteBuffer.wrap(tableBytes), false);
                    break;
                case DECIMAL: {
                    BigDecimal decimal_val = SerializationHelper.getBigDecimal(in);
                    if (decimal_val == null) {
                        value = VoltType.NULL_DECIMAL;
                    }
                    else {
                        value = decimal_val;
                    }
                    break;
                }
                default:
                    throw new RuntimeException("ParameterSet doesn't support type " + nextType);
            }
        }

        OneParamInfo retval = new OneParamInfo();
        retval.value = value;
        retval.encodedString = encodedString;
        retval.encodedStringArray = encodedStringArray;
        return retval;
    }

    public void flattenToBuffer(ByteBuffer buf) throws IOException {

        buf.putShort((short)m_params.length);

        for (int i = 0; i < m_params.length; i++) {
            Object obj = m_params[i];
            if ((obj == null) || (obj == JSONObject.NULL)) {
                VoltType type = VoltType.NULL;
                buf.put(type.getValue());
                continue;
            }

            Class<?> cls = obj.getClass();
            if (cls.isArray()) {

                // Since arrays of bytes could be varbinary or strings,
                // and they are the only kind of array needed by the EE,
                // special case them as the VARBINARY type.
                if (obj instanceof byte[]) {
                    final byte[] b = (byte[]) obj;
                    // commented out this bit... presumably the EE will do this check upon recipt
                    /*if (b.length > VoltType.MAX_VALUE_LENGTH) {
                        throw new IOException("Value of byte[] larger than allowed max string or varbinary " + VoltType.MAX_VALUE_LENGTH_STR);
                    }*/
                    buf.put(VoltType.VARBINARY.getValue());
                    buf.putInt(b.length);
                    buf.put(b);
                    continue;
                }

                buf.put(ARRAY);

                VoltType type;
                try {
                    type = VoltType.typeFromClass(cls.getComponentType());
                }
                catch (VoltTypeException e) {
                    obj = getAKosherArray((Object[]) obj);
                    cls = obj.getClass();
                    type = VoltType.typeFromClass(cls.getComponentType());
                }

                buf.put(type.getValue());
                switch (type) {
                    case SMALLINT:
                        SerializationHelper.writeArray((short[]) obj, buf);
                        break;
                    case INTEGER:
                        SerializationHelper.writeArray((int[]) obj, buf);
                        break;
                    case BIGINT:
                        SerializationHelper.writeArray((long[]) obj, buf);
                        break;
                    case FLOAT:
                        SerializationHelper.writeArray((double[]) obj, buf);
                        break;
                    case STRING:
                        if (m_encodedStringArrays[i] == null) {
                            // should not happen
                            throw new IOException("String array not encoded");
                        }
                        // This check used to be done by FastSerializer.writeArray(), but things changed?
                        if (m_encodedStringArrays[i].length > Short.MAX_VALUE) {
                            throw new IOException("Array exceeds maximum length of "
                                                  + Short.MAX_VALUE + " bytes");
                        }
                        buf.putShort((short)m_encodedStringArrays[i].length);
                        for (int zz = 0; zz < m_encodedStringArrays[i].length; zz++) {
                            SerializationHelper.writeVarbinary(m_encodedStringArrays[i][zz], buf);
                        }
                        break;
                    case TIMESTAMP:
                        SerializationHelper.writeArray((TimestampType[]) obj, buf);
                        break;
                    case DECIMAL:
                        // converted long128 in serializer api
                        SerializationHelper.writeArray((BigDecimal[]) obj, buf);
                        break;
                    case VOLTTABLE:
                        SerializationHelper.writeArray((VoltTable[]) obj, buf);
                        break;
                    case VARBINARY:
                        SerializationHelper.writeArray((byte[][]) obj, buf);
                        break;
                    default:
                        throw new RuntimeException("FIXME: Unsupported type " + type);
                }
                continue;
            }

            // Handle NULL mappings not encoded by type.min_value convention
            if (obj == VoltType.NULL_TIMESTAMP) {
                buf.put(VoltType.TIMESTAMP.getValue());
                buf.putLong(VoltType.NULL_BIGINT);  // corresponds to EE value.h isNull()
                continue;
            }
            else if (obj == VoltType.NULL_STRING_OR_VARBINARY) {
                buf.put(VoltType.STRING.getValue());
                buf.putInt(VoltType.NULL_STRING_LENGTH);
                continue;
            }
            else if (obj == VoltType.NULL_DECIMAL) {
                buf.put(VoltType.DECIMAL.getValue());
                VoltDecimalHelper.serializeNull(buf);
                continue;
            }

            VoltType type = VoltType.typeFromClass(cls);
            buf.put(type.getValue());
            switch (type) {
                case TINYINT:
                    buf.put((Byte)obj);
                    break;
                case SMALLINT:
                    buf.putShort((Short)obj);
                    break;
                case INTEGER:
                    buf.putInt((Integer) obj);
                    break;
                case BIGINT:
                    buf.putLong((Long) obj);
                    break;
                case FLOAT:
                    if (cls == Float.class)
                        buf.putDouble(((Float) obj).doubleValue());
                    else if (cls == Double.class)
                        buf.putDouble(((Double) obj).doubleValue());
                    else
                        throw new RuntimeException("Can't cast parameter type to Double");
                    break;
                case STRING:
                    if (m_encodedStrings[i] == null) {
                        // should not happen
                        throw new IOException("String not encoded: " + (String) obj);
                    }
                    SerializationHelper.writeVarbinary(m_encodedStrings[i], buf);
                    break;
                case TIMESTAMP:
                    long micros = timestampToMicroseconds(obj);
                    buf.putLong(micros);
                    break;
                case DECIMAL:
                    VoltDecimalHelper.serializeBigDecimal((BigDecimal)obj, buf);
                    break;
                case VOLTTABLE:
                    ((VoltTable)obj).flattenToBuffer(buf);
                    break;
                default:
                    throw new RuntimeException("FIXME: Unsupported type " + type);
            }
        }
    }

    static long timestampToMicroseconds(Object obj) {
        long micros = 0;
        // Adapt the Java standard classes' millisecond count to TIMESTAMP's microseconds.
        if (obj instanceof java.util.Date) {
            micros = ((java.util.Date) obj).getTime()*1000;
            // For Timestamp, also preserve exactly the right amount of fractional second precision.
            if (obj instanceof java.sql.Timestamp) {
                long nanos = ((java.sql.Timestamp) obj).getNanos();
                // This may be slightly controversial, but...
                // Throw a conversion error rather than silently rounding/dropping sub-microsecond precision.
                if ((nanos % 1000) != 0) {
                    throw new RuntimeException("Can't serialize TIMESTAMP value with fractional microseconds");
                }
                // Use MOD 1000000 to prevent double-counting of milliseconds which figure into BOTH getTime() and getNanos().
                // DIVIDE nanoseconds by 1000 to get microseconds.
                micros += ((nanos % 1000000)/1000);
            }
        } else if (obj instanceof TimestampType) {
            // Let this throw a cast exception if obj is not actually a TimestampType instance.
            micros = ((TimestampType) obj).getTime();
        }
        return micros;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ParameterSet)) {
            return false;
        }
        ParameterSet other = (ParameterSet) obj;
        return Arrays.deepEquals(m_params, other.m_params);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; // any arbitrary constant will do
    }
}
