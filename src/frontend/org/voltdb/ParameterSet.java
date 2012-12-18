/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

/**
 * The ordered set of parameters of the proper types that is passed into
 * a stored procedure OR a plan fragment.
 *
 */
 public class ParameterSet implements FastSerializable, JSONString {

    static final byte ARRAY = -99;

    /*
     * The same ParameterSet instance could be accessed by multiple threads to
     * serialize the parameters. These two member variables keeps the encoded
     * copy of strings and string arrays besides the decoded versions in
     * m_params. There should only be one thread that adds the encoded strings.
     * Once created, they are not mutated. So it should be safe to not
     * synchronize on them.
     */
    private byte[][] m_encodedStrings = null;
    private byte[][][] m_encodedStringArrays = null;
    // memoized serialized size (start assuming valid size for empty ParameterSet)
    private int m_serializedSize = 2;

    public ParameterSet() {
    }

    static Object limitType(Object o) {
        Class<?> ctype = o.getClass();
        if (ctype == Integer.class) {
            return ((Integer) o).longValue();
        }

        return o;
    }
    private Object m_params[] = new Object[0];

    /** Sets the internal array to params. Note: this does *not* copy the argument. */
    public void setParameters(Object... params) {
        this.m_params = params;

        // create encoded copies of strings and calculate size
        m_serializedSize = calculateSerializedSize();
    }

    /**
     * Returns the parameters. Don't modify the returned array!
     * @return
     */
    public Object[] toArray() {
        return m_params;
    }

    public int size() {
        return m_params.length;
    }

    public int getSerializedSize() {
        return m_serializedSize;
    }

    /*
     * Get a single indexed parameter. No size limits are enforced.
     * Do not use for large strings or varbinary (> 1MB).
     */
    static Object getParameterAtIndex(int partitionIndex, ByteBuffer unserializedParams) throws IOException {
        FastDeserializer in = new FastDeserializer(unserializedParams);
        int paramLen = in.readShort();
        if (partitionIndex >= paramLen) {
            // error if caller desires out of bounds parameter
            throw new RuntimeException("Invalid partition parameter requested.");
        }
        for (int i = 0; i < partitionIndex; ++i) {
            readOneParameter(in);
        }
        Object retval = readOneParameter(in);
        unserializedParams.rewind();
        return retval;
    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        int paramLen = in.readShort();
        m_params = new Object[paramLen];

        for (int i = 0; i < paramLen; i++) {
            m_params[i] = readOneParameter(in);
        }
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        out.writeShort(m_params.length);

        for (Object obj : m_params) {
            if ((obj == null) || (obj == JSONObject.NULL)) {
                VoltType type = VoltType.NULL;
                out.writeByte(type.getValue());
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
                    out.writeByte(VoltType.VARBINARY.getValue());
                    out.writeInt(b.length);
                    out.write(b);
                    continue;
                }

                out.writeByte(ARRAY);

                VoltType type;
                try {
                    type = VoltType.typeFromClass(cls.getComponentType());
                }
                catch (VoltTypeException e) {
                    obj = getAKosherArray((Object[]) obj);
                    cls = obj.getClass();
                    type = VoltType.typeFromClass(cls.getComponentType());
                }

                out.writeByte(type.getValue());
                switch (type) {
                    case TINYINT:
                        out.writeArray((byte[])obj);
                        break;
                    case SMALLINT:
                        out.writeArray((short[]) obj);
                        break;
                    case INTEGER:
                        out.writeArray((int[]) obj);
                        break;
                    case BIGINT:
                        out.writeArray((long[]) obj);
                        break;
                    case FLOAT:
                        out.writeArray((double[]) obj);
                        break;
                    case STRING:
                        out.writeArray((String[]) obj);
                        break;
                    case TIMESTAMP:
                        out.writeArray((TimestampType[]) obj);
                        break;
                    case DECIMAL:
                        // converted long128 in serializer api
                        out.writeArray((BigDecimal[]) obj);
                        break;
                    case VOLTTABLE:
                        out.writeArray((VoltTable[]) obj);
                        break;
                    case VARBINARY:
                        out.writeArray((byte[][]) obj);
                        break;
                    default:
                        throw new RuntimeException("FIXME: Unsupported type " + type);
                }
                continue;
            }

            // Handle NULL mappings not encoded by type.min_value convention
            if (obj == VoltType.NULL_TIMESTAMP) {
                out.writeByte(VoltType.TIMESTAMP.getValue());
                out.writeLong(VoltType.NULL_BIGINT);  // corresponds to EE value.h isNull()
                continue;
            }
            else if (obj == VoltType.NULL_STRING_OR_VARBINARY) {
                out.writeByte(VoltType.STRING.getValue());
                out.writeInt(VoltType.NULL_STRING_LENGTH);
                continue;
            }
            else if (obj == VoltType.NULL_DECIMAL) {
                out.writeByte(VoltType.DECIMAL.getValue());
                VoltDecimalHelper.serializeNull(out);
                continue;
            }

            VoltType type = VoltType.typeFromClass(cls);
            out.writeByte(type.getValue());
            switch (type) {
                case TINYINT:
                    out.writeByte((Byte)obj);
                    break;
                case SMALLINT:
                    out.writeShort((Short)obj);
                    break;
                case INTEGER:
                    out.writeInt((Integer) obj);
                    break;
                case BIGINT:
                    out.writeLong((Long) obj);
                    break;
                case FLOAT:
                    if (cls == Float.class)
                        out.writeDouble(((Float) obj).doubleValue());
                    else if (cls == Double.class)
                        out.writeDouble(((Double) obj).doubleValue());
                    else
                        throw new RuntimeException("Can't cast paramter type to Double");
                    break;
                case STRING:
                    out.writeString((String) obj);
                    break;
                case TIMESTAMP:
                    // FastSerializer does not need to distinguish time stamp types from long counts of microseconds.
                    long micros = timestampToMicroseconds(obj);
                    out.writeLong(micros);
                    break;
                case DECIMAL:
                    VoltDecimalHelper.serializeBigDecimal((BigDecimal)obj, out);
                    break;
                case VOLTTABLE:
                    out.writeObject((VoltTable) obj);
                    break;
                default:
                    throw new RuntimeException("FIXME: Unsupported type " + type);
            }
        }
    }

    static Object getAKosherArray(Object[] array) {
        int tables = 0;
        int integers = 0;
        int strings = 0;
        int doubles = 0;

        // handle empty arrays (too bad this is ints...)
        if (array.length == 0)
            return new int[0];

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

        if (strings > 0) {
            if ((integers + doubles) > 0) {
                String msg = "Cannot mix strings and numbers in parameter set arrays.";
                throw new RuntimeException(msg);
            }
            assert(strings == array.length);
            String[] retval = new String[strings];
            for (int i = 0; i < array.length; i++)
                retval[i] = (String) array[i];
            return retval;
        }

        // note: there can't be any strings past this point

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
        if (m_params == null) {
            b.append("NULL");
        } else {
            for (int i = 0; i < m_params.length; ++i) {
                b.append(",param[" + i + "]=" + (m_params[i] == null ? "NULL"
                        : m_params[i].toString() + "(" + m_params[i].getClass().getName() + ")"));
            }
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

    public static ParameterSet fromJSONString(String json) throws JSONException, IOException {
        JSONArray jArray = new JSONArray(json);
        return fromJSONArray(jArray);
    }

    public static ParameterSet fromJSONArray(JSONArray paramArray) throws JSONException, IOException {
        ParameterSet pset = new ParameterSet();
        pset.m_params = new Object[paramArray.length()];
        for (int i = 0; i < paramArray.length(); i++) {
            pset.m_params[i] = paramFromPossibleJSON(paramArray.get(i));
        }
        // create encoded copies of strings and calculate size
        pset.m_serializedSize = pset.calculateSerializedSize();
        return pset;
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

    static private Object readOneParameter(FastDeserializer in)
            throws IOException {
        byte nextTypeByte = in.readByte();
        if (nextTypeByte == ARRAY) {
            VoltType nextType = VoltType.get(in.readByte());
            if (nextType == null) return null;
            return in.readArray(nextType.classFromType());
        }
        else {
            VoltType nextType = VoltType.get(nextTypeByte);
            switch (nextType) {
                case NULL:
                    return null;
                case TINYINT:
                    return in.readByte();
                case SMALLINT:
                    return in.readShort();
                case INTEGER:
                    return in.readInt();
                case BIGINT:
                    return in.readLong();
                case FLOAT:
                    return in.readDouble();
                case STRING:
                    String string_val = in.readString();
                    if (string_val == null) {
                        return VoltType.NULL_STRING_OR_VARBINARY;
                    }
                    return string_val;
                case VARBINARY:
                    byte[] bin_val = in.readVarbinary();
                    if (bin_val == null) {
                        return VoltType.NULL_STRING_OR_VARBINARY;
                    }
                    return bin_val;
                case TIMESTAMP:
                    final long micros = in.readLong();
                    return new TimestampType(micros);
                case VOLTTABLE:
                    return in.readObject(VoltTable.class);
                case DECIMAL: {
                    BigDecimal decimal_val = in.readBigDecimal();
                    if (decimal_val == null)
                    {
                        return VoltType.NULL_DECIMAL;
                    }
                    return decimal_val;
                }
                case DECIMAL_STRING: {
                    BigDecimal decimal_val = in.readBigDecimalFromString();
                    if (decimal_val == null)
                    {
                        return VoltType.NULL_DECIMAL;
                    }
                    return decimal_val;
                }
                default:
                    throw new RuntimeException("ParameterSet doesn't support type " + nextType);
            }
        }
    }

    /**
     * Calculate the serialized size of this parameter set and encode all the
     * strings and string arrays. This should only be called once when
     * parameters are set.
     *
     * @return
     */
    private int calculateSerializedSize() {
        m_encodedStringArrays = new byte[m_params.length][][];
        m_encodedStrings = new byte[m_params.length][];

        if (m_serializedSize != 2) {
            throw new RuntimeException("Trying to calculate the serialized size " +
                                       "of the parameter set twice");
        }

        int size = 2;

        for (int ii = 0; ii < m_params.length; ii++) {
            Object obj = m_params[ii];
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
                        byte encodedStrings[][] = new byte[strings.length][];
                        for (int zz = 0; zz < strings.length; zz++) {
                            if (strings[zz] == null) {
                                size += 4;
                            } else {
                                try {
                                    encodedStrings[zz] = strings[zz].getBytes("UTF-8");
                                } catch (UnsupportedEncodingException e) {
                                    VoltDB.crashLocalVoltDB("Shouldn't happen", false, e);
                                }
                                size += 4 + encodedStrings[zz].length;
                            }
                        }
                        m_encodedStringArrays[ii] = encodedStrings;
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
                    try {
                        byte encodedString[] = ((String)obj).getBytes("UTF-8");
                        size += 4 + encodedString.length;
                        m_encodedStrings[ii] = encodedString;
                    } catch (UnsupportedEncodingException e) {
                        VoltDB.crashLocalVoltDB("Shouldn't happen", false, e);
                    }
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

        return size;
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
                        FastSerializer.writeArray((short[]) obj, buf);
                        break;
                    case INTEGER:
                        FastSerializer.writeArray((int[]) obj, buf);
                        break;
                    case BIGINT:
                        FastSerializer.writeArray((long[]) obj, buf);
                        break;
                    case FLOAT:
                        FastSerializer.writeArray((double[]) obj, buf);
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
                            FastSerializer.writeString(m_encodedStringArrays[i][zz], buf);
                        }
                        break;
                    case TIMESTAMP:
                        FastSerializer.writeArray((TimestampType[]) obj, buf);
                        break;
                    case DECIMAL:
                        // converted long128 in serializer api
                        FastSerializer.writeArray((BigDecimal[]) obj, buf);
                        break;
                    case VOLTTABLE:
                        FastSerializer.writeArray((VoltTable[]) obj, buf);
                        break;
                    case VARBINARY:
                        FastSerializer.writeArray((byte[][]) obj, buf);
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
                    FastSerializer.writeString(m_encodedStrings[i], buf);
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

    /**
     * Used to get a CRC of params on their way to the EE.
     * Assumes scalar values compatible with the EE (i.e. no VoltTables)
     * @param crc
     * @throws IOException
     */
    void addToCRC(PureJavaCrc32C crc) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(8);
        DataOutputStream dos = new DataOutputStream(bos);
        byte[] b;

        for (int i = 0; i < m_params.length; i++) {
            Object obj = m_params[i];

            if ((obj == null) || (obj == JSONObject.NULL)) {
                crc.update(new byte[] { 0 });
                continue;
            }

            Class<?> cls = obj.getClass();

            // Handle NULL mappings not encoded by type.min_value convention
            if (obj == VoltType.NULL_TIMESTAMP) {
                crc.update(new byte[] { 0 });
                continue;
            }
            else if (obj == VoltType.NULL_STRING_OR_VARBINARY) {
                crc.update(new byte[] { 0 });
                continue;
            }
            else if (obj == VoltType.NULL_DECIMAL) {
                crc.update(new byte[] { 0 });
                continue;
            }

            bos.reset();

            VoltType type = VoltType.typeFromClass(cls);
            switch (type) {
                case TINYINT:
                    dos.writeLong((Byte) obj);
                    b = bos.toByteArray();
                    crc.update(b);
                    break;
                case SMALLINT:
                    dos.writeLong((Short) obj);
                    b = bos.toByteArray();
                    crc.update(b);
                    break;
                case INTEGER:
                    dos.writeLong((Integer) obj);
                    b = bos.toByteArray();
                    crc.update(b);
                    break;
                case BIGINT:
                    dos.writeLong((Long) obj);
                    b = bos.toByteArray();
                    crc.update(b);
                    break;
                case FLOAT:
                    if (cls == Float.class) {
                        dos.writeInt(0);
                        dos.writeFloat((Float) obj);
                    }
                    else if (cls == Double.class) {
                        dos.writeDouble((Double) obj);
                    }
                    else {
                        throw new RuntimeException("Can't cast parameter type to Double");
                    }
                    b = bos.toByteArray();
                    crc.update(b);
                    break;
                case STRING:
                    if (m_encodedStrings[i] == null) {
                        // should not happen
                        throw new IOException("String not encoded: " + (String) obj);
                    }
                    crc.update(m_encodedStrings[i]);
                    break;
                case VARBINARY:
                    if (m_params[i] instanceof byte[]) {
                        crc.update((byte[]) m_params[i]);
                    }
                    else if (m_params[i] instanceof byte[]) {
                        for (Byte B : (Byte[]) m_params[i]) {
                            if (B != null) {
                                crc.update(B.byteValue());
                            }
                            else {
                                crc.update(0);
                            }
                        }
                    }
                    else if (m_encodedStrings[i] != null) {
                        crc.update(m_encodedStrings[i]);
                    }
                    else {
                        throw new IOException("Failed to computer CRC of VARBINARY value: " + (String) obj);
                    }
                case TIMESTAMP:
                    long micros = timestampToMicroseconds(obj);
                    dos.writeLong(micros);
                    b = bos.toByteArray();
                    crc.update(b);
                    break;
                case DECIMAL:
                    crc.update(VoltDecimalHelper.getUnscaledBytes((BigDecimal) obj));
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
                // XXX: This may be slightly controversial, but...
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

    public Integer getHashinatedParam(int index) {
        if (m_params.length > 0) {
            return TheHashinator.hashToPartition(m_params[index]);
        }
        return null;
    }
}
