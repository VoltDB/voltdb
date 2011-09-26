/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

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

    private final boolean m_serializingToEE;

    public ParameterSet() {
        m_serializingToEE = false;
    }

    public ParameterSet(boolean serializingToEE) {
        m_serializingToEE = serializingToEE;
    }

    static Object limitType(Object o) {
        Class<?> ctype = o.getClass();
        if (ctype == Integer.class) {
            return ((Integer) o).longValue();
        }

        return o;
    }
    Object m_params[] = new Object[0];

    /** Sets the internal array to params. Note: this does *not* copy the argument. */
    public void setParameters(Object... params) {
        this.m_params = params;
    }

    public Object[] toArray() {
        return m_params;
    }

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
                    out.writeTimestamp((TimestampType) obj);
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

    static private Object readOneParameter(FastDeserializer in) throws IOException {
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
                    return in.readTimestamp();
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
}
