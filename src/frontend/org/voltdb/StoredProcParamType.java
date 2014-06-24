/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb;

import java.math.BigDecimal;
import java.util.HashMap;

import org.voltdb.common.Constants;
import org.voltdb.types.TimestampType;

import com.google_voltpatches.common.collect.ImmutableMap;

public enum StoredProcParamType {
    INVALID               (VoltType.INVALID, false, null)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramClz)
        {
            throw new VoltTypeException(
                    "AllowedStoredProcParams: The provided value: (" + param.toString() +
                    ") for invalid Target Parameter.");
        }
    },

    /**
     * 1-byte signed 2s-compliment byte.
     * Lowest value means NULL in the database.
     */
    TINYINT               (VoltType.TINYINT, false, byte.class)
    {
        public Object getNullValue() { return VoltType.NULL_TINYINT; }
        public boolean isNullValue(Object val) { return (Byte)val == VoltType.NULL_TINYINT; }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToByte(param, paramType);
        }
    },

    /**
     * 2-byte signed 2s-compliment short.
     * Lowest value means NULL in the database.
     */
    SMALLINT              (VoltType.SMALLINT, false, short.class)
    {
        public Object getNullValue() { return VoltType.NULL_SMALLINT; }
        public boolean isNullValue(Object val) { return (Short)val == VoltType.NULL_SMALLINT; }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToShort(param, paramType);
        }
    },
    SMALLINT_VECTOR       (VoltType.SMALLINT, true, short[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },

    /**
     * 4-byte signed 2s-compliment integer.
     * Lowest value means NULL in the database.
     */
    INTEGER               (VoltType.INTEGER, false, int.class)
    {
        public Object getNullValue() { return VoltType.NULL_INTEGER; }
        public boolean isNullValue(Object val) { return (Integer)val == VoltType.NULL_INTEGER; }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToInteger(param, paramType);
        }
    },
    INTEGER_VECTOR        (VoltType.INTEGER, true, int[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },

    /**
     * 8-byte signed 2s-compliment long.
     * Lowest value means NULL in the database.
     */
    BIGINT                (VoltType.BIGINT, false, long.class)
    {
        public Object getNullValue() { return VoltType.NULL_BIGINT; }
        public boolean isNullValue(Object val) { return (Long)val == VoltType.NULL_BIGINT; }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToLong(param, paramType);
        }
    },
    BIGINT_VECTOR         (VoltType.BIGINT, true, long[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },

    /**
     * 8-bytes in IEEE 754 "double format".
     * Some NaN values may represent NULL in the database (TBD).
     */
    FLOAT                 (VoltType.BIGINT, false, double.class)
    {
        public Object getNullValue() { return VoltType.NULL_FLOAT; }
        public boolean isNullValue(Object val) { return (Double)val == VoltType.NULL_FLOAT; }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToFloat(param, paramType);
        }
    },
    FLOAT_VECTOR          (VoltType.BIGINT, true, double[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },

    /**
     * 8-byte long value representing microseconds after the epoch.
     * The epoch is Jan. 1 1970 00:00:00 GMT. Negative values represent
     * time before the epoch. This covers roughly 4000BC to 8000AD.
     */
    VOLTTIMESTAMP         (VoltType.TIMESTAMP, false, TimestampType.class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return val == VoltType.NULL_TIMESTAMP;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToVoltTimestamp(param, paramType);
        }
    },
    JAVADATESTAMP         (VoltType.TIMESTAMP, false, java.util.Date.class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return val == VoltType.NULL_TIMESTAMP;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToJavaDate(param, paramType);
        }
    },
    SQLDATESTAMP          (VoltType.TIMESTAMP, false, java.sql.Date.class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return val == VoltType.NULL_TIMESTAMP;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToSqlDate(param, paramType);
        }
    },
    SQLTIMESTAMP          (VoltType.TIMESTAMP, false, java.sql.Timestamp.class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return val == VoltType.NULL_TIMESTAMP;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToSqlTimestamp(param, paramType);
        }
    },
    VOLTTIMESTAMP_VECTOR  (VoltType.TIMESTAMP, true, TimestampType[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },
    JAVADATESTAMP_VECTOR  (VoltType.TIMESTAMP, true, java.util.Date[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },
    SQLDATESTAMP_VECTOR   (VoltType.TIMESTAMP, true, java.sql.Date[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },
    SQLTIMESTAMP_VECTOR   (VoltType.TIMESTAMP, true, java.sql.Timestamp[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },

    /**
     * UTF-8 string with up to 32K chars.
     * The database supports char arrays and varchars
     * but the API uses strings.
     */
    STRING                (VoltType.STRING, false, String.class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return val == VoltType.NULL_STRING_OR_VARBINARY || ((String) val).trim().equals(Constants.CSV_NULL);
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToString(param, paramType);
        }
    },
    STRING_VECTOR         (VoltType.STRING, true, String[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },

    /**
     * VoltTable type for Procedure parameters
     */
    VOLTTABLE             (VoltType.VOLTTABLE, false, VoltTable.class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToVoltTable(param, paramType);
        }
    },
    VOLTTABLE_VECTOR      (VoltType.VOLTTABLE, true, VoltTable[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },

    /**
     * Fixed precision=38, scale=12 storing sign and null-status in a preceding byte
     */
    DECIMAL               (VoltType.DECIMAL, false, BigDecimal.class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToDecimal(param, paramType);
        }
    },
    DECIMAL_VECTOR        (VoltType.DECIMAL, true, BigDecimal[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    },

    /**
     * Array of bytes of variable length
     */
    VARBINARY             (VoltType.VARBINARY, false, byte[].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return val == VoltType.NULL_STRING_OR_VARBINARY;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToByteArray(param, paramType);
        }

    },
    VARBINARY_VECTOR      (VoltType.VARBINARY, true, byte[][].class)
    {
        public Object getNullValue() { return null; }
        public boolean isNullValue(Object val)
        {
            assert(val != null);
            return false;
        }
        public Object convertToParamType(final Object param, final StoredProcParamType paramType)
        {
            return ParameterConverter.convertToArray(param, param.getClass(), this.m_baseClass);
        }
    };

    public abstract Object getNullValue();
    public abstract boolean isNullValue(Object val);
    public abstract Object convertToParamType(final Object param, final StoredProcParamType paramType);


    protected final Class<?> m_baseClass;
    private final VoltType m_voltType;
    private boolean m_isVector;

    private final static ImmutableMap<Class<?>, StoredProcParamType> s_classes;
    static {
        ImmutableMap.Builder<Class<?>, StoredProcParamType> b = ImmutableMap.builder();
        HashMap<Class<?>, StoredProcParamType> validation = new HashMap<Class<?>, StoredProcParamType>();
        for (StoredProcParamType type : values()) {
            // Avoid subtle effects when AllowedStoredProcParams have duplicate m_baseClass entries (java classes),
            // so that the association of a java class with the earlier AllowedStoredProcParams gets obliterated
            // by its association with the later AllowedStoredProcParams.
            // The effects of an assert in the middle of class initialization is surprisingly cryptic,
            // at least when exercised by the "ant junit" suite, so for a SLIGHTLY less cryptic response,
            // throw a generic runtime exception.
            if (type.m_baseClass != null) {
                if (validation.get(type.m_baseClass) != null) {
                    // This message seems to just get buried by the java runtime.
                    throw new RuntimeException("Associate each java class with at most one VoltType.");
                }
                validation.put(type.m_baseClass, type);
                b.put(type.m_baseClass, type);
            }
        }
        s_classes = b.build();
    }

    private StoredProcParamType(VoltType baseType, boolean isVector, Class<?> javaClass)
    {
        m_voltType = baseType;
        m_baseClass = javaClass;
        m_isVector = isVector;
    }

    static public StoredProcParamType typeFromClass(Class<?> rawParamType) {
        StoredProcParamType rslt = s_classes.get(rawParamType);
        if (rslt == null) {
            // Could be a SystemProcedureContext
            return StoredProcParamType.INVALID;
        }
        return rslt;
    }

    static public StoredProcParamType typeFromVoltType(VoltType type, boolean isVector) {
        if (isVector) {
            switch (type) {
                case SMALLINT:
                    return SMALLINT_VECTOR;
                case INTEGER:
                    return INTEGER_VECTOR;
                case BIGINT:
                    return BIGINT_VECTOR;
                case FLOAT:
                    return FLOAT_VECTOR;
                case TIMESTAMP:
                    return VOLTTIMESTAMP_VECTOR;
                case STRING:
                    return STRING_VECTOR;
                case VOLTTABLE:
                    return VOLTTABLE_VECTOR;
                case DECIMAL:
                    return DECIMAL_VECTOR;
                case VARBINARY:
                    return VARBINARY_VECTOR;
                default:
                    return INVALID;
            }
        }
        else {
            return type.getProcParamType();
        }
    }

    public Class<?> classFromType() {
        return m_baseClass;
    }

    public VoltType getVoltType() {
        return m_voltType;
    }

    public boolean isVector() {
        return m_isVector;
    }
}
