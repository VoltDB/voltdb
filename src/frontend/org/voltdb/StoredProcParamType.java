package org.voltdb;

import java.math.BigDecimal;
import java.util.HashMap;

import org.voltdb.types.TimestampType;

import com.google_voltpatches.common.collect.ImmutableMap;

public enum StoredProcParamType {
    INVALID               (VoltType.INVALID, false, null)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
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
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToByte(param, paramClz);
        }
    },

    /**
     * 2-byte signed 2s-compliment short.
     * Lowest value means NULL in the database.
     */
    SMALLINT              (VoltType.SMALLINT, false, short.class)
    {
        public Object getNullValue() { return VoltType.NULL_SMALLINT; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToShort(param, paramClz);
        }
    },
    SMALLINT_VECTOR       (VoltType.SMALLINT, true, short[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    },

    /**
     * 4-byte signed 2s-compliment integer.
     * Lowest value means NULL in the database.
     */
    INTEGER               (VoltType.INTEGER, false, int.class)
    {
        public Object getNullValue() { return VoltType.NULL_INTEGER; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToInteger(param, paramClz);
        }
    },
    INTEGER_VECTOR        (VoltType.INTEGER, true, int[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    },

    /**
     * 8-byte signed 2s-compliment long.
     * Lowest value means NULL in the database.
     */
    BIGINT                (VoltType.BIGINT, false, long.class)
    {
        public Object getNullValue() { return VoltType.NULL_BIGINT; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToLong(param, paramClz);
        }
    },
    BIGINT_VECTOR         (VoltType.BIGINT, true, long[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    },

    /**
     * 8-bytes in IEEE 754 "double format".
     * Some NaN values may represent NULL in the database (TBD).
     */
    FLOAT                 (VoltType.BIGINT, false, double.class)
    {
        public Object getNullValue() { return VoltType.NULL_FLOAT; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToFloat(param, paramClz);
        }
    },
    FLOAT_VECTOR          (VoltType.BIGINT, true, double[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
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
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToVoltTimestamp(param, paramClz);
        }
    },
    JAVADATESTAMP         (VoltType.TIMESTAMP, false, java.util.Date.class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToJavaDate(param, paramClz);
        }
    },
    SQLDATESTAMP          (VoltType.TIMESTAMP, false, java.sql.Date.class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToSqlDate(param, paramClz);
        }
    },
    SQLTIMESTAMP          (VoltType.TIMESTAMP, false, java.sql.Timestamp.class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToSqlTimestamp(param, paramClz);
        }
    },
    VOLTTIMESTAMP_VECTOR  (VoltType.TIMESTAMP, true, TimestampType[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    },
    JAVADATESTAMP_VECTOR  (VoltType.TIMESTAMP, true, java.util.Date[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    },
    SQLDATESTAMP_VECTOR   (VoltType.TIMESTAMP, true, java.sql.Date[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    },
    SQLTIMESTAMP_VECTOR   (VoltType.TIMESTAMP, true, java.sql.Timestamp[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
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
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToString(param, paramClz);
        }
    },
    STRING_VECTOR         (VoltType.STRING, true, String[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    },

    /**
     * VoltTable type for Procedure parameters
     */
    VOLTTABLE             (VoltType.VOLTTABLE, false, VoltTable.class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToVoltTable(param, paramClz);
        }
    },
    VOLTTABLE_VECTOR      (VoltType.VOLTTABLE, true, VoltTable[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    },

    /**
     * Fixed precision=38, scale=12 storing sign and null-status in a preceding byte
     */
    DECIMAL               (VoltType.DECIMAL, false, BigDecimal.class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToDecimal(param, paramClz);
        }
    },
    DECIMAL_VECTOR        (VoltType.DECIMAL, true, BigDecimal[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    },

    /**
     * Array of bytes of variable length
     */
    VARBINARY             (VoltType.VARBINARY, false, byte[].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToByteArray(param, paramClz);
        }

    },
    VARBINARY_VECTOR      (VoltType.VARBINARY, true, byte[][].class)
    {
        public Object getNullValue() { return null; }
        public Object convertToParamType(final Object param, Class<?> paramClz)
        {
            return ParameterConverter.convertToArray(param, paramClz, this.m_baseClass);
        }
    };

//    SYSTEM_PROC_CONTEXT   (VoltType.INVALID, false, SystemProcedureExecutionContext.class)
//    {
//        public Object getNullValue() { return null; }
//        public Object convertToParamType(final Object param, Class<?> paramClz)
//        {
//            return ParameterConverter.convertToSystemProcContext(param, paramClz, this.m_baseClass);
//        }
//    };

    public abstract Object getNullValue();
    public abstract Object convertToParamType(final Object param, Class<?> paramClz);


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
