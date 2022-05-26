/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.common.Constants;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Represents a type for a {@link VoltTable VoltTable} column or a SQLStmt
 * parameter.
 * Note that types in the database don't map 1-1 with types in the
 * Java Stored Procedure API. For example,
 *   VARBINARY has no equivalent java class -- just byte[].
 *   TIMESTAMP corresponds "best" to VoltDB.TimeStampType but
 *   also, conveniently, to java.sql.Types.TIMESTAMP.
 */
public enum VoltType {
    // This implementation tries to take an 80/20 approach to modeling the
    // behavior of all the specific types. That is, the VoltType class
    // typically provides not-too-complex method implementations that suit
    // the majority of the current and anticipated future types
    // (VoltType enum instances). Yet, specific instances, ideally as few
    // as possible for any given method, will use the java 7 "smart enum"
    // feature to override default method implementations. The intent is that
    // adding new instances will have a minimal and localized impact --
    // requiring few default method implementations to be changed or to be
    // overridden by the new instance.
    // Secondarily, the need for overrides on existing types, especially
    // jdbc-invisible types, numeric types, and variable length types is
    // minimized by making many of the default method implementations
    // sensitive to these general type categories.

    /**
     * Used for uninitialized types in some places. Not a valid value
     * for actual user data.
     */
    INVALID   ((byte)0, new Class[] {}, null),

    /**
     * Used to type java null values that have no type. Not a valid value
     * for actual user data.
     */
    NULL      ((byte)1, new Class[] {}, null),

    /**
     * Used for some literal constants parsed by our SQL parser. Not a
     * valid value for actual user data. See {@link #DECIMAL} for decimal
     * type.
     */
    NUMERIC   ((byte)2, new Class[] {}, null),

    /**
     * 1-byte signed 2s-complement byte.
     * Lowest value means NULL in the database.
     */
    TINYINT   ((byte)3, "tinyint", 1,
            new Class[] {byte.class, Byte.class},
            byte[].class,
            't',
            java.sql.Types.TINYINT,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            "java.lang.Byte" // getObject return type
    ) {
        @Override
        public Byte decodeValue(ByteBuffer buffer) {
            return buffer.get();
        }
    },

    /**
     * 2-byte signed 2s-complement short.
     * Lowest value means NULL in the database.
     */
    SMALLINT  ((byte)4, "smallint", 2,
            new Class[] {short.class, Short.class},
            short[].class,
            's',
            java.sql.Types.SMALLINT,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            "java.lang.Short" // getObject return type
    ) {
        @Override
        public Short decodeValue(ByteBuffer buffer) {
            return buffer.getShort();
        }
    },

    /**
     * 4-byte signed 2s-complement integer.
     * Lowest value means NULL in the database.
     */
    INTEGER   ((byte)5, "integer", 4,
            new Class[] {int.class, Integer.class, AtomicInteger.class},
            int[].class,
            'i',
            java.sql.Types.INTEGER,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            "java.lang.Integer" // getObject return type
    ) {
        @Override
        public Integer decodeValue(ByteBuffer buffer) {
            return buffer.getInt();
        }
    },

    /**
     * 8-byte signed 2s-complement long.
     * Lowest value means NULL in the database.
     */
    BIGINT    ((byte)6, "bigint", 8,
            new Class[] {long.class, Long.class, AtomicLong.class},
            long[].class,
            'b',
            java.sql.Types.BIGINT,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            "java.lang.Long" // getObject return type
    ) {
        @Override
        public Long decodeValue(ByteBuffer buffer) {
            return buffer.getLong();
        }
    },

    /**
     * Special purpose internal type to describe expectations for parameters to
     * statements that contain syntax like " integer_expr IN ? ".
     * This type most commonly occurs as an expected parameter type for
     * such statements.
     * It is not expected to ever be used as a VoltTable column type.
     */
    INLIST_OF_BIGINT ((byte)7, // enum value
            "INLIST OF BIGINT", // unused SQL name
            -1, // variable length
            // Normally, only compatible NON-ARRAY types are listed,
            // but long array is included here as the special case
            // most suitable representation.
            new Class[] {long[].class},
            long[][].class, // unused vector type
            'B', // take-off on 'b' for bigint/long
            java.sql.Types.OTHER, // unused JDBC getObject result type
            java.sql.DatabaseMetaData.typePredNone, // basic where-clauses supported
            "org.voltdb.types.Long[]") // unused JDBC getObject return type
    {
        private final Class<?> COMPATIBLE_ARRAYS[] =
                new Class<?>[] {long[].class, Long[].class, int[].class, Integer[].class,
                short[].class, Short[].class, byte[].class, Byte[].class,};

        @Override
        public boolean acceptsArray(Class<?> arrayArgClass) {
            for (Class<?> allowedArray : COMPATIBLE_ARRAYS) {
                if (allowedArray == arrayArgClass) {
                    return true;
                }
            }
            return false;
        }
    },

    /**
     * 8-bytes in IEEE 754 "double format".
     * Some NaN values may represent NULL in the database (TBD).
     */
    FLOAT     ((byte)8, "float", 8,
            new Class[] {double.class, Double.class, float.class, Float.class},
            double[].class,
            'f',
            java.sql.Types.FLOAT,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            "java.lang.Double" // getObject return type
    ) {
        @Override
        public Double decodeValue(ByteBuffer buffer) {
            return buffer.getDouble();
        }
    },

    /**
     * UTF-8 string with up to 32K chars.
     * The database supports char arrays and varchars
     * but the API uses strings.
     */
    STRING    ((byte)9, "varchar", new LengthRange("max_length"),
            new Class[] {String.class},
            String[].class,
            'v',
            java.sql.Types.VARCHAR,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typeSearchable, // where-clauses supported
            "java.lang.String") // getObject return type
    {
        @Override
        public boolean acceptsArray(Class<?> arrayArgClass) {
            return byte[].class == arrayArgClass;
        }

        @Override
        public boolean isCaseSensitive() { return true; }

        @Override
        public String getLiteralPrefix() { return "'"; }

        @Override
        public String getLiteralSuffix() { return "'"; }

        // Non-numeric and yet indexable.
        @Override
        public boolean isIndexable() { return true; }
        @Override
        public boolean isUniqueIndexable() { return true; }

        @Override
        public String decodeValue(ByteBuffer buffer) {
            int length = buffer.getInt();
            if (length < 0) {
                return null;
            }
            int origLimit = buffer.limit();
            buffer.limit(buffer.position() + length);
            String result = Constants.UTF8ENCODING.decode(buffer).toString();
            buffer.limit(origLimit);
            return result;
        }
    },

    /**
     * Special purpose internal type to describe expectations for parameters to
     * statements that contain syntax like " varchar_expr IN ? ".
     * This type most commonly occurs as an expected parameter type for
     * such statements.
     * It is not expected to ever be used as a VoltTable column type.
     */
    INLIST_OF_STRING ((byte)10, // enum value
            "INLIST OF STRING", // unused SQL name
            -1, // variable length
            // Normally, only compatible NON-ARRAY types are listed,
            // but String array is included here as the special case
            // most suitable representation.
            new Class[] {String[].class},
            String[][].class, // unused vector type
            'V', // take-off on 'v' for varchar/STRING
            java.sql.Types.OTHER, // unused JDBC getObject result type
            java.sql.DatabaseMetaData.typePredNone, // basic where-clauses supported
            "org.voltdb.types.String[]")  // unused getObject return type
    {
        @Override
        public boolean acceptsArray(Class<?> arrayArgClass) {
            return String[].class == arrayArgClass;
        }
    },

    /**
     * 8-byte long value representing microseconds after the epoch.
     * The epoch is Jan. 1 1970 00:00:00 GMT. Negative values represent
     * time before the epoch. This covers roughly 4000BC to 8000AD.
     */
    TIMESTAMP ((byte)11, "timestamp", 8,
            new Class[] {TimestampType.class,
                         java.util.Date.class,
                         java.sql.Date.class,
                         java.sql.Timestamp.class},
            TimestampType[].class,
            'p',
            java.sql.Types.TIMESTAMP,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            "java.sql.Timestamp") // getObject return type
    {
        @Override
        public String getLiteralPrefix() { return "'"; }

        @Override
        public String getLiteralSuffix() { return "'"; }

        // Non-numeric and yet indexable.
        @Override
        public boolean isIndexable() { return true; }
        @Override
        public boolean isUniqueIndexable() { return true; }

        @Override
        public TimestampType decodeValue(ByteBuffer buffer) {
            return new TimestampType(buffer.getLong());
        }
    },

    /**
     * VoltTable type for Procedure parameters
     */
    VOLTTABLE ((byte)21, new Class[] {VoltTable.class}, VoltTable[].class),

    /**
     * Fixed precision=38, scale=12 storing sign and null-status in a preceding byte
     */
    DECIMAL  ((byte)22, "decimal", 16,
            new Class[] {BigDecimal.class},
            BigDecimal[].class,
            'd',
            java.sql.Types.DECIMAL,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            "java.math.BigDecimal") // getObject return type
    {
        @Override
        public Integer getMinimumScale() { return 12; }

        @Override
        public Integer getMaximumScale() { return 12; }

        @Override
        public BigDecimal decodeValue(ByteBuffer buffer) {
            int scale = buffer.get();
            int precisionBytes = buffer.get();
            byte[] bytes = new byte[precisionBytes];
            buffer.get(bytes);
            return new BigDecimal(new BigInteger(bytes), scale);
        }
    },

    /**
     * Boolean type. Not (yet) a valid value for actual user data.
     */
    BOOLEAN   ((byte)23, "boolean", 1,
            new Class[] {boolean.class, Boolean.class},
            boolean[].class,
            'o', //'b' is taken by BIGINT
            java.sql.Types.BOOLEAN,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            "java.lang.Boolean") // getObject return type
    {
        // These MAY not actually be necessary,
        // since BOOLEAN is not really a numeric type?
        @Override
        public Integer getMinimumScale() { return 0; }

        @Override
        public Integer getMaximumScale() { return 0; }
    },

    /**
     * Array of bytes of variable length
     */
    VARBINARY ((byte)25, "varbinary", new LengthRange("max_length"),
            // Normally, only compatible NON-ARRAY types are listed,
            // but byte array is included here as the special case
            // most suitable representation of VARBINARY.
            new Class[] {byte[].class, Byte[].class, ByteBuffer.class},
            byte[][].class,
            'l',
            java.sql.Types.VARBINARY,  // java.sql.Types DATA_TYPE
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            "java.lang.Byte[]")  // getObject return type
    {

        // Streamlined implementation avoids any copying.
        @Override
        public Object bytesToValue(byte[] value) {
            assert(value != null);
            return value;
        }

        @Override
        public boolean acceptsArray(Class<?> arrayArgClass) {
            return byte[].class == arrayArgClass ||
                    Byte[].class == arrayArgClass;
        }

        @Override
        public String getLiteralPrefix() { return "'"; }

        @Override
        public String getLiteralSuffix() { return "'"; }

        // Non-numeric and yet indexable.
        @Override
        public boolean isIndexable() { return true; }
        @Override
        public boolean isUniqueIndexable() { return true; }

        @Override
        public byte[] decodeValue(ByteBuffer buffer) {
            int length = buffer.getInt();
            if (length < 0) {
                return null;
            }
            byte[] result = new byte[length];
            buffer.get(result);
            return result;
        }
    },

    /**
     * Point type, for a geographical point (long, lat)
     */
    GEOGRAPHY_POINT ((byte)26, // enum value
            "GEOGRAPHY_POINT", // SQL name
            GeographyPointValue.getLengthInBytes(),
            new Class[] {GeographyPointValue.class}, // Java types supported in conversion
            GeographyPointValue[].class, // vector type
            'P', // signature char
            java.sql.Types.OTHER, // JDBC type (this is used for vendor specific types)
            java.sql.DatabaseMetaData.typePredBasic, // basic where-clauses supported
            "org.voltdb.types.GeographyPointValue" // JDBC getObject return type
    ) {
        @Override
        public GeographyPointValue decodeValue(ByteBuffer buffer) {
            return GeographyPointValue.unflattenFromBuffer(buffer);
        }
    },

    /**
     * Geography type, for geographical objects (polygons, etc)
     */
    GEOGRAPHY ((byte)27, // enum value
            "GEOGRAPHY", // SQL name
            new LengthRange(GeographyValue.MIN_SERIALIZED_LENGTH,
                    GeographyValue.MAX_SERIALIZED_LENGTH,
                    GeographyValue.DEFAULT_LENGTH,
                    "max_length"), // variable length
            new Class[] {GeographyValue.class}, // Java types supported in conversion
            GeographyValue[].class, // vector type
            'g', // signature char
            java.sql.Types.OTHER, // JDBC type (this is used for vendor specific types)
            java.sql.DatabaseMetaData.typePredBasic, // basic where-clauses supported
            "org.voltdb.types.GeographyValue") // JDBC getObject return type
    {
        /** GEOGRAPHY values ARE indexable within the limitations of the
         * specialized geo indexes, but one of these limitations is no
         * support for uniqueness. */
        @Override
        public boolean isIndexable() { return true; }
        @Override
        public boolean isUniqueIndexable() { return false; }

        @Override
        public GeographyValue decodeValue(ByteBuffer buffer) {
            buffer.getInt(); // Length of value which is not used
            return GeographyValue.unflattenFromBuffer(buffer);
        }
    },
    ;

    // PUBLIC STATIC members

    /** Size in bytes of the maximum length for a VoltDB field value, presumably a
     * <code>STRING</code> or <code>VARBINARY</code> */
    public static final int MAX_VALUE_LENGTH = 1024 * 1024;
    public static final int MAX_VALUE_LENGTH_IN_CHARACTERS = MAX_VALUE_LENGTH / 4;

    /** String representation of <code>MAX_VALUE_LENGTH</code>.
     * @param size The size you want to represent in human readable string.
     * @return String representation of Size passed in. */
    public static String humanReadableSize(int size) {
        if (size > 9999) {
            return size / 1024 + "K";
        }
        return size + "B";
    }

    // INSTANCE members

    /** The unique enum value for this VoltType,
     * allows a compact representation of the VoltType. */
    private final byte m_value;
    /** The value length in bytes if fixed length, otherwise -1 */
    private final int m_lengthInBytes;
    /** The value length range and default in bytes if variable length,
     * and the associated sql type length param name,
     * otherwise null */
    private final LengthRange m_lengthAsBytesRange;
    /** How this type is named in sql,
     * for example, when declaring a column or "casting as". */
    private final String m_sqlString;
    /** Java classes for values (normally not including arrays) that are
     * convertible to this type.
     * The first entry (if any) is considered the best matching Java class
     * and is also used by default in the reverse process when converting
     * FROM SQL column data TO java values. */
    private final Class<?>[] m_classes;
    /** The java array type which is the preferred format for representing
     * multiple values of the type. These make useful procedure parameter
     * types. */
    private final Class<?> m_vectorClass;
    /** Yet another compact unique representation for this VoltType,
     * handier than m_value for concatenating into strings that describe
     * compound type structures like table schema. */
    private final char m_signatureChar;

    /** Is this type visible to JDBC?
     * If false, the other m_jdbc* members are ignored. */
    private final boolean m_jdbcVisible;

    // JDBC getTypeInfo values
    // If we add yet more stuff to this for MySQL or ODBC or something,
    // it might be time to consider some sort of data-driven type specification
    // mechanism.

    /** Either a typical SQL column type well known to jdbc or the special
     * OTHER value for VoltDB's non-standard vendor-specific column types. */
    private final int m_jdbcSqlDataType;
    /** It's unclear (at least to Paul) how the jdbc users/systems/tools
     * makes use of this jdbc standard attribute. */
    private final int m_jdbcSearchable;
    /** This is the type that will be returned by JDBC's ResultSet.getObject(),
     * which usually corresponds to to VoltTable.get(), except for timestamps. */
    private final String m_jdbcClass;

    // CONSTRUCTORS

    /** Constructor for non-JDBC-visible types.
     * This can safely stub out any attributes that are only used by jdbc. */
    VoltType(byte value, Class<?>[] classes, Class<?> vectorClass) {
        this(value, -1, null, null, classes, vectorClass, '0',
                false,
                // With m_jdbcVisible set false, these remaining attributes
                // related to JDBC are just stubbed out with
                // "don't care" values. They are actually N/A.
                java.sql.Types.OTHER, Integer.MIN_VALUE, null);
    }

    /** Constructor for JDBC-visible types.  Only types constructed in this way
     * appear in the JDBC getTypeInfo() metadata.
     * Note that this includes the standard JDBC SQL types as well as VoltDB's
     * vendor-specific types that are exposed as JDBC extensions.
     * The latter specify a jdbcSqlDataType of OTHER and typically
     * name a VoltDB defined class as their jdbcClass value. */
    VoltType(byte value,
             String sqlString,
             int lengthInBytes,
             Class<?>[] classes,
             Class<?> vectorClass,
             char signatureChar,
             int jdbcSqlDataType,
             int jdbcSearchable,
             String jdbcClass) {
        this(value, lengthInBytes, null, sqlString,
                classes, vectorClass, signatureChar,
                true,
                jdbcSqlDataType, jdbcSearchable, jdbcClass);
    }

    /** Constructor for JDBC-visible types.  Only types constructed in this way
     * appear in the JDBC getTypeInfo() metadata.
     * Note that this includes the standard JDBC SQL types as well as VoltDB's
     * vendor-specific types that are exposed as JDBC extensions.
     * The latter specify a jdbcSqlDataType of OTHER and typically
     * name a VoltDB defined class as their jdbcClass value. */
    VoltType(byte value,
             String sqlString,
             LengthRange lengthAsBytesRange,
             Class<?>[] classes,
             Class<?> vectorClass,
             char signatureChar,
             int jdbcSqlDataType,
             int jdbcSearchable,
             String jdbcClass) {
        this(value, -1, lengthAsBytesRange, sqlString,
                classes, vectorClass, signatureChar,
                true,
                jdbcSqlDataType, jdbcSearchable, jdbcClass);
    }

    /** Common constructor implementation for JDBC-visible and JDBC-invisible types.
     * This common code should ALWAYS be called and should ONLY be called through
     * the other special-case constructors defined above. */
    VoltType(byte value,
             int lengthInBytes,
             LengthRange lengthAsBytesRange,
             String sqlString,
             Class<?>[] classes,
             Class<?> vectorClass,
             char signatureChar,
             boolean jdbcVisible,
             int jdbcSqlDataType,
             int jdbcSearchable,
             String jdbcClass) {
        if (value > VOLT_TYPE_MAX_ENUM) {
            // The last time Paul checked (admittedly back in Java 7), a
            // RuntimeException gave better diagnostics than an assert when a
            // programmer error caused an unrecoverable issue during class
            // initialization.
            // So, throw one here and hope it shows up somewhere in the noisy
            // traces reported by the jvm.
            throw new RuntimeException("The VoltType enum byte value " + value +
                    "falls outside the expected range. Consider reassigning " +
                    "the value to fill a gap within the existing range OR " +
                    "extending the range from its current value of " +
                    "VOLT_TYPE_MAX_ENUM = " + VOLT_TYPE_MAX_ENUM);
        }
        m_value = value;
        m_lengthInBytes = lengthInBytes;
        m_lengthAsBytesRange = lengthAsBytesRange;
        m_sqlString = sqlString;
        m_classes = classes;
        m_vectorClass = vectorClass;
        m_signatureChar = signatureChar;

        m_jdbcVisible = jdbcVisible;

        m_jdbcSqlDataType = jdbcSqlDataType;
        m_jdbcSearchable = jdbcSearchable;
        m_jdbcClass = jdbcClass;
    }

    /** Support class to represent optional value length variability. */
    public static final class LengthRange {
        private final int m_min;
        private final int m_max;
        private final int m_default;
        private final String m_name;

        /** The simplest variable length type model matches the usage of
         * STRING and VARBINARY. */
        LengthRange(String maxLengthParamName) {
            this(1, MAX_VALUE_LENGTH, DEFAULT_COLUMN_SIZE, maxLengthParamName);
        }

        /** The variable length type model can also be customized to meet
         * the needs of new classes with constraints different from
         * STRING and VARBINARY, for example GEOGRAPHY. */
        LengthRange(int minBytes, int maxBytes, int defaultLength,
                String maxLengthParamName) {
            m_min = minBytes;
            m_max = maxBytes;
            m_default = defaultLength;
            m_name = maxLengthParamName;
        }

        // These constants should be kept up-to-date with those in DDLCompiler.
        // Don't reference DDLCompiler here since this class is used in the client.
        private static final int MAX_COLUMNS = 1024;
        private static final int MAX_ROW_SIZE = 1024 * 1024 * 2;

        public static final int DEFAULT_COLUMN_SIZE = MAX_ROW_SIZE / MAX_COLUMNS;

        int getMaxLengthInBytes() { return m_max; }

        int getMinLengthInBytes() { return m_min; }

        int getDefaultLengthInBytes() { return m_default; }

        String getLengthParamName() { return m_name; }

    };

    // PRIVATE STATIC members:

    private final static ImmutableMap<Class<?>, VoltType> s_classes;
    /** The maximum byte value that is allotted to a VoltType instance.
     * Update this MAX if you MUST add a VoltType byte value beyond the
     * current range -- instead of filling gaps in the range.
     * This MAX allows s_types, the byte-value-to-VoltType-object "map" to be
     * implemented with a simple (small, dense) VoltType array.
     * There's a hard ceiling of 127 imposed by the use of signed bytes
     * as unique keys/indexes. */
    private final static byte VOLT_TYPE_MAX_ENUM = 30;
    private final static VoltType[] s_types =
            new VoltType[(VOLT_TYPE_MAX_ENUM)+1];
    static {
        ImmutableMap.Builder<Class<?>, VoltType> b = ImmutableMap.builder();
        HashMap<Class<?>, VoltType> validation = new HashMap<>();
        for (VoltType type : values()) {
            s_types[type.m_value] = type;
            for (Class<?> cls : type.m_classes) {
                // Avoid subtle effects when VoltTypes have duplicate m_classes entries (java classes),
                // so that the association of a java class with the earlier VoltType gets obliterated
                // by its association with the later VoltType.
                // The effects of an assert in the middle of class initialization is surprisingly cryptic,
                // at least when exercised by the "ant junit" suite, so for a SLIGHTLY less cryptic response,
                // throw a generic runtime exception.
                // assert(s_classes.get(cls) == null);
                if (validation.get(cls) != null) {
                    // This message seems to just get buried by the java runtime.
                    throw new RuntimeException("Associate each java class like " +
                            cls.getSimpleName() + " with at most one VoltType.");
                }
                validation.put(cls, type);
                b.put(cls, type);
            }
        }
        s_classes = b.build();
    }

    // PUBLIC ACCESSORS and other instance methods and static conversion methods

    /** Gets the byte that corresponds to the VoltType (for serialization).
     * @return A byte representing the VoltType */
    public byte getValue() { return m_value; }

    /** Return the java class that is matched to a given <tt>VoltType</tt>.
     * @return A java class object.
     * @throws RuntimeException if a type doesn't have an associated class,
     * such as {@link #INVALID}.
     * @see #typeFromClass */
    public Class<?> classFromType() {
        if (m_classes.length == 0) {
            throw new RuntimeException("Unsupported type " + this);
        }
        return m_classes[0];
    }

    /**
     * Return the java class that is matched to the given value.
     *
     * @param value of a VoltType as returned by {@link #getValue()}
     * @return The java class.
     */
    public static Class<?> classFromByteValue(byte value) {
        VoltType returnVT = VoltType.get(value);
        return returnVT.classFromType();
    }

    /** Return the java class that is matched to a given <tt>VoltType</tt>.
     * @return A java class object.
     * @throws RuntimeException if a type doesn't have an associated class,
     * such as {@link #INVALID}.
     * @see #typeFromClass */
    public Class<?> vectorClassFromType() {
        if (m_vectorClass == null) {
            throw new RuntimeException("Unsupported type " + this);
        }
        return m_vectorClass;
    }

    /** Statically create an enum value from the corresponding byte.
     * @param val A byte representing an enum value
     * @return The appropriate enum value */
    public static VoltType get(byte val) {
        VoltType type = (val < s_types.length) ? s_types[val] : null;
        if (type == null) {
            throw new AssertionError("Unknown type: " + String.valueOf(val));
        }
        return type;
    }

    private boolean matchesString(String str) {
        return str.toUpperCase().endsWith(name());
    }

    /** Converts string representations to an enum value.
     * @param str A string in the form "TYPENAME" or "VoltType.TYPENAME",
     * e.g. "BIGINT" or "VoltType.VARCHAR"
     * @return One of the valid instances of VoltType */
    public static VoltType typeFromString(String str) {
        if (str == null) {
            return NULL;
        }

        if (str.startsWith("VoltType.")) {
            str = str.substring("VoltType.".length());
        }

        if (str.compareToIgnoreCase("null") == 0) {
            return NULL;
        }

        for (VoltType type: values()) {
            if (type.matchesString(str)) {
                return type;
            }
        }

        if (str.equalsIgnoreCase("DOUBLE")) {
            return FLOAT;
        }

        if (str.equalsIgnoreCase("CHARACTER") ||
                str.equalsIgnoreCase("CHAR") ||
                str.equalsIgnoreCase("VARCHAR")) {
            return STRING;
        }

        if (str.equalsIgnoreCase("BINARY")) {
            return VoltType.VARBINARY;
        }

        throw new RuntimeException("Can't find type: " + str);
    }

    /** Ascertain the most appropriate <tt>VoltType</tt> given a
     * java object.
     * @param obj The java object to type.
     * @return A <tt>VoltType</tt>.
     * @throws VoltTypeException if none applies.
     * @see #typeFromClass */
    public static VoltType typeFromObject(Object obj) {
        assert obj != null;

        Class<?> cls = obj.getClass();
        return typeFromClass(cls);
    }

    /** Ascertain the most appropriate <tt>VoltType</tt> given a
     * java class.
     * @param cls The java class to type.
     * @return A <tt>VoltType</tt>.
     * @throws VoltTypeException if none applies.
     * @see #typeFromObject
     * @see #classFromType */
    public static VoltType typeFromClass(Class<?> cls) {
        VoltType type = s_classes.get(cls);
        if (type == null) {
            // Deal with private HeapByteBuffer.class and DirectByteBuffer.class (subclass of ByteBuffer.class)
            if (cls != null && ByteBuffer.class.isAssignableFrom(cls)) {
                type = VoltType.VARBINARY;
            } else {
                throw new VoltTypeException("Unimplemented Object Type: " + cls);
            }
        }
        return type;
    }

    /** Return the string representation of this type. Note that
     * <tt>VoltType.typeFromString(voltTypeInstance.toString) == voltTypeInstance</tt>.
     * @return The string representation of this type. */
    @Override public String toString() {
        return "VoltType." + name();
    }

    public String getName() { return name(); }

    public boolean isVariableLength() { return m_lengthAsBytesRange != null; }

    /** Get the number of bytes required to store the fixed length type.
     * Variable-length types should throw a RuntimeException.
     * @return An integer value representing a number of bytes. */
    public int getLengthInBytesForFixedTypes() {
        if (m_lengthAsBytesRange != null) {
            throw new RuntimeException(
                    "Asking for fixed size for non-fixed or unknown type:" + m_sqlString);
        }
        return m_lengthInBytes;
    }

    // Variable-length types simply return -1.
    public int getLengthInBytesForFixedTypesWithoutCheck() { return m_lengthInBytes; }

    /** Get the minimum number of bytes required to store the type
     * @return An integer value representing a number of bytes. */
    public int getMinLengthInBytes() {
        return m_lengthAsBytesRange == null ?
                m_lengthInBytes :
                    m_lengthAsBytesRange.getMinLengthInBytes();
    }

    /** Get the maximum number of bytes required to store the type
     * @return An integer value representing a number of bytes. */
    public int getMaxLengthInBytes() {
        return m_lengthAsBytesRange == null ?
                m_lengthInBytes :
                    m_lengthAsBytesRange.getMaxLengthInBytes();
    }

    // JDBC getTypeInfo() accessors

    /** For JDBC, returns the prefix (if any, otherwise null)
     * used with SQL literal constants of this type.
     * Individual VoltTypes can override to enable this,
     * typically to return a single quote.
     * @return null, or, if overridden for a type,
     * the prefix string. */
    public String getLiteralPrefix() { return null; }

    /** For JDBC, returns the suffix (if any, otherwise null)
     * used with SQL literal constants of this type.
     * Individual VoltTypes can override to enable this,
     * typically to return a single quote.
     * @return null, or, if overridden for a type,
     * the suffix string */
    public String getLiteralSuffix() { return null; }

    /** For JDBC, the name(s) of any type-specific parameter(s),
     * e.g. "max_length" used when defining sql columns of this type.
     * FUTURE?: It's not clear what format the JDBC would expect if there
     * were more than one -- maybe comma separated?
     * @return null for fixed-length types,
     * usually "max_length" for variable length type */
    public String getCreateParams() {
        return m_lengthAsBytesRange == null ?
                null :
                    m_lengthAsBytesRange.getLengthParamName();
    }

    /** Individual VoltTypes like String can override to enable this functionality.
     * Normally, other types ignore case when expressed as strings, like in
     * hex values for varbinary, or wkt values for geo types.
     * @return false unless overridden for a case sensitivite type like String */
    public boolean isCaseSensitive() { return false; }

    /** Non-integer numeric VoltTypes must override this method.
     * @return 0 for integer types, null for non-numeric, or some
     * other value if overridden for a specific type like DECIMAL */
    public Integer getMinimumScale() {
        return isAnyIntegerType() ? (Integer)0 : null;
    }

    /** Non-integer numeric VoltTypes must override this method.
     * @return 0 for integer types, null for non-numeric, or some
     * other value if overridden for a specific type like DECIMAL */
    public Integer getMaximumScale() {
        return isAnyIntegerType() ? (Integer)0 : null;
    }

    /** VoltTypes for indexable non-numeric values must override.
     * @return true if the type is supported by VoltDB indexes */
    public boolean isIndexable() { return isNumber(); }

    /** VoltTypes with special restrictions about uniqueness support must override.
     * @return true if the type is supported by VoltDB (assume)unique indexes */
    public boolean isUniqueIndexable() { return isNumber(); }

    /** Most VoltTypes are not compatible with an array-typed value.
     * @see #VARBINARY
     * @param arrayArgClass a java array class like byte[] or String[]
     * @return false, unless overridden to enable a specific VoltType
     * (like VARBINARY) to support certain specific array types (like byte[]). */
    public boolean acceptsArray(Class<?> arrayArgClass) { return false; }

    /** Get the corresponding SQL type as for a given <tt>VoltType</tt> enum.
     * For example, {@link #STRING} will probably convert to "VARCHAR".
     * @return A string representing the SQL type. */
    public String toSQLString() { return m_sqlString; }

    /** <p>Is this type visible to JDBC</p>
     * @return JDBC visibility */
    public boolean isJdbcVisible() { return m_jdbcVisible; }

    /** Get the java.sql.Types type of this type.
     * @return int representing SQL type of the VoltDB type. */
    public int getJdbcSqlType() { return m_jdbcSqlDataType; }

    /** VoltDB treats nullability as orthogonal to type,
     * so all types are nullable.
     * @return the jdbc constant representing a nullable type */
    public int getNullable() {
        return java.sql.DatabaseMetaData.typeNullable;
    }

    public int getSearchable() { return m_jdbcSearchable; }

    /** Numeric types are all signed types, so return false.
     * isUnsigned is N/A to other types, so return null.
     * If/when we support unsigned types, their VoltTypes
     * should override this function to return true.
     * @return null for non-numeric types, false for numeric,
     * unless overridden by a (hypothetical) unsigned numeric type */
    public Boolean isUnsigned() {
        return isNumber() ? (Boolean)false : null;
    }

    public String getJdbcClass() {
        return m_jdbcClass;
    }

    /** Is the type a number and is it an exact value (no rounding errors)?
     * @return true for integers and decimals. False for floats and strings
     * and anything else */
    public boolean isExactNumeric() {
        return isAnyIntegerType() || this == DECIMAL;
    }

    /** Is this type an integer type in the EE?
     * True for <code>TINYINT</code>, <code>SMALLINT</code>,
     * <code>INTEGER</code>, <code>BIGINT</code> and <code>TIMESTAMP</code>.
     * @return True if integer type. False if not */
    public boolean isBackendIntegerType() {
        return isAnyIntegerType() || this == TIMESTAMP;
    }

    /** Is this type an integer type? True for <code>TINYINT</code>, <code>SMALLINT</code>,
     * <code>INTEGER</code>, <code>BIGINT</code>.
     * @return True if integer type. False if not */
    public boolean isAnyIntegerType() {
        switch (this)  {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
            return true;
        default:
            return false;
        }
    }

    public boolean isNumber() {
        switch (this) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }

    /* Indicate whether a value can be assigned to this type without loss of
     * range or precision, important for index key and partition key
     * initialization */
    public boolean canExactlyRepresentAnyValueOf(VoltType otherType) {
        // self to self conversion is obviously fine.
        if (this == otherType) {
            return true;
        }

        if (otherType.isBackendIntegerType()) {
            if (this.isBackendIntegerType()) {
                // Don't allow integers getting smaller.
                return this.getMaxLengthInBytes() >= otherType.getMaxLengthInBytes();
            }
            else if (this == VoltType.FLOAT) {
                // Non-big integers make acceptable (exact) floats
                if (otherType != VoltType.BIGINT) {
                    return true;
                }
            }
            // Not sure about integer-to-decimal: for now, just give up.
        }
        return false;
    }

    /** Get a char that uniquely identifies a type.
     * Used to create concise schema signatures.
     * @return A char representing the type. */
    public char getSignatureChar() {
        // This should never be called for an incomplete or invalid VoltType.
        assert(m_signatureChar != '0');
        return m_signatureChar;
    }

    // Integer[0] is the column size and Integer[1] is the radix
    // I'd love to get this magic into the type construction, but
    // not happening this go-round.  --izzy
    public Integer[] getTypePrecisionAndRadix()
    {
        Integer[] col_size_radix = {null, null};
        switch (this) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case TIMESTAMP:
            col_size_radix[0] = (getLengthInBytesForFixedTypes() * 8) - 1;
            col_size_radix[1] = 2;
            break;
        case FLOAT:
            col_size_radix[0] = 53;  // magic for double
            col_size_radix[1] = 2;
            break;
        case DECIMAL:
            col_size_radix[0] = VoltDecimalHelper.kDefaultPrecision;
            col_size_radix[1] = 10;
            break;
        case STRING:
        case VARBINARY:
        case GEOGRAPHY:
            col_size_radix[0] = VoltType.MAX_VALUE_LENGTH;
            break;
        default:
            // What's the right behavior here?
        }
        return col_size_radix;
    }

    /** The size specifier for columns with a variable-length type is optional in a
     * CREATE TABLE or ALTER TABLE statement.  If no size is specified, VoltDB chooses
     * a default size.
     * @return the default size for the given type */
    public int defaultLengthForVariableLengthType() {
        assert(m_lengthAsBytesRange != null);
        return m_lengthAsBytesRange.getDefaultLengthInBytes();
    }

    public String getMostCompatibleJavaTypeName() {
        if (m_classes.length > 0) {
            Class<?> javaClass = m_classes[0];
            return javaClass.getSimpleName();
        }
        return "(unknown?)";
    }

    // OTHER METHODS that are as much about specific VALUES as about their TYPES

    public String getMaxValueForKeyPadding() {
        switch (this) {
        case TINYINT: return MAX_TINYINT.toString();
        case SMALLINT: return MAX_SMALLINT.toString();
        case INTEGER: return MAX_INTEGER.toString();
        case BIGINT: return MAX_BIGINT.toString();
        case TIMESTAMP: return MAX_TIMESTAMP.toString();
        case FLOAT: return MAX_FLOAT.toString();
        default: return null;
        }
    }

    // Really hacky cast overflow detection for primitive types
    // Comparison to MIN_VALUEs are <= to avoid collisions with the NULL
    // bit pattern
    // Probably eventually want a generic wouldCastDiscardInfo() call or
    // something
    boolean wouldCastOverflow(Number value) {
        switch (this) {
        case TINYINT:
            return (value.longValue() <= Byte.MIN_VALUE ||
                    value.longValue() > Byte.MAX_VALUE);
        case SMALLINT:
            return (value.longValue() <= Short.MIN_VALUE ||
                    value.longValue() > Short.MAX_VALUE);
        case INTEGER:
            return (value.longValue() <= Integer.MIN_VALUE ||
                    value.longValue() > Integer.MAX_VALUE);
        case BIGINT:
            // overflow isn't detectable for Longs, just look for NULL value
            // In practice, I believe that we should never get here in VoltTable
            // since we check for NULL before checking for cast overflow
            return (value.longValue() == NULL_BIGINT);
        case FLOAT:
            // this really should never occur, also, just look for NULL
            // In practice, I believe that we should never get here in VoltTable
            // since we check for NULL before checking for cast overflow
            return (value.doubleValue() == NULL_FLOAT);
        default:
            throw new VoltTypeException("Unhandled cast overflow case, " +
                                        "casting to: " + toString());
        }
    }

    /** Get a value representing whichever null value is appropriate for
     * the current <tt>VoltType</tt> enum. For example, if this type is
     * {@link #TINYINT}, this will return a java <tt>byte</tt> with value
     * -128, which is the constant NULL_TINYINT in VoltDB.
     * @return A new final instance with value equal to null for a given
     * type. */
    public Object getNullValue() {
        switch (this) {
        case TINYINT:
            return NULL_TINYINT;
        case SMALLINT:
            return NULL_SMALLINT;
        case INTEGER:
            return NULL_INTEGER;
        case BIGINT:
            return NULL_BIGINT;
        case FLOAT:
            return NULL_FLOAT;
        case STRING:
            return NULL_STRING_OR_VARBINARY;
        case TIMESTAMP:
            return NULL_TIMESTAMP;
        case DECIMAL:
            return NULL_DECIMAL;
        case VARBINARY:
            return NULL_STRING_OR_VARBINARY;
        case GEOGRAPHY_POINT:
            return NULL_POINT;
        case GEOGRAPHY:
            return NULL_GEOGRAPHY;
        default:
            throw new VoltTypeException("No NULL value for " + toString());
        }
    }

    public static boolean isVoltNullValue(Object obj)
    {
        if ((obj == null) ||
            (obj == VoltType.NULL_TIMESTAMP) ||
            (obj == VoltType.NULL_STRING_OR_VARBINARY) ||
            (obj == VoltType.NULL_DECIMAL) ||
            (obj == VoltType.NULL_POINT) ||
            (obj == VoltType.NULL_GEOGRAPHY)) {
            return true;
        }

        switch (typeFromObject(obj)) {
        case TINYINT:
            return (((Number) obj).byteValue() == NULL_TINYINT);
        case SMALLINT:
            return (((Number) obj).shortValue() == NULL_SMALLINT);
        case INTEGER:
            return (((Number) obj).intValue() == NULL_INTEGER);
        case BIGINT:
            return (((Number) obj).longValue() == NULL_BIGINT);
        case FLOAT:
            return (((Number) obj).doubleValue() == NULL_FLOAT);
        case TIMESTAMP:
        case STRING:
        case VARBINARY:
        case DECIMAL:
        case GEOGRAPHY_POINT:
        case GEOGRAPHY:
            // already checked these above
            return false;
        default:
            throw new VoltTypeException("Unsupported type: " +
                                        typeFromObject(obj));
        }
    }

    /** Converts the object into bytes for hashing.
     * @param obj a value to be hashed
     * @return a byte array representation of obj
     * OR null if the obj is java null or any other Volt representation
     * of a null value. */
    public static byte[] valueToBytes(Object obj) {
        if (isVoltNullValue(obj)) {
            return null;
        }
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        if (obj instanceof String ) {
            return ((String) obj).getBytes(Constants.UTF8ENCODING);
        }

        long value = 0;
        if (obj instanceof Long) {
            value = (Long) obj;
        }
        else if (obj instanceof Integer) {
            value = (Integer) obj;
        }
        else if (obj instanceof Short) {
            value = (Short) obj;
        }
        else if (obj instanceof Byte) {
            value = (Byte) obj;
        }

        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(value);
        return buf.array();
    }

    /** Converts a byte array with type back to the original partition value.
     * This is the inverse of @see VoltType#valueToBytes(Object) valueToBytes
     * @param value Byte array representation of partition parameter.
     * @return Java object of the correct type. */
    public Object bytesToValue(byte[] value) {
        assert(value != null);
        if ((this == NULL)) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.wrap(value);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        switch (this) {
        case BIGINT:
            return buf.getLong();
        case STRING:
            return new String(value, Constants.UTF8ENCODING);
        case INTEGER:
            return buf.getInt();
        case SMALLINT:
            return buf.getShort();
        case TINYINT:
            return buf.get();
        default:
            throw new RuntimeException(
                    "bytesToValue failed to convert a non-partitionable type.");
        }
    }

    /**
     * Decode a on object of this type from {@code buffer}
     *
     * @param buffer to read object from
     * @return Decoded object
     * @throws UnsupportedOperationException if this type does not support decoding
     */
    public Object decodeValue(ByteBuffer buffer) throws UnsupportedOperationException {
        throw new UnsupportedOperationException(name() + " does not supporte decoding");
    }

    // VALUE constants

    /** Length value for a null string. */
    public static final int NULL_STRING_LENGTH = -1;
    /** Null value for <code>TINYINT</code>. */
    public static final byte NULL_TINYINT = Byte.MIN_VALUE;
    /** Null value for <code>SMALLINT</code>. */
    public static final short NULL_SMALLINT = Short.MIN_VALUE;
    /** Null value for <code>INTEGER</code>. */
    public static final int NULL_INTEGER = Integer.MIN_VALUE;
    /** Null value for <code>BIGINT</code>. */
    public static final long NULL_BIGINT = Long.MIN_VALUE;
    /** Null value for <code>FLOAT</code>. */
    public static final double NULL_FLOAT = -1.7E+308;

    /** Max value for a <code>TINYINT</code> index component. */
    private static final Byte MAX_TINYINT = Byte.MAX_VALUE;
    /** Max value for a <code>SMALLINT</code> index component. */
    private static final Short MAX_SMALLINT = Short.MAX_VALUE;
    /** Max value for a <code>INTEGER</code> index component.  */
    private static final Integer MAX_INTEGER = Integer.MAX_VALUE;
    /** Max value for a <code>BIGINT</code> index component. */
    private static final Long MAX_BIGINT = Long.MAX_VALUE;
    /** Max value for a <code>TIMESTAMP</code> index component. */
    private static final Long MAX_TIMESTAMP = Long.MAX_VALUE;
    /** Max value for a <code>FLOAT</code> index component. */
    private static final Float MAX_FLOAT = Float.MAX_VALUE;

    // for consistency at the API level, provide symbolic nulls for these types, too
    private static final class NullTimestampSigil{}
    /** Null value for <code>TIMESTAMP</code>. */
    public static final NullTimestampSigil NULL_TIMESTAMP = new NullTimestampSigil();

    private static final class NullStringOrVarbinarySigil{}
    /** Null value for <code>STRING</code> or <code>VARBINARY</code>. */
    public static final NullStringOrVarbinarySigil NULL_STRING_OR_VARBINARY = new NullStringOrVarbinarySigil();

    private static final class NullDecimalSigil{}
    /** Null value for <code>DECIMAL</code>. */
    public static final NullDecimalSigil NULL_DECIMAL = new NullDecimalSigil();

    private static final class NullPointSigil{}
    /** Null value for <code>GEOGRAPHY_POINT</code>. */
    public static final NullPointSigil NULL_POINT = new NullPointSigil();

    private static final class NullGeographySigil{}
    /** Null value for <code>GEOGRAPHY</code>. */
    public static final NullGeographySigil NULL_GEOGRAPHY = new NullGeographySigil();

}
