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
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.collect.ImmutableMap;


/**
 * Represents a type in a VoltDB Stored Procedure or {@link VoltTable VoltTable}.
 * Note that types in the database don't map 1-1 with types in the
 * Stored Procedure API. Varchars, Chars and Char Arrays in the DB
 * map to Strings in the API.
 */
public enum VoltType {

    /**
     * Used for uninitialized types in some places. Not a valid value
     * for actual user data.
     */
    INVALID   ((byte)0, -1, null, new Class[] {}, null, '0'),

    /**
     * Used to type java null values that have no type. Not a valid value
     * for actual user data.
     */
    NULL      ((byte)1, -1, null, new Class[] {}, null, '0'),

    /**
     * Used for some literal constants parsed by our SQL parser. Not a
     * valid value for actual user data. See {@link #DECIMAL} for decimal
     * type.
     */
    NUMERIC   ((byte)2, 0, null, new Class[] {}, null, '0'),

    /**
     * 1-byte signed 2s-compliment byte.
     * Lowest value means NULL in the database.
     */
    TINYINT   ((byte)3, 1, "tinyint", new Class[] {byte.class, Byte.class}, byte[].class, 't',
            java.sql.Types.TINYINT,  // java.sql.Types DATA_TYPE
            null, // prefix to specify a literal
            null, // suffix to specify a literal
            null, // necessary params to create
            false, // case-sensitive
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            false, // unsigned?
            0, // minimum scale
            0, // maximum scale
            "java.lang.Byte"), // getObject return type

    /**
     * 2-byte signed 2s-compliment short.
     * Lowest value means NULL in the database.
     */
    SMALLINT  ((byte)4, 2, "smallint", new Class[] {short.class, Short.class}, short[].class, 's',
            java.sql.Types.SMALLINT,  // java.sql.Types DATA_TYPE
            null, // prefix to specify a literal
            null, // suffix to specify a literal
            null, // necessary params to create
            false, // case-sensitive
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            false, // unsigned?
            0, // minimum scale
            0, // maximum scale
            "java.lang.Short"), // getObject return type

    /**
     * 4-byte signed 2s-compliment integer.
     * Lowest value means NULL in the database.
     */
    INTEGER   ((byte)5, 4, "integer",
               new Class[] {int.class, Integer.class, AtomicInteger.class}, int[].class, 'i',
            java.sql.Types.INTEGER,  // java.sql.Types DATA_TYPE
            null, // prefix to specify a literal
            null, // suffix to specify a literal
            null, // necessary params to create
            false, // case-sensitive
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            false, // unsigned?
            0, // minimum scale
            0, // maximum scale
            "java.lang.Integer"), // getObject return type

    /**
     * 8-byte signed 2s-compliment long.
     * Lowest value means NULL in the database.
     */
    BIGINT    ((byte)6, 8, "bigint",
               new Class[] {long.class, Long.class, AtomicLong.class}, long[].class, 'b',
            java.sql.Types.BIGINT,  // java.sql.Types DATA_TYPE
            null, // prefix to specify a literal
            null, // suffix to specify a literal
            null, // necessary params to create
            false, // case-sensitive
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            false, // unsigned?
            0, // minimum scale
            0, // maximum scale
            "java.lang.Long"), // getObject return type

    /**
     * 8-bytes in IEEE 754 "double format".
     * Some NaN values may represent NULL in the database (TBD).
     */
    FLOAT     ((byte)8, 8, "float",
            new Class[] {double.class, Double.class, float.class, Float.class}, double[].class, 'f',
            java.sql.Types.FLOAT,  // java.sql.Types DATA_TYPE
            null, // prefix to specify a literal
            null, // suffix to specify a literal
            null, // necessary params to create
            false, // case-sensitive
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            false, // unsigned?
            null, // minimum scale
            null, // maximum scale
            "java.lang.Double"), // getObject return type

    /**
     * 8-byte long value representing microseconds after the epoch.
     * The epoch is Jan. 1 1970 00:00:00 GMT. Negative values represent
     * time before the epoch. This covers roughly 4000BC to 8000AD.
     */
    TIMESTAMP ((byte)11, 8, "timestamp",
            new Class[] {TimestampType.class,
                         java.util.Date.class,
                         java.sql.Date.class,
                         java.sql.Timestamp.class}, TimestampType[].class, 'p',
            java.sql.Types.TIMESTAMP,  // java.sql.Types DATA_TYPE
            "'", // prefix to specify a literal
            "'", // suffix to specify a literal
            null, // necessary params to create
            false, // case-sensitive
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            null, // unsigned?
            null, // minimum scale
            null, // maximum scale
            "java.sql.Timestamp"), // getObject return type

    /**
     * UTF-8 string with up to 32K chars.
     * The database supports char arrays and varchars
     * but the API uses strings.
     */
    STRING    ((byte)9, -1, "varchar", new Class[] {String.class}, String[].class, 'v',
            java.sql.Types.VARCHAR,  // java.sql.Types DATA_TYPE
            "'", // prefix to specify a literal
            "'", // suffix to specify a literal
            "max_length", // necessary params to create
            true, // case-sensitive
            java.sql.DatabaseMetaData.typeSearchable, // where-clauses supported
            null, // unsigned?
            null, // minimum scale
            null, // maximum scale
            "java.lang.String"), // getObject return type

    /**
     * VoltTable type for Procedure parameters
     */
    VOLTTABLE ((byte)21, -1, null, new Class[] {VoltTable.class}, VoltTable[].class, '0'),

    /**
     * Fixed precision=38, scale=12 storing sign and null-status in a preceding byte
     */
    DECIMAL  ((byte)22, 16, "decimal", new Class[] {BigDecimal.class}, BigDecimal[].class, 'd',
            java.sql.Types.DECIMAL,  // java.sql.Types DATA_TYPE
            null, // prefix to specify a literal
            null, // suffix to specify a literal
            null, // necessary params to create
            false, // case-sensitive
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            false, // unsigned?
            12, // minimum scale
            12, // maximum scale
            "java.math.BigDecimal"), // getObject return type

    /**
     * Array of bytes of variable length
     */
    VARBINARY    ((byte)25, -1, "varbinary",
            new Class[] {byte[].class,
                         Byte[].class },
                         byte[][].class, 'l',
            java.sql.Types.VARBINARY,  // java.sql.Types DATA_TYPE
            "'", // prefix to specify a literal
            "'", // suffix to specify a literal
            "max_length", // necessary params to create
            false, // case-sensitive
            java.sql.DatabaseMetaData.typePredBasic, // where-clauses supported
            null, // unsigned?
            null, // minimum scale
            null, // maximum scale
            "java.lang.Byte[]"); // getObject return type

    /**
     * Size in bytes of the maximum length for a VoltDB field value, presumably a
     * <code>STRING</code> or <code>VARBINARY</code>
     */
    public static final int MAX_VALUE_LENGTH = 1048576;
    public static final int MAX_VALUE_LENGTH_IN_CHARACTERS = MAX_VALUE_LENGTH / 4;
    /**
     * String representation of <code>MAX_VALUE_LENGTH</code>.
     */
    public static String humanReadableSize(int size)
    {
        if (size > 9999) return String.valueOf(size / 1024) + "K";
        return String.valueOf(size) + "B";
    }

    private final byte m_val;
    private final int m_lengthInBytes;
    private final String m_sqlString;
    private final Class<?>[] m_classes;
    private final Class<?> m_vectorClass;
    private final char m_signatureChar;
    // Is this type visible to JDBC?
    private final boolean m_jdbcVisible;
    // JDBC getTypeInfo values
    // If we add yet more stuff to this for MySQL or ODBC or something,
    // it might be time to consider some sort of data-driven type specification
    // mechanism.
    private final int m_dataType;
    private final String m_literalPrefix;
    private final String m_literalSuffix;
    private final String m_createParams;
    private final int m_nullable;
    private final boolean m_caseSensitive;
    private final int m_searchable;
    private final Boolean m_unsignedAttribute;
    private final Integer m_minimumScale;
    private final Integer m_maximumScale;
    // I wanted to use the first entry in m_classes, but it doesn't match what
    // VoltTable.get() returns in some cases, and I'm fearful of arbitrarily changing
    // what classFromType() returns to various parts of the system
    // This is the type that will be returned by ResultSet.getObject(), which
    // boils down to VoltTable.get(), with a special case for timestamps
    private final String m_jdbcClass;

    // Constructor for non-JDBC-visible types
    private VoltType(byte val, int lengthInBytes, String sqlString,
                     Class<?>[] classes, Class<?> vectorClass, char signatureChar)
    {
        this(val, lengthInBytes, sqlString, classes, vectorClass, signatureChar,
                false,
                java.sql.Types.OTHER,
                null,
                null,
                null,
                java.sql.DatabaseMetaData.typeNullable,
                false,
                Integer.MIN_VALUE,
                null,
                null,
                null,
                null);
    }

    // Constructor for JDBC-visible types.  Only types constructed in this way will
    // appear in the JDBC getTypeInfo() metadata.
    private VoltType(byte val, int lengthInBytes, String sqlString,
                     Class<?>[] classes, Class<?> vectorClass, char signatureChar,
                     int dataType,
                     String literalPrefix,
                     String literalSuffix,
                     String createParams,
                     boolean caseSensitive,
                     int searchable,
                     Boolean unsignedAttribute,
                     Integer minimumScale,
                     Integer maximumScale,
                     String jdbcClass)
    {
        this(val, lengthInBytes, sqlString, classes, vectorClass, signatureChar,
                true,
                dataType,
                literalPrefix,
                literalSuffix,
                createParams,
                java.sql.DatabaseMetaData.typeNullable,
                caseSensitive,
                searchable,
                unsignedAttribute,
                minimumScale,
                maximumScale,
                jdbcClass);
    }

    private VoltType(byte val, int lengthInBytes, String sqlString,
                     Class<?>[] classes, Class<?> vectorClass, char signatureChar,
                     boolean jdbcVisible,
                     int dataType,
                     String literalPrefix,
                     String literalSuffix,
                     String createParams,
                     int nullable,
                     boolean caseSensitive,
                     int searchable,
                     Boolean unsignedAttribute,
                     Integer minimumScale,
                     Integer maximumScale,
                     String jdbcClass)
    {
        m_val = val;
        m_lengthInBytes = lengthInBytes;
        m_sqlString = sqlString;
        m_classes = classes;
        m_vectorClass = vectorClass;
        m_signatureChar = signatureChar;
        m_jdbcVisible = jdbcVisible;
        m_dataType = dataType;
        m_literalPrefix = literalPrefix;
        m_literalSuffix = literalSuffix;
        m_createParams = createParams;
        m_nullable = nullable;
        m_caseSensitive = caseSensitive;
        m_searchable = searchable;
        m_unsignedAttribute = unsignedAttribute;
        m_minimumScale = minimumScale;
        m_maximumScale = maximumScale;
        m_jdbcClass = jdbcClass;
    }

    private final static ImmutableMap<Class<?>, VoltType> s_classes;
    //Update this if you add a type.
    private final static VoltType s_types[] = new VoltType[26];
    static {
        ImmutableMap.Builder<Class<?>, VoltType> b = ImmutableMap.builder();
        HashMap<Class<?>, VoltType> validation = new HashMap<Class<?>, VoltType>();
        for (VoltType type : values()) {
            s_types[type.m_val] = type;
            for (Class<?> cls : type.m_classes) {
                // Avoid subtle effects when VoltTypes have duplicate m_classes entries (java classes),
                // so that the association of a java class with the earlier VoltType gets obliterated
                // by its association with the later VoltType.
                // The effects of an assert in the middle of class initialization is surprisingly cryptic,
                // at least when exercised by the "ant junit" suite, so for a SLIGHTLY less cryptic response,
                // throw a generic runtime exception.
                // Unfortunately, either response gets associated with the source lines of the first call to
                // VoltType (like in DDLCompiler), rather than here.
                // assert(s_classes.get(cls) == null);
                if (validation.get(cls) != null) {
                    // This message seems to just get buried by the java runtime.
                    throw new RuntimeException("Associate each java class with at most one VoltType.");
                }
                validation.put(cls, type);
                b.put(cls, type);
            }
        }
        s_classes = b.build();
    }

    /**
     * Gets the byte that corresponds to the enum value (for serialization).
     * @return A byte representing the enum value
     */
    public byte getValue() {
        return m_val;
    }

    /**
     * Return the java class that is matched to a given <tt>VoltType</tt>.
     * @return A java class object.
     * @throws RuntimeException if a type doesn't have an associated class,
     * such as {@link #INVALID}.
     * @see #typeFromClass
     */
    public Class<?> classFromType() {
        if (m_classes.length == 0) {
            throw new RuntimeException("Unsupported type " + this);
        }
        return m_classes[0];
    }

    /**
     * Return the java class that is matched to a given <tt>VoltType</tt>.
     * @return A java class object.
     * @throws RuntimeException if a type doesn't have an associated class,
     * such as {@link #INVALID}.
     * @see #typeFromClass
     */
    public Class<?> vectorClassFromType() {
        if (m_vectorClass == null) {
            throw new RuntimeException("Unsupported type " + this);
        }
        return m_vectorClass;
    }

    /**
     * Statically create an enum value from the corresponding byte.
     * @param val A byte representing an enum value
     * @return The appropriate enum value
     */
    public static VoltType get(byte val) {
        VoltType type = (val < s_types.length) ? s_types[val] : null;
        if (type == null) {
            throw new AssertionError("Unknown type: " + String.valueOf(val));
        }
        return type;
    }

    private boolean matchesString(String str) {
        return str.toLowerCase().endsWith(name().toLowerCase());
    }

    /**
     * Converts string representations to an enum value.
     * @param str A string in the form "TYPENAME" or "VoltType.TYPENAME",
     * e.g. "BIGINT" or "VoltType.VARCHAR"
     * @return One of the valid enum values for VoltType
     */
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
        if (str.equalsIgnoreCase("DOUBLE")) return FLOAT;
        if (str.equalsIgnoreCase("CHARACTER") || str.equalsIgnoreCase("CHAR") || str.equalsIgnoreCase("VARCHAR")) return STRING;

        throw new RuntimeException("Can't find type: " + str);
    }

    /**
     * Ascertain the most appropriate <tt>VoltType</tt> given a
     * java object.
     * @param obj The java object to type.
     * @return A <tt>VoltType</tt> or invalid if none applies.
     * @see #typeFromClass
     */
    public static VoltType typeFromObject(Object obj) {
        assert obj != null;

        Class<?> cls = obj.getClass();
        return typeFromClass(cls);
    }

    /**
     * Ascertain the most appropriate <tt>VoltType</tt> given a
     * java class.
     * @param cls The java class to type.
     * @return A <tt>VoltType</tt> or invalid if none applies.
     * @see #typeFromObject
     * @see #classFromType
     */
    public static VoltType typeFromClass(Class<?> cls) {
        VoltType type = s_classes.get(cls);
        if (type == null) {
            throw new VoltTypeException("Unimplemented Object Type: " + cls);
        }
        return type;
    }

    /**
     * Return the string representation of this type. Note that
     * <tt>VoltType.typeFromString(voltTypeInstance.toString) == true</tt>.
     * @return The string representation of this type.
     */
    @Override public String toString() {
        return "VoltType." + name();
    }

    /**
     * Get the number of bytes required to store the type for types
     * with fixed length.
     * @return An integer value representing a number of bytes.
     */
    public int getLengthInBytesForFixedTypes() {
        if (m_lengthInBytes == -1) {
            throw new RuntimeException(
                    "Asking for fixed size for non-fixed or unknown type.");

        }
        return m_lengthInBytes;
    }

    public int getLengthInBytesForFixedTypesWithoutCheck() {
        return m_lengthInBytes;
    }

    /**
     * Get the maximum number of bytes required to store the type
     * @return An integer value representing a number of bytes.
     */
    public int getMaxLengthInBytes() {
        if (m_lengthInBytes == -1) {
            return MAX_VALUE_LENGTH;
        }
        return m_lengthInBytes;
    }

    /** JDBC getTypeInfo() accessors */

    /**
     * Get the corresponding SQL type as for a given <tt>VoltType</tt> enum.
     * For example, {@link #STRING} will probably convert to "VARCHAR".
     * @return A string representing the SQL type.
     */
    public String toSQLString() {
        return m_sqlString;
    }

    /**
     * <p>Is this type visible to JDBC</p>
     *
     * @return JDBC visibility.
     */
    public boolean isJdbcVisible() {
        return m_jdbcVisible;
    }

    /**
     * Get the java.sql.Types type of this type.
     *
     * @return int representing SQL type of the VoltDB type.
     */
    public int getJdbcSqlType() {
        return m_dataType;
    }

    public String getLiteralPrefix() {
        return m_literalPrefix;
    }

    public String getLiteralSuffix() {
        return m_literalSuffix;
    }

    public String getCreateParams() {
        return m_createParams;
    }

    public int getNullable() {
        return m_nullable;
    }

    public boolean isCaseSensitive() {
        return m_caseSensitive;
    }

    public int getSearchable() {
        return m_searchable;
    }

    public Boolean isUnsigned() {
        return m_unsignedAttribute;
    }

    public Integer getMinimumScale() {
        return m_minimumScale;
    }

    public Integer getMaximumScale() {
        return m_maximumScale;
    }

    public String getJdbcClass() {
        return m_jdbcClass;
    }

    // Really hacky cast overflow detection for primitive types
    // Comparison to MIN_VALUEs are <= to avoid collisions with the NULL
    // bit pattern
    // Probably eventually want a generic wouldCastDiscardInfo() call or
    // something
    boolean wouldCastOverflow(Number value)
    {
        boolean retval = false;
        switch (this)
        {
        case TINYINT:
            if (value.longValue() <= Byte.MIN_VALUE ||
                    value.longValue() > Byte.MAX_VALUE)
            {
                retval = true;
            }
            break;
        case SMALLINT:
            if (value.longValue() <= Short.MIN_VALUE ||
                    value.longValue() > Short.MAX_VALUE)
            {
                retval = true;
            }
            break;
        case INTEGER:
            if (value.longValue() <= Integer.MIN_VALUE ||
                    value.longValue() > Integer.MAX_VALUE)
            {
                retval = true;
            }
            break;
        case BIGINT:
            // overflow isn't detectable for Longs, just look for NULL value
            // In practice, I believe that we should never get here in VoltTable
            // since we check for NULL before checking for cast overflow
            if (value.longValue() == NULL_BIGINT)
            {
                retval = true;
            }
            break;
        case FLOAT:
            // this really should never occur, also, just look for NULL
            // In practice, I believe that we should never get here in VoltTable
            // since we check for NULL before checking for cast overflow
            if (value.doubleValue() == NULL_FLOAT)
            {
                retval = true;
            }
            break;
        default:
            throw new VoltTypeException("Unhandled cast overflow case, " +
                                        "casting to: " + toString());
        }
        return retval;
    }

    // I feel like it should be possible to jam this into the enum
    // constructor somehow but java hates me when I move constant definitions
    // above the enum constructors, so, meh

    /**
     * Get a value representing whichever null value is appropriate for
     * the current <tt>VoltType</tt> enum. For example, if this type is
     * {@link #TINYINT}, this will return a java <tt>byte</tt> with value
     * -128, which is the constant NULL_TINYINT in VoltDB.
     * @return A new final instance with value equal to null for a given
     * type.
     */
    public Object getNullValue()
    {
        switch (this)
        {
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
        default:
            throw new VoltTypeException("No NULL value for " + toString());
        }
    }

    public static boolean isNullVoltType(Object obj)
    {
        boolean retval = false;
        if (obj == null)
        {
            retval = true;
        }
        else if (obj == VoltType.NULL_TIMESTAMP ||
                obj == VoltType.NULL_STRING_OR_VARBINARY ||
                obj == VoltType.NULL_DECIMAL)
        {
            retval = true;
        }
        else
        {
            switch(typeFromObject(obj))
            {
            case TINYINT:
                retval = (((Number) obj).byteValue() == NULL_TINYINT);
                break;
            case SMALLINT:
                retval = (((Number) obj).shortValue() == NULL_SMALLINT);
                break;
            case INTEGER:
                retval = (((Number) obj).intValue() == NULL_INTEGER);
                break;
            case BIGINT:
                retval = (((Number) obj).longValue() == NULL_BIGINT);
                break;
            case FLOAT:
                retval = (((Number) obj).doubleValue() == NULL_FLOAT);
                break;
            case TIMESTAMP:
                retval = (obj == VoltType.NULL_TIMESTAMP);
                break;
            case STRING:
                retval = (obj == VoltType.NULL_STRING_OR_VARBINARY);
                break;
            case VARBINARY:
                retval = (obj == VoltType.NULL_STRING_OR_VARBINARY);
                break;
            case DECIMAL:
                retval = (obj == VoltType.NULL_DECIMAL);
                break;
            default:
                throw new VoltTypeException("Unsupported type: " +
                                            typeFromObject(obj));
            }
        }
        return retval;
    }

    /**
     * Is the type a number and is it an exact value (no rounding errors)?
     * @return true for integers and decimals. False for floats and strings
     * and anything else.
     */
    public boolean isExactNumeric() {
        switch(this)  {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case DECIMAL:
            return true;
        default:
            return false;
        }
    }

    /**
     * Is this type an integer type? True for <code>TINYINT</code>, <code>SMALLINT</code>,
     * <code>INTEGER</code>, <code>BIGINT</code> and <code>TIMESTAMP</code>.
     * @return True if integer type. False if not.
     */
    public boolean isInteger() {
        switch(this)  {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case TIMESTAMP:
            return true;
        default:
            return false;
        }
    }

    public boolean isMaxValuePaddable() {
        switch (this) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case TIMESTAMP:
        case FLOAT:
            return true;
        default:
            return false;
        }
    }

    public static Object getPaddedMaxTypeValue(VoltType type) {
        switch (type) {
        case TINYINT: return new Byte(Byte.MAX_VALUE);
        case SMALLINT: return new Short(Short.MAX_VALUE);
        case INTEGER: return new Integer(Integer.MAX_VALUE);
        case BIGINT:
        case TIMESTAMP:
            return new Long(Long.MAX_VALUE);
        case FLOAT:
            return new Float(Float.MAX_VALUE);
        default:
            return null;
        }
    }

    public boolean isNumber() {
        switch (this) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
                return true;
            default:
                return false;
        }
    }

    // Used by TheHashinator.getPartitionForParameter() to determine if the
    // type of the partition parameter is one that we can coerce from a string
    // value.  isInteger includes TIMESTAMP, which is bad, and isNumber
    // includes FLOAT, which is also bad, hence the creation of this method.
    public boolean isPartitionableNumber() {
        switch (this) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return true;
            default:
                return false;
        }
    }

    /* Indicate whether a value can be assigned to this type without loss of range or precision,
     * important for index key and partition key initialization. */
    public boolean canExactlyRepresentAnyValueOf(VoltType otherType) {
        // self to self conversion is obviously fine.
        if (this == otherType)
            return true;

        if (otherType.isInteger()) {
            if (this.isInteger()) {
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

    /**
     * Get a char that uniquely identifies a type. Used to create
     * concise schema signatures.
     * @return A char representing the type.
     */
    public char getSignatureChar() {
        assert(m_signatureChar != '0');
        return m_signatureChar;
    }

    /**
     * Make a printable, short string for a varbinary.
     * String includes a CRC and the contents of the varbinary in hex.
     * Contents longer than 13 chars are truncated and elipsized.
     * Yes, "elipsized" is totally a word.
     *
     * Example: "bin[crc:1298399436,value:0xABCDEF12345...]"
     *
     * @param bin The bytes to print out.
     * @return A string representation that is printable and short.
     */
    public static String varbinaryToPrintableString(byte[] bin) {
        PureJavaCrc32 crc = new PureJavaCrc32();
        StringBuilder sb = new StringBuilder();
        sb.append("bin[crc:");
        crc.update(bin);
        sb.append(crc.getValue());
        sb.append(",value:0x");
        String hex = Encoder.hexEncode(bin);
        if (hex.length() > 13) {
            sb.append(hex.substring(0, 10));
            sb.append("...");
        }
        else {
            sb.append(hex);
        }
        sb.append("]");
        return sb.toString();
    }

    // Integer[0] is the column size and Integer[1] is the radix
    // I'd love to get this magic into the type construction, but
    // not happening this go-round.  --izzy
    public Integer[] getTypePrecisionAndRadix()
    {
        Integer[] col_size_radix = {null, null};
        switch(this)
        {
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
        case STRING:
            col_size_radix[0] = VoltType.MAX_VALUE_LENGTH;
            col_size_radix[1] = null;
            break;
        case DECIMAL:
            col_size_radix[0] = VoltDecimalHelper.kDefaultPrecision;
            col_size_radix[1] = 10;
            break;
        case VARBINARY:
            col_size_radix[0] = VoltType.MAX_VALUE_LENGTH;
            col_size_radix[1] = null;
            break;
        default:
            // What's the right behavior here?
        }
        return col_size_radix;
    }

    /** Length value for a null string. */
    public static final int NULL_STRING_LENGTH = -1;
    /** Null value for <code>TINYINT</code>.  */
    public static final byte NULL_TINYINT = Byte.MIN_VALUE;
    /** Null value for <code>SMALLINT</code>.  */
    public static final short NULL_SMALLINT = Short.MIN_VALUE;
    /** Null value for <code>INTEGER</code>.  */
    public static final int NULL_INTEGER = Integer.MIN_VALUE;
    /** Null value for <code>BIGINT</code>.  */
    public static final long NULL_BIGINT = Long.MIN_VALUE;
    /** Null value for <code>FLOAT</code>.  */
    public static final double NULL_FLOAT = -1.7E+308;

    // for consistency at the API level, provide symbolic nulls for these types, too
    private static final class NullTimestampSigil{}
    /** Null value for <code>TIMESTAMP</code>.  */
    public static final NullTimestampSigil NULL_TIMESTAMP = new NullTimestampSigil();

    private static final class NullStringOrVarbinarySigil{}
    /** Null value for <code>STRING</code> or <code>VARBINARY</code>.  */
    public static final NullStringOrVarbinarySigil NULL_STRING_OR_VARBINARY = new NullStringOrVarbinarySigil();

    private static final class NullDecimalSigil{}
    /** Null value for <code>DECIMAL</code>.  */
    public static final NullDecimalSigil NULL_DECIMAL = new NullDecimalSigil();

}
