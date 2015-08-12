package org.voltdb.sqlparser.semantics.symtab;

/**
 * This is the type kinds of types.  Each enumeral has a set of
 * attributes, accessed by methods.  The size in bytes is -1 for
 * variable sized types, such as varchar or varbinary.
 * <ul>
 *   <li>There is an error type which has a name which is illegal in
 *       SQL.  It contains a dollar sign, which is not legal in our
 *       version of SQL.</li>
 *   <li>The size in bytes is -1 for variable sized types and 0 for
 *       internal types.  The only internal type is the error type.</li>
 * </ul>
 *
 * @author bwhite
 *
 */
public enum TypeKind {
           /*  bool,   int,   float, str,   u-code, fixed, tmst, error,  size */
    ERROR      (false, false, false, false, false, false, false, true,    0),
    BOOLEAN    (true,  false, false, false, false, false, false, false,   4),
    TINYINT    (false, true,  false, false, false, false, false, false,   1),
    SMALLINT   (false, true , false, false, false, false, false, false,   2),
    INTEGER    (false, true,  false, false, false, false, false, false,   4),
    BIGINT     (false, true,  false, false, false, false, false, false,   8),
    FLOAT      (false, false, true,  false, false, false, false, false,   8),
    VARCHAR    (false, false, false, true,  true,  false, false, false,  -1),
    VARBINARY  (false, false, false, true,  false, false, false, false,  -1),
    DECIMAL    (false, false, false, false, false, true,  false, false,   8),
    TIMESTAMP  (false, false, false, false, false, false, true,  false,   8);
    private boolean m_isBoolean;
    private boolean m_isInteger;
    private boolean m_isString;
    private boolean m_isFixedPoint;
    private boolean m_isFloat;
    private boolean m_isTimeStamp;
    private boolean m_isUnicode;
    private int m_sizeInBytes;

    private TypeKind(boolean aIsBoolean,
                     boolean aIsInteger,
                     boolean aIsFloat,
                     boolean aIsString,
                     boolean aIsUnicode,
                     boolean aIsFixedPoint,
                     boolean aIsTimestamp,
                     boolean aIsError,
                     int     aSizeInBytes) {
        m_isBoolean    = aIsBoolean;
        m_isInteger    = aIsInteger;
        m_isString     = aIsString;
        m_isUnicode    = aIsUnicode;
        m_isFixedPoint = aIsFixedPoint;
        m_isFloat      = aIsFloat;
        m_isTimeStamp  = aIsTimestamp;
        m_sizeInBytes  = aSizeInBytes;
    }

    public final boolean isBoolean() {
        return m_isBoolean;
    }

    public final boolean isInteger() {
        return m_isInteger;
    }

    public final boolean isString() {
        return m_isString;
    }

    public final boolean isFixedPoint() {
        return m_isFixedPoint;
    }

    public final boolean isFloat() {
        return m_isFloat;
    }

    public final boolean isTimeStamp() {
        return m_isTimeStamp;
    }

    public final int getSizeInBytes() {
        return m_sizeInBytes;
    }

    public final boolean isVariableSized() {
        return getSizeInBytes() == -1;
    }

    public final boolean isInternal() {
        return getSizeInBytes() == 0;
    }
}
