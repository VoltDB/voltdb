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

package org.voltdb.expressions;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

/**
 *
 */
public class ConstantValueExpression extends AbstractValueExpression {

    public enum Members {
        VALUE,
        ISNULL;
    }

    protected String m_value = null;
    protected boolean m_isNull = true;

    public ConstantValueExpression() {
        super(ExpressionType.VALUE_CONSTANT);
    }

    @Override
    public Object clone() {
        ConstantValueExpression clone = (ConstantValueExpression)super.clone();
        clone.m_value = m_value;
        clone.m_isNull = m_isNull;
        return clone;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Make sure our value is not null
        if (m_value == null && !m_isNull)
        {
            throw new Exception("ERROR: The constant value for '" + this +
                                "' is inconsistently null");
        // Make sure the value type is something we support here
        } else if (m_valueType == VoltType.NULL ||
                   m_valueType == VoltType.VOLTTABLE) {
            throw new Exception("ERROR: Invalid constant value type '" + m_valueType + "' for '" + this + "'");
        }
    }

    /**
     * @return the value
     */
    public String getValue() {
        return m_value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(String value) {
        m_value = value;
        m_isNull = false;
        if (m_value == null)
        {
            m_isNull = true;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConstantValueExpression == false) {
            return false;
        }
        ConstantValueExpression expr = (ConstantValueExpression) obj;

        if (m_isNull != expr.m_isNull) {
            return false;
        }

        if (m_isNull) { // implying that both sides are null
            return true;
        }
        return m_value.equals(expr.m_value);
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        int result = super.hashCode();
        if (m_isNull) {
            result += 1;
        } else {
            result += m_value.hashCode();
        }
        return result;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.ISNULL.name());
        stringer.value(m_isNull);
        stringer.key(Members.VALUE.name());
        if (m_isNull)
        {
            stringer.value("NULL");
        }
        else
        {
            switch (m_valueType)
            {
            case INVALID:
                throw new JSONException("ConstantValueExpression.toJSONString(): value_type should never be VoltType.INVALID");
            case NULL:
                stringer.value("null");
                break;
            case TINYINT:
                stringer.value(Long.valueOf(m_value));
                break;
            case SMALLINT:
                stringer.value(Long.valueOf(m_value));
                break;
            case INTEGER:
                stringer.value(Long.valueOf(m_value));
                break;
            case BIGINT:
                stringer.value(Long.valueOf(m_value));
                break;
            case FLOAT:
                stringer.value(Double.valueOf(m_value));
                break;
            case STRING:
                stringer.value(m_value);
                break;
            case VARBINARY:
                stringer.value(m_value);
                break;
            case TIMESTAMP:
                stringer.value(Long.valueOf(m_value));
                break;
            case DECIMAL:
                stringer.value(m_value);
                break;
            default:
                throw new JSONException("ConstantValueExpression.toJSONString(): Unrecognized value_type " + m_valueType);
            }
        }
    }

    @Override
    public void loadFromJSONObject(JSONObject obj) throws JSONException
    {
        m_isNull = false;
        if (!obj.isNull(Members.VALUE.name())) {
            m_value = obj.getString(Members.VALUE.name());
        }
        else {
            m_isNull = true;
        }
        if (!obj.isNull(Members.ISNULL.name())) {
            m_isNull = obj.getBoolean(Members.ISNULL.name());
        }
    }

    public static Object extractPartitioningValue(VoltType voltType, AbstractExpression constExpr) {
        // TODO: There is currently no way to pass back as a partition key value
        // the constant value resulting from a general constant expression such as
        // "WHERE a.pk = b.pk AND b.pk = SQRT(3*3+4*4)" because the planner has no expression evaluation capabilities.
        if (constExpr instanceof ConstantValueExpression) {
            // ConstantValueExpression exports its value as a string, which is handy for serialization,
            // but the hashinator wants a partition-key-column-type-appropriate value.
            // For safety, don't trust the constant's type
            // -- it's apparently comparable to the column, but may not be an exact match(?).
            // XXX: Actually, there may need to be additional filtering in the code above to not accept
            // constant equality filters that would require the COLUMN type to be non-trivially converted (?)
            // -- it MAY not be safe to limit execution of such a filter on any single partition.
            // For now, for partitioning purposes, leave constants for string columns as they are,
            // and process matches for integral columns via constant-to-string-to-bigInt conversion.
            String stringValue = ((ConstantValueExpression) constExpr).getValue();
            if (voltType.isInteger()) {
                try {
                    return new Long(stringValue);
                } catch (NumberFormatException nfe) {
                    // Disqualify this constant by leaving objValue null -- probably should have caught this earlier?
                    // This causes the statement to fall back to being identified as multi-partition.
                }
            } else {
                return stringValue;
            }
        }
        return null;
    }

    @Override
    public void refineValueType(VoltType neededType, int neededSize)
    {
        int size_unit = 1;
        if (neededType == m_valueType) {

            if (neededSize == m_valueSize) {
                return;
            }
            // Variably sized types need to fit within the target width.
            if (neededType == VoltType.VARBINARY) {
                if ( ! Encoder.isHexEncodedString(getValue())) {
                    throw new PlanningErrorException("Value (" + getValue() +
                                                     ") has an invalid format for a constant " +
                                                     neededType.toSQLString() + " value");
                }
                size_unit = 2;
            }
            else {
                assert neededType == VoltType.STRING;
            }

            if (getValue().length() > size_unit*neededSize ) {
                throw new PlanningErrorException("Value (" + getValue() +
                                                 ") is too wide for a constant " +
                                                 neededType.toSQLString() +
                                                 " value of size " + neededSize);
            }
            setValueSize(neededSize);
            return;
        }

        if (m_isNull) {
            setValueType(neededType);
            setValueSize(neededSize);
            return;
        }

        // Constant's apparent type may not exactly match the target type needed.
        if (neededType == VoltType.VARBINARY &&
                (m_valueType == VoltType.STRING || m_valueType == null)) {
            if ( ! Encoder.isHexEncodedString(getValue())) {
                throw new PlanningErrorException("Value (" + getValue() +
                                                 ") has an invalid format for a constant " +
                                                 neededType.toSQLString() + " value");
            }
            size_unit = 2;
            if (getValue().length() > size_unit*neededSize ) {
                throw new PlanningErrorException("Value (" + getValue() +
                                                 ") is too wide for a constant " +
                                                 neededType.toSQLString() +
                                                 " value of size " + neededSize);
            }
            setValueType(neededType);
            setValueSize(neededSize);
            return;
        }

        if (neededType == VoltType.STRING && m_valueType == null) {
            if (getValue().length() > size_unit*neededSize ) {
                throw new PlanningErrorException("Value (" + getValue() +
                                                 ") is too wide for a constant " +
                                                 neededType.toSQLString() +
                                                 " value of size " + neededSize);
            }
            setValueType(neededType);
            setValueSize(neededSize);
            return;
        }

        if (neededType == VoltType.TIMESTAMP) {
            if (m_valueType == VoltType.STRING) {
                try {
                    // Convert date value in whatever format is supported by TimeStampType
                    // into VoltDB native microsecond count.
                    TimestampType ts = new TimestampType(m_value);
                    m_value = String.valueOf(ts.getTime());
                }
                // It couldn't be converted to timestamp.
                catch (IllegalArgumentException e) {
                    throw new PlanningErrorException("Value (" + getValue() +
                                                     ") has an invalid format for a constant " +
                                                     neededType.toSQLString() + " value");

                }
                setValueType(neededType);
                setValueSize(neededSize);
                return;
            }
        }

        if ((neededType == VoltType.FLOAT) || (neededType == VoltType.DECIMAL)) {
            if (m_valueType == null ||
                    (m_valueType != VoltType.NUMERIC && ! m_valueType.isExactNumeric())) {
                try {
                    Double.parseDouble(getValue());
                } catch (NumberFormatException nfe) {
                    throw new PlanningErrorException("Value (" + getValue() +
                                                     ") has an invalid format for a constant " +
                                                     neededType.toSQLString() + " value");
                }
            }
            setValueType(neededType);
            setValueSize(neededSize);
            return;
        }

        if (neededType.isInteger()) {
            long value = 0;
            try {
                value = Long.parseLong(getValue());
            } catch (NumberFormatException nfe) {
                throw new PlanningErrorException("Value (" + getValue() +
                                                 ") has an invalid format for a constant " +
                                                 neededType.toSQLString() + " value");
            }
            checkIntegerValueRange(value, neededType);
            m_valueType = neededType;
            m_valueSize = neededType.getLengthInBytesForFixedTypes();
            return;
        }

        // That's it for known type conversions.
        throw new PlanningErrorException("Value (" + getValue() +
                ") has an invalid format for a constant " +
                neededType.toSQLString() + " value");
    }

    private static void checkIntegerValueRange(long value, VoltType integerType) {

        // Note that while Long.MIN_VALUE is used to represent NULL in VoltDB, we have decided that
        // pass in the literal for Long.MIN_VALUE makes very little sense when you have the option
        // to use the literal NULL. Thus the NULL values for each of the 4 integer types are considered
        // an underflow exception for the type.

        if (integerType == VoltType.BIGINT || integerType == VoltType.TIMESTAMP) {
            if (value == VoltType.NULL_BIGINT)
                throw new PlanningErrorException("Constant value underflows BIGINT type.");
        }
        if (integerType == VoltType.INTEGER) {
            if ((value > Integer.MAX_VALUE) || (value <= VoltType.NULL_INTEGER))
                throw new PlanningErrorException("Constant value overflows/underflows INTEGER type.");
        }
        if (integerType == VoltType.SMALLINT) {
            if ((value > Short.MAX_VALUE) || (value <= VoltType.NULL_SMALLINT))
                throw new PlanningErrorException("Constant value overflows/underflows SMALLINT type.");
        }
        if (integerType == VoltType.TINYINT) {
            if ((value > Byte.MAX_VALUE) || (value <= VoltType.NULL_TINYINT))
                throw new PlanningErrorException("Constant value overflows/underflows TINYINT type.");
        }
    }


    @Override
    public void refineOperandType(VoltType columnType) {
        if (m_valueType != VoltType.NUMERIC) {
            return;
        }
        if (columnType == null || columnType == VoltType.NUMERIC) {
            return;
        }
        if ((columnType == VoltType.FLOAT) || (columnType == VoltType.DECIMAL)) {
            m_valueType = columnType;
            m_valueSize = columnType.getLengthInBytesForFixedTypes();
            return;
        }
        if (columnType.isInteger()) {
            try {
                Long.parseLong(getValue());
            } catch (NumberFormatException e) {
                //TODO: Or DECIMAL? Either is OK for integer comparison, but math gets different results?
                columnType = VoltType.FLOAT;
            }
            m_valueType = columnType;
            m_valueSize = columnType.getLengthInBytesForFixedTypes();
        }
        else {
            throw new NumberFormatException("NUMERIC constant value type must match a FLOAT, DECIMAL, or integral column, not " + columnType.toSQLString());
        }
    }

    @Override
    public void finalizeValueTypes() {
        if (m_valueType != VoltType.NUMERIC) {
            return;
        }
        m_valueType = VoltType.FLOAT;
        m_valueSize = m_valueType.getLengthInBytesForFixedTypes();
    }

    /**
     * Tests if the value is a string that would represent a prefix if used as a LIKE pattern.
     * The value must end in a '%' and contain no other wildcards ('_' or '%').
     **/
    public boolean isPrefixPatternString() {
        String patternString = getValue();
        int length = patternString.length();
        if (length == 0) {
            return false;
        }
        // '_' is not allowed.
        int disallowedWildcardPos = patternString.indexOf('_');
        if (disallowedWildcardPos != -1) {
            return false;
        }
        int firstWildcardPos = patternString.  indexOf('%');
        // Indexable filters have only a trailing '%'.
        // NOTE: not bothering to check for silly synonym patterns with multiple trailing '%'s.
        if (firstWildcardPos != length-1) {
            return false;
        }
        return true;
    }

    @Override
    public String explain(String unused) {
        if (m_valueType == VoltType.STRING) {
            return "'" + m_value + "'";
        }
        return m_value;
    }

}
