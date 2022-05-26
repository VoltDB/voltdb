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

package org.voltdb.serdes;

import java.util.EnumSet;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;

/**
 * An enum listing the encoding formats
 */
public enum EncodeFormat {
    UNDEFINED(false, true),
    CSV(false, true),
    AVRO(false, true),
    JSON(false, false),
    OPAQUE(false, true),
    INT(true, true),
    LONG(true, true),
    DOUBLE(true, true),
    STRING(true, true),
    BYTEARRAY(true, true);

    private final boolean m_simple;
    private final boolean m_inline;

    /**
     * Parse an {@link EncodeFormat} from a {@link String} whose
     * value is assumed to have been checked beforehand.
     *
     * @param name
     * @return {@link EncodeFormat} value
     */
    public static EncodeFormat checkedValueOf(String name) {
        try {
            return valueOf(name.toUpperCase());
        }
        catch(IllegalArgumentException ex) {
            return EncodeFormat.UNDEFINED;
        }
        catch (Exception ex) {
            VoltDB.crashLocalVoltDB("Illegal encoding format " + name, true, ex);
            return EncodeFormat.UNDEFINED; // never get here
        }
    }

    /**
     * Parse an {@link EncodeFormat} from a {@link String} and apply defaults.
     *
     * @param isKey     {@code true} if for key format, {@code false} for value format
     * @param isOpaque  {@code true} if opaque
     * @param fmt       the format string
     * @return
     */
    public static EncodeFormat parseFormat(boolean isKey, boolean isOpaque, String fmt) {
        if (isOpaque) {
            return EncodeFormat.OPAQUE;
        }
        else if (StringUtils.isBlank(fmt)) {
            return isKey ? EncodeFormat.STRING : EncodeFormat.CSV;
        }
        return EncodeFormat.checkedValueOf(fmt.toUpperCase());
    }

    /**
     * Get the {@link EncodeFormat} for the given {@link VoltType} or {@link IllegalArgumentException} is thrown
     *
     * @param type to get encoding for
     * @return format used for {@code type}
     */
    public static EncodeFormat forType(VoltType type) {
        switch (type) {
            case INTEGER:
                return INT;
            case BIGINT:
                return LONG;
            case FLOAT:
                return DOUBLE;
            case VARBINARY:
                return BYTEARRAY;
            case STRING:
            default:
                return STRING;
        }
    }

    /**
     * @return A set of all valid formats which can encode a multiple objects
     */
    public static EnumSet<EncodeFormat> complexFormats() {
        return EnumSet.of(EncodeFormat.CSV, EncodeFormat.AVRO, EncodeFormat.JSON);
    }

    /**
     * @return the set of acceptable values in configuration
     */
    public static EnumSet<EncodeFormat> valueSet() {
        EnumSet<EncodeFormat> allowedValues = EnumSet.allOf(EncodeFormat.class);
        allowedValues.remove(UNDEFINED);
        allowedValues.remove(OPAQUE);
        return allowedValues;
    }

    private EncodeFormat(boolean simple, boolean inline) {
        m_simple = simple;
        m_inline = inline;
    }

    /**
     * @return {@code true} if simple format
     */
    public boolean isSimple() {
        return m_simple;
    }

    /**
     * @return {@code true} if format available to inline encoding
     */
    public boolean supportsInline() {
        return m_inline;
    }
}
