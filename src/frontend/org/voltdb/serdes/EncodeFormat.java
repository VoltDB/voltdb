/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
    INVALID(-1, false),
    CSV(0, false),
    AVRO(1, false),
    OPAQUE(2, false),
    INT(3, true),
    LONG(4, true),
    STRING(5, true),
    BYTEARRAY(6, true);

    /** ID for the encode format used in serialization of the format */
    private final byte m_id;
    private final boolean m_simple;

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
            return EncodeFormat.INVALID;
        }
        catch (Exception ex) {
            throw VoltDB.crashLocalVoltDB("Illegal encoding format " + name, true, ex);
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
            case STRING:
                return STRING;
            case VARBINARY:
                return BYTEARRAY;
            default:
                return CSV;
        }
    }

    /**
     * @return A set of all valid formats which can encode a multiple objects
     */
    public static EnumSet<EncodeFormat> complexFormats() {
        return EnumSet.of(EncodeFormat.CSV, EncodeFormat.AVRO);
    }

    /**
     * @return the set of acceptable values in configuration
     */
    public static EnumSet<EncodeFormat> valueSet() {
        EnumSet<EncodeFormat> allowedValues = EnumSet.allOf(EncodeFormat.class);
        allowedValues.remove(INVALID);
        allowedValues.remove(OPAQUE);
        return allowedValues;
    }

    /**
     * Convert from id returned by {@link #getId()} to {@code EncodeFormat}
     *
     * @param id of encode format
     * @return {@code EncodeFormat} represented by {@code id} or {@link #INVALID}
     */
    public static EncodeFormat byId(byte id) {
        for (EncodeFormat ef : values()) {
            if (ef.m_id == id) {
                return ef;
            }
        }

        return INVALID;
    }

    private EncodeFormat(int id, boolean simple) {
        m_id = (byte) id;
        m_simple = simple;
    }

    /**
     * @return ID of this EncodeFormat
     */
    public byte getId() {
        return m_id;
    }

    /**
     * @return {@code true} if simple format
     */
    public boolean isSimple() {
        return m_simple;
    }
}
