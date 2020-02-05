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

import org.voltdb.VoltDB;

/**
 * An enum listing the encoding formats
 */
public enum EncodeFormat {
    CSV,
    AVRO;

    /**
     * Parse an {@link EncodeFormat} from a {@link String} whose
     * value is assumed to have been checked beforehand.
     *
     * @param name
     * @return {@link EncodeFormat} value
     */
    public static EncodeFormat checkedValueOf(String name) {
        try {
            return valueOf(EncodeFormat.class, name);
        }
        catch (Exception ex) {
            VoltDB.crashLocalVoltDB("Illegal encoding format " + name, true, ex);
        }
        return null;
    }

    /**
     * @return the set of acceptable values
     */
    public static EnumSet<EncodeFormat> valueSet() {
        return EnumSet.allOf(EncodeFormat.class);
    }
}
