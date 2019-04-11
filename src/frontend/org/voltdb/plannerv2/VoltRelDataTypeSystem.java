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

package org.voltdb.plannerv2;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.voltdb.types.VoltDecimalHelper;

public class VoltRelDataTypeSystem extends RelDataTypeSystemImpl {

    public static final RelDataTypeSystem VOLT_REL_DATATYPE_SYSTEM = new VoltRelDataTypeSystem();

    @Override
    public int getMaxNumericScale() {
        // DECIMAL: 16-byte fixed scale of 12 and precision of 38
        return VoltDecimalHelper.kDefaultScale;
    }

    @Override
    public int getMaxNumericPrecision() {
        // DECIMAL: 16-byte fixed scale of 12 and precision of 38
        return VoltDecimalHelper.kDefaultPrecision;
    }

    @Override
    public boolean isSchemaCaseSensitive() {
        // Volt Schema is case-insensitive
        return false;
    }

}
