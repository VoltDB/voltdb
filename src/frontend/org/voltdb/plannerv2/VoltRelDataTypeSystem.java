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

package org.voltdb.plannerv2;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.voltdb.VoltType;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
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

    @Override public int getDefaultPrecision(SqlTypeName typeName) {
        // Following BasicSqlType precision as the default
        switch (typeName) {
            case CHAR:
            case VARCHAR:
            case VARBINARY:
                return VoltType.LengthRange.DEFAULT_COLUMN_SIZE;
            case GEOGRAPHY:
                return GeographyValue.DEFAULT_LENGTH;
            case GEOGRAPHY_POINT:
                return  GeographyPointValue.getLengthInBytes();
            case DECIMAL:
                return getMaxNumericPrecision();
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INTEGER:
                return 10;
            case BIGINT:
                return 19;
            case REAL:
                return 7;
            case FLOAT:
            case DOUBLE:
                return 15;
            case TIMESTAMP:
                return 8;  // 8-byte long value representing microseconds after the epoch.
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return SqlTypeName.DEFAULT_INTERVAL_START_PRECISION;
            default:
                return RelDataType.PRECISION_NOT_SPECIFIED;
        }
    }

    @Override public int getMaxPrecision(SqlTypeName typeName) {
        switch (typeName) {
            case DECIMAL:
                return getMaxNumericPrecision();
            case VARCHAR:
            case CHAR:
            case VARBINARY:
                return GeographyValue.MAX_SERIALIZED_LENGTH;
            case GEOGRAPHY:
                return GeographyValue.DEFAULT_LENGTH;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return SqlTypeName.MAX_INTERVAL_START_PRECISION;
            default:
                return getDefaultPrecision(typeName);
        }
    }

}
