/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.util.stream.Stream;

import org.apache.calcite.sql.type.SqlTypeName;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;

import com.google.common.collect.ImmutableBiMap;

/**
 * Adaptor between <code>org.voltdb.VoltType</code> and <code>org.apache.calcite.sql.SqlDataTypeSpec</code>.
 * VoltDB needs to understand Calcite data types, because we create catalog from Calcite SqlNode;
 * Calcite needs to understand Volt data types in the planning stage. (right?)
 */
public class ColumnType {

    private static final ImmutableBiMap<SqlTypeName, VoltType> COLUMN_TYPES;

    static {
        ImmutableBiMap.Builder<SqlTypeName, VoltType> builder = new ImmutableBiMap.Builder<>();
        Stream.of(
            Pair.of(VoltType.BOOLEAN, SqlTypeName.BOOLEAN),
            Pair.of(VoltType.FLOAT, SqlTypeName.FLOAT),
            Pair.of(VoltType.VARBINARY, SqlTypeName.VARBINARY),
            // string types
            Pair.of(VoltType.STRING, SqlTypeName.CHAR),
            Pair.of(VoltType.STRING, SqlTypeName.VARCHAR),
            Pair.of(VoltType.TIMESTAMP, SqlTypeName.TIMESTAMP),

            Pair.of(VoltType.INTEGER, SqlTypeName.INTEGER),
            Pair.of(VoltType.BIGINT, SqlTypeName.BIGINT),
            Pair.of(VoltType.SMALLINT, SqlTypeName.SMALLINT),
            Pair.of(VoltType.TINYINT, SqlTypeName.TINYINT),

            Pair.of(VoltType.DECIMAL, SqlTypeName.DECIMAL),

            Pair.of(VoltType.GEOGRAPHY, SqlTypeName.GEOMETRY), // NOTE!
            Pair.of(VoltType.GEOGRAPHY_POINT, SqlTypeName.GEOMETRY)
        ).forEach(types -> {
            final VoltType vt = types.getFirst();
            final SqlTypeName ct = types.getSecond();
            builder.put(ct, vt);
        });
        COLUMN_TYPES = builder.build();
    }

    public static VoltType getVoltType(SqlTypeName calciteType) {
        return COLUMN_TYPES.get(calciteType);
    }

    public static VoltType getVoltType(String calciteType) {
        return VoltType.typeFromString(calciteType);
    }

    public static SqlTypeName getCalciteType(VoltType voltType) {
        return COLUMN_TYPES.inverse().get(voltType);
    }
}
