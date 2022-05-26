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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;

/**
 * An adapter between {@link VoltType} and {@link SqlTypeName}}.
 * VoltDB needs to understand Calcite data types, because we populate the catalog
 * using Calcite {@code SqlNode};
 * Calcite needs to understand VoltDB data types in the planning stage.
 *
 * @author Lukai Liu
 * @since 9.0
 */
public class ColumnTypes {

    private static final Map<SqlTypeName, VoltType> CALC_TO_VOLT;
    private static final Map<VoltType, SqlTypeName> VOLT_TO_CALC;

    private static<K, V> void tryAdd(Map<K, V> map, Pair<K, V> kv) {
        final K key = kv.getFirst();
        final V value = kv.getSecond();
        Preconditions.checkState(!map.containsKey(key),
                "Map already contains key " + key.toString());
        map.put(key, value);
    }

    static {
        Map<SqlTypeName, VoltType> calcToVolt = new HashMap<>();
        Map<VoltType, SqlTypeName> voltToCalc = new HashMap<>();

        Stream.of(  // one-to-one mappings get added to both maps
            Pair.of(VoltType.STRING, SqlTypeName.VARCHAR),
            Pair.of(VoltType.VARBINARY, SqlTypeName.VARBINARY),

            Pair.of(VoltType.BOOLEAN, SqlTypeName.BOOLEAN),

            Pair.of(VoltType.TINYINT, SqlTypeName.TINYINT),
            Pair.of(VoltType.SMALLINT, SqlTypeName.SMALLINT),
            Pair.of(VoltType.INTEGER, SqlTypeName.INTEGER),
            Pair.of(VoltType.BIGINT, SqlTypeName.BIGINT),

            Pair.of(VoltType.TIMESTAMP, SqlTypeName.TIMESTAMP),
            Pair.of(VoltType.FLOAT, SqlTypeName.FLOAT),
            Pair.of(VoltType.DECIMAL, SqlTypeName.DECIMAL),

            // Note - ethan - 12/28/2018:
            // Not sure this works well. Internally, VoltDB stores GEOGRAPHY as VARBINARY.
            Pair.of(VoltType.GEOGRAPHY, SqlTypeName.GEOGRAPHY),
            Pair.of(VoltType.GEOGRAPHY_POINT, SqlTypeName.GEOGRAPHY_POINT)
        ).forEach(types -> {
            final VoltType vt = types.getFirst();
            final SqlTypeName ct = types.getSecond();
            tryAdd(calcToVolt, Pair.of(ct, vt));
            tryAdd(voltToCalc, types);
        });

        // more-to-one mappings only get to add to either map
        Stream.of(
                Pair.of(SqlTypeName.CHAR, VoltType.STRING),
                Pair.of(SqlTypeName.BINARY, VoltType.VARBINARY),
                Pair.of(SqlTypeName.DOUBLE, VoltType.FLOAT)
        ).forEach(types -> tryAdd(calcToVolt, types));

        CALC_TO_VOLT = Collections.unmodifiableMap(calcToVolt);
        VOLT_TO_CALC = Collections.unmodifiableMap(voltToCalc);
    }

    /**
     * Get the matching {@link VoltType} of the given {@link SqlTypeName}.
     *
     * @param calciteType the Calcite {@link SqlTypeName}
     * @return the matching {@link VoltType}
     */
    public static VoltType getVoltType(SqlTypeName calciteType) {
        return CALC_TO_VOLT.get(calciteType);
    }

    /**
     * Get the matching {@link VoltType} of the given SQL type name in string.
     *
     * @param calciteType SQL type name string
     * @return the matching {@link VoltType}
     */
    public static VoltType getVoltType(String calciteType) {
        return VoltType.typeFromString(calciteType);
    }

    /**
     * Get the matching Calcite {@link SqlTypeName} of the given {@link VoltType}.
     *
     * @param voltType The VoltDB {@link VoltType}
     * @return the matching Calcite {@link SqlTypeName}
     */
    public static SqlTypeName getCalciteType(VoltType voltType) {
        return VOLT_TO_CALC.get(voltType);
    }
}
