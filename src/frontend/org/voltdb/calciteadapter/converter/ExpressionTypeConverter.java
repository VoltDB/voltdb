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

package org.voltdb.calciteadapter.converter;

import org.apache.calcite.sql.SqlKind;
import org.voltdb.types.ExpressionType;

import com.google_voltpatches.common.collect.ImmutableMap;

public class ExpressionTypeConverter {

    private final static ImmutableMap<String, ExpressionType> calciteToVoltDBExpressionTypeMap;

    private static ImmutableMap<String, ExpressionType> mapCalciteToVoltDB() {
        ImmutableMap.Builder<String, ExpressionType> mapBuilder = ImmutableMap.builder();

        mapBuilder.put(SqlKind.MIN.lowerName, ExpressionType.AGGREGATE_MIN);
        mapBuilder.put(SqlKind.MAX.lowerName, ExpressionType.AGGREGATE_MAX);
        mapBuilder.put(SqlKind.SUM.lowerName, ExpressionType.AGGREGATE_SUM);
        mapBuilder.put(SqlKind.SUM0.lowerName, ExpressionType.AGGREGATE_SUM);
        mapBuilder.put(SqlKind.AVG.lowerName, ExpressionType.AGGREGATE_AVG);
        mapBuilder.put(SqlKind.COUNT.lowerName, ExpressionType.AGGREGATE_COUNT);

        return mapBuilder.build();
    }

    static {
        calciteToVoltDBExpressionTypeMap = mapCalciteToVoltDB();
    }

    public static ExpressionType calicteTypeToVoltType(SqlKind sqlKind) {
        return calciteToVoltDBExpressionTypeMap.get(sqlKind.lowerName);
    }
}
