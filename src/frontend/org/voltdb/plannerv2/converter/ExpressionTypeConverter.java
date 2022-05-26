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

package org.voltdb.plannerv2.converter;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.SqlKind;
import org.voltdb.types.ExpressionType;

/**
 * Covert from Calcite aggregation function type to Volt aggregation function type.
 */
public class ExpressionTypeConverter {
    private ExpressionTypeConverter() {}

    private final static ImmutableMap<String, ExpressionType> calciteToVoltDBExpressionTypeMap =
            ImmutableMap.<String, ExpressionType>builder()
                    .put(SqlKind.MIN.lowerName, ExpressionType.AGGREGATE_MIN)
                    .put(SqlKind.MAX.lowerName, ExpressionType.AGGREGATE_MAX)
                    .put(SqlKind.SUM.lowerName, ExpressionType.AGGREGATE_SUM)
                    .put(SqlKind.SUM0.lowerName, ExpressionType.AGGREGATE_SUM)
                    .put(SqlKind.AVG.lowerName, ExpressionType.AGGREGATE_AVG)
                    .put(SqlKind.COUNT.lowerName, ExpressionType.AGGREGATE_COUNT)
                    .build();

    public static ExpressionType calciteTypeToVoltType(SqlKind sqlKind) {
        ExpressionType exprType = calciteToVoltDBExpressionTypeMap.get(sqlKind.lowerName);
        return exprType != null ? exprType : ExpressionType.INVALID;
    }
}
